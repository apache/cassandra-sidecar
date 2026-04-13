/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverExecutionException;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy;
import org.apache.cassandra.sidecar.cluster.driver.MultiplexingNodeStateListener;
import org.apache.cassandra.sidecar.cluster.driver.SidecarLoadBalancingPolicy;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.utils.DriverUtils;
import org.apache.cassandra.sidecar.config.DriverConfiguration;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.RECONNECTION_BASE_DELAY;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.RECONNECTION_MAX_DELAY;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.RECONNECTION_POLICY_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REPREPARE_ENABLED;
import static org.apache.cassandra.sidecar.cluster.driver.CustomDriverOption.LOCAL_INSTANCES;
import static org.apache.cassandra.sidecar.cluster.driver.CustomDriverOption.NUM_CONNECTIONS;
import static org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException.Service.CQL;

/**
 * Provides connections to the local Cassandra cluster as defined in the Configuration. Currently, it only supports
 * returning the local connection.
 */
public class CQLSessionProviderImpl implements CQLSessionProvider
{
    private static final Logger logger = LoggerFactory.getLogger(CQLSessionProviderImpl.class);
    private final List<InetSocketAddress> contactPoints;
    private final int numAdditionalConnections;
    private final String localDc;
    private final SslConfiguration sslConfiguration;
    private final List<InetSocketAddress> localInstances;
    private final MultiplexingNodeStateListener multiplexingNodeStateListener;
    private final String username;
    private final String password;
    private final long healthCheckFrequencyMillis;
    private final DriverUtils driverUtils;
    private volatile CqlSession session;

    @VisibleForTesting
    public CQLSessionProviderImpl(List<InetSocketAddress> contactPoints,
                                  List<InetSocketAddress> localInstances,
                                  int healthCheckFrequencyMillis,
                                  String localDc,
                                  int numAdditionalConnections)
    {
        this(contactPoints,
             localInstances,
             healthCheckFrequencyMillis,
             localDc,
             numAdditionalConnections,
             null,
             null,
             null);
    }

    @VisibleForTesting
    public CQLSessionProviderImpl(List<InetSocketAddress> contactPoints,
                                  List<InetSocketAddress> localInstances,
                                  int healthCheckFrequencyMillis,
                                  String localDc,
                                  int numAdditionalConnections,
                                  String username,
                                  String password,
                                  SslConfiguration sslConfiguration)
    {
        this.contactPoints = contactPoints;
        this.localInstances = localInstances;
        this.localDc = localDc;
        this.numAdditionalConnections = numAdditionalConnections;
        this.username = username;
        this.password = password;
        this.sslConfiguration = sslConfiguration;
        this.healthCheckFrequencyMillis = healthCheckFrequencyMillis;
        this.multiplexingNodeStateListener = new MultiplexingNodeStateListener();
        this.driverUtils = new DriverUtils();
    }

    public CQLSessionProviderImpl(SidecarConfiguration configuration,
                                  DriverUtils driverUtils)
    {
        this.driverUtils = driverUtils;
        DriverConfiguration driverConfiguration = configuration.driverConfiguration();
        this.contactPoints = driverConfiguration.contactPoints();
        this.localInstances = configuration.cassandraInstances()
                                           .stream()
                                           .map(i -> new InetSocketAddress(i.host(), i.port()))
                                           .collect(Collectors.toList());
        this.localDc = driverConfiguration.localDc();
        this.username = driverConfiguration.username();
        this.password = driverConfiguration.password();
        this.sslConfiguration = driverConfiguration.sslConfiguration();
        this.numAdditionalConnections = driverConfiguration.numConnections();
        this.healthCheckFrequencyMillis = configuration.healthCheckConfiguration().executeInterval().toMillis();
        this.multiplexingNodeStateListener = new MultiplexingNodeStateListener();
    }

    static RuntimeException propagateCause(ExecutionException e)
    {
        Throwable cause = e.getCause();

        if (cause instanceof Error) throw ((Error) cause);

        // We could just rethrow e.getCause(). However, the cause of the ExecutionException has likely
        // been created on the I/O thread receiving the response. Which means that the stacktrace associated
        // with said cause will make no mention of the current thread. This is painful for say, finding
        // out which execute() statement actually raised the exception. So instead, we re-create the
        // exception.
        if (cause instanceof DriverException) throw ((DriverException) cause).copy();
        else throw new DriverExecutionException(cause);
    }

    /**
     * Provides a Session connected to the cluster. If null it means the connection was
     * could not be established. The session still might throw a NoHostAvailableException if the
     * cluster is otherwise unreachable.
     *
     * @return Session
     */
    @Override
    @NotNull
    public synchronized CqlSession get() throws CassandraUnavailableException
    {
        if (session != null)
        {
            return session;
        }

        try
        {
            logger.info("Connecting to cluster using contact points {}", contactPoints);
            DriverConfigLoader configLoader =
            DriverConfigLoader.programmaticBuilder()
                              .withString(LOAD_BALANCING_LOCAL_DATACENTER, localDc)
                              .withInt(NUM_CONNECTIONS, numAdditionalConnections)
                              .withStringList(LOCAL_INSTANCES, localInstances
                                                               .stream()
                                                               .map(i -> i.getHostString() + ":" + i.getPort())
                                                               .collect(Collectors.toList()))
                              .withClass(LOAD_BALANCING_POLICY_CLASS, SidecarLoadBalancingPolicy.class)
                              .withClass(RECONNECTION_POLICY_CLASS, ExponentialReconnectionPolicy.class)
                              .withDuration(RECONNECTION_BASE_DELAY, Duration.ofMillis(500))
                              .withDuration(RECONNECTION_MAX_DELAY, Duration.ofMillis(healthCheckFrequencyMillis))
                              .withBoolean(REPREPARE_ENABLED, false)
                              .withStringList(METADATA_SCHEMA_REFRESHED_KEYSPACES, Collections.emptyList())
                              .build();
            CqlSessionBuilder builder = CqlSession.builder()
                                        .addContactPoints(contactPoints)
                                        .withNodeStateListener(multiplexingNodeStateListener)
                                        .withConfigLoader(configLoader);

            SSLContext sslContext = createSslContext(sslConfiguration);
            builder.withSslContext(sslContext);

            if (username != null && password != null)
            {
                builder.withAuthCredentials(username, password);
            }

            session = builder.build();
            logger.info("Successfully connected to Cassandra!");
            return session;
        }
        catch (Exception connectionException)
        {
            logger.error("Failed to reach Cassandra", connectionException);
            throw new CassandraUnavailableException(CQL, connectionException);
        }
    }

    @Override
    @Nullable
    public CqlSession getIfConnected()
    {
        return session;
    }

    @Override
    public void registerNodeStateListener(NodeStateListener nodeStateListener)
    {
        multiplexingNodeStateListener.register(nodeStateListener);
    }

    @Override
    public void unregisterNodeStateListener(NodeStateListener nodeStateListener)
    {
        multiplexingNodeStateListener.unregister(nodeStateListener);
    }

    @Override
    public void close()
    {
        CqlSession localSession;
        synchronized (this)
        {
            localSession = this.session;
            this.session = null;
        }
        if (localSession != null)
        {
            try
            {
                localSession.closeAsync().toCompletableFuture().get(1, TimeUnit.MINUTES);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
            catch (TimeoutException e)
            {
                logger.warn("Unable to close session after 1 minute for provider {}", this, e);
            }
            catch (ExecutionException e)
            {
                throw propagateCause(e);
            }
        }
    }

    /**
     * We configure the SslContext in the driver when establishing an SSL or mTLS connection with Cassandra. For an
     * SSL connection, the driver only needs to provide the truststore, while Cassandra supplies its keystore for
     * validation. In the case of an mTLS connection, both the keystore and truststore are configured on the driver side.
     */
    private SSLContext createSslContext(SslConfiguration sslConfiguration)
    {
        if (sslConfiguration == null || !sslConfiguration.enabled())
        {
            return null;
        }

        try
        {
            // TODO: If we wish to explicitly limit allowed SSL protocols,
            //  we need to implement custom DefaultSslEngineFactory and use SSLEngine.setEnabledProtocols().
            SSLContext sslContext = SSLContext.getInstance("TLS");

            KeyManagerFactory kmf = null;
            if (sslConfiguration.isKeystoreConfigured())
            {
                KeyStore keyStore = createKeystore(sslConfiguration.keystore());
                kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keyStore, sslConfiguration.keystore().password().toCharArray());
            }

            // We set the truststore only if it is configured. For an SSL connection, if the truststore is required
            // but the user has not provided one, the default Java truststore is used.
            TrustManagerFactory tmf = null;
            if (sslConfiguration.isTrustStoreConfigured())
            {
                KeyStore truststore = createKeystore(sslConfiguration.truststore());
                tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(truststore);
            }

            sslContext.init(kmf != null ? kmf.getKeyManagers() : null,
                            tmf != null ? tmf.getTrustManagers() : null,
                            null);
            return sslContext;
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Error creating SSLContext for Cassandra connections", e);
        }
    }

    private KeyStore createKeystore(KeyStoreConfiguration config)
    throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException
    {
        KeyStore keystore = KeyStore.getInstance(config.type());
        try (InputStream inputStream = Files.newInputStream(Paths.get(config.path())))
        {
            keystore.load(inputStream, config.password().toCharArray());
        }
        return keystore;
    }
}
