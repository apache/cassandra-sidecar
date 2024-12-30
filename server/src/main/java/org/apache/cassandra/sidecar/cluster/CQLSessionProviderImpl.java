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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
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
    private final NettyOptions nettyOptions;
    private final ReconnectionPolicy reconnectionPolicy;
    private final List<InetSocketAddress> localInstances;
    private final String username;
    private final String password;
    private final DriverUtils driverUtils;
    private volatile Session session;

    @VisibleForTesting
    public CQLSessionProviderImpl(List<InetSocketAddress> contactPoints,
                                  List<InetSocketAddress> localInstances,
                                  int healthCheckFrequencyMillis,
                                  String localDc,
                                  int numAdditionalConnections,
                                  NettyOptions options)
    {
        this(contactPoints,
             localInstances,
             healthCheckFrequencyMillis,
             localDc,
             numAdditionalConnections,
             null,
             null,
             null,
             options);
    }

    @VisibleForTesting
    public CQLSessionProviderImpl(List<InetSocketAddress> contactPoints,
                                  List<InetSocketAddress> localInstances,
                                  int healthCheckFrequencyMillis,
                                  String localDc,
                                  int numAdditionalConnections,
                                  String username,
                                  String password,
                                  SslConfiguration sslConfiguration,
                                  NettyOptions options)
    {
        this.contactPoints = contactPoints;
        this.localInstances = localInstances;
        this.localDc = localDc;
        this.numAdditionalConnections = numAdditionalConnections;
        this.username = username;
        this.password = password;
        this.sslConfiguration = sslConfiguration;
        this.nettyOptions = options;
        this.reconnectionPolicy = new ExponentialReconnectionPolicy(500, healthCheckFrequencyMillis);
        this.driverUtils = new DriverUtils();
    }

    public CQLSessionProviderImpl(SidecarConfiguration configuration,
                                  NettyOptions options,
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
        this.nettyOptions = options;
        int maxDelayMs = configuration.healthCheckConfiguration().checkIntervalMillis();
        this.reconnectionPolicy = new ExponentialReconnectionPolicy(500, maxDelayMs);
    }

    static RuntimeException propagateCause(ExecutionException e)
    {
        Throwable cause = e.getCause();

        if (cause instanceof Error) throw ((Error) cause);

        // We could just rethrow e.getCause(). However, the cause of the ExecutionException has likely
        // been
        // created on the I/O thread receiving the response. Which means that the stacktrace associated
        // with said cause will make no mention of the current thread. This is painful for say, finding
        // out which execute() statement actually raised the exception. So instead, we re-create the
        // exception.
        if (cause instanceof DriverException) throw ((DriverException) cause).copy();
        else throw new DriverInternalError("Unexpected exception thrown", cause);
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
    public synchronized Session get() throws CassandraUnavailableException
    {
        if (session != null)
        {
            return session;
        }

        Cluster cluster = null;
        try
        {
            logger.info("Connecting to cluster using contact points {}", contactPoints);

            LoadBalancingPolicy lbp = new SidecarLoadBalancingPolicy(localInstances, localDc, numAdditionalConnections,
                                                                     driverUtils);
            // Prevent spurious reconnects of ignored down nodes on `onUp` events
            QueryOptions queryOptions = new QueryOptions().setReprepareOnUp(false);
            Cluster.Builder builder
            = Cluster.builder()
                     .addContactPointsWithPorts(contactPoints)
                     .withReconnectionPolicy(reconnectionPolicy)
                     .withoutMetrics()
                     .withLoadBalancingPolicy(lbp)
                     .withQueryOptions(queryOptions)
                     // tests can create a lot of these Cluster objects, to avoid creating HWTs and
                     // event thread pools for each we have the override
                     .withNettyOptions(nettyOptions);

            SslContext sslContext = createSslContext(sslConfiguration);
            if (sslContext != null)
            {
                RemoteEndpointAwareNettySSLOptions sslOptions
                = new RemoteEndpointAwareNettySSLOptions(sslContext);
                builder.withSSL(sslOptions);
            }

            if (username != null && password != null)
            {
                builder.withCredentials(username, password);
            }
            // During mTLS connections, when client sends in keystore, we should have an AuthProvider passed along.
            // hence we pass empty username and password in PlainTextAuthProvider here, in case user hasn't already
            // configured username and password.
            else if (sslConfiguration != null && sslConfiguration.isKeystoreConfigured())
            {
                builder.withAuthProvider(new PlainTextAuthProvider("", ""));
            }

            cluster = builder.build();
            session = cluster.connect();
            logger.info("Successfully connected to Cassandra!");
            return session;
        }
        catch (Exception connectionException)
        {
            logger.error("Failed to reach Cassandra", connectionException);
            if (cluster != null)
            {
                try
                {
                    cluster.close();
                }
                catch (Exception closeException)
                {
                    logger.error("Failed to close cluster in cleanup", closeException);
                    connectionException.addSuppressed(closeException);
                }
            }
            throw new CassandraUnavailableException(CQL, connectionException);
        }
    }

    @Override
    @Nullable
    public Session getIfConnected()
    {
        return session;
    }

    @Override
    public void close()
    {
        Session localSession;
        synchronized (this)
        {
            localSession = this.session;
            this.session = null;
        }
        if (localSession != null)
        {
            try
            {
                localSession.getCluster().closeAsync().get(1, TimeUnit.MINUTES);
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
    private SslContext createSslContext(SslConfiguration sslConfiguration)
    {
        if (sslConfiguration == null || !sslConfiguration.enabled())
        {
            return null;
        }

        SslContextBuilder sslContextBuilder;
        try
        {
            sslContextBuilder = SslContextBuilder.forClient()
                                                 .protocols(sslConfiguration.secureTransportProtocols());

            if (sslConfiguration.isKeystoreConfigured())
            {
                KeyStore keyStore = createKeystore(sslConfiguration.keystore());
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keyStore, sslConfiguration.keystore().password().toCharArray());
                sslContextBuilder.keyManager(kmf);
            }

            // We set the truststore only if it is configured. For an SSL connection, if the truststore is required
            // but the user has not provided one, the default Java truststore is used.
            if (sslConfiguration.isTrustStoreConfigured())
            {
                KeyStore truststore = createKeystore(sslConfiguration.truststore());
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(truststore);
                sslContextBuilder.trustManager(tmf);
            }
            return sslContextBuilder.build();
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Error creating SsLContext for Cassandra connections", e);
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
