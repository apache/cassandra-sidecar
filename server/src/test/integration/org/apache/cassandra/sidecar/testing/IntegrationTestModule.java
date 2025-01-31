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

package org.apache.cassandra.sidecar.testing;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Session;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.CoordinationConfiguration;
import org.apache.cassandra.sidecar.config.ParameterizedClassConfiguration;
import org.apache.cassandra.sidecar.config.PeriodicTaskConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.yaml.AccessControlConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.CacheConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.CoordinationConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.KeyStoreConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ParameterizedClassConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.PeriodicTaskConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SchemaKeyspaceConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SslConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.TestServiceConfiguration;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.exceptions.NoSuchCassandraInstanceException;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SERVER_STOP;

/**
 * Provides the basic dependencies for integration tests
 */
public class IntegrationTestModule extends AbstractModule
{
    public static final String ADMIN_IDENTITY = "spiffe://cassandra/sidecar/admin";
    private CassandraSidecarTestContext cassandraTestContext;
    private Path serverKeystorePath;
    private Path truststorePath;

    public void setCassandraTestContext(CassandraSidecarTestContext cassandraTestContext)
    {
        this.cassandraTestContext = cassandraTestContext;
    }

    public void setServerKeystorePath(Path serverKeystorePath)
    {
        this.serverKeystorePath = serverKeystorePath;
    }

    public void setTruststorePath(Path truststorePath)
    {
        this.truststorePath = truststorePath;
    }

    @Provides
    @Singleton
    public InstancesMetadata instancesMetadata()
    {
        return new WrapperInstancesMetadata();
    }

    @Provides
    @Singleton
    public SidecarConfiguration configuration(CoordinationConfiguration clusterLeaseClaimTaskConfiguration)
    {
        ServiceConfiguration conf
        = TestServiceConfiguration.builder()
                                  .schemaKeyspaceConfiguration(SchemaKeyspaceConfigurationImpl.builder()
                                                                                              .isEnabled(true)
                                                                                              .build())
                                  .coordinationConfiguration(clusterLeaseClaimTaskConfiguration)
                                  .build();
        PeriodicTaskConfiguration healthCheckConfiguration
        = new PeriodicTaskConfigurationImpl(true,
                                            MillisecondBoundConfiguration.parse("50ms"),
                                            MillisecondBoundConfiguration.parse("500ms"));

        SslConfiguration sslConfiguration =
        SslConfigurationImpl.builder()
                            .enabled(true)
                            .useOpenSsl(true)
                            .handshakeTimeout(SecondBoundConfiguration.parse("10s"))
                            .clientAuth("REQUEST")
                            .keystore(new KeyStoreConfigurationImpl(serverKeystorePath.toAbsolutePath().toString(),
                                                                    "password"))
                            .truststore(new KeyStoreConfigurationImpl(truststorePath.toAbsolutePath().toString(),
                                                                      "password"))
                            .build();
        AccessControlConfiguration accessControlConfiguration = accessControlConfiguration();
        return SidecarConfigurationImpl.builder()
                                       .sslConfiguration(sslConfiguration)
                                       .accessControlConfiguration(accessControlConfiguration)
                                       .serviceConfiguration(conf)
                                       .healthCheckConfiguration(healthCheckConfiguration)
                                       .build();
    }

    @Provides
    @Singleton
    public CoordinationConfiguration clusterLeaseClaimTaskConfiguration()
    {
        return new CoordinationConfigurationImpl(new PeriodicTaskConfigurationImpl());
    }

    @Provides
    @Singleton
    public CQLSessionProvider cqlSessionProvider(Vertx vertx)
    {
        CQLSessionProvider cqlSessionProvider = new CQLSessionProvider()
        {
            @Override
            @NotNull
            public Session get()
            {
                return cassandraTestContext.session();
            }

            @Override
            public void close()
            {
                cassandraTestContext.closeSessionProvider();
            }

            @Override
            public Session getIfConnected()
            {
                return get();
            }
        };
        vertx.eventBus().localConsumer(ON_SERVER_STOP.address(), message -> cqlSessionProvider.close());
        return cqlSessionProvider;
    }

    @Provides
    @Singleton
    public ClusterLease clusterLease()
    {
        return new ClusterLease(ClusterLease.Ownership.CLAIMED);
    }

    private AccessControlConfiguration accessControlConfiguration()
    {
        Map<String, String> params = new HashMap<String, String>()
        {
            {
                put("certificate_validator", "io.vertx.ext.auth.mtls.impl.CertificateValidatorImpl");
                put("certificate_identity_extractor", "org.apache.cassandra.sidecar.acl.authentication.CassandraIdentityExtractor");
            }
        };
        ParameterizedClassConfiguration mTLSConfig
        = new ParameterizedClassConfigurationImpl("org.apache.cassandra.sidecar.acl.authentication.MutualTlsAuthenticationHandlerFactory",
                                                  params);
        ParameterizedClassConfiguration rbacConfig
        = new ParameterizedClassConfigurationImpl("org.apache.cassandra.sidecar.acl.authorization.RoleBasedAuthorizationProvider",
                                                  Collections.emptyMap());
        return new AccessControlConfigurationImpl(true,
                                                  Collections.singletonList(mTLSConfig),
                                                  rbacConfig,
                                                  Collections.singleton(ADMIN_IDENTITY),
                                                  new CacheConfigurationImpl(MillisecondBoundConfiguration.parse("1s"),
                                                                             100,
                                                                             true,
                                                                             5,
                                                                             MillisecondBoundConfiguration.parse("1s")));
    }

    class WrapperInstancesMetadata implements InstancesMetadata
    {
        /**
         * @return metadata of instances owned by the sidecar
         */
        @Override
        @NotNull
        public List<InstanceMetadata> instances()
        {
            if (cassandraTestContext != null && cassandraTestContext.isClusterBuilt())
                return cassandraTestContext.instancesMetadata().instances();
            return Collections.emptyList();
        }

        /**
         * Lookup instance metadata by id.
         *
         * @param id instance's id
         * @return instance meta information
         * @throws NoSuchCassandraInstanceException when the instance with {@code id} does not exist
         */
        @Override
        public InstanceMetadata instanceFromId(int id) throws NoSuchCassandraInstanceException
        {
            return cassandraTestContext.instancesMetadata().instanceFromId(id);
        }

        /**
         * Lookup instance metadata by host name.
         *
         * @param host host address of instance
         * @return instance meta information
         * @throws NoSuchCassandraInstanceException when the instance for {@code host} does not exist
         */
        @Override
        public InstanceMetadata instanceFromHost(String host) throws NoSuchCassandraInstanceException
        {
            return cassandraTestContext.instancesMetadata().instanceFromHost(host);
        }
    }
}
