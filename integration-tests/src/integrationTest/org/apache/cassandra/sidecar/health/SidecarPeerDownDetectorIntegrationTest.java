/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.health;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.common.client.SidecarInstance;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.utils.DriverUtils;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarClientConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.yaml.KeyStoreConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SchemaKeyspaceConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarClientConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarPeerHealthConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SslConfigurationImpl;
import org.apache.cassandra.sidecar.coordination.CassandraClientTokenRingProvider;
import org.apache.cassandra.sidecar.coordination.InnerDcTokenAdjacentPeerProvider;
import org.apache.cassandra.sidecar.coordination.SidecarPeerHealthMonitorTask;
import org.apache.cassandra.sidecar.coordination.SidecarPeerHealthProvider;
import org.apache.cassandra.sidecar.coordination.SidecarPeerProvider;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;

import static org.apache.cassandra.sidecar.testing.MtlsTestHelper.CASSANDRA_INTEGRATION_TEST_ENABLE_MTLS;
import static org.apache.cassandra.sidecar.testing.MtlsTestHelper.EMPTY_PASSWORD_STRING;
import static org.apache.cassandra.sidecar.testing.SharedClusterIntegrationTestBase.IntegrationTestModule.cassandraInstanceHostname;
import static org.apache.cassandra.sidecar.testing.SharedClusterIntegrationTestBase.IntegrationTestModule.defaultConfigurationBuilder;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Integration test for the Sidecar peer monitoring feature
 */
class SidecarPeerDownDetectorIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarPeerDownDetectorIntegrationTest.class);
    private static final SchemaKeyspaceConfiguration SCHEMA_KEYSPACE_CONFIG = SchemaKeyspaceConfigurationImpl.builder()
                                                                                                             .isEnabled(true)
                                                                                                             .build();

    // Key is the sidecar IP address and value is the server wrapper
    private final Map<String, ServerWrapper> sidecarServerMap = new HashMap<>();
    private final DriverUtils driverUtils = new DriverUtils();

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration().nodesPerDc(3);
    }

    @Override
    protected void startSidecar(ICluster<? extends IInstance> cluster) throws InterruptedException
    {
        for (IInstance cassandraInstance : cluster)
        {
            // Storing all the created sidecar instances into a list for further reference.
            LOGGER.info("Starting Sidecar instance for Cassandra instance {}",
                        cassandraInstance.config().num());
            String cassandraInstanceHostname = cassandraInstanceHostname(cassandraInstance, dnsResolver);
            PeersTestModule peersModule = new PeersTestModule(cassandraInstanceHostname);
            ServerWrapper serverWrapper = startSidecarWithInstances(List.of(cassandraInstance), peersModule);
            sidecarServerMap.put(cassandraInstanceHostname, serverWrapper);

            if (this.serverWrapper == null)
            {
                // assign the server to the first instance
                this.serverWrapper = serverWrapper;
            }
        }

        assertThat(sidecarServerMap.size()).as("Each Cassandra Instance will be managed by a single Sidecar instance")
                                           .isEqualTo(cluster.size());
    }

    @Override
    protected void stopSidecar()
    {
        sidecarServerMap.values().forEach(serverWrapper -> {
            try
            {
                closeServer(serverWrapper.server);
            }
            catch (Exception e)
            {
                LOGGER.error("Error trying to close sidecar server", e);
            }
        });
    }

    @Test
    void testOnePeerDown()
    {
        SidecarPeerHealthMonitorTask monitor = serverWrapper.injector.getInstance(SidecarPeerHealthMonitorTask.class);
        assertThat(monitor.status()).as("Monitor hasn't had time to perform checks").isEmpty();

        loopAssert(30, () -> assertThat(checkHostUp(monitor, "localhost2"))
                             .as("After some time, peer is up")
                             .isTrue());

        stopSidecarInstanceForTest("localhost2");
        loopAssert(30, () -> assertThat(checkHostDown(monitor, "localhost2"))
                             .as("After killing peer sidecar instance, monitor caches up and the host is down")
                             .isTrue());
        startSidecarInstanceForTest("localhost2");
        loopAssert(30, () -> assertThat(checkHostUp(monitor, "localhost2"))
                             .as("After restarting peer sidecar instance, monitor caches up and the host is down")
                             .isTrue());
    }

    @Override
    protected void initializeSchemaForTest()
    {
        QualifiedName qualifiedName = new QualifiedName("cdc", "test");
        createTestKeyspace(qualifiedName, DC1_RF3);
        createTestTable(qualifiedName, "CREATE TABLE %s (id text PRIMARY KEY, name text) WITH cdc=true");
    }

    private boolean checkHostUp(SidecarPeerHealthMonitorTask monitor, String hostname)
    {
        return checkHostStatus(monitor, hostname, SidecarPeerHealthProvider.Health.UP);
    }

    private boolean checkHostDown(SidecarPeerHealthMonitorTask monitor, String hostname)
    {
        return checkHostStatus(monitor, hostname, SidecarPeerHealthProvider.Health.DOWN);
    }

    private boolean checkHostStatus(SidecarPeerHealthMonitorTask monitor, String hostname, SidecarPeerHealthProvider.Health status)
    {
        for (Map.Entry<SidecarInstance, SidecarPeerHealthProvider.Health> entry : monitor.status().entrySet())
        {
            if (hostname.equals(entry.getKey().hostname()))
            {
                return status.equals(entry.getValue());
            }
        }
        fail("Status unavailable for host " + hostname);
        // this code block is not reachable but required for compilation
        // we could alternatively just throw an AssertionError above
        return false;
    }

    void stopSidecarInstanceForTest(String hostname)
    {
        assertThat(sidecarServerMap).containsKey(hostname);
        Server server = sidecarServerMap.get(hostname).server;
        String deploymentId = server.deploymentId();
        LOGGER.info("Stopping Sidecar server {} with deployment ID {}", hostname, deploymentId);
        getBlocking(server.stop(deploymentId), 30, TimeUnit.SECONDS,
                    "Stopping Sidecar server " + hostname + " with deployment ID " + deploymentId);
    }

    void startSidecarInstanceForTest(String hostname)
    {
        ServerWrapper serverWrapper = sidecarServerMap.get(hostname);
        assertThat(serverWrapper).isNotNull();
        Server server = serverWrapper.server;
        LOGGER.info("Starting Sidecar server {}...", hostname);
        getBlocking(server.start(), 30, TimeUnit.SECONDS, "Starting Sidecar server " + hostname + "...");
        // Update the server port after starting a new time
        // because a new port will be assigned
        serverWrapper.serverPort = server.actualPort();
    }

    class PeersTestModule extends AbstractModule
    {
        private final String cassandraInstanceHostname;

        public PeersTestModule(String cassandraInstanceHostname)
        {
            this.cassandraInstanceHostname = cassandraInstanceHostname;
        }

        @Provides
        @Singleton
        public SidecarConfiguration sidecarConfiguration()
        {
            Function<SidecarConfigurationImpl.Builder, SidecarConfigurationImpl.Builder> configurationOverrides = builder -> {

                // Override the service configuration such that we listen on the host associated with the
                // Cassandra instance associated with this Sidecar instance
                ServiceConfiguration conf = ServiceConfigurationImpl.builder()
                                                                    .host(cassandraInstanceHostname)
                                                                    .port(0) // let the test find an available port
                                                                    .schemaKeyspaceConfiguration(SCHEMA_KEYSPACE_CONFIG)
                                                                    .build();

                // We need to provide mTLS configuration for the Sidecar client so it can talk to
                // other sidecars using mTLS
                SslConfiguration clientSslConfiguration = null;
                if (mtlsTestHelper.isEnabled())
                {
                    LOGGER.info("Enabling test mTLS certificate/keystore.");

                    KeyStoreConfiguration truststoreConfiguration =
                    new KeyStoreConfigurationImpl(mtlsTestHelper.trustStorePath(),
                                                  mtlsTestHelper.trustStorePassword(),
                                                  mtlsTestHelper.trustStoreType(),
                                                  SecondBoundConfiguration.parse("60s"));

                    KeyStoreConfiguration keyStoreConfiguration =
                    new KeyStoreConfigurationImpl(mtlsTestHelper.clientKeyStorePath(),
                                                  EMPTY_PASSWORD_STRING,
                                                  mtlsTestHelper.serverKeyStoreType(), // server and client keystore types are the same
                                                  SecondBoundConfiguration.parse("60s"));

                    clientSslConfiguration = SslConfigurationImpl.builder()
                                                                 .enabled(true)
                                                                 .keystore(keyStoreConfiguration)
                                                                 .truststore(truststoreConfiguration)
                                                                 .build();
                }
                else
                {
                    LOGGER.info("Not enabling mTLS for testing purposes. Set '{}' to 'true' if you would " +
                                "like mTLS enabled.", CASSANDRA_INTEGRATION_TEST_ENABLE_MTLS);
                }

                SidecarClientConfiguration sidecarClientConfiguration = new SidecarClientConfigurationImpl(clientSslConfiguration);

                // Let's run this very frequently for testing purposes
                SidecarPeerHealthConfigurationImpl sidecarPeerHealthConfiguration
                = new SidecarPeerHealthConfigurationImpl(true,
                                                         MillisecondBoundConfiguration.parse("100ms"),
                                                         1,
                                                         MillisecondBoundConfiguration.parse("50ms"));
                builder.serviceConfiguration(conf)
                       .sidecarClientConfiguration(sidecarClientConfiguration)
                       .sidecarPeerHealthConfiguration(sidecarPeerHealthConfiguration);
                return builder;
            };

            return defaultConfigurationBuilder(mtlsTestHelper, configurationOverrides).build();
        }

        @Provides
        @Singleton
        public SidecarPeerProvider sidecarPeerProvider(InstanceMetadataFetcher metadataFetcher,
                                                       CassandraClientTokenRingProvider cassandraClientTokenRingProvider,
                                                       SidecarConfiguration configuration,
                                                       DnsResolver dnsResolver)
        {
            return new InnerDcTokenAdjacentPeerTestProvider(metadataFetcher,
                                                            cassandraClientTokenRingProvider,
                                                            configuration.serviceConfiguration(),
                                                            dnsResolver);
        }
    }

    /**
     * Test helper to find out server ports on integration tests.
     */
    class InnerDcTokenAdjacentPeerTestProvider extends InnerDcTokenAdjacentPeerProvider
    {
        InnerDcTokenAdjacentPeerTestProvider(InstanceMetadataFetcher metadataFetcher,
                                             CassandraClientTokenRingProvider cassandraClientTokenRingProvider,
                                             ServiceConfiguration serviceConfiguration,
                                             DnsResolver dnsResolver)
        {
            super(metadataFetcher, cassandraClientTokenRingProvider, serviceConfiguration, dnsResolver, driverUtils);
        }

        @Override
        protected int sidecarServicePort(String sidecarHostname)
        {
            return sidecarServerMap.get(sidecarHostname).serverPort;
        }
    }
}

