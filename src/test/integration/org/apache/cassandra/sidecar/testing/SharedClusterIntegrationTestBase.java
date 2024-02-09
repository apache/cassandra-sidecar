///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package org.apache.cassandra.sidecar.testing;
//
//import java.io.IOException;
//import java.net.BindException;
//import java.net.InetSocketAddress;
//import java.net.UnknownHostException;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//import java.util.Optional;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.TimeUnit;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//
//import org.apache.commons.lang3.StringUtils;
//import org.junit.jupiter.api.AfterAll;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.TestInstance;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.google.inject.AbstractModule;
//import com.google.inject.Guice;
//import com.google.inject.Injector;
//import com.google.inject.Provides;
//import com.google.inject.Singleton;
//import com.google.inject.util.Modules;
//import io.vertx.core.Vertx;
//import io.vertx.junit5.VertxExtension;
//import io.vertx.junit5.VertxTestContext;
//import org.apache.cassandra.config.CassandraRelevantProperties;
//import org.apache.cassandra.distributed.UpgradeableCluster;
//import org.apache.cassandra.distributed.api.IInstance;
//import org.apache.cassandra.distributed.api.IInstanceConfig;
//import org.apache.cassandra.distributed.impl.AbstractCluster;
//import org.apache.cassandra.distributed.shared.JMXUtil;
//import org.apache.cassandra.distributed.shared.ShutdownException;
//import org.apache.cassandra.distributed.shared.WithProperties;
//import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
//import org.apache.cassandra.sidecar.cluster.InstancesConfig;
//import org.apache.cassandra.sidecar.cluster.InstancesConfigImpl;
//import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
//import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
//import org.apache.cassandra.sidecar.common.CQLSessionProvider;
//import org.apache.cassandra.sidecar.common.JmxClient;
//import org.apache.cassandra.sidecar.common.dns.DnsResolver;
//import org.apache.cassandra.sidecar.common.utils.DriverUtils;
//import org.apache.cassandra.sidecar.common.utils.SidecarVersionProvider;
//import org.apache.cassandra.sidecar.config.JmxConfiguration;
//import org.apache.cassandra.sidecar.config.ServiceConfiguration;
//import org.apache.cassandra.sidecar.config.SidecarConfiguration;
//import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
//import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
//import org.apache.cassandra.sidecar.server.MainModule;
//import org.apache.cassandra.sidecar.server.Server;
//import org.apache.cassandra.sidecar.utils.CassandraVersionProvider;
//import org.apache.cassandra.testing.TestVersion;
//import org.apache.cassandra.testing.TestVersionSupplier;
//import org.apache.cassandra.utils.Throwables;
//
//import static org.assertj.core.api.Assertions.assertThat;
//
///**
// * This class provides an opinionated way to run integration tests. The {@link #setup()} method runs once at the
// * beginning of all the tests in the implementation, as well as the {@link #tearDown()} method. The tests will share
// * the same cluster throughout the lifetime of the tests, which means that implementers must be aware that any cluster
// * alteration will have an impact on subsequent test runs, so it is recommended that tests run in isolated
// * keyspaces/tables when required. Additionally, the state of the cluster should ideally remain the same for all
// * tests, so ideally tests should not alter the state of the cluster in a way that would affect other tests.
// *
// * <p>The setup will run the following steps:
// *
// * <ol>
// *     <li>Find the first version from the {@link TestVersionSupplier#testVersions()}
// *     <li>Provision a cluster for the test using the version from the previous step (implementer must supply)
// *     <li>Initialize schemas required for the test (implementer must supply)
// *     <li>Start sidecar that talks to the provisioned cluster
// *     <li>(Optional) Run the before test start method (implementer can supply)
// * </ol>
// *
// * <p>The above order guarantees that the cluster and Sidecar are both ready by the time the test
// * setup completes. Removing the need to wait for schema propagation from the cluster to Sidecar,
// * and removing the need to poll for schema changes to propagate. This helps in improving test
// * time.
// *
// * <p>For the teardown of the test the steps are the following:
// *
// * <ol>
// *     <li>(Optional) Before sidecar stops (implementer can supply)
// *     <li>Stop sidecar
// *     <li>(Optional) Before cluster shutdowns (implementer can supply)
// *     <li>Close cluster
// *     <li>(Optional) Before tear down ends (implementer can supply)
// * </ol>
// */
//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@ExtendWith(VertxExtension.class)
//public abstract class SharedClusterIntegrationTestBase
//{
//    protected final Logger logger = LoggerFactory.getLogger(SharedClusterIntegrationTestBase.class);
//    private static final int MAX_CLUSTER_PROVISION_RETRIES = 5;
//
//    protected Vertx vertx;
//    protected DnsResolver dnsResolver;
//    protected AbstractCluster<? extends IInstance> cluster;
//    protected Server server;
//    protected Injector injector;
//    protected WithProperties properties;
//
//    @BeforeAll
//    protected void setup() throws InterruptedException, IOException
//    {
//        properties = configurePropertiesForCassandraInJvmDTest();
//        Optional<TestVersion> testVersion = TestVersionSupplier.testVersions().findFirst();
//        assertThat(testVersion).isPresent();
//        logger.info("Testing with version={}", testVersion);
//        cluster = provisionClusterWithRetries(testVersion.get());
//        assertThat(cluster).isNotNull();
//        initializeSchemaForTest();
//        startSidecar(cluster);
//        beforeTestStart();
//    }
//
//    /**
//     * @return properties to configure the Cassandra cluster provisioned the dtest-jar
//     */
//    protected WithProperties configurePropertiesForCassandraInJvmDTest()
//    {
//        WithProperties withProperties = new WithProperties();
//        // Settings to reduce the test setup delay incurred if gossip is enabled
//        withProperties.set(CassandraRelevantProperties.RING_DELAY, TimeUnit.SECONDS.toMillis(5)); // down from 30s
//        withProperties.set(CassandraRelevantProperties.CONSISTENT_RANGE_MOVEMENT, false);
//        withProperties.set(CassandraRelevantProperties.CONSISTENT_SIMULTANEOUS_MOVES_ALLOW, true);
//        // End gossip delay settings
//        // Disable tcnative in netty as it can cause jni issues and logs lots errors
//        withProperties.set(CassandraRelevantProperties.DISABLE_TCACTIVE_OPENSSL, true);
//        // As we enable gossip by default, make the checks happen faster
//        withProperties.set(CassandraRelevantProperties.GOSSIP_SETTLE_MIN_WAIT_MS, 500); // Default 5000
//        withProperties.set(CassandraRelevantProperties.GOSSIP_SETTLE_POLL_INTERVAL_MS, 250); // Default 1000
//        withProperties.set(CassandraRelevantProperties.GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED, 6); // Default 3
//        // Disable direct memory allocator as it doesn't release properly
//        withProperties.set(CassandraRelevantProperties.CASSANDRA_NETTY_USE_HEAP_ALLOCATOR, true);
//        // NOTE: This setting is named opposite of what it does
//        // Disable requiring native file hints, which allows some native functions to fail and the test to continue.
//        withProperties.set(CassandraRelevantProperties.IGNORE_MISSING_NATIVE_FILE_HINTS, true);
//        // Disable all native stuff in Netty as streaming isn't functional with native enabled
//        withProperties.set(CassandraRelevantProperties.RELOCATED_SHADED_IO_NETTY_TRANSPORT_NONATIVE, true);
//        // Lifted from the Simulation runner (we're running into similar errors):
//        // this property is used to allow non-members of the ring to exist in gossip without breaking RF changes
//        // it would be nice not to rely on this, but hopefully we'll have consistent range movements before it matters
//        withProperties.set(CassandraRelevantProperties.ALLOW_ALTER_RF_DURING_RANGE_MOVEMENT, true);
//        return withProperties;
//    }
//
//    protected AbstractCluster<? extends IInstance> provisionClusterWithRetries(TestVersion testVersion) throws IOException
//    {
//        for (int retry = 0; retry < MAX_CLUSTER_PROVISION_RETRIES; retry++)
//        {
//            try
//            {
//                return provisionCluster(testVersion);
//            }
//            catch (RuntimeException runtimeException)
//            {
//                boolean addressAlreadyInUse = Throwables.anyCauseMatches(runtimeException,
//                                                                         ex -> ex instanceof BindException &&
//                                                                               StringUtils.contains(ex.getMessage(), "Address already in use"));
//                if (addressAlreadyInUse)
//                {
//                    logger.warn("Failed to provision cluster after {} retries", retry, runtimeException);
//                }
//                else
//                {
//                    throw runtimeException;
//                }
//            }
//        }
//        throw new RuntimeException("Unable to provision cluster");
//    }
//
//    @AfterAll
//    protected void tearDown() throws InterruptedException
//    {
//        beforeSidecarStop();
//        stopSidecar();
//        beforeClusterShutdown();
//        closeCluster();
//        closeProperties();
//        afterClusterShutdown();
//    }
//
//    /**
//     * @param testVersion the Cassandra version to use for the test
//     * @return a provisioned cluster to use for tests
//     * @throws IOException when provisioning a cluster fails
//     */
//    protected abstract UpgradeableCluster provisionCluster(TestVersion testVersion) throws IOException;
//
//    /**
//     * Initialize required schemas for the tests upfront before the test starts
//     */
//    protected abstract void initializeSchemaForTest();
//
//    /**
//     * Override to perform an action before the tests start
//     */
//    protected void beforeTestStart()
//    {
//    }
//
//    /**
//     * Override to perform an action before Sidecar stops
//     */
//    protected void beforeSidecarStop()
//    {
//    }
//
//    /**
//     * Override to perform an action before the cluster stops
//     */
//    protected void beforeClusterShutdown()
//    {
//    }
//
//    /**
//     * Override to perform an action after the cluster has shutdown
//     */
//    protected void afterClusterShutdown()
//    {
//    }
//
//    protected void createTestKeyspace(QualifiedName name, Map<String, Integer> rf)
//    {
//        createTestKeyspace(name.maybeQuotedKeyspace(), rf);
//    }
//
//    protected void createTestKeyspace(String keyspace, Map<String, Integer> rf)
//    {
//        cluster.schemaChangeIgnoringStoppedInstances("CREATE KEYSPACE IF NOT EXISTS " + keyspace
//                                                     + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', " +
//                                                     generateRfString(rf) + " };");
//    }
//
//    protected void createTestTable(QualifiedName name, String createTableStatement)
//    {
//        cluster.schemaChangeIgnoringStoppedInstances(String.format(createTableStatement, name));
//    }
//
//    /**
//     * Starts Sidecar configured to run against the provided Cassandra {@code cluster}.
//     *
//     * @param cluster the cluster to use
//     * @throws InterruptedException when the startup times out
//     */
//    protected void startSidecar(AbstractCluster<? extends IInstance> cluster) throws InterruptedException
//    {
//        VertxTestContext context = new VertxTestContext();
//        injector = Guice.createInjector(Modules.override(new MainModule()).with(new IntegrationTestModule(cluster)));
//        dnsResolver = injector.getInstance(DnsResolver.class);
//        vertx = injector.getInstance(Vertx.class);
//        server = injector.getInstance(Server.class);
//        server.start()
//              .onSuccess(s -> context.completeNow())
//              .onFailure(context::failNow);
//
//        context.awaitCompletion(5, TimeUnit.SECONDS);
//    }
//
//    /**
//     * Stops the Sidecar service
//     *
//     * @throws InterruptedException when stopping sidecar times out
//     */
//    protected void stopSidecar() throws InterruptedException
//    {
//        if (server == null)
//        {
//            return;
//        }
//        CountDownLatch closeLatch = new CountDownLatch(1);
//        server.close().onSuccess(res -> closeLatch.countDown());
//        if (closeLatch.await(60, TimeUnit.SECONDS))
//        {
//            logger.info("Close event received before timeout.");
//        }
//        else
//        {
//            logger.error("Close event timed out.");
//        }
//    }
//
//    /**
//     * Closes the cluster and its resources
//     */
//    protected void closeCluster()
//    {
//        if (cluster == null)
//        {
//            return;
//        }
//        logger.info("Closing cluster={}", cluster);
//        try
//        {
//            cluster.close();
//        }
//        // ShutdownException may be thrown from a different classloader, and therefore the standard
//        // `catch (ShutdownException)` won't always work - compare the canonical names instead.
//        catch (Throwable t)
//        {
//            if (Objects.equals(t.getClass().getCanonicalName(), ShutdownException.class.getCanonicalName()))
//            {
//                logger.debug("Encountered shutdown exception which closing the cluster", t);
//            }
//            else
//            {
//                throw t;
//            }
//        }
//    }
//
//    /**
//     * Closes the properties used for the tests
//     */
//    protected void closeProperties()
//    {
//        if (properties != null)
//        {
//            properties.close();
//        }
//    }
//
//    protected String generateRfString(Map<String, Integer> rf)
//    {
//        return rf.entrySet()
//                 .stream()
//                 .map(entry -> String.format("'%s':%d", entry.getKey(), entry.getValue()))
//                 .collect(Collectors.joining(","));
//    }
//
//    static class IntegrationTestModule extends AbstractModule
//    {
//        private final AbstractCluster<? extends IInstance> cluster;
//
//        IntegrationTestModule(AbstractCluster<? extends IInstance> cluster)
//        {
//            this.cluster = cluster;
//        }
//
//        @Provides
//        @Singleton
//        public InstancesConfig instancesConfig(Vertx vertx,
//                                               SidecarConfiguration configuration,
//                                               CassandraVersionProvider cassandraVersionProvider,
//                                               SidecarVersionProvider sidecarVersionProvider,
//                                               DnsResolver dnsResolver)
//        {
//            JmxConfiguration jmxConfiguration = configuration.serviceConfiguration().jmxConfiguration();
//
//            List<InetSocketAddress> contactPoints = buildContactPoints();
//            CQLSessionProvider cqlSessionProvider = new TemporaryCqlSessionProvider(contactPoints,
//                                                                                    SharedExecutorNettyOptions.INSTANCE);
//
//            List<InstanceMetadata> instanceMetadataList =
//            IntStream.range(0, cluster.size())
//                     .mapToObj(i -> buildInstanceMetadata(vertx,
//                                                          cluster.get(i + 1),
//                                                          cassandraVersionProvider,
//                                                          sidecarVersionProvider.sidecarVersion(),
//                                                          jmxConfiguration,
//                                                          cqlSessionProvider,
//                                                          dnsResolver))
//                     .collect(Collectors.toList());
//            return new InstancesConfigImpl(instanceMetadataList, dnsResolver);
//        }
//
//        @Provides
//        @Singleton
//        public SidecarConfiguration sidecarConfiguration()
//        {
//            ServiceConfiguration conf = ServiceConfigurationImpl.builder()
//                                                                .host("0.0.0.0") // binds to all interfaces, potential security issue if left running for long
//                                                                .port(0) // let the test find an available port
//                                                                .build();
//            return SidecarConfigurationImpl.builder()
//                                           .serviceConfiguration(conf)
//                                           .build();
//        }
//
//        @Provides
//        @Singleton
//        public DnsResolver dnsResolver()
//        {
//            return new IntegrationTestBase.LocalhostResolver();
//        }
//
//        private List<InetSocketAddress> buildContactPoints()
//        {
//            return cluster.stream()
//                          .map(instance -> new InetSocketAddress(instance.config().broadcastAddress().getAddress(),
//                                                                 tryGetIntConfig(instance.config(), "native_transport_port", 9042)))
//                          .collect(Collectors.toList());
//        }
//
//        static InstanceMetadata buildInstanceMetadata(Vertx vertx,
//                                                      IInstance cassandraInstance,
//                                                      CassandraVersionProvider versionProvider,
//                                                      String sidecarVersion,
//                                                      JmxConfiguration jmxConfiguration,
//                                                      CQLSessionProvider session,
//                                                      DnsResolver dnsResolver)
//        {
//            IInstanceConfig config = cassandraInstance.config();
//            String ipAddress = JMXUtil.getJmxHost(config);
//            String hostName;
//            try
//            {
//                hostName = dnsResolver.reverseResolve(ipAddress);
//            }
//            catch (UnknownHostException e)
//            {
//                hostName = ipAddress;
//            }
//            int port = tryGetIntConfig(config, "native_transport_port", 9042);
//            String[] dataDirectories = (String[]) config.get("data_file_directories");
//            String stagingDir = stagingDir(dataDirectories);
//
//            JmxClient jmxClient = JmxClient.builder()
//                                           .host(ipAddress)
//                                           .port(config.jmxPort())
//                                           .connectionMaxRetries(jmxConfiguration.maxRetries())
//                                           .connectionRetryDelayMillis(jmxConfiguration.retryDelayMillis())
//                                           .build();
//            CassandraAdapterDelegate delegate = new CassandraAdapterDelegate(vertx,
//                                                                             config.num(),
//                                                                             versionProvider,
//                                                                             session,
//                                                                             jmxClient,
//                                                                             new DriverUtils(),
//                                                                             sidecarVersion,
//                                                                             ipAddress,
//                                                                             port);
//            return InstanceMetadataImpl.builder()
//                                       .id(config.num())
//                                       .host(hostName)
//                                       .port(port)
//                                       .dataDirs(Arrays.asList(dataDirectories))
//                                       .stagingDir(stagingDir)
//                                       .delegate(delegate)
//                                       .build();
//        }
//
//        private static String stagingDir(String[] dataDirectories)
//        {
//            // Use the parent of the first data directory as the staging directory
//            Path dataDirParentPath = Paths.get(dataDirectories[0]).getParent();
//            // If the cluster has not started yet, the node's root directory doesn't exist yet
//            assertThat(dataDirParentPath).isNotNull();
//            Path stagingPath = dataDirParentPath.resolve("staging");
//            return stagingPath.toFile().getAbsolutePath();
//        }
//    }
//}
