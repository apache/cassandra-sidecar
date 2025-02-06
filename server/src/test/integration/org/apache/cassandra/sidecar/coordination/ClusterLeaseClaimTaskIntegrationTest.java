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

package org.apache.cassandra.sidecar.coordination;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.vdurmont.semver4j.Semver;
import io.vertx.core.Vertx;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.sidecar.cluster.CQLSessionProviderImpl;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.yaml.SchemaKeyspaceConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.db.SidecarLeaseDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarLeaseSchema;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.metrics.CoordinationMetrics;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetricsImpl;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;
import org.apache.cassandra.sidecar.testing.SharedExecutorNettyOptions;
import org.apache.cassandra.testing.TestVersion;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException.Service.CQL;
import static org.apache.cassandra.sidecar.testing.CassandraSidecarTestContext.tryGetIntConfig;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration tests for the {@link ClusterLeaseClaimTask}
 */
@Tag("heavy")
class ClusterLeaseClaimTaskIntegrationTest
{
    public static final int CONCURRENT_PROCESSES = 12;
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterLeaseClaimTaskIntegrationTest.class);
    private static final int LEASE_SCHEMA_TTL_SECONDS = 10;
    final List<CQLSessionProvider> sessionProviderList = new ArrayList<>();
    final Vertx vertx = Vertx.vertx();
    SchemaKeyspaceConfiguration mockSchemaConfig;

    @BeforeEach
    void setup()
    {
        // mock the schema config because the actual implementation doesn't allow a low value for TTL
        mockSchemaConfig = mock(SchemaKeyspaceConfiguration.class);

        when(mockSchemaConfig.isEnabled()).thenReturn(SchemaKeyspaceConfigurationImpl.DEFAULT_IS_ENABLED);
        when(mockSchemaConfig.keyspace()).thenReturn(SchemaKeyspaceConfigurationImpl.DEFAULT_KEYSPACE);
        when(mockSchemaConfig.replicationStrategy()).thenReturn(SchemaKeyspaceConfigurationImpl.DEFAULT_REPLICATION_STRATEGY);
        when(mockSchemaConfig.replicationFactor()).thenReturn(SchemaKeyspaceConfigurationImpl.DEFAULT_REPLICATION_FACTOR);
        when(mockSchemaConfig.leaseSchemaTTL()).thenReturn(SecondBoundConfiguration.parse(LEASE_SCHEMA_TTL_SECONDS + "s"));
    }

    @ParameterizedTest(name = "{index} => version {0}")
    @MethodSource("org.apache.cassandra.testing.TestVersionSupplier#testVersions")
    void test(TestVersion version) throws IOException
    {
        Versions versions = Versions.find();
        assertThat(versions).as("No dtest jar versions found").isNotNull();
        Versions.Version requestedVersion = versions.getLatest(new Semver(version.version(), Semver.SemverType.LOOSE));

        // Spin up a 3-node cluster
        try (AbstractCluster<?> cluster = UpgradeableCluster.build(3)
                                                            .withDynamicPortAllocation(true) // to allow parallel test runs
                                                            .withVersion(requestedVersion)
                                                            .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL))
                                                            .start())
        {
            // initialize internal sidecar schema for testing
            initializeSchemas(cluster, new SidecarLeaseSchema(mockSchemaConfig));
            // Run different scenarios for selection of best-effort single conditional executor
            simulate(cluster);
        }
    }

    private void simulate(AbstractCluster<?> cluster)
    {
        // The following simulates that we have 1 Sidecar instance managing
        // 1 Cassandra instance. All Sidecar instances participate in the
        // simulation.
        List<TestInstanceWrapper> simulatedInstances = buildSimulatedInstances(cluster);
        ExecutorService pool = Executors.newFixedThreadPool(simulatedInstances.size());
        assertThat(simulatedInstances).as("There are no leaseholders when the process has not run yet")
                                      .allMatch(e -> !e.clusterLease.isClaimedByLocalSidecar());
        assertThat(simulatedInstances).as("And the state for all of them is indeterminate")
                                      .allMatch(e -> e.clusterLease.toScheduleDecision() == ScheduleDecision.RESCHEDULE);

        AtomicReference<Object[][]> currentLeaseholderQueryResult = new AtomicReference<>();
        AtomicReference<TestInstanceWrapper> currentLeaseholder = new AtomicReference<>();
        loopAssert(3, () -> {
            cleanupDeltaGaugeMetrics(simulatedInstances);
            runLeaseAcquireProcess(pool, simulatedInstances);
            Object[][] resultSet = queryCurrentLeaseholders(cluster);
            currentLeaseholderQueryResult.set(resultSet);
            // Search for the leaseholder
            TestInstanceWrapper holder = getCurrentLeaseholder(simulatedInstances);
            currentLeaseholder.set(holder);
            assertThat(currentLeaseholder.get().clusterLeaseClaimTask.sidecarHostId()).as("Expecting leaseholder to match the entry in the database")
                                                                                      .isEqualTo(resultSet[0][1]);
            validateMetrics(simulatedInstances, 1);
        });

        // Now simulate the case where the current leaseholder forgets this information.
        // The current leaseholder must be able to recover the information from the persisted state.
        currentLeaseholder.get().clusterLeaseClaimTask.resetLeaseholder();
        assertThat(simulatedInstances).as("No instances are expected as we've just reset the existing leaseholder information")
                                      .allMatch(e -> !e.clusterLease.isClaimedByLocalSidecar());
        assertThat(currentLeaseholder.get().clusterLease.toScheduleDecision())
        .as("And the state for the current leaseholder is indeterminate")
        .isEqualTo(ScheduleDecision.RESCHEDULE);

        loopAssert(3, () -> {
            runLeaseAcquireProcess(pool, simulatedInstances);
            Object[][] newLeaseholderQueryResult = queryCurrentLeaseholders(cluster);
            TestInstanceWrapper newLeaseholder = getCurrentLeaseholder(simulatedInstances);
            assertThat(newLeaseholder.clusterLeaseClaimTask).as("leaseholder is expected to be the same since we are only recovering persisted state")
                                                            .isSameAs(currentLeaseholder.get().clusterLeaseClaimTask);
            assertThat(currentLeaseholderQueryResult.get()[0][0]).as("Timestamps are expected to be the same since we are only recovering persisted state")
                                                                 .isEqualTo(newLeaseholderQueryResult[0][0]);
            validateMetrics(simulatedInstances, 1);
        });

        loopAssert(3, () -> {
            // Now let's simulate the case where the leaseholder will extend its lease
            // we will see different write timestamps
            runLeaseAcquireProcess(pool, simulatedInstances);
            Object[][] extendedLeaseQueryResult = queryCurrentLeaseholders(cluster);
            assertThat(currentLeaseholderQueryResult.get()[0][0]).as("Timestamps are NOT expected to be the same after a lease extension")
                                                                 .isNotEqualTo(extendedLeaseQueryResult[0][0]);
            assertThat(currentLeaseholderQueryResult.get()[0][1]).as("But the owner remains the same")
                                                                 .isEqualTo(extendedLeaseQueryResult[0][1]);
            validateMetrics(simulatedInstances, 1);
        });

        int disabledInstanceNum = simulateDisableBinaryOfLeaseholder(simulatedInstances);
        assertThat(disabledInstanceNum).as("Disabling binary of the current leaseholder")
                                       .isGreaterThanOrEqualTo(0)
                                       .isLessThan(simulatedInstances.size());

        loopAssert(3, () -> {
            // Run a new process to acquire the lease where we would expect the existing leaseholder to
            // retain the lease until TTL elapses
            runLeaseAcquireProcess(pool, simulatedInstances);

            // Search for the leaseholder
            TestInstanceWrapper newLeaseholder = getCurrentLeaseholder(simulatedInstances);
            assertThat(newLeaseholder.clusterLeaseClaimTask).as("Leaseholder is expected to be the same since the entry exists in the database")
                                                            .isSameAs(currentLeaseholder.get().clusterLeaseClaimTask);
            validateMetrics(simulatedInstances, 1);
        });

        // simulate a TTL by deleting the table entry
        removeLeaseholderFromDatabase(cluster);

        loopAssert(3, () -> {
            // Run a new process where we expect a different leaseholder to be
            // elected since the current leaseholder doesn't have db connectivity.
            // We should have 2 instances be executors
            runLeaseAcquireProcess(pool, simulatedInstances);

            Object[][] newLeaseholderQueryResult1 = queryCurrentLeaseholders(cluster);
            List<TestInstanceWrapper> currentLeaseholderInstances = getCurrentLeaseholderInstances(simulatedInstances);
            assertThat(currentLeaseholderInstances).as("2 instances are expected when binary is disabled for the original leaseholder")
                                                   .hasSize(2);
            assertThat(currentLeaseholderInstances).as("Existing leaseholder is part of the selected instances")
                                                   .anyMatch(l -> l.clusterLeaseClaimTask == currentLeaseholder.get().clusterLeaseClaimTask);
            assertThat(currentLeaseholderInstances).as("New leaseholder is also part of the selected instances")
                                                   .anyMatch(l -> l.clusterLeaseClaimTask.sidecarHostId().equals(newLeaseholderQueryResult1[0][1]));
            assertThat(currentLeaseholder.get().clusterLeaseClaimTask.sidecarHostId()).as("New leaseholder is not the same as the previous leaseholder")
                                                                                      .isNotEqualTo(newLeaseholderQueryResult1[0][1]);
            validateMetrics(simulatedInstances, 2);
        });

        // Re-enable binary on the original leaseholder. The original leaseholder
        // will learn that it is no longer the owner of the lease.
        simulateEnableBinaryOnInstance(simulatedInstances, disabledInstanceNum);

        loopAssert(3, () -> {
            runLeaseAcquireProcess(pool, simulatedInstances);
            List<TestInstanceWrapper> instances = getCurrentLeaseholderInstances(simulatedInstances);
            assertThat(instances).as("After binary is re-enabled, the previous leaseholder learns it has lost the lease")
                                 .hasSize(1);
            validateMetrics(simulatedInstances, 1);
        });

        // Now let's actually wait for the TTL to expire and ensure the leaseholder gives up the lease
        TestInstanceWrapper leaseholder = getCurrentLeaseholder(simulatedInstances);
        assertThat(leaseholder).as("First find out who the leaseholder is").isNotNull();
        // then disable binary
        simulateDisableBinaryOfLeaseholder(simulatedInstances);

        // Wait for lease to expire in both leaseholder and database
        Uninterruptibles.sleepUninterruptibly(LEASE_SCHEMA_TTL_SECONDS, TimeUnit.SECONDS);

        loopAssert(3, 1000, () -> {
            leaseholder.clusterLeaseClaimTask.runClaimProcess();
            ScheduleDecision scheduleDecision = leaseholder.clusterLease.toScheduleDecision();
            assertThat(scheduleDecision).as("The leaseholder should give up the lease")
                                        .isEqualTo(ScheduleDecision.RESCHEDULE);
            // ensure the data is TTL'd in the database
            long rowCount = rowCountInLeaseTable(cluster);
            assertThat(rowCount).describedAs("Lease should be TTL'd").isZero();
        });
    }

    private void validateMetrics(List<TestInstanceWrapper> simulatedInstances, int expectedLeaseholderCount)
    {
        // Validate metrics, metrics instance is shared so we check on any instance
        CoordinationMetrics coordinationMetrics = simulatedInstances.get(0).metrics.server().coordination();
        assertThat(coordinationMetrics.participants.metric.getValue()).as("Everyone participates in this simulation")
                                                                      .isEqualTo(CONCURRENT_PROCESSES);
        assertThat(coordinationMetrics.leaseholders.metric.getValue()).as("We only have %s leaseholder(s)", expectedLeaseholderCount)
                                                                      .isEqualTo(expectedLeaseholderCount);
    }

    // similar to validateMetrics but w/o validation. Read the delta gauge values out to reset
    private void cleanupDeltaGaugeMetrics(List<TestInstanceWrapper> simulatedInstances)
    {
        // Validate metrics, metrics instance is shared so we check on any instance
        CoordinationMetrics coordinationMetrics = simulatedInstances.get(0).metrics.server().coordination();
        coordinationMetrics.participants.metric.getValue();
        coordinationMetrics.leaseholders.metric.getValue();
    }

    private void runLeaseAcquireProcess(ExecutorService pool, List<TestInstanceWrapper> simulatedInstances)
    {
        cleanupDeltaGaugeMetrics(simulatedInstances);
        int electorateSize = simulatedInstances.size();
        CountDownLatch latch = new CountDownLatch(electorateSize);
        CountDownLatch completedLatch = new CountDownLatch(electorateSize);
        for (int i = 0; i < electorateSize; i++)
        {
            int finalI = i;
            pool.submit(() -> {
                try
                {
                    // Invoke process roughly at the same time
                    latch.countDown();
                    latch.await();

                    // Every instance will try to run determineSingleInstanceExecutor at roughly the same time
                    simulatedInstances.get(finalI).clusterLeaseClaimTask.runClaimProcess();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
                finally
                {
                    completedLatch.countDown();
                }
            });
        }
        assertThat(awaitUninterruptibly(completedLatch, 1, TimeUnit.MINUTES)).isTrue();
    }

    void initializeSchemas(AbstractCluster<?> cluster, SidecarLeaseSchema tableSchema)
    {
        String createKeyspaceStatement = String.format("CREATE KEYSPACE %s WITH REPLICATION = { " +
                                                       "   'class' : 'NetworkTopologyStrategy', " +
                                                       "   'replication_factor' : 3 " +
                                                       "  } ;", mockSchemaConfig.keyspace());
        cluster.schemaChange(createKeyspaceStatement);
        LOGGER.info("Creating keyspace with DDL: {}", createKeyspaceStatement);
        cluster.schemaChange(tableSchema.createSchemaStatement());
        LOGGER.info("Creating table with DDL: {}", tableSchema.createSchemaStatement());
    }

    List<TestInstanceWrapper> buildSimulatedInstances(AbstractCluster<?> cluster)
    {
        List<InetSocketAddress> address = buildContactList(cluster.get(1));
        List<TestInstanceWrapper> processes = new ArrayList<>();
        Set<String> hostIdSet = new HashSet<>();
        SidecarMetrics metrics = buildMetrics();
        for (int i = 0; i < CONCURRENT_PROCESSES; i++)
        {
            // unique service configuration every time to ensure we have
            // different UUIDs for hostId
            ServiceConfiguration serviceConfiguration = new TestServiceConfigurationImpl(mockSchemaConfig);
            assertThat(hostIdSet.add(serviceConfiguration.hostId())).isTrue();
            DisconnectableCQLSessionProvider cqlSessionProvider = buildCqlSession(address);
            SidecarLeaseDatabaseAccessor accessor = buildAccessor(cqlSessionProvider);

            ClusterLease clusterLease = new ClusterLease();
            ClusterLeaseClaimTask task = new ClusterLeaseClaimTask(vertx,
                                                                   serviceConfiguration,
                                                                   null,
                                                                   accessor,
                                                                   clusterLease,
                                                                   metrics);
            processes.add(new TestInstanceWrapper(cqlSessionProvider, task, clusterLease, metrics));
        }
        return processes;
    }

    private SidecarMetrics buildMetrics()
    {
        MetricRegistryFactory mockRegistryFactory = mock(MetricRegistryFactory.class);
        when(mockRegistryFactory.getOrCreate()).thenReturn(registry());
        return new SidecarMetricsImpl(mockRegistryFactory, null);
    }

    DisconnectableCQLSessionProvider buildCqlSession(List<InetSocketAddress> address)
    {
        CQLSessionProvider sessionProvider =
        new CQLSessionProviderImpl(address, address, 500, null, 0, SharedExecutorNettyOptions.INSTANCE);
        sessionProviderList.add(sessionProvider);
        return new DisconnectableCQLSessionProvider(sessionProvider);
    }

    SidecarLeaseDatabaseAccessor buildAccessor(CQLSessionProvider sessionProvider)
    {
        Session session = sessionProvider.get();
        assertThat(session).isNotNull();
        assertThat(session.getCluster()).isNotNull();
        assertThat(session.getCluster().getMetadata()).isNotNull();
        assertThat(session.getCluster().getMetadata().getKeyspace("sidecar_internal")).isNotNull();
        SidecarLeaseSchema tableSchema = new SidecarLeaseSchema(mockSchemaConfig);
        tableSchema.prepareStatements(session);
        return new SidecarLeaseDatabaseAccessor(tableSchema, sessionProvider);
    }

    static List<InetSocketAddress> buildContactList(IInstance instance)
    {
        IInstanceConfig config = instance.config();
        return Collections.singletonList(new InetSocketAddress(config.broadcastAddress().getAddress(),
                                                               tryGetIntConfig(config, "native_transport_port", 9042)));
    }

    static int simulateDisableBinaryOfLeaseholder(List<TestInstanceWrapper> simulatedInstances)
    {
        for (int i = 0; i < simulatedInstances.size(); i++)
        {
            TestInstanceWrapper instance = simulatedInstances.get(i);
            if (instance.clusterLease.isClaimedByLocalSidecar())
            {
                DisconnectableCQLSessionProvider sessionProvider = instance.sessionProvider;
                sessionProvider.disconnect();
                assertThatExceptionOfType(CassandraUnavailableException.class).as("Simulating disable binary of instance %s", (i + 1))
                                                                              .isThrownBy(sessionProvider::get);
                return i;
            }
        }
        return -1;
    }

    static void simulateEnableBinaryOnInstance(List<TestInstanceWrapper> allSimulatedInstances, int disabledInstanceNum)
    {
        DisconnectableCQLSessionProvider sessionProvider = allSimulatedInstances.get(disabledInstanceNum).sessionProvider;
        sessionProvider.reconnect();
        assertThat(sessionProvider.get()).as("Enabled binary on instance %s", disabledInstanceNum).isNotNull();
    }

    static TestInstanceWrapper getCurrentLeaseholder(List<TestInstanceWrapper> allSimulatedInstances)
    {
        List<TestInstanceWrapper> currentInstances = getCurrentLeaseholderInstances(allSimulatedInstances);
        assertThat(currentInstances).as("There is more than one leaseholder. This is unexpected in the simulation").hasSize(1);
        return currentInstances.get(0);
    }

    static List<TestInstanceWrapper> getCurrentLeaseholderInstances(List<TestInstanceWrapper> allSimulatedInstances)
    {
        List<TestInstanceWrapper> instances = new ArrayList<>();
        for (TestInstanceWrapper instance : allSimulatedInstances)
        {
            if (instance.clusterLease.isClaimedByLocalSidecar())
            {
                instances.add(instance);
            }
        }
        assertThat(instances).as("Expected to have at least one instance").isNotNull();
        return instances;
    }

    static long rowCountInLeaseTable(AbstractCluster<?> cluster)
    {
        SimpleQueryResult rows = cluster.getFirstRunningInstance()
                                        .coordinator()
                                        .executeWithResult("SELECT * FROM sidecar_internal.sidecar_lease_v1 ALLOW FILTERING",
                                                           ConsistencyLevel.SERIAL);
        return StreamSupport.stream(rows.spliterator(), false).count();
    }

    static Object[][] queryCurrentLeaseholders(AbstractCluster<?> cluster)
    {
        Object[][] result =
        cluster.getFirstRunningInstance()
               .coordinator()
               .execute("SELECT writetime(owner), owner FROM sidecar_internal.sidecar_lease_v1 WHERE name = 'cluster_lease_holder'",
                        ConsistencyLevel.SERIAL);
        assertThat(result).isNotNull();
        assertThat(result).hasDimensions(1, 2);
        return result;
    }

    void removeLeaseholderFromDatabase(AbstractCluster<?> cluster)
    {
        LOGGER.info("Removing current leaseholder from the database");
        for (int retry = 1; retry <= 20; retry++)
        {
            try
            {
                cluster.getFirstRunningInstance().coordinator().execute("DELETE FROM sidecar_internal.sidecar_lease_v1 " +
                                                                        "WHERE name = 'cluster_lease_holder' " +
                                                                        "IF EXISTS",
                                                                        ConsistencyLevel.QUORUM);
                LOGGER.info("Successfully removed current leaseholder from database");
                return;
            }
            catch (Exception e)
            {
                LOGGER.error("Error removing leaseholder after {} attempts", retry, e);
                sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
        }
        fail("Unable to remove current leaseholder from database");
    }

    /**
     * A {@link ServiceConfigurationImpl} extension that generates a unique host ID per instance
     */
    static class TestServiceConfigurationImpl extends ServiceConfigurationImpl
    {
        private final String hostId = UUID.randomUUID().toString();
        private final SchemaKeyspaceConfiguration schemaKeyspaceConfiguration;

        TestServiceConfigurationImpl(SchemaKeyspaceConfiguration schemaKeyspaceConfiguration)
        {
            this.schemaKeyspaceConfiguration = schemaKeyspaceConfiguration;
        }

        @Override
        public String hostId()
        {
            return hostId;
        }

        @Override
        public SchemaKeyspaceConfiguration schemaKeyspaceConfiguration()
        {
            return schemaKeyspaceConfiguration;
        }
    }

    /**
     * Simulates a session that can be disconnected/reconnected from the database
     */
    static class DisconnectableCQLSessionProvider implements CQLSessionProvider
    {
        private final CQLSessionProvider delegate;
        private boolean isConnected = true;

        DisconnectableCQLSessionProvider(CQLSessionProvider delegate)
        {
            this.delegate = delegate;
        }

        void disconnect()
        {
            isConnected = false;
        }

        void reconnect()
        {
            isConnected = true;
        }

        @Override
        @NotNull
        public Session get() throws CassandraUnavailableException
        {
            if (isConnected)
            {
                return delegate.get();
            }

            throw new CassandraUnavailableException(CQL, "Simulated CQL disconnection");
        }

        @Override
        public @Nullable Session getIfConnected()
        {
            return isConnected ? delegate.getIfConnected() : null;
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }

    /**
     * An object that encapsulates objects related to the same simulated Sidecar instance for testing purposes
     */
    static class TestInstanceWrapper
    {
        final DisconnectableCQLSessionProvider sessionProvider;
        final ClusterLeaseClaimTask clusterLeaseClaimTask;
        final ClusterLease clusterLease;
        final SidecarMetrics metrics;

        TestInstanceWrapper(DisconnectableCQLSessionProvider sessionProvider,
                            ClusterLeaseClaimTask clusterLeaseClaimTask,
                            ClusterLease clusterLease,
                            SidecarMetrics metrics)
        {
            this.sessionProvider = sessionProvider;
            this.clusterLeaseClaimTask = clusterLeaseClaimTask;
            this.clusterLease = clusterLease;
            this.metrics = metrics;
        }
    }
}
