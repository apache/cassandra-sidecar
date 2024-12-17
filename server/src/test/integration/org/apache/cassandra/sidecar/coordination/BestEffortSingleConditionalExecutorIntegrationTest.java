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
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.sidecar.cluster.CQLSessionProviderImpl;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.yaml.SchemaKeyspaceConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.db.SidecarLeaseDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarLeaseSchema;
import org.apache.cassandra.sidecar.metrics.CoordinationMetrics;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetricsImpl;
import org.apache.cassandra.sidecar.testing.SharedExecutorNettyOptions;
import org.apache.cassandra.testing.TestVersion;
import org.jetbrains.annotations.Nullable;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.apache.cassandra.sidecar.AssertionUtils.loopAssert;
import static org.apache.cassandra.sidecar.ExecutorPoolsHelper.createdSharedTestPool;
import static org.apache.cassandra.sidecar.testing.CassandraSidecarTestContext.tryGetIntConfig;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BestEffortSingleConditionalExecutorIntegrationTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BestEffortSingleConditionalExecutorIntegrationTest.class);
    static final SchemaKeyspaceConfiguration CONFIG = SchemaKeyspaceConfigurationImpl.builder().build();
    public static final int CONCURRENT_PROCESSES = 12;
    final List<CQLSessionProvider> sessionProviderList = new ArrayList<>();
    final Vertx vertx = Vertx.vertx();

    @ParameterizedTest(name = "{index} => version {0}")
    @MethodSource("org.apache.cassandra.testing.TestVersionSupplier#testVersions")
    void test(TestVersion version) throws IOException
    {
        Versions versions = Versions.find();
        assertThat(versions).as("No dtest jar versions found").isNotNull();
        Versions.Version requestedVersion = versions.getLatest(new Semver(version.version(), Semver.SemverType.LOOSE));

        // Spin up a 3-node cluster
        try (AbstractCluster<?> cluster = UpgradeableCluster.build(0)
                                                            .withDynamicPortAllocation(true) // to allow parallel test runs
                                                            .withVersion(requestedVersion)
                                                            .withDC("dc0", 3)
                                                            .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL))
                                                            .start())
        {
            // initialize internal sidecar schema for testing
            initializeSchemas(cluster, new SidecarLeaseSchema(CONFIG));
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
        assertThat(simulatedInstances).as("There are no lease holders when the process has not run yet")
                                      .allMatch(e -> !e.executor.executionDetermination().shouldExecuteOnLocalInstance());

        AtomicReference<Object[][]> currentLeaseHolderQueryResult = new AtomicReference<>();
        AtomicReference<BestEffortSingleConditionalExecutor> currentLeaseHolder = new AtomicReference<>();
        loopAssert(3, () -> {
            runLeaseAcquireProcess(pool, simulatedInstances);
            Object[][] resultSet = queryCurrentLeaseHolders(cluster);
            currentLeaseHolderQueryResult.set(resultSet);
            // Search for the lease-holder
            BestEffortSingleConditionalExecutor holder = getCurrentLeaseHolder(simulatedInstances);
            currentLeaseHolder.set(holder);
            assertThat(currentLeaseHolder.get().sidecarHostId()).as("Expecting lease holder to match the entry in the database")
                                                                .isEqualTo(resultSet[0][1]);
            validateMetrics(simulatedInstances, 1);
        });

        // Now simulate the case where the current lease-holder forgets that it is the current executor
        // The current lease-holder must be able to recover the information from the database.
        currentLeaseHolder.get().resetExecutor();
        assertThat(simulatedInstances).as("No instances are expected as we've just reset the existing lease holder information")
                                      .allMatch(e -> !e.executor.executionDetermination().shouldExecuteOnLocalInstance());

        loopAssert(3, () -> {
            runLeaseAcquireProcess(pool, simulatedInstances);
            Object[][] newLeaseHolderQueryResult = queryCurrentLeaseHolders(cluster);
            BestEffortSingleConditionalExecutor newLeaseHolder = getCurrentLeaseHolder(simulatedInstances);
            assertThat(newLeaseHolder).as("Lease-holder is expected to be the same since we are only recovering persisted state")
                                      .isSameAs(currentLeaseHolder.get());
            assertThat(currentLeaseHolderQueryResult.get()[0][0]).as("Timestamps are expected to be the same since we are only recovering persisted state")
                                                                 .isEqualTo(newLeaseHolderQueryResult[0][0]);
            validateMetrics(simulatedInstances, 1);
        });

        loopAssert(3, () -> {
            // Now let's simulate the case where the lease-holder will extend its lease
            // we will see different write timestamps
            runLeaseAcquireProcess(pool, simulatedInstances);
            Object[][] extendedLeaseQueryResult = queryCurrentLeaseHolders(cluster);
            assertThat(currentLeaseHolderQueryResult.get()[0][0]).as("Timestamps are NOT expected to be the same after a lease extension")
                                                                 .isNotEqualTo(extendedLeaseQueryResult[0][0]);
            assertThat(currentLeaseHolderQueryResult.get()[0][1]).as("But the owner remains the same")
                                                                 .isEqualTo(extendedLeaseQueryResult[0][1]);
            validateMetrics(simulatedInstances, 1);
        });

        int disabledInstanceNum = simulateDisableBinaryOfLeaseHolder(simulatedInstances);
        assertThat(disabledInstanceNum).as("Disabling binary of the current lease-holder")
                                       .isGreaterThanOrEqualTo(0)
                                       .isLessThan(simulatedInstances.size());

        loopAssert(3, () -> {
            // Run a new process to acquire the lease where we would expect the existing lease-holder to
            // retain the lease until TTL elapses
            runLeaseAcquireProcess(pool, simulatedInstances);

            // Search for the lease-holder
            BestEffortSingleConditionalExecutor newLeaseHolder = getCurrentLeaseHolder(simulatedInstances);
            assertThat(newLeaseHolder).as("Lease holder is expected to be the same since the entry exists in the database")
                                      .isSameAs(currentLeaseHolder.get());
            validateMetrics(simulatedInstances, 1);
        });

        // simulate a TTL by deleting the table entry
        removeLeaseHolderFromDatabase(cluster);

        loopAssert(3, () -> {
            // Run a new process where we expect a different lease-holder to be
            // elected since the current lease-holder doesn't have db connectivity.
            // We should have 2 instances be executors
            runLeaseAcquireProcess(pool, simulatedInstances);

            Object[][] newLeaseHolderQueryResult1 = queryCurrentLeaseHolders(cluster);
            List<TestInstanceWrapper> currentLeaseHolderInstances = getCurrentLeaseHolderInstances(simulatedInstances);
            assertThat(currentLeaseHolderInstances).as("2 instances are expected when binary is disabled for the original lease-holder")
                                                   .hasSize(2);
            assertThat(currentLeaseHolderInstances).as("Existing lease-holder is part of the selected instances")
                                                   .anyMatch(l -> l.executor == currentLeaseHolder.get());
            assertThat(currentLeaseHolderInstances).as("New lease-holder is also part of the selected instances")
                                                   .anyMatch(l -> l.executor.sidecarHostId().equals(newLeaseHolderQueryResult1[0][1]));
            assertThat(currentLeaseHolder.get().sidecarHostId()).as("New lease-holder is not the same as the previous lease-holder")
                                                                .isNotEqualTo(newLeaseHolderQueryResult1[0][1]);
            validateMetrics(simulatedInstances, 2);
        });

        // Re-enable binary on the original lease-holder. The original lease-holder
        // will learn that it is no longer the owner of the lease.
        simulateEnableBinaryOnInstance(simulatedInstances, disabledInstanceNum);

        loopAssert(3, () -> {
            runLeaseAcquireProcess(pool, simulatedInstances);
            List<TestInstanceWrapper> instances = getCurrentLeaseHolderInstances(simulatedInstances);
            assertThat(instances).as("After binary is re-enabled, the previous lease-holder learns it has lost the lease")
                                 .hasSize(1);
            validateMetrics(simulatedInstances, 1);
        });
    }

    private void validateMetrics(List<TestInstanceWrapper> simulatedInstances, int expectedLeaseHolderCount)
    {
        // Validate metrics, metrics instance is shared so we check on any instance
        CoordinationMetrics coordinationMetrics = simulatedInstances.get(0).metrics.server().coordination();
        assertThat(coordinationMetrics.participants.metric.getValue()).as("Everyone participates in this simulation")
                                                                      .isEqualTo(CONCURRENT_PROCESSES);
        assertThat(coordinationMetrics.leaseHolders.metric.getValue()).as("We only have %s lease-holder(s)", expectedLeaseHolderCount)
                                                                      .isEqualTo(expectedLeaseHolderCount);
    }

    private void runLeaseAcquireProcess(ExecutorService pool, List<TestInstanceWrapper> simulatedInstances)
    {
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
                    simulatedInstances.get(finalI).executor.determineSingleConditionalExecutor(() -> true);
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
                                                       "  } ;", CONFIG.keyspace());
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
            ServiceConfiguration serviceConfiguration = new TestServiceConfigurationImpl();
            assertThat(hostIdSet.add(serviceConfiguration.hostId())).isTrue();
            DisconnectableCQLSessionProvider cqlSessionProvider = buildCqlSession(address);
            SidecarLeaseDatabaseAccessor accessor = buildAccessor(cqlSessionProvider);

            BestEffortSingleConditionalExecutor executor =
            new BestEffortSingleConditionalExecutor(vertx,
                                                    createdSharedTestPool(vertx),
                                                    serviceConfiguration,
                                                    null,
                                                    accessor,
                                                    metrics);
            processes.add(new TestInstanceWrapper(cqlSessionProvider, executor, metrics));
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
        SidecarLeaseSchema tableSchema = new SidecarLeaseSchema(CONFIG);
        tableSchema.prepareStatements(session);
        return new SidecarLeaseDatabaseAccessor(tableSchema, sessionProvider);
    }

    static List<InetSocketAddress> buildContactList(IInstance instance)
    {
        IInstanceConfig config = instance.config();
        return Collections.singletonList(new InetSocketAddress(config.broadcastAddress().getAddress(),
                                                               tryGetIntConfig(config, "native_transport_port", 9042)));
    }

    static int simulateDisableBinaryOfLeaseHolder(List<TestInstanceWrapper> simulatedInstances)
    {
        for (int i = 0; i < simulatedInstances.size(); i++)
        {
            TestInstanceWrapper instance = simulatedInstances.get(i);
            if (instance.executor.executionDetermination().shouldExecuteOnLocalInstance())
            {
                DisconnectableCQLSessionProvider sessionProvider = instance.sessionProvider;
                sessionProvider.disconnect();
                assertThat(sessionProvider.get()).as("Simulating disable binary of instance %s", (i + 1)).isNull();
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

    static BestEffortSingleConditionalExecutor getCurrentLeaseHolder(List<TestInstanceWrapper> allSimulatedInstances)
    {
        List<TestInstanceWrapper> currentInstances = getCurrentLeaseHolderInstances(allSimulatedInstances);
        assertThat(currentInstances).as("There is more than one executor. This is unexpected in the simulation").hasSize(1);
        return currentInstances.get(0).executor;
    }

    static List<TestInstanceWrapper> getCurrentLeaseHolderInstances(List<TestInstanceWrapper> allSimulatedInstances)
    {
        List<TestInstanceWrapper> instances = new ArrayList<>();
        for (TestInstanceWrapper instance : allSimulatedInstances)
        {
            if (instance.executor.executionDetermination().shouldExecuteOnLocalInstance())
            {
                instances.add(instance);
            }
        }
        assertThat(instances).as("Expected to have at least one instance").isNotNull();
        return instances;
    }

    static Object[][] queryCurrentLeaseHolders(AbstractCluster<?> cluster)
    {
        Object[][] result =
        cluster.getFirstRunningInstance()
               .coordinator()
               .execute("SELECT writetime(owner), owner FROM sidecar_internal.sidecar_lease_v1 WHERE name = 'single_sidecar_instance_executor'",
                        ConsistencyLevel.LOCAL_QUORUM);
        assertThat(result).isNotNull();
        assertThat(result).hasDimensions(1, 2);
        return result;
    }

    void removeLeaseHolderFromDatabase(AbstractCluster<?> cluster)
    {
        LOGGER.info("Removing current lease-holder from the database");
        for (int retry = 1; retry <= 20; retry++)
        {
            try
            {
                cluster.schemaChangeIgnoringStoppedInstances("DELETE FROM sidecar_internal.sidecar_lease_v1 WHERE name = 'single_sidecar_instance_executor'");
                LOGGER.info("Successfully removed current lease-holder from database");
                return;
            }
            catch (Exception e)
            {
                LOGGER.error("Error removing lease-holder after {} attempts", retry, e);
                sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
        }
        fail("Unable to remove current lease-holder from database");
    }

    /**
     * A {@link ServiceConfigurationImpl} extension that generates a unique host ID per instance
     */
    static class TestServiceConfigurationImpl extends ServiceConfigurationImpl
    {
        private final String hostId = UUID.randomUUID().toString();

        @Override
        public String hostId()
        {
            return hostId;
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
        public @Nullable Session get()
        {
            return isConnected ? delegate.get() : null;
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
        final BestEffortSingleConditionalExecutor executor;
        final SidecarMetrics metrics;

        TestInstanceWrapper(DisconnectableCQLSessionProvider sessionProvider,
                            BestEffortSingleConditionalExecutor executor,
                            SidecarMetrics metrics)
        {
            this.sessionProvider = sessionProvider;
            this.executor = executor;
            this.metrics = metrics;
        }
    }
}
