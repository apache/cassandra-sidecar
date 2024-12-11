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
import org.apache.cassandra.sidecar.testing.SharedExecutorNettyOptions;
import org.apache.cassandra.testing.TestVersion;
import org.jetbrains.annotations.Nullable;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.apache.cassandra.sidecar.AssertionUtils.loopAssert;
import static org.apache.cassandra.sidecar.ExecutorPoolsHelper.createdSharedTestPool;
import static org.apache.cassandra.sidecar.testing.CassandraSidecarTestContext.tryGetIntConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class BestEffortSingleInstanceExecutorIntegrationTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BestEffortSingleInstanceExecutorIntegrationTest.class);
    static final SchemaKeyspaceConfiguration CONFIG = SchemaKeyspaceConfigurationImpl.builder().build();
    public static final int CONCURRENT_ELECTION_PROCESSES = 12;
    final List<CQLSessionProvider> sessionProviderList = new ArrayList<>();
    final Vertx vertx = Vertx.vertx();
    final List<DisconnectableCQLSessionProvider> cqlSessionProviderList = new ArrayList<>();

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
            // Run different scenarios for leader election
            simulateElection(cluster);
        }
    }

    private void simulateElection(AbstractCluster<?> cluster)
    {
        // The following simulates that we have 1 Sidecar instance managing
        // 1 Cassandra instance. All Sidecar instances participate in the
        // election.
        List<BestEffortSingleInstanceExecutor> electionInProcess = buildElectionProcess(cluster);
        ExecutorService pool = Executors.newFixedThreadPool(electionInProcess.size());
        assertThat(electionInProcess).as("There are no leaders when the process has not run yet")
                                     .allMatch(e -> !e.isLocalSidecarSingleInstanceExecutor());

        runElection(pool, electionInProcess);
        Object[][] currentLeaderQueryResult = queryCurrentLeaders(cluster);
        // Search for the leader
        BestEffortSingleInstanceExecutor currentLeader = getCurrentLeader(electionInProcess);
        assertThat(currentLeader.owner()).as("Expected owner to be %s but it %s", currentLeaderQueryResult[0][1], currentLeader.owner())
                                             .isEqualTo(currentLeaderQueryResult[0][1]);

        // Now simulate the case where the current leader forgets that it is the current leader
        // The current leader must be able to recover the leadership information
        currentLeader.resetExecutor();
        assertThat(electionInProcess).as("There are no leaders").allMatch(e -> !e.isLocalSidecarSingleInstanceExecutor());

        loopAssert(3, () -> {
            runElection(pool, electionInProcess);
            Object[][] newLeaderQueryResult = queryCurrentLeaders(cluster);
            BestEffortSingleInstanceExecutor newLeader = getCurrentLeader(electionInProcess);
            assertThat(newLeader).as("Leader is expected to be the same since we are only recovering persisted state").isSameAs(currentLeader);
            assertThat(currentLeaderQueryResult[0][0]).as("Timestamps are expected to be the same since we are only recovering persisted state")
                                                      .isEqualTo(newLeaderQueryResult[0][0]);
        });

        // Now let's simulate the case where the leader will extend its lease
        // we will see different write timestamps
        runElection(pool, electionInProcess);
        Object[][] extendedLeaseLeaderQueryResult = queryCurrentLeaders(cluster);
        assertThat(currentLeaderQueryResult[0][0]).as("Timestamps are NOT expected to be the same after a lease extension")
                                                  .isNotEqualTo(extendedLeaseLeaderQueryResult[0][0]);
        assertThat(currentLeaderQueryResult[0][1]).as("But the owner remains the same")
                                                  .isEqualTo(extendedLeaseLeaderQueryResult[0][1]);

        int disabledInstanceNum = simulateDisableBinaryOfLeader(electionInProcess, cqlSessionProviderList);
        assertThat(disabledInstanceNum).as("Disabling binary of the current leader")
                                       .isGreaterThanOrEqualTo(0)
                                       .isLessThan(electionInProcess.size());

        // Run a new election process where we would expect the existing leader to
        // retain leadership until TTL elapses
        runElection(pool, electionInProcess);

        // Search for the leader
        BestEffortSingleInstanceExecutor newLeader = getCurrentLeader(electionInProcess);
        assertThat(newLeader).as("Leader is expected to be the same since the entry exists in the database")
                             .isSameAs(currentLeader);

        // simulate a TTL by deleting the table entry
        removeLeaderFromDatabase(cluster);

        // Run a new election process where we expect a different leader
        // to be elected since the current leader doesn't have db connectivity.
        // We should have 2 elected leaders
        runElection(pool, electionInProcess);

        Object[][] newLeaderQueryResult1 = queryCurrentLeaders(cluster);
        List<BestEffortSingleInstanceExecutor> currentLeaders = getCurrentLeaders(electionInProcess);
        assertThat(currentLeaders).as("2 leaders are expected when binary is disabled for the original leader")
                                  .hasSize(2);
        assertThat(currentLeaders).as("Existing leader is part of the leaders")
                                  .anyMatch(leader -> leader == currentLeader);
        assertThat(currentLeaders).as("New leader is also part of the leaders")
                                  .anyMatch(leader -> leader.owner().equals(newLeaderQueryResult1[0][1]));
        assertThat(currentLeader.owner()).as("New leader is not the same as the previous leader")
                                             .isNotEqualTo(newLeaderQueryResult1[0][1]);

        // Re-enable binary on the original leader, the original leader
        // will learn that it is no longer a leader.
        simulateEnableBinaryOnInstance(cqlSessionProviderList, disabledInstanceNum);

        loopAssert(3, () -> {
            runElection(pool, electionInProcess);
            List<BestEffortSingleInstanceExecutor> leaders = getCurrentLeaders(electionInProcess);
            assertThat(leaders).as("After binary is re-enabled, the previous leader learns it has lost the leadership")
                               .hasSize(1);
        });
    }

    private void runElection(ExecutorService pool, List<BestEffortSingleInstanceExecutor> electionInProcess)
    {
        int electorateSize = electionInProcess.size();
        CountDownLatch latch = new CountDownLatch(electorateSize);
        CountDownLatch electionCompletedLatch = new CountDownLatch(electorateSize);
        for (int i = 0; i < electorateSize; i++)
        {
            int finalI = i;
            pool.submit(() -> {
                try
                {
                    // Invoke election roughly at the same time
                    latch.countDown();
                    latch.await();

                    // Execute will run the election process
                    electionInProcess.get(finalI).determineSingleInstanceExecutor(() -> true);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
                finally
                {
                    electionCompletedLatch.countDown();
                }
            });
        }
        assertThat(awaitUninterruptibly(electionCompletedLatch, 1, TimeUnit.MINUTES)).isTrue();
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

    List<BestEffortSingleInstanceExecutor> buildElectionProcess(AbstractCluster<?> cluster)
    {
        List<InetSocketAddress> address = buildContactList(cluster.get(1));
        List<BestEffortSingleInstanceExecutor> processes = new ArrayList<>();
        Set<String> hostIdSet = new HashSet<>();
        for (int i = 0; i < CONCURRENT_ELECTION_PROCESSES; i++)
        {
            // unique service configuration every time to ensure we have
            // different UUIDs for hostId
            ServiceConfiguration serviceConfiguration = new TestServiceConfigurationImpl();
            assertThat(hostIdSet.add(serviceConfiguration.hostId())).isTrue();
            DisconnectableCQLSessionProvider cqlSessionProvider = buildCqlSession(address);
            cqlSessionProviderList.add(cqlSessionProvider);
            SidecarLeaseDatabaseAccessor accessor = buildAccessor(cqlSessionProvider);
            processes.add(new BestEffortSingleInstanceExecutor(vertx, createdSharedTestPool(vertx), serviceConfiguration, null, accessor));
        }
        return processes;
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

    static int simulateDisableBinaryOfLeader(List<BestEffortSingleInstanceExecutor> electionInProcess,
                                             List<DisconnectableCQLSessionProvider> cqlSessionProviderList)
    {
        for (int i = 0; i < electionInProcess.size(); i++)
        {
            BestEffortSingleInstanceExecutor l = electionInProcess.get(i);
            if (l.isLocalSidecarSingleInstanceExecutor())
            {
                DisconnectableCQLSessionProvider sessionProvider = cqlSessionProviderList.get(i);
                sessionProvider.disconnect();
                assertThat(sessionProvider.get()).as("Simulating disable binary of instance %s", (i + 1)).isNull();
                return i;
            }
        }
        return -1;
    }

    static void simulateEnableBinaryOnInstance(List<DisconnectableCQLSessionProvider> cqlSessionProviderList,
                                               int disabledInstanceNum)
    {
        DisconnectableCQLSessionProvider sessionProvider = cqlSessionProviderList.get(disabledInstanceNum);
        sessionProvider.reconnect();
        assertThat(sessionProvider.get()).as("Enabled binary on instance %s", disabledInstanceNum).isNotNull();
    }

    static BestEffortSingleInstanceExecutor getCurrentLeader(List<BestEffortSingleInstanceExecutor> electionInProcess)
    {
        List<BestEffortSingleInstanceExecutor> currentLeaders = getCurrentLeaders(electionInProcess);
        assertThat(currentLeaders).as("There is more than one leader, this is unexpected in the simulation").hasSize(1);
        return currentLeaders.get(0);
    }

    static List<BestEffortSingleInstanceExecutor> getCurrentLeaders(List<BestEffortSingleInstanceExecutor> electionInProcess)
    {
        List<BestEffortSingleInstanceExecutor> leaders = new ArrayList<>();
        for (BestEffortSingleInstanceExecutor l : electionInProcess)
        {
            if (l.isLocalSidecarSingleInstanceExecutor())
            {
                leaders.add(l);
            }
        }
        assertThat(leaders).as("Expected to have at least one leader").isNotNull();
        return leaders;
    }

    static Object[][] queryCurrentLeaders(AbstractCluster<?> cluster)
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

    void removeLeaderFromDatabase(AbstractCluster<?> cluster)
    {
        LOGGER.info("Removing current leader from the database");
        for (int retry = 1; retry <= 20; retry++)
        {
            try
            {
                cluster.schemaChangeIgnoringStoppedInstances("DELETE FROM sidecar_internal.sidecar_lease_v1 WHERE name = 'single_sidecar_instance_executor'");
                LOGGER.info("Successfully removed current leader from the database");
                return;
            }
            catch (Exception e)
            {
                LOGGER.error("Error removing leader after {} attempts", retry, e);
                sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
        }
        fail("Unable to remove current leader from database");
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
}
