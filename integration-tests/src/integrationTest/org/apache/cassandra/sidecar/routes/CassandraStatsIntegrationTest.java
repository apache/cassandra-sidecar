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

package org.apache.cassandra.sidecar.routes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpResponseExpectation;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.cassandra.sidecar.common.response.CompactionStatsResponse;
import org.apache.cassandra.sidecar.common.response.ConnectedClientStatsResponse;
import org.apache.cassandra.sidecar.common.response.TableStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.ClientConnectionEntry;
import org.apache.cassandra.sidecar.common.response.data.CompactionInfo;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.TEST_TABLE_PREFIX;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the stats endpoints (that require a single node cluster) with cassandra container.
 */
class CassandraStatsIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    private static final int DEFAULT_CONNECTION_COUNT = 2;
    private static final QualifiedName TEST_TABLE = new QualifiedName(TEST_KEYSPACE, TEST_TABLE_PREFIX);
    private static final String COMPACTION_STATS_ROUTE = "/api/v1/cassandra/stats/compaction";
    private static final int MAX_POLL_ATTEMPTS = 10;
    private static final List<QualifiedName> COMPACTION_TEST_TABLES = new ArrayList<>();
    private static final int TABLE_COUNT = 5;

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(TEST_TABLE,
                        "CREATE TABLE %s ( \n" +
                        "  race_year int, \n" +
                        "  race_name text, \n" +
                        "  cyclist_name text, \n" +
                        "  rank int, \n" +
                        "  PRIMARY KEY ((race_year, race_name), rank) \n" +
                        ");");

        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);

        for (int i = 1; i <= TABLE_COUNT; i++)
        {
            COMPACTION_TEST_TABLES.add(new QualifiedName(TEST_KEYSPACE, TEST_TABLE_PREFIX + "_compaction_" + i));
        }

        // Create test tables for compaction activity
        for (QualifiedName tableName : COMPACTION_TEST_TABLES)
        {
            createTestTable(tableName,
                            "CREATE TABLE %s ( \n" +
                            "  id int PRIMARY KEY, \n" +
                            "  data text \n" +
                            ");");
        }
    }

    @Override
    protected void beforeTestStart()
    {
        // wait for the schema initialization
        waitForSchemaReady(30, TimeUnit.SECONDS);
    }

    @Test
    void retrieveClientStatsDefault()
    {
        Map<String, Boolean> expectedParams = Map.of("summary", true);
        String testRoute = "/api/v1/cassandra/stats/connected-clients";
        HttpResponse<Buffer> response = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", testRoute)
                                                                   .send()
                                                                   .expecting(HttpResponseExpectation.SC_OK));
        assertClientStatsResponse(response, expectedParams);
    }

    @Test
    void retrieveClientStatsListConnections()
    {
        Map<String, Boolean> expectedParams = Map.of("summary", false);
        String testRoute = "/api/v1/cassandra/stats/connected-clients?summary=false";
        HttpResponse<Buffer> response = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", testRoute)
                                                                   .send()
                                                                   .expecting(HttpResponseExpectation.SC_OK));
        assertClientStatsResponse(response, expectedParams);
    }

    @Test
    void retrieveClientStatsListConnectionsWithKeyspace()
    {
        try (Cluster driverCluster = createDriverCluster(cluster.delegate()); Session session = driverCluster.connect())
        {
            session.execute("USE " + TEST_KEYSPACE);

            Map<String, Boolean> expectedParams = Map.of("summary", false);
            String testRoute = "/api/v1/cassandra/stats/connected-clients?summary=false";
            HttpResponse<Buffer> response = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", testRoute)
                                                                       .send()
                                                                       .expecting(HttpResponseExpectation.SC_OK));
            assertClientStatsResponse(response, expectedParams, 4, true);
        }
    }

    @Test
    void retrieveClientStatsMultipleConnections()
    {
        // Creates an additional connection pair
        try (Cluster driverCluster = createDriverCluster(cluster.delegate()); Session ignored = driverCluster.connect())
        {
            Map<String, Boolean> expectedParams = Map.of("summary", false);
            String testRoute = "/api/v1/cassandra/stats/connected-clients?summary=false";
            HttpResponse<Buffer> response = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", testRoute)
                                                                       .send()
                                                                       .expecting(HttpResponseExpectation.SC_OK));
            assertClientStatsResponse(response, expectedParams, 4);
        }
    }

    /**
     * Expects unrecognized params to be ignored and invalid value for the expected parameter to be defaulted to true
     * to prevent heavyweight query in the bad request case.
     */
    @Test
    void retrieveClientStatsInvalidParameterValue()
    {
        Map<String, Boolean> expectedParams = Map.of("summary", true);
        String testRoute = "/api/v1/cassandra/stats/connected-clients?summary=123&bad-arg=xyz";
        HttpResponse<Buffer> response = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", testRoute)
                                                                   .send()
                                                                   .expecting(HttpResponseExpectation.SC_OK));
        assertClientStatsResponse(response, expectedParams);
    }

    @Test
    void retrieveTableStats()
    {
        /*
         * "SnapshotSize" table stats metric reports the size of snapshot files which are not links for "live" SSTables.
         * In order to simulate non-zero data for this metric, we do the following:
         * 1. Insert data
         * 2. Create snapshot
         * 3. Truncate table to ensure snapshot references non-live sstables
         * 4. Insert more data (and flush) to ensure other metrics, have non-zero values
         */
        insertData(TEST_TABLE);
        createSnapshot(TEST_TABLE);
        cluster.schemaChangeIgnoringStoppedInstances("TRUNCATE TABLE " + TEST_TABLE);
        insertData(TEST_TABLE);
        cluster.stream().forEach(instance -> instance.flush(TEST_KEYSPACE));
        tableStats(TEST_TABLE);
    }

    private void insertData(QualifiedName tableName)
    {
        for (int i = 1; i <= 10; i++)
        {
            String statement = "INSERT INTO " + tableName + " (race_year, race_name, rank, cyclist_name) " +
                               "VALUES (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', " + i + ", 'Benjamin PRADES');";
            cluster.schemaChangeIgnoringStoppedInstances(statement);
        }
    }

    private void createSnapshot(QualifiedName tableName)
    {
        String testRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/" + tableName.table() + "-snapshot",
                                         tableName.keyspace(), tableName.table());
        HttpResponse<Buffer> resp;
        resp = getBlocking(trustedClient().put(serverWrapper.serverPort, "localhost", testRoute)
                                          .send());
        assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
    }

    private void tableStats(QualifiedName tableName)
    {
        String testRoute = "/api/v1/cassandra/keyspaces/" + tableName.keyspace() + "/tables/" + tableName.table() + "/stats";
        HttpResponse<Buffer> resp;
        resp = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", testRoute)
                                          .send());
        assertTableStatsResponse(tableName, resp);
    }

    void assertTableStatsResponse(QualifiedName tableName, HttpResponse<Buffer> response)
    {
        TableStatsResponse stats = response.bodyAsJson(TableStatsResponse.class);
        assertThat(stats).isNotNull();
        assertThat(stats.table()).isEqualTo(tableName.table());
        assertThat(stats.keyspace()).isEqualTo(tableName.keyspace());
        assertThat(stats.snapshotsSizeBytes()).isGreaterThan(0);
        assertThat(stats.sstableCount()).isGreaterThan(0);
        assertThat(stats.diskSpaceUsedBytes()).isGreaterThan(0);
        assertThat(stats.totalDiskSpaceUsedBytes()).isGreaterThan(0);
    }


    void assertClientStatsResponse(HttpResponse<Buffer> response, Map<String, Boolean> params)
    {
        assertClientStatsResponse(response, params, DEFAULT_CONNECTION_COUNT);
    }

    void assertClientStatsResponse(HttpResponse<Buffer> response, Map<String, Boolean> params, int expectedConnections)
    {
        assertClientStatsResponse(response, params, expectedConnections, false);
    }

    void assertClientStatsResponse(HttpResponse<Buffer> response, Map<String, Boolean> params, int expectedConnections, boolean usingKeyspace)
    {
        boolean isSummary = params.get("summary");

        logger.info("Response: {}", response.bodyAsString());
        ConnectedClientStatsResponse clientStats = response.bodyAsJson(ConnectedClientStatsResponse.class);
        assertThat(clientStats).isNotNull();
        assertThat(clientStats.connectionsByUser()).isNotEmpty();
        assertThat(clientStats.connectionsByUser()).containsKey("anonymous");
        assertThat(clientStats.totalConnectedClients()).isEqualTo(expectedConnections);

        List<ClientConnectionEntry> stats = clientStats.clientConnections();
        if (isSummary)
        {
            assertThat(stats).isNull();
        }
        else
        {
            SimpleCassandraVersion releaseVersion = SimpleCassandraVersion.create(cluster.get(1).getReleaseVersionString());
            SimpleCassandraVersion majorVersion = SimpleCassandraVersion.create(releaseVersion.major, releaseVersion.minor, 0);
            SimpleCassandraVersion fourZero = SimpleCassandraVersion.create("4.0");
            assertThat(stats.size()).isEqualTo(expectedConnections);
            for (ClientConnectionEntry stat : stats)
            {
                assertThat(stat.address()).contains("127.0.0.1");
                assertThat(stat.sslEnabled()).isEqualTo(false);
                assertThat(stat.driverName()).isEqualTo("DataStax Java Driver");
                assertThat(stat.driverVersion()).isNotNull();
                assertThat(stat.username()).isEqualTo("anonymous");
                if (majorVersion.isGreaterThan(fourZero))
                {
                    assertThat(stat.clientOptions()).isNotNull();
                    assertThat(stat.clientOptions().containsKey("CQL_VERSION")).isTrue();
                }
            }

            // TODO: Add validations for fields in trunk once dtest jars can advance beyond TCM commit
            if (usingKeyspace
                && majorVersion.compareTo(SimpleCassandraVersion.create("5.0.0")) >= 0)
            {
                assertThat(stats.stream().map(ClientConnectionEntry::keyspaceName).collect(Collectors.toSet())).contains(TEST_KEYSPACE);
            }
        }
    }

    @Test
    void testCompactionStatsRetrieval()
    {
        logger.info("Starting compaction stats test with {} tables", COMPACTION_TEST_TABLES.size());

        // Generate SSTables for all test tables
        for (QualifiedName tableName : COMPACTION_TEST_TABLES)
        {
            generateSSTables(tableName, 100);
        }

        // Create threads to trigger compaction on all tables
        List<Thread> compactionThreads = new ArrayList<>();
        for (QualifiedName tableName : COMPACTION_TEST_TABLES)
        {
            Thread thread = new Thread(() -> triggerCompactionForTable(tableName));
            compactionThreads.add(thread);
        }

        // Start all compaction threads
        for (Thread thread : compactionThreads)
        {
            thread.start();
        }

        // Poll immediately and repeatedly to catch active compactions
        CompactionStatsResponse stats = null;
        HttpResponse<Buffer> response;
        boolean foundActiveCompactions;

        for (int attempt = 0; attempt < MAX_POLL_ATTEMPTS; attempt++)
        {
            try
            {
                response = getBlocking(
                trustedClient().get(serverWrapper.serverPort, "localhost", COMPACTION_STATS_ROUTE)
                               .send()
                               .expecting(HttpResponseExpectation.SC_OK));

                stats = response.bodyAsJson(CompactionStatsResponse.class);
                foundActiveCompactions = !stats.activeCompactions().isEmpty();

                if (foundActiveCompactions)
                {
                    logger.info("SUCCESS: Found {} active compactions on attempt {}",
                                stats.activeCompactionsCount(), attempt + 1);
                    break;
                }
                else
                {
                    logger.info("Attempt {}: No active compactions yet", attempt + 1);
                }

                Thread.sleep(100); // Short sleep between attempts
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Wait for all compaction threads to complete
        for (Thread thread : compactionThreads)
        {
            try
            {
                thread.join(5000);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                break;
            }
        }
        assertThat(stats).isNotNull();
        logger.info("Response:{}", stats);
        validateCompactionStatsResponse(stats);
    }


    private void generateSSTables(QualifiedName tableName, int numSSTables)
    {
        for (int batch = 0; batch < numSSTables; batch++)
        {
            for (int i = batch * 1000; i < (batch + 1) * 1000; i++)
            {
                String statement = String.format("INSERT INTO %s (id, data) VALUES (%d, '%s');",
                                                 tableName, i, "data" + i);
                cluster.schemaChangeIgnoringStoppedInstances(statement);
            }
            cluster.stream().forEach(instance -> instance.flush(TEST_KEYSPACE));
        }
    }

    private void triggerCompactionForTable(QualifiedName tableName)
    {
        cluster.stream().forEach(instance ->
                                 {
                                     try
                                     {
                                         instance.nodetool("compact", tableName.keyspace(), tableName.table());
                                     }
                                     catch (Exception e)
                                     {
                                         logger.warn("Failed to trigger compaction for {}: {}", tableName, e.getMessage());
                                     }
                                 });
    }

    private void validateCompactionStatsResponse(CompactionStatsResponse stats)
    {
        assertThat(stats).isNotNull();

        // Basic counters validation - all should be non-negative
        assertThat(stats.concurrentCompactors()).isGreaterThanOrEqualTo(0);
        assertThat(stats.totalPendingTasks()).isGreaterThanOrEqualTo(0);
        assertThat(stats.completedCompactions()).isGreaterThanOrEqualTo(0);
        assertThat(stats.dataCompacted()).isGreaterThanOrEqualTo(0);
        assertThat(stats.abortedCompactions()).isGreaterThanOrEqualTo(0);
        assertThat(stats.reducedCompactions()).isGreaterThanOrEqualTo(0);
        assertThat(stats.sstablesDroppedFromCompaction()).isGreaterThanOrEqualTo(0);

        // Pending tasks validation - should not be null
        assertThat(stats.pendingTasks()).isNotNull();

        // Validate pending task entries structure and values
        if (!stats.pendingTasks().isEmpty())
        {
            validatePendingTasks(stats);
        }

        // Completion rates validation - should not be null
        assertThat(stats.completedCompactionsRate()).isNotNull();

        // Validate mean rate is not null
        assertThat(stats.completedCompactionsRate().meanRate())
        .as("Mean rate should not be null")
        .isNotNull();

        // Validate fifteen minute rate is not null
        assertThat(stats.completedCompactionsRate().fifteenMinuteRate())
        .as("Fifteen minute rate should not be null")
        .isNotNull();

        // Active compactions validation - list should not be null, count should match size
        assertThat(stats.activeCompactions()).isNotNull();
        assertThat(stats.activeCompactionsCount()).isEqualTo(stats.activeCompactions().size());
        assertThat(stats.activeCompactionsRemainingTime()).isGreaterThanOrEqualTo(0L);

        // Validate active compaction details when compactions are present
        if (!stats.activeCompactions().isEmpty())
        {
            validateActiveCompactions(stats);

            logger.info("All {} active compactions validated successfully", stats.activeCompactionsCount());
        }
        else
        {
            logger.info("No active compactions to validate - basic structure validation completed");
        }

        logger.info("Compaction stats validation successful. Active: {}, Completed: {}, Pending: {}",
                    stats.activeCompactionsCount(), stats.completedCompactions(), stats.totalPendingTasks());
    }

    private void validatePendingTasks(CompactionStatsResponse stats)
    {
        // Validate each keyspace and its associated table map structure
        stats.pendingTasks().forEach((keyspace, tableMap) -> {
            assertThat(keyspace)
            .as("Pending task keyspace should not be blank")
            .isNotBlank();
            assertThat(tableMap)
            .as("Pending task table map should not be null")
            .isNotNull();

            // Validate each table name and its pending task count
            tableMap.forEach((table, count) -> {
                assertThat(table)
                .as("Pending task table name should not be blank")
                .isNotBlank();
                assertThat(count)
                .as("Pending task count should be non-negative")
                .isGreaterThanOrEqualTo(0);
            });
        });
        logger.info("Validated {} pending task keyspaces", stats.pendingTasks().size());
    }

    private void validateActiveCompactions(CompactionStatsResponse stats)
    {
        logger.info("Validating {} active compaction entries", stats.activeCompactionsCount());

        // Validate each active compaction entry
        for (int i = 0; i < stats.activeCompactions().size(); i++)
        {
            CompactionInfo compaction = stats.activeCompactions().get(i);
            logger.info("Validating active compaction {}: {}", i + 1, compaction.id());

            // Validate required fields are not null or blank
            assertThat(compaction.id())
            .as("Active compaction ID should not be null")
            .isNotNull();

            assertThat(compaction.keyspace())
            .as("Active compaction keyspace should not be null")
            .isNotBlank();

            assertThat(compaction.table())
            .as("Active compaction table should not be null")
            .isNotBlank();

            assertThat(compaction.taskType())
            .as("Active compaction task type should not be null")
            .isNotBlank();

            // Validate byte counters are within expected ranges
            assertThat(compaction.completedBytes())
            .as("Completed bytes should be non-negative")
            .isGreaterThanOrEqualTo(0);

            assertThat(compaction.totalBytes())
            .as("Total bytes should be greater than 0")
            .isGreaterThan(0);

            assertThat(compaction.completedBytes())
            .as("Completed bytes should not exceed total bytes")
            .isLessThanOrEqualTo(compaction.totalBytes());

            // Validate percentage completion is within valid range
            assertThat(compaction.percentCompleted())
            .as("Percent completed should be between 0 and 100")
            .isBetween(0.0, 100.0);

            // Ensure percentage matches the completed/total bytes ratio
            double expectedPercentage = (double) compaction.completedBytes() / compaction.totalBytes() * 100;
            assertThat(compaction.percentCompleted())
            .as("Percent completed should be consistent with completed/total bytes ratio")
            .isCloseTo(expectedPercentage, org.assertj.core.data.Percentage.withPercentage(1.0));

            // Validate SSTables list structure and content
            assertThat(compaction.ssTables())
            .as("SSTables list should not be null")
            .isNotNull();

            // Validate individual SSTable names if any exist
            if (!compaction.ssTables().isEmpty())
            {
                for (String ssTable : compaction.ssTables())
                {
                    assertThat(ssTable)
                    .as("SSTable name should not be null or blank")
                    .isNotBlank();
                }
            }

            // Ensure compaction is operating on our test keyspace
            assertThat(compaction.keyspace())
            .as("Compaction should be on our test keyspace")
            .isEqualTo(TEST_KEYSPACE);

            // Ensure compaction is operating on one of our test tables
            boolean isTestTable = COMPACTION_TEST_TABLES.stream()
                                                        .anyMatch(table -> table.table().equals(compaction.table()));
            assertThat(isTestTable)
            .as("Compaction should be on one of our test tables: " + compaction.table())
            .isTrue();

            logger.info("Active compaction {} validation successful: {}% complete, {} bytes",
                        compaction.id(), compaction.percentCompleted(), compaction.completedBytes());
        }

        // Validate remaining time estimate is valid (-1 means unavailable)
        long remainingTime = stats.activeCompactionsRemainingTime();
        assertThat(remainingTime)
        .as("Remaining time should be >= -1 (where -1 indicates unavailable)")
        .isGreaterThanOrEqualTo(-1L);
    }
}
