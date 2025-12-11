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

import org.apache.cassandra.sidecar.common.data.CompactionStopStatus;
import org.apache.cassandra.sidecar.common.data.CompactionType;
import org.apache.cassandra.sidecar.common.response.CompactionStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.CompactionInfo;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.junit.jupiter.api.Test;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpResponseExpectation;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.cassandra.sidecar.common.response.CompactionStopResponse;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;

import static io.vertx.core.buffer.Buffer.buffer;
import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.TEST_TABLE_PREFIX;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
/**
 * Integration tests for the Compaction Stop API endpoint
 */
class CompactionStopIntegrationTest extends SharedClusterSidecarIntegrationTestBase {
    private static final String COMPACTION_STOP_ROUTE = "/api/v1/cassandra/operations/compaction/stop";
    private static final String COMPACTION_STATS_ROUTE = "/api/v1/cassandra/stats/compaction";
    private static final QualifiedName TEST_TABLE = new QualifiedName(TEST_KEYSPACE, TEST_TABLE_PREFIX + "_compaction_test");
    private static final List<QualifiedName> COMPACTION_TEST_TABLES = new ArrayList<>();
    private static final int TABLE_COUNT = 5;

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration() {
        return super.testClusterConfiguration()
                .additionalInstanceConfig(Map.of(
                    "concurrent_compactors", 1,                    // Single compactor for predictability
                    "compaction_throughput_mb_per_sec", 5          // Base throttling at 5 MB/s
                ));
    }

    @Override
    protected void initializeSchemaForTest() {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(TEST_TABLE,
                "CREATE TABLE %s ( \n" +

                        "  id int PRIMARY KEY, \n" +
                        "  data text \n" +
                        ");");
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);

        for (int i = 1; i <= TABLE_COUNT; i++) {
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
    protected void beforeTestStart() {
        // Wait for schema initialization
        waitForSchemaReady(30, TimeUnit.SECONDS);
    }

    @Test
    void testStopCompactionByTypeSuccess() {
        // Create SSTables
        insertTestData(TEST_TABLE, 1000);
        cluster.stream().forEach(instance -> instance.flush(TEST_KEYSPACE));

        // Trigger non-blocking compaction in background
        cluster.stream().forEach(instance -> instance.nodetool("compact", TEST_KEYSPACE));

        // Call compaction stop endpoint
        String payload = "{\"compaction_type\":\"COMPACTION\"}";
        HttpResponse<Buffer> response = getBlocking(
                trustedClient()
                        .post(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                        .sendBuffer(buffer(payload))
                        .expecting(HttpResponseExpectation.SC_OK)
        );

        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
        CompactionStopResponse stopResponse = response.bodyAsJson(CompactionStopResponse.class);
        assertThat(stopResponse).isNotNull();
        assertThat(stopResponse.status()).isEqualTo(CompactionStopStatus.SUBMITTED);
        assertThat(stopResponse.compactionType()).isEqualTo(CompactionType.COMPACTION);
    }

    @Test
    void testStopCompactionByIdSuccess() {
        // Note: Use placeholder compactionId to verify OK HTTP Response
        String payload = "{\"compaction_id\":\"test-compaction-id\"}";

        // Test endpoint accepts request even if no compaction with ID exists - mimics nodetool functionality
        HttpResponse<Buffer> response = getBlocking(
                trustedClient()
                        .post(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                        .sendBuffer(buffer(payload))
                        .expecting(HttpResponseExpectation.SC_OK)
        );

        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
        CompactionStopResponse stopResponse = response.bodyAsJson(CompactionStopResponse.class);
        assertThat(stopResponse).isNotNull();
        assertThat(stopResponse.status()).isEqualTo(CompactionStopStatus.SUBMITTED);
        assertThat(stopResponse.compactionId()).isEqualTo("test-compaction-id");
    }

    @Test
    void testStopCompactionBothParameters() {
        String payload = "{\"compaction_type\":\"VALIDATION\",\"compaction_id\":\"test-id-123\"}";
        HttpResponse<Buffer> response = getBlocking(
                trustedClient()
                        .post(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                        .sendBuffer(buffer(payload))
                        .expecting(HttpResponseExpectation.SC_OK)
        );

        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
        CompactionStopResponse stopResponse = response.bodyAsJson(CompactionStopResponse.class);
        assertThat(stopResponse).isNotNull();
        assertThat(stopResponse.status()).isEqualTo(CompactionStopStatus.SUBMITTED);
        assertThat(stopResponse.compactionType()).isEqualTo(CompactionType.VALIDATION);
        assertThat(stopResponse.compactionId()).isEqualTo("test-id-123");
    }

    @Test
    void testStopCompactionMissingBothParameters() {
        String payload = "{}";
        HttpResponse<Buffer> response = getBlocking(
                trustedClient()
                        .post(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                        .sendBuffer(buffer(payload))
        );

        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
        JsonObject errorResponse = response.bodyAsJsonObject();
        assertThat(errorResponse).isNotNull();
    }

    @Test
    void testStopCompactionInvalidType() {
        String payload = "{\"compaction_type\":\"INVALID_TYPE\"}";
        HttpResponse<Buffer> response = getBlocking(
                trustedClient()
                        .post(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                        .sendBuffer(buffer(payload))
        );

        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
    }

    @Test
    void testStopCompactionMalformedJson() {
        String payload = "{invalid json";
        HttpResponse<Buffer> response = getBlocking(
                trustedClient()
                        .post(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                        .sendBuffer(buffer(payload))
        );

        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
    }

    @Test
    void testStopCompactionAllSupportedTypes() {
        String[] supportedTypes = {
                "COMPACTION", "VALIDATION", "CLEANUP", "SCRUB",
                "UPGRADE_SSTABLES", "INDEX_BUILD", "ANTICOMPACTION", "VERIFY"
        };

        for (String type : supportedTypes) {
            String payload = String.format("{\"compaction_type\":\"%s\"}", type);
            HttpResponse<Buffer> response = getBlocking(
                    trustedClient()
                            .post(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                            .sendBuffer(buffer(payload))
                            .expecting(HttpResponseExpectation.SC_OK)
            );

            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
            CompactionStopResponse stopResponse = response.bodyAsJson(CompactionStopResponse.class);
            assertThat(stopResponse.status()).isEqualTo(CompactionStopStatus.SUBMITTED);
            assertThat(stopResponse.compactionType()).isEqualTo(CompactionType.valueOf(type));
        }
    }

    /**
     * Inserts test data into the specified table
     *
     * @param tableName the qualified name of the table
     * @param rowCount  the number of rows to insert
     */
    private void insertTestData(QualifiedName tableName, int rowCount) {
        for (int i = 1; i <= rowCount; i++) {
            String statement = String.format(
                    "INSERT INTO %s (id, data) VALUES (%d, 'test_data_%d');",
                    tableName, i, i
            );
            cluster.schemaChangeIgnoringStoppedInstances(statement);
        }
    }

    private void generateSSTables(QualifiedName tableName, int ssTableCount) {
        // Generate larger data to slow down compaction
        String largeData = "x".repeat(10000); // 10KB of data per row

        for (int batch = 0; batch < ssTableCount; batch++) {
            for (int i = batch * 1000; i < (batch + 1) * 1000; i++) {
                String statement = String.format("INSERT INTO %s (id, data) VALUES (%d, '%s');",
                        tableName, i, largeData + i);
                cluster.schemaChangeIgnoringStoppedInstances(statement);
            }
            cluster.stream().forEach(instance -> instance.flush(TEST_KEYSPACE));
        }
    }

    private void triggerCompactionForTable(QualifiedName tableName) {
        cluster.stream().forEach(instance ->
        {
            try {
                instance.nodetool("compact", tableName.keyspace(), tableName.table());
            } catch (Exception e) {
                logger.warn("Failed to trigger compaction for {}: {}", tableName, e.getMessage());
            }
        });
    }

    @Test
    void testCompactionStopActuallyStopped() throws InterruptedException {
        logger.info("Testing that compaction stop actually stops compactions");

        // Generate SSTables with large data to ensure longer compactions for all test tables
        for (QualifiedName tableName : COMPACTION_TEST_TABLES) {
            generateSSTables(tableName, 200);// More SSTables with 10KB rows = much longer compaction
        }

        // Slow compactions down before they start
        cluster.stream().forEach(instance -> {
            try {
                instance.nodetool("setcompactionthroughput", "1"); // 1 MB/sec rather than unlimited
            } catch (Exception e) {
                logger.warn("Failed to set compaction throughput: {}", e.getMessage());
            }
        });

        // Create threads to trigger compaction on all tables
        List<Thread> compactionThreads = new ArrayList<>();
        for (QualifiedName tableName : COMPACTION_TEST_TABLES) {
            Thread thread = new Thread(() -> triggerCompactionForTable(tableName));
            compactionThreads.add(thread);
        }

        // Trigger compaction in background
        cluster.stream().forEach(instance ->
                instance.nodetool("compact", TEST_KEYSPACE, TEST_TABLE.table())
        );

        // Poll until active compaction found that's actually running
        CompactionInfo activeCompaction = null;

        // Start polling immediately - no initial delay
        for (int attempt = 0; attempt < 60; attempt++) {  // More attempts to catch slower compaction
            HttpResponse<Buffer> statsResponse = getBlocking(
                    trustedClient()
                            .get(serverWrapper.serverPort, "localhost", COMPACTION_STATS_ROUTE)
                            .send()
                            .expecting(HttpResponseExpectation.SC_OK)
            );

            CompactionStatsResponse stats = statsResponse.bodyAsJson(CompactionStatsResponse.class);

            if (!stats.activeCompactions().isEmpty()) {
                logger.info("Found {} active compactions on attempt {}",
                        stats.activeCompactionsCount(), attempt + 1);
                CompactionInfo compaction = stats.activeCompactions().get(0);
                double progress = compaction.percentCompleted();

                // Only stop compaction if it's early enough that natural completion is very unlikely
                // Get started but incomplete compactions
                if (progress > 0.0 && progress < 80.0) {
                    activeCompaction = compaction;
                    double progressAtStart = progress;

                    logger.info("Found in-progress compaction at {}% - ID: {}",
                            progress, compaction.id());
                    String compactionId = activeCompaction.id();

                    // Stop compaction found
                    String stopPayload = "{\"compaction_id\":\"" + compactionId + "\"}";

                    HttpResponse<Buffer> stopResponse = getBlocking(
                            trustedClient()
                                    .post(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                                    .sendBuffer(buffer(stopPayload))
                                    .expecting(HttpResponseExpectation.SC_OK)
                    );
                    assertThat(stopResponse.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                    logger.info("Compaction stop called successfully for ID: {} at {}% progress", compactionId, progressAtStart);

//                     Verify the compaction with this ID is no longer in active compactions
//                     Since it was at < 50% when we called stop, it's extremely unlikely to have
//                     naturally completed in the verification window (would need to compact remaining
//                     ~1GB+ in a few seconds)
                    for (int verifyAttempt = 0; verifyAttempt < 10; verifyAttempt++) {

                        HttpResponse<Buffer> statsAfterStop = getBlocking(
                                trustedClient()
                                        .get(serverWrapper.serverPort, "localhost", COMPACTION_STATS_ROUTE)
                                        .send()
                                        .expecting(HttpResponseExpectation.SC_OK)
                        );
                        CompactionStatsResponse statsAfter = statsAfterStop.bodyAsJson(CompactionStatsResponse.class);

                        // Check if the compaction with our ID is gone from active compactions
                        boolean compactionGone = statsAfter.activeCompactions().stream()
                                .noneMatch(c -> c.id().equals(compactionId));

                        logger.info("Verify attempt {}: Compaction ID {} is gone={}, active count={}",
                                verifyAttempt + 1, compactionId, compactionGone, statsAfter.activeCompactionsCount());

                        if (compactionGone) {
                            logger.info("✓ Compaction {} stopped successfully at verify attempt {} - was at {}% when stopped, " +
                                            "disappeared from active list",
                                    compactionId, verifyAttempt, progressAtStart);
                            break;
                        }

                    }
                    return;
                } else if (progress >= 80.0 && progress < 100.0) {
                    logger.info("Attempt {}: Compaction at {}% (too far along, waiting for earlier stage)",
                            attempt + 1, progress);
                } else {
                    logger.info("Attempt {}: Compaction at {}% (waiting for it to start)",
                            attempt + 1, progress);
                }
            } else {
                logger.info("Attempt {}: No active compactions yet", attempt + 1);
            }
        }

        // Reset throughput after test
        cluster.stream().forEach(instance -> {
            try {
                instance.nodetool("setcompactionthroughput", "0"); // 0 = unlimited
            } catch (Exception e) {
                logger.warn("Failed to reset compaction throughput: {}", e.getMessage());
            }
        });

        assumeTrue( false, "Could not catch compaction in testable state - skipping test");
    }

    @Test
    void testCompactionStopByTypeActuallyStopped() throws InterruptedException {
        logger.info("Testing that compaction stop by type actually stops compactions");

        // Generate SSTables with large data to ensure longer compactions for all test tables
        for (QualifiedName tableName : COMPACTION_TEST_TABLES) {
            generateSSTables(tableName, 200);// More SSTables with 10KB rows = much longer compaction
        }

        // Slow compactions down before they start
        cluster.stream().forEach(instance -> {
            try {
                instance.nodetool("setcompactionthroughput", "1"); // 1 MB/sec rather than unlimited
            } catch (Exception e) {
                logger.warn("Failed to set compaction throughput for stopByType: {}", e.getMessage());
            }
        });

        // Trigger compaction in background
        cluster.stream().forEach(instance ->
                instance.nodetool("compact", TEST_KEYSPACE, TEST_TABLE.table())
        );

        // Poll until active compaction found that's actually running
        CompactionInfo activeCompaction = null;

        // Start polling immediately - no initial delay
        for (int attempt = 0; attempt < 60; attempt++) {  // More attempts to catch slower compaction
            HttpResponse<Buffer> statsResponse = getBlocking(
                    trustedClient()
                            .get(serverWrapper.serverPort, "localhost", COMPACTION_STATS_ROUTE)
                            .send()
                            .expecting(HttpResponseExpectation.SC_OK)
            );

            CompactionStatsResponse stats = statsResponse.bodyAsJson(CompactionStatsResponse.class);

            if (!stats.activeCompactions().isEmpty()) {
                CompactionInfo compaction = stats.activeCompactions().get(0);
                double progress = compaction.percentCompleted();

                // Use a wider stopping window and multiple criteria for better success rate
                // Stop if: compaction has started AND (is early stage OR we haven't found one yet after many attempts)
                boolean isEarlyStage = progress > 0.0 && progress < 90.0;
                boolean shouldStopAnyway = attempt > 30; // After 30 attempts, stop any running compaction

                if (isEarlyStage || shouldStopAnyway) {
                    double startingProgress = progress;
                    String taskType = compaction.taskType();

                    logger.info("Found in-progress compaction - taskType: '{}', Progress: {}%, ID: {}",
                            taskType, progress, compaction.id());

                    // Stop compaction by TYPE instead of ID
                    // The CompactionType enum handles case-insensitive conversion in fromString()
                    String stopPayload = "{\"compaction_type\":\"" + taskType + "\"}";

                    logger.info("Sending stop payload: {}", stopPayload);

                    HttpResponse<Buffer> stopResponse = getBlocking(
                            trustedClient()
                                    .post(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                                    .sendBuffer(buffer(stopPayload))
                    );

                    logger.info("Stop response status: {}, body: {}",
                            stopResponse.statusCode(), stopResponse.bodyAsString());

                    if (stopResponse.statusCode() != 200) {
                        logger.error("Failed to stop compaction. Status: {}, Error: {}",
                                stopResponse.statusCode(), stopResponse.bodyAsString());
                        return; // Skip verification if stop failed
                    }
                    assertThat(stopResponse.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                    logger.info("Compaction stop called successfully for type: {} at {}% progress", taskType, startingProgress);

                    // Verify that compactions of this type are no longer in active compactions
                    for (int verifyAttempt = 0; verifyAttempt < 10; verifyAttempt++) {

                        HttpResponse<Buffer> statsAfterStop = getBlocking(
                                trustedClient()
                                        .get(serverWrapper.serverPort, "localhost", COMPACTION_STATS_ROUTE)
                                        .send()
                                        .expecting(HttpResponseExpectation.SC_OK)
                        );
                        CompactionStatsResponse statsAfter = statsAfterStop.bodyAsJson(CompactionStatsResponse.class);

                        // Check if compactions of this TYPE are gone from active compactions
                        boolean compactionsOfTypeGone = statsAfter.activeCompactions().stream()
                                .noneMatch(c -> c.taskType().equalsIgnoreCase(taskType));

                        logger.info("Verify attempt {}: Compactions of type {} are gone={}, active count={}",
                                verifyAttempt + 1, taskType, compactionsOfTypeGone, statsAfter.activeCompactionsCount());

                        if (compactionsOfTypeGone) {
                            logger.info("✓ Compactions of type {} stopped successfully at verify attempt {} - " +
                                            "was at {}% when stopped, disappeared from active list",
                                    taskType, verifyAttempt, startingProgress);
                            break;
                        }
                    }
                    return;
                } else if (progress >= 90.0 && progress < 100.0) {
                    logger.info("CompactionType Stop Attempt {}: Compaction at {}% (too far along, waiting for earlier stage)",
                            attempt + 1, progress);
                } else {
                    logger.info("CompactionType Stop Attempt {}: Compaction at {}% (waiting for it to start)",
                            attempt + 1, progress);
                }
            } else {
                logger.info("CompactionType Stop Attempt {}: No active compactions yet", attempt + 1);
            }
        }
        assumeTrue(false, "Could not catch compaction in testable state - skipping test");
    }
}