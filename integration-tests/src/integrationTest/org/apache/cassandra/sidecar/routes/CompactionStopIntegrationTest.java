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
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.sidecar.common.data.CompactionStopStatus;
import org.apache.cassandra.sidecar.common.response.CompactionStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.CompactionInfo;
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
    protected void initializeSchemaForTest() {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(TEST_TABLE,
                "CREATE TABLE %s ( \n" +

                        "  id int PRIMARY KEY, \n" +
                        "  data text \n" +
                        ");");
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
        assertThat(stopResponse.compactionType()).isEqualTo("COMPACTION");
        assertThat(stopResponse.reason()).isEqualTo("Operation Succeeded");
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
        assertThat(stopResponse.compactionType()).isEqualTo("VALIDATION");
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
            assertThat(stopResponse.compactionType()).isEqualTo(type);
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

        // Generate many SSTables with large data to ensure longer compactions
        generateSSTables(TEST_TABLE, 200);  // More SSTables with 10KB rows = much longer compaction

        // Trigger compaction in background
        Thread compactionThread = new Thread(() -> {
            cluster.stream().forEach(instance ->
                    instance.nodetool("compact", TEST_KEYSPACE, TEST_TABLE.table())
            );
        });
        compactionThread.start();

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

                // Only stop compaction if it's early enough that natural completion is very unlikely
                // We want progress > 0 (has started) but < 50% (far from completion)
                if (progress > 0.0 && progress < 50.0) {
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

                    // Verify the compaction with this ID is no longer in active compactions
                    // Since it was at < 50% when we called stop, it's extremely unlikely to have
                    // naturally completed in the verification window (would need to compact remaining
                    // ~1GB+ in a few seconds)
                    boolean compactionStopped = false;
                    for (int verifyAttempt = 0; verifyAttempt < 10; verifyAttempt++) {
                        Thread.sleep(300);

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
                            compactionStopped = true;
                            logger.info("✓ Compaction {} stopped successfully - was at {}% when stopped, disappeared from active list",
                                    compactionId, progressAtStart);
                            break;
                        }

                        // Additional verification: if compaction is still present, check if progress is increasing
                        // If progress is increasing, the stop didn't work
                        statsAfter.activeCompactions().stream()
                                .filter(c -> c.id().equals(compactionId))
                                .findFirst()
                                .ifPresent(c -> logger.warn("Compaction {} still running at {}% (was {}% when stop called)",
                                        compactionId, c.percentCompleted(), progressAtStart));
                    }

                    assertThat(compactionStopped)
                            .withFailMessage("Compaction with ID %s was not stopped - still appears in active compactions after stop call at %.1f%% progress",
                                    compactionId, progressAtStart)
                            .isTrue();
                    return;
                } else if (progress >= 50.0 && progress < 100.0) {
                    logger.info("Attempt {}: Compaction at {}% (too far along, waiting for earlier stage)",
                            attempt + 1, progress);
                } else {
                    logger.info("Attempt {}: Compaction at {}% (waiting for it to start)",
                            attempt + 1, progress);
                }
            } else {
                logger.info("Attempt {}: No active compactions yet", attempt + 1);
            }

            Thread.sleep(200);
        }
    }

    @Test
    void testCompactionStopByTypeActuallyStopped() throws InterruptedException {
        logger.info("Testing that compaction stop by type actually stops compactions");

        // Generate many SSTables with large data to ensure longer compactions
        generateSSTables(TEST_TABLE, 200);  // More SSTables with 10KB rows = much longer compaction

        // Trigger compaction in background
        Thread compactionThread = new Thread(() -> {
            cluster.stream().forEach(instance ->
                    instance.nodetool("compact", TEST_KEYSPACE, TEST_TABLE.table())
            );
        });
        compactionThread.start();

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

                // Only stop compaction if it's early enough that natural completion is very unlikely
                // We want progress > 0 (has started) but < 50% (far from completion)
                if (progress > 0.0 && progress < 50.0) {
                    activeCompaction = compaction;
                    double startingProgress = progress;
                    String compactionType = compaction.taskType();  // Get the type

                    logger.info("Found in-progress compaction of type {} at {}% - ID: {}",
                            compactionType, progress, compaction.id());

                    // Stop compaction by TYPE instead of ID
                    String stopPayload = "{\"compaction_type\":\"" + compactionType + "\"}";

                    HttpResponse<Buffer> stopResponse = getBlocking(
                            trustedClient()
                                    .post(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                                    .sendBuffer(buffer(stopPayload))
                                    .expecting(HttpResponseExpectation.SC_OK)
                    );
                    assertThat(stopResponse.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                    logger.info("Compaction stop called successfully for type: {} at {}% progress", compactionType, startingProgress);

                    // Verify that compactions of this type are no longer in active compactions
                    boolean compactionsStopped = false;
                    for (int verifyAttempt = 0; verifyAttempt < 10; verifyAttempt++) {
                        Thread.sleep(300);

                        HttpResponse<Buffer> statsAfterStop = getBlocking(
                                trustedClient()
                                        .get(serverWrapper.serverPort, "localhost", COMPACTION_STATS_ROUTE)
                                        .send()
                                        .expecting(HttpResponseExpectation.SC_OK)
                        );
                        CompactionStatsResponse statsAfter = statsAfterStop.bodyAsJson(CompactionStatsResponse.class);

                        // Check if compactions of this TYPE are gone from active compactions
                        boolean compactionsOfTypeGone = statsAfter.activeCompactions().stream()
                                .noneMatch(c -> c.taskType().equals(compactionType));

                        logger.info("Verify attempt {}: Compactions of type {} are gone={}, active count={}",
                                verifyAttempt + 1, compactionType, compactionsOfTypeGone, statsAfter.activeCompactionsCount());

                        if (compactionsOfTypeGone) {
                            compactionsStopped = true;
                            logger.info("✓ Compactions of type {} stopped successfully - was at {}% when stopped, disappeared from active list",
                                    compactionType, startingProgress);
                            break;
                        }

                        // Additional verification: if compactions of this type are still present, log progress
                        statsAfter.activeCompactions().stream()
                                .filter(c -> c.taskType().equals(compactionType))
                                .forEach(c -> logger.warn("Compaction {} of type {} still running at {}% (was {}% when stop called)",
                                        c.id(), compactionType, c.percentCompleted(), startingProgress));
                    }

                    assertThat(compactionsStopped)
                            .withFailMessage("Compactions of type %s were not stopped - still appear in active compactions after stop call at %.1f%% progress",
                                    compactionType, startingProgress)
                            .isTrue();
                    return;
                } else if (progress >= 50.0 && progress < 100.0) {
                    logger.info("CompactionType Stop Attempt {}: Compaction at {}% (too far along, waiting for earlier stage)",
                            attempt + 1, progress);
                } else {
                    logger.info("CompactionType Stop Attempt {}: Compaction at {}% (waiting for it to start)",
                            attempt + 1, progress);
                }
            } else {
                logger.info("CompactionType Stop Attempt {}: No active compactions yet", attempt + 1);
            }

            Thread.sleep(200);
        }
    }
}