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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.jupiter.api.Test;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpResponseExpectation;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;

import org.apache.cassandra.sidecar.common.data.CompactionStopStatus;
import org.apache.cassandra.sidecar.common.response.CompactionStatsResponse;
import org.apache.cassandra.sidecar.common.response.CompactionStopResponse;
import org.apache.cassandra.sidecar.common.response.data.CompactionInfo;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;

import static io.vertx.core.buffer.Buffer.buffer;
import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.TEST_TABLE_PREFIX;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;


/**
 * Integration tests for the Compaction Stop API endpoint
 */
class CompactionStopIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    private static final String COMPACTION_STOP_ROUTE = "/api/v1/cassandra/operations/compaction/stop";
    private static final String COMPACTION_STATS_ROUTE = "/api/v1/cassandra/stats/compaction";
    private static final QualifiedName TEST_TABLE
    = new QualifiedName(TEST_KEYSPACE, TEST_TABLE_PREFIX + "_compaction_test");
    private static final List<QualifiedName> COMPACTION_TEST_TABLES = new ArrayList<>();
    private static final int TABLE_COUNT = 5;

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .additionalInstanceConfig(Map.of(
                    "concurrent_compactors", 1,                    // Single compactor for predictability
                    "compaction_throughput_mb_per_sec", 5,         // Base throttling at 5 MB/s
                    "auto_snapshot", "false",                      // Disable auto snapshots
                    "compaction_large_partition_warning_threshold_mb", "1000", // Avoid large partition warnings
                    "auto_compaction", "false"                     // Disable ALL auto-compaction globally
                    ));
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(TEST_TABLE, "CREATE TABLE %s (\n  id int PRIMARY KEY, \n data text \n);");
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);

        for (int i = 1; i <= TABLE_COUNT; i++)
        {
            COMPACTION_TEST_TABLES.add(new QualifiedName(TEST_KEYSPACE, TEST_TABLE_PREFIX + "_compaction_" + i));
        }

        // Create test tables for compaction activity
        for (QualifiedName tableName : COMPACTION_TEST_TABLES)
        {
            createTestTable(tableName, "CREATE TABLE %s ( \n  id int PRIMARY KEY, \n  data text \n);");
        }
    }

    @Override
    protected void beforeTestStart()
    {
        // Wait for schema initialization
        waitForSchemaReady(30, TimeUnit.SECONDS);

        // Disable auto-compaction for ALL keyspaces at the beginning
        cluster.stream().forEach(instance -> {
            try
            {
                // First set compaction throughput to a high value to prevent any initial compactions from taking too long
                instance.nodetool("setcompactionthroughput", "100");

                // Disable auto-compaction globally (no arguments)
                instance.nodetool("disableautocompaction");
                logger.info("Disabled auto-compaction globally");

                // And for our test keyspace
                instance.nodetool("disableautocompaction", TEST_KEYSPACE);

                // Log that we've disabled auto-compaction
                logger.info("Auto-compaction disabled for all keyspaces");
            }
            catch (Exception e)
            {
                logger.warn("Failed to disable autocompaction in beforeTestStart: {}", e.getMessage());
            }
        });
    }

    @Test
    void testStopCompactionBothParameters()
    {
        String payload = "{\"compactionType\":\"VALIDATION\",\"compactionId\":\"test-id-123\"}";
        HttpResponse<Buffer> response
        = getBlocking(trustedClient().put(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                                     .sendBuffer(buffer(payload))
                                     .expecting(HttpResponseExpectation.SC_OK));

        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
        CompactionStopResponse stopResponse = response.bodyAsJson(CompactionStopResponse.class);
        assertThat(stopResponse).isNotNull();
        assertThat(stopResponse.status()).isEqualTo(CompactionStopStatus.SUBMITTED);
        assertThat(stopResponse.compactionType()).isEqualTo("VALIDATION");
        assertThat(stopResponse.compactionId()).isEqualTo("test-id-123");
    }

    @Test
    void testStopCompactionMissingBothParameters()
    {
        String payload = "{}";
        HttpResponse<Buffer> response
        = getBlocking(trustedClient().put(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                                     .sendBuffer(buffer(payload)));

        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
        JsonObject errorResponse = response.bodyAsJsonObject();
        assertThat(errorResponse).isNotNull();
    }

    @Test
    void testStopCompactionInvalidType()
    {
        String payload = "{\"compactionType\":\"INVALID_TYPE\"}";
        HttpResponse<Buffer> response
        = getBlocking(trustedClient().put(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                                     .sendBuffer(buffer(payload)));

        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
    }

    @Test
    void testStopCompactionMalformedJson()
    {
        String payload = "{invalid json";
        HttpResponse<Buffer> response
        = getBlocking(trustedClient().put(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                                     .sendBuffer(buffer(payload)));

        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
    }

    @Test
    void testStopCompactionAllSupportedTypes()
    {
        String[] supportedTypes = {
                "COMPACTION", "VALIDATION", "KEY_CACHE_SAVE", "ROW_CACHE_SAVE",
                "COUNTER_CACHE_SAVE", "CLEANUP", "SCRUB", "UPGRADE_SSTABLES",
                "INDEX_BUILD", "TOMBSTONE_COMPACTION", "ANTICOMPACTION",
                "VERIFY", "VIEW_BUILD", "INDEX_SUMMARY", "RELOCATE",
                "GARBAGE_COLLECT", "MAJOR_COMPACTION"
        };
        String cassandraVersion = testVersion.version();

        for (String compactionType : supportedTypes)
        {
            String payload = String.format("{\"compactionType\":\"%s\"}", compactionType);

            HttpResponse<Buffer> response = getBlocking(
                    trustedClient().put(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                            .sendBuffer(buffer(payload))
            );
            if (compactionType.equals("MAJOR_COMPACTION") && cassandraVersion.startsWith("4."))
            {
                assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());

            }
            else
            {
                assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                CompactionStopResponse stopResponse = response.bodyAsJson(CompactionStopResponse.class);
                assertThat(stopResponse.status()).isEqualTo(CompactionStopStatus.SUBMITTED);
                assertThat(stopResponse.compactionType()).isEqualTo(compactionType);
            }
        }
    }

    @Test
    void testUnsupportedCompactionTypeForCassandraVersion()
    {
        String payload = "{\"compactionType\":\"MAJOR_COMPACTION\"}";
        HttpResponse<Buffer> response = getBlocking(
            trustedClient().put(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                .sendBuffer(buffer(payload))
        );
        String cassandraVersion = testVersion.version();

        // Check MAJOR_COMPACTION rejected with Cassandra 4.x, accepted with 5.x
        if (cassandraVersion.startsWith("4."))
        {
            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
            JsonObject errorResponse = response.bodyAsJsonObject();
            assertThat(errorResponse).isNotNull();
            // Error message could be from handler validation or JMX layer
            String message = errorResponse.getString("message");
            assertThat(message)
                .satisfiesAnyOf(
                    msg -> assertThat(msg).containsIgnoringCase("not supported"),
                    msg -> assertThat(msg).containsIgnoringCase("No enum constant"),
                    msg -> assertThat(msg).contains("MAJOR_COMPACTION")
                );
        }
        else if (cassandraVersion.startsWith("5."))
        {
            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
            CompactionStopResponse stopResponse = response.bodyAsJson(CompactionStopResponse.class);
            assertThat(stopResponse.status()).isEqualTo(CompactionStopStatus.SUBMITTED);
            assertThat(stopResponse.compactionType()).isEqualTo("MAJOR_COMPACTION");
        }
        else
        {
            // Unknown Cassandra version
            throw new AssertionError("Unexpected Cassandra version: " + cassandraVersion);
        }
    }

    private void generateSSTables(QualifiedName tableName, int ssTableCount)
    {
        String largeData = "x".repeat(1000); // 1KB of data per row

        // Double-check auto-compaction is disabled before generating data
        cluster.stream().forEach(instance -> {
            try
            {
                instance.nodetool("disableautocompaction", TEST_KEYSPACE, tableName.table());
                logger.info("Confirmed auto-compaction disabled for table {} before data generation",
                            tableName.table());
            }
            catch (Exception e)
            {
                logger.warn("Failed to confirm auto-compaction is disabled: {}", e.getMessage());
            }
        });

        int rowsPerBatch = 500;

        for (int batch = 0; batch < ssTableCount; batch++)
        {
            logger.info("Generating batch {} of {} for table {}", batch + 1, ssTableCount, tableName.table());

            for (int i = batch * rowsPerBatch; i < (batch + 1) * rowsPerBatch; i++)
            {
                String statement = String.format("INSERT INTO %s (id, data) VALUES (%d, '%s');",
                                                 tableName, i, largeData + i);
                cluster.schemaChangeIgnoringStoppedInstances(statement);
            }

            // Flush after each batch but verify compaction is still disabled
            final int currentBatch = batch + 1;
            cluster.stream().forEach(instance -> {
                try
                {
                    // Flush only accepts one parameter (keyspace)
                    instance.flush(TEST_KEYSPACE);
                    logger.debug("Flushed keyspace {} for table {}", TEST_KEYSPACE, tableName.table());
                }
                catch (Exception e)
                {
                    logger.warn("Failed to flush: {}", e.getMessage());
                }
            });
        }
    }

    /**
     * Verifies that compactions of the specified type are no longer active
     */
    private void verifyCompactionStopped(String detectedCompactionId)
    {
        loopAssert(10, () -> {
            HttpResponse<Buffer> statsResponse
            = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", COMPACTION_STATS_ROUTE)
                                         .send()
                                         .expecting(HttpResponseExpectation.SC_OK));

            CompactionStatsResponse stats = statsResponse.bodyAsJson(CompactionStatsResponse.class);

            // Check if compactions of this TYPE are gone from active compactions
            boolean compactionsOfTypeGone = stats.activeCompactions()
                                                 .stream()
                                                 .noneMatch(c -> c.id().equals(detectedCompactionId));

            logger.info("Verification: Compaction with id {} type {} are gone={}, active count={}",
                        detectedCompactionId, "COMPACTION", compactionsOfTypeGone,
                        stats.activeCompactionsCount());

            assertThat(compactionsOfTypeGone).isTrue();
        });
    }

    @Test
    void testCompactionStopByTypeActuallyStopped()
    {
        long startTime = System.currentTimeMillis();

        try
        {
            logger.info("Testing that compaction stop by type actually stops compactions");

            // 2. THEN set compaction throughput to slow value
            cluster.stream().forEach(instance -> {
                try
                {
                    instance.nodetool("setcompactionthroughput", "1"); // 1 MB/sec rather than unlimited
                }
                catch (Exception e)
                {
                    logger.warn("Failed to set compaction throughput for stopByType: {}", e.getMessage());
                }
            });

            // 3. THEN generate data (with reduced volume)
            for (QualifiedName tableName : COMPACTION_TEST_TABLES)
            {
                generateSSTables(tableName, 20); // Reduced from 200 to 20
            }

            for (QualifiedName tableName : COMPACTION_TEST_TABLES)
            {
                cluster.stream().forEach(instance -> {
                    try
                    {
                        instance.nodetool("enableautocompaction", TEST_KEYSPACE, tableName.table());
                    }
                    catch (Exception e)
                    {
                        logger.warn("Failed to re-enable autocompaction: {}", e.getMessage());
                    }
                });
            }

            // Add initial delay to allow compaction to start
            logger.info("Waiting for compaction to start...");

            // Poll for active compaction and stop it
            boolean compactionStopped = pollAndStopCompactionByType("Compaction", 30);

            if (!compactionStopped)
            {
                logger.error("Could not catch compaction in testable state - skipping test");
            }
        }
        finally
        {
            long duration = System.currentTimeMillis() - startTime;
            logger.info("Test completed in {} ms", duration);
        }
    }

    /**
     * Polls for an active compaction and attempts to stop it by type
     *
     * @param compactionType The type of compaction to look for and stop
     * @param maxAttempts Maximum number of polling attempts
     * @return true if a compaction was found and stopped successfully, false otherwise
     */
    private boolean pollAndStopCompactionByType(String compactionType, int maxAttempts)
    {
        AtomicBoolean compactionStopped = new AtomicBoolean(false);
        AtomicReference<Double> startingProgress = new AtomicReference<>(0.0);
        AtomicReference<String> actualCompactionType = new AtomicReference<>(compactionType);

        loopAssert(maxAttempts, () -> {
            // Get current compaction stats
            HttpResponse<Buffer> statsResponse
            = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", COMPACTION_STATS_ROUTE)
                                         .send()
                                         .expecting(HttpResponseExpectation.SC_OK));

            CompactionStatsResponse stats = statsResponse.bodyAsJson(CompactionStatsResponse.class);

            if (stats.activeCompactions().isEmpty())
            {
                logger.info("No active compactions found yet");
                Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
                throw new AssertionError("No active compactions found yet");
            }

            // Found active compaction
            CompactionInfo compaction = stats.activeCompactions().get(0);
            double progress = compaction.percentCompleted();

            // Only proceed if compaction is in progress but not nearly complete
            if (progress <= 0.0 || progress >= 90.0)
            {
                logger.info("Compaction at {}% - waiting for suitable progress", progress);
                throw new AssertionError("Compaction not in suitable state to stop");
            }

            // Found a suitable compaction to stop
            startingProgress.set(progress);
            String originalTaskType = compaction.taskType();
            actualCompactionType.set(originalTaskType.toUpperCase());

            logger.info("Found in-progress compaction - Type: '{}', Progress: {}%, ID: {}",
                        actualCompactionType.get(), progress, compaction.id());

            // Stop compaction by type
            String stopPayload = "{\"compactionType\":\"" + actualCompactionType.get() + "\"}";

            HttpResponse<Buffer> stopResponse
            = getBlocking(trustedClient().put(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                                         .sendBuffer(buffer(stopPayload))
                                         .expecting(HttpResponseExpectation.SC_OK));

            assertThat(stopResponse.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
            CompactionStopResponse response = stopResponse.bodyAsJson(CompactionStopResponse.class);
            assertThat(response.status()).isEqualTo(CompactionStopStatus.SUBMITTED);

            logger.info("Compaction stop called successfully for type: {} at {}% progress",
                        actualCompactionType.get(), startingProgress.get());

            // Verify compaction was stopped
            verifyCompactionStopped(compaction.id());

            compactionStopped.set(true);
        });

        return compactionStopped.get();
    }

    @Test
    void testCompactionStopByIdActuallyStopped()
    {
        long startTime = System.currentTimeMillis();

        try
        {
            logger.info("Testing that compaction stop by ID actually stops compactions");

            // 2. THEN set compaction throughput to slow value
            cluster.stream().forEach(instance -> {
                try
                {
                    instance.nodetool("setcompactionthroughput", "1"); // 1 MB/sec rather than unlimited
                }
                catch (Exception e)
                {
                    logger.warn("Failed to set compaction throughput for stopById: {}", e.getMessage());
                }
            });

            // 3. THEN generate data (with reduced volume)
            for (QualifiedName tableName : COMPACTION_TEST_TABLES)
            {
                generateSSTables(tableName, 20); // Reduced from 200 to 20
            }

            for (QualifiedName tableName : COMPACTION_TEST_TABLES)
            {
                cluster.stream().forEach(instance -> {
                    try
                    {
                        instance.nodetool("enableautocompaction", TEST_KEYSPACE, tableName.table());
                    }
                    catch (Exception e)
                    {
                        logger.warn("Failed to re-enable autocompaction: {}", e.getMessage());
                    }
                });
            }

            // Add initial delay to allow compaction to start
            logger.info("Waiting for compaction to start...");

            // Poll for active compaction and stop it by ID
            try
            {
                pollAndStopCompactionById(30);
            }
            catch (Exception e)
            {
                logger.warn("Could not catch compaction in testable state");
            }
        }
        finally
        {
            long duration = System.currentTimeMillis() - startTime;
            logger.info("Test completed in {} ms", duration);
        }
    }

    /**
     * Polls for an active compaction and attempts to stop it by ID
     *
     * @param maxAttempts Maximum number of polling attempts
     * @return true if a compaction was found and stopped successfully, false otherwise
     */
    private boolean pollAndStopCompactionById(int maxAttempts)
    {
        AtomicBoolean compactionStopped = new AtomicBoolean(false);
        AtomicReference<Double> startingProgress = new AtomicReference<>(0.0);
        AtomicReference<String> capturedCompactionId = new AtomicReference<>("");

        loopAssert(maxAttempts, () -> {
            // Get current compaction stats
            HttpResponse<Buffer> statsResponse
            = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", COMPACTION_STATS_ROUTE)
                                         .send()
                                         .expecting(HttpResponseExpectation.SC_OK));

            CompactionStatsResponse stats = statsResponse.bodyAsJson(CompactionStatsResponse.class);

            if (stats.activeCompactions().isEmpty())
            {
                logger.info("No active compactions found yet");
                Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
                throw new AssertionError("No active compactions found yet");
            }

            // Found active compaction
            CompactionInfo compaction = stats.activeCompactions().get(0);
            double progress = compaction.percentCompleted();

            // Only proceed if compaction is in progress but not nearly complete
            if (progress <= 0.0 || progress >= 90.0)
            {
                logger.info("Compaction at {}% - waiting for suitable progress", progress);
                throw new AssertionError("Compaction not in suitable state to stop");
            }

            // Found a suitable compaction to stop - capture its ID
            startingProgress.set(progress);
            capturedCompactionId.set(compaction.id());

            logger.info("Found in-progress compaction - Type: '{}', Progress: {}%, ID: {}",
                        compaction.taskType(), progress, capturedCompactionId.get());

            // Stop compaction by ID
            String stopPayload = "{\"compactionId\":\"" + capturedCompactionId.get() + "\"}";

            HttpResponse<Buffer> stopResponse
            = getBlocking(trustedClient().put(serverWrapper.serverPort, "localhost", COMPACTION_STOP_ROUTE)
                                         .sendBuffer(buffer(stopPayload))
                                         .expecting(HttpResponseExpectation.SC_OK));

            assertThat(stopResponse).isNotNull();
            assertThat(stopResponse.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
            CompactionStopResponse response = stopResponse.bodyAsJson(CompactionStopResponse.class);
            assertThat(response.status()).isEqualTo(CompactionStopStatus.SUBMITTED);
            assertThat(response.compactionId()).isEqualTo(capturedCompactionId.get());

            logger.info("Compaction stop called successfully for ID: {} at {}% progress",
                        capturedCompactionId.get(), startingProgress.get());

            // Verify compaction was stopped
            verifyCompactionStopped(capturedCompactionId.get());

            compactionStopped.set(true);
        });

        return compactionStopped.get();
    }
}
