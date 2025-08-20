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

import org.junit.jupiter.api.Test;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpResponseExpectation;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.cassandra.sidecar.common.response.CompactionStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.ActiveCompactionEntry;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.TEST_TABLE_PREFIX;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the Compaction Statistics API endpoint.
 * 
 * <p>Tests the compaction statistics functionality by generating large-scale SSTable data across
 * multiple tables, triggering concurrent compactions, and validating API responses in both active
 * and completed compaction scenarios.</p>
 * 
 * <h2>Testing Mechanism</h2>
 * <ul>
 *   <li><strong>Data Setup:</strong> Creates 5 tables with 100 SSTables each (500 total, 1000 records per SSTable)</li>
 *   <li><strong>Concurrency:</strong> Triggers compactions simultaneously using separate threads</li>
 *   <li><strong>Active Detection:</strong> Polls API immediately with retry logic to catch in-progress compactions</li>
 *   <li><strong>Comprehensive Validation:</strong> Validates all response fields including statistics, rates, 
 *       pending tasks, and detailed active compaction data when available</li>
 *   <li><strong>Dual Success:</strong> Test passes whether active compactions are captured or completed</li>
 * </ul>
 */
class CompactionStatsIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    private static final String COMPACTION_STATS_ROUTE = "/api/v1/cassandra/stats/compaction";
    private static final int MAX_POLL_ATTEMPTS = 10;
    private static final List<QualifiedName> TEST_TABLES = new ArrayList<>();
    private static final int TABLE_COUNT = 5;

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);

        for (int i = 1; i <= TABLE_COUNT; i++)
        {
            TEST_TABLES.add(new QualifiedName(TEST_KEYSPACE, TEST_TABLE_PREFIX + "_compaction_" + i));
        }
        
        // Create test tables for compaction activity
        for (QualifiedName tableName : TEST_TABLES)
        {
            createTestTable(tableName,
                            "CREATE TABLE %s ( \n" +
                            "  id int PRIMARY KEY, \n" +
                            "  data text \n" +
                            ");");
        }
    }

    @Test
    void testCompactionStatsRetrieval()
    {
        logger.info("Starting compaction stats test with {} tables", TEST_TABLES.size());
        
        // Generate SSTables for all test tables
        for (QualifiedName tableName : TEST_TABLES)
        {
            generateSSTables(tableName, 100);
        }
        
        // Create threads to trigger compaction on all tables
        List<Thread> compactionThreads = new ArrayList<>();
        for (QualifiedName tableName : TEST_TABLES)
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
        
        // Basic counters validation
        assertThat(stats.concurrentCompactors()).isGreaterThanOrEqualTo(0);
        assertThat(stats.totalPendingTasks()).isGreaterThanOrEqualTo(0);
        assertThat(stats.completedCompactions()).isGreaterThanOrEqualTo(0);
        assertThat(stats.dataCompacted()).isGreaterThanOrEqualTo(0);
        assertThat(stats.abortedCompactions()).isGreaterThanOrEqualTo(0);
        assertThat(stats.reducedCompactions()).isGreaterThanOrEqualTo(0);
        assertThat(stats.sstablesDroppedFromCompaction()).isGreaterThanOrEqualTo(0);
        
        // Pending tasks validation
        assertThat(stats.pendingTasks()).isNotNull();
        
        // Validate each pending task entry if there are any
        if (!stats.pendingTasks().isEmpty())
        {
            validatePendingTasks(stats);
        }
        
        // Completion rates validation
        assertThat(stats.completedCompactionsRate()).isNotNull();
        
        // Validate mean rate format is X.XX/hour
        assertThat(stats.completedCompactionsRate().meanRate())
            .as("Mean rate should match format 'X.XX/hour'")
            .isNotNull()
            .matches("\\d+\\.\\d{2}/hour");
            
        // Validate fifteen minute rate format is X.XX/minute
        assertThat(stats.completedCompactionsRate().fifteenMinuteRate())
            .as("Fifteen minute rate should match format 'X.XX/minute'")
            .isNotNull()
            .matches("\\d+\\.\\d{2}/minute");

        // Active compactions validation
        assertThat(stats.activeCompactions()).isNotNull();
        assertThat(stats.activeCompactionsCount()).isEqualTo(stats.activeCompactions().size());
        assertThat(stats.activeCompactionsRemainingTime()).isNotNull();
        
        // Detailed active compaction validation when compactions are found
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
        stats.pendingTasks().forEach((keyspace, tableMap) -> {
            assertThat(keyspace)
                .as("Pending task keyspace should not be blank")
                .isNotBlank();
            assertThat(tableMap)
                .as("Pending task table map should not be null")
                .isNotNull();

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

        for (int i = 0; i < stats.activeCompactions().size(); i++)
        {
            ActiveCompactionEntry compaction = stats.activeCompactions().get(i);
            logger.info("Validating active compaction {}: {}", i + 1, compaction.id());

            // Required fields validation
            assertThat(compaction.id())
                .as("Active compaction ID should not be null")
                .isNotNull();

            assertThat(compaction.keyspace())
                .as("Active compaction keyspace should not be null")
                .isNotNull()
                .isNotBlank();

            assertThat(compaction.columnFamily())
                .as("Active compaction column family should not be null")
                .isNotNull()
                .isNotBlank();

            assertThat(compaction.taskType())
                .as("Active compaction task type should not be null")
                .isNotNull()
                .isNotBlank();

            // Byte counters validation
            assertThat(compaction.completedBytes())
                .as("Completed bytes should be non-negative")
                .isGreaterThanOrEqualTo(0);

            assertThat(compaction.totalBytes())
                .as("Total bytes should be greater than 0")
                .isGreaterThan(0);

            assertThat(compaction.completedBytes())
                .as("Completed bytes should not exceed total bytes")
                .isLessThanOrEqualTo(compaction.totalBytes());

            // Percentage validation
            assertThat(compaction.percentCompleted())
                .as("Percent completed should be between 0 and 100")
                .isBetween(0.0, 100.0);

            // Validate percentage consistency with bytes
            double expectedPercentage = (double) compaction.completedBytes() / compaction.totalBytes() * 100;
            assertThat(compaction.percentCompleted())
                .as("Percent completed should be consistent with completed/total bytes ratio")
                .isCloseTo(expectedPercentage, org.assertj.core.data.Percentage.withPercentage(1.0));

            // SSTables validation
            assertThat(compaction.ssTables())
                .as("SSTables list should not be null")
                .isNotNull();

            if (!compaction.ssTables().isEmpty())
            {
                for (String ssTable : compaction.ssTables())
                {
                    assertThat(ssTable)
                        .as("SSTable name should not be null or blank")
                        .isNotNull()
                        .isNotBlank();
                }
            }

            // Target directory validation
            assertThat(compaction.targetDirectory())
                .as("Target directory should not be null")
                .isNotNull()
                .isNotBlank();

            // Keyspace should match our test keyspace
            assertThat(compaction.keyspace())
                .as("Compaction should be on our test keyspace")
                .isEqualTo(TEST_KEYSPACE);

            // Column family should be one of our test tables
            boolean isTestTable = TEST_TABLES.stream()
                .anyMatch(table -> table.table().equals(compaction.columnFamily()));
            assertThat(isTestTable)
                .as("Compaction should be on one of our test tables: " + compaction.columnFamily())
                .isTrue();

            logger.info("Active compaction {} validation successful: {}% complete, {} bytes",
                       compaction.id(), compaction.percentCompleted(), compaction.completedBytes());
        }

        // Validate remaining time format when active compactions exist
        String remainingTime = stats.activeCompactionsRemainingTime();
        assertThat(remainingTime)
            .as("Remaining time should match expected format")
            .matches("(\\d+h\\d{2}m\\d{2}s|n/a)");
    }
}
