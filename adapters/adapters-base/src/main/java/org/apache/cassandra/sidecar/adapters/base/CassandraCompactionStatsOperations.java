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

package org.apache.cassandra.sidecar.adapters.base;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.sidecar.adapters.base.data.CompactionStatsMetrics;
import org.apache.cassandra.sidecar.adapters.base.utils.DataTypeUtils;
import org.apache.cassandra.sidecar.common.server.CompactionManagerOperations;
import org.apache.cassandra.sidecar.common.server.CompactionStatsOperations;
import org.apache.cassandra.sidecar.common.server.MetricsOperations;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.data.ActiveCompactionEntryData;
import org.apache.cassandra.sidecar.common.server.data.CompactionStatsData;
import org.apache.cassandra.sidecar.common.server.data.CompletedCompactionsRateData;

import static org.apache.cassandra.sidecar.adapters.base.data.CompactionStatsMetrics.BYTES_COMPACTED;
import static org.apache.cassandra.sidecar.adapters.base.data.CompactionStatsMetrics.COMPACTIONS_ABORTED;
import static org.apache.cassandra.sidecar.adapters.base.data.CompactionStatsMetrics.COMPACTIONS_REDUCED;
import static org.apache.cassandra.sidecar.adapters.base.data.CompactionStatsMetrics.PENDING_TASKS_BY_TABLE_NAME;
import static org.apache.cassandra.sidecar.adapters.base.data.CompactionStatsMetrics.SSTABLES_DROPPED_FROM_COMPACTION;
import static org.apache.cassandra.sidecar.adapters.base.data.CompactionStatsMetrics.TOTAL_COMPACTIONS_COMPLETED;
import static org.apache.cassandra.sidecar.adapters.base.utils.DataTypeUtils.safeCast;
import static org.apache.cassandra.sidecar.adapters.base.utils.DataTypeUtils.safeParseLong;

/**
 * Service responsible for fetching compaction stats from various sources including
 * CassandraStorageOperations, CassandraMetricsOperations, and CompactionManagerOperations.
 */
public class CassandraCompactionStatsOperations implements CompactionStatsOperations
{
    // Default values
    // Default string value when actual value is unavailable
    public static final String DEFAULT_STRING_VALUE = "";
    // Default numeric value when actual value is unavailable
    public static final String DEFAULT_NUMBER_VALUE = "-1";

    // Constants for compaction info map keys
    // Keyspace name being compacted
    public static final String KEYSPACE = "keyspace";
    // Column family (table) name being compacted
    public static final String COLUMNFAMILY = "columnfamily";
    // Number of bytes already processed in the compaction
    public static final String COMPLETED = "completed";
    // Total number of bytes to be processed in the compaction
    public static final String TOTAL = "total";
    // Type of compaction task (e.g., COMPACTION, VALIDATION, etc.)
    public static final String TASK_TYPE = "taskType";
    // Unique compaction identifier
    public static final String COMPACTION_ID = "compactionId";
    // Comma-separated list of SSTable names involved in the compaction
    public static final String SSTABLES = "sstables";
    // Directory where compaction output will be written
    public static final String TARGET_DIRECTORY = "targetDirectory";

    private final StorageOperations storageOperations;
    private final MetricsOperations metricsOperations;
    private final CompactionManagerOperations compactionManagerOperations;

    public CassandraCompactionStatsOperations(StorageOperations storageOperations,
                                              MetricsOperations metricsOperations,
                                              CompactionManagerOperations compactionManagerOperations)
    {
        this.storageOperations = storageOperations;
        this.metricsOperations = metricsOperations;
        this.compactionManagerOperations = compactionManagerOperations;
    }

    @Override
    public CompactionStatsData compactionStats()
    {
        // Get concurrent compactors from StorageOperations
        long concurrentCompactors = storageOperations.getConcurrentCompactors();

        // Get pending tasks grouped by keyspace and table
        Map<String, Map<String, Integer>> pendingTasks = getPendingCompactionTasksByTable();
        long totalPendingTasks = pendingTasks.values().stream()
                                             .mapToLong(tableMap -> tableMap.values().stream().mapToInt(Integer::intValue).sum())
                                             .sum();

        // Get compaction metrics from JMX counters and meters
        long completedCompactionsCount = getCompactionMetric(TOTAL_COMPACTIONS_COMPLETED);
        long dataCompactedBytes = getCompactionMetric(BYTES_COMPACTED);
        long abortedCompactionsCount = getCompactionMetric(COMPACTIONS_ABORTED);
        long reducedCompactionsCount = getCompactionMetric(COMPACTIONS_REDUCED);
        long sstablesDroppedFromCompactionCount = getCompactionMetric(SSTABLES_DROPPED_FROM_COMPACTION);

        // Get completed compactions rate with proper time conversions
        CompletedCompactionsRateData completedCompactionsRate = metricsOperations.getCompletedCompactionsRate();

        // Get active compactions with all required fields
        List<ActiveCompactionEntryData> activeCompactions = getActiveCompactions(compactionManagerOperations.getCompactions());
        long activeCompactionsCount = activeCompactions.size();

        // Calculate remaining time in seconds based on throughput and remaining bytes
        long activeCompactionsRemainingTime = calculateRemainingTimeSeconds(activeCompactions);

        return CompactionStatsData.builder()
                                  .concurrentCompactors(concurrentCompactors)
                                  .pendingTasks(pendingTasks)
                                  .totalPendingTasks(totalPendingTasks)
                                  .completedCompactions(completedCompactionsCount)
                                  .dataCompacted(dataCompactedBytes)
                                  .abortedCompactions(abortedCompactionsCount)
                                  .reducedCompactions(reducedCompactionsCount)
                                  .sstablesDroppedFromCompaction(sstablesDroppedFromCompactionCount)
                                  .completedCompactionsRate(completedCompactionsRate)
                                  .activeCompactions(activeCompactions)
                                  .activeCompactionsCount(activeCompactionsCount)
                                  .activeCompactionsRemainingTime(activeCompactionsRemainingTime)
                                  .build();
    }

    private long getCompactionMetric(CompactionStatsMetrics metric)
    {
        return DataTypeUtils.getValueAsLong(metricsOperations.getCompactionMetric(metric.metricName(), metric.type));
    }

    private Map<String, Map<String, Integer>> getPendingCompactionTasksByTable()
    {
        // Get pending tasks by table name from Gauge metric
        Object value = metricsOperations.getCompactionMetric(PENDING_TASKS_BY_TABLE_NAME.metricName(), PENDING_TASKS_BY_TABLE_NAME.type);
        return parsePendingTasksMap(value);
    }

    private Map<String, Map<String, Integer>> parsePendingTasksMap(Object value)
    {
        Map<?, ?> rawMap = safeCast(value, Map.class, "pending tasks");
        Map<String, Map<String, Integer>> result = new HashMap<>();

        for (Map.Entry<?, ?> entry : rawMap.entrySet())
        {
            String keyspace = safeCast(entry.getKey(), String.class, "keyspace name");
            Map<?, ?> rawTableMap = safeCast(entry.getValue(), Map.class, "table data");
            Map<String, Integer> tableMap = new HashMap<>();

            for (Map.Entry<?, ?> tableEntry : rawTableMap.entrySet())
            {
                String tableName = safeCast(tableEntry.getKey(), String.class, "table name");
                Number taskCount = safeCast(tableEntry.getValue(), Number.class, "task count");
                tableMap.put(tableName, taskCount.intValue());
            }

            result.put(keyspace, tableMap);
        }

        return result;
    }

    private List<ActiveCompactionEntryData> getActiveCompactions(List<Map<String, String>> compactions)
    {
        return compactions.stream().map(compactionInfo -> {
            // Extract fields according to specification
            String id = compactionInfo.getOrDefault(COMPACTION_ID, DEFAULT_STRING_VALUE);
            String keyspace = compactionInfo.getOrDefault(KEYSPACE, DEFAULT_STRING_VALUE);
            String table = compactionInfo.getOrDefault(COLUMNFAMILY, DEFAULT_STRING_VALUE);
            String taskType = compactionInfo.getOrDefault(TASK_TYPE, DEFAULT_STRING_VALUE);

            // Parse byte values
            long completedBytes = safeParseLong(compactionInfo.getOrDefault(COMPLETED, DEFAULT_NUMBER_VALUE), "completed bytes");
            long totalBytes = safeParseLong(compactionInfo.getOrDefault(TOTAL, DEFAULT_NUMBER_VALUE), "total bytes");

            // Calculate percentage completed
            double percentCompleted = 0.0;
            if (totalBytes != 0 && totalBytes != -1)
            {
                percentCompleted = (double) completedBytes / totalBytes * 100.0;
            }

            // Parse SSTables list
            String ssTablesStr = compactionInfo.getOrDefault(SSTABLES, DEFAULT_STRING_VALUE);
            List<String> ssTables = ssTablesStr.isEmpty() ? List.of() : List.of(ssTablesStr.split(","));

            String targetDirectory = compactionInfo.getOrDefault(TARGET_DIRECTORY, DEFAULT_STRING_VALUE);

            return ActiveCompactionEntryData.builder()
                                            .id(id)
                                            .keyspace(keyspace)
                                            .table(table)
                                            .taskType(taskType)
                                            .completedBytes(completedBytes)
                                            .totalBytes(totalBytes)
                                            .percentCompleted(percentCompleted)
                                            .sstables(ssTables)
                                            .targetDirectory(targetDirectory)
                                            .build();
        }).collect(Collectors.toList());
    }

    private long calculateRemainingTimeSeconds(List<ActiveCompactionEntryData> activeCompactions)
    {
        if (activeCompactions.isEmpty())
        {
            return 0;
        }

        // Calculate total remaining bytes across all active compactions
        long totalRemainingBytes = activeCompactions.stream()
                                                    .filter(compaction -> compaction.totalBytes >= 0 && compaction.completedBytes >= 0)
                                                    .mapToLong(compaction -> Math.max(0, compaction.totalBytes - compaction.completedBytes))
                                                    .sum();

        long throughputBytesPerSec = storageOperations.getCompactionThroughputBytesPerSec();
        if (totalRemainingBytes < 0 || throughputBytesPerSec <= 0)
        {
            return 0;
        }

        // Calculate time in seconds (throughput is already in bytes/sec)
        double remainingTimeInSecs = (double) totalRemainingBytes / throughputBytesPerSec;

        return Math.round(remainingTimeInSecs);
    }
}
