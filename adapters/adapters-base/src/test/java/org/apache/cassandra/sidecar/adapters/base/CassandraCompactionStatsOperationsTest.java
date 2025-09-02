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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.server.CompactionManagerOperations;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CassandraCompactionStatsOperationsTest
{
    private CassandraCompactionStatsOperations compactionStatsOperations;
    private StorageOperations mockStorageOperations;
    private MetricsOperations mockMetricsOperations;
    private CompactionManagerOperations mockCompactionManagerOperations;

    @BeforeEach
    void setUp()
    {
        mockStorageOperations = mock(StorageOperations.class);
        mockMetricsOperations = mock(MetricsOperations.class);
        mockCompactionManagerOperations = mock(CompactionManagerOperations.class);
        compactionStatsOperations = new CassandraCompactionStatsOperations(mockStorageOperations,
                                                                           mockMetricsOperations,
                                                                           mockCompactionManagerOperations);
    }

    @Test
    void testCompactionStatsHappyPath()
    {
        // Mock storage operations
        when(mockStorageOperations.getConcurrentCompactors()).thenReturn(4);

        // Mock metrics operations
        Map<String, Map<String, Integer>> pendingTasksMap = new HashMap<>();
        Map<String, Integer> tableMap = new HashMap<>();
        tableMap.put("test_table", 2);
        pendingTasksMap.put("test_ks", tableMap);

        when(mockMetricsOperations.getCompactionMetric(PENDING_TASKS_BY_TABLE_NAME.metricName(), PENDING_TASKS_BY_TABLE_NAME.type))
        .thenReturn(pendingTasksMap);
        when(mockMetricsOperations.getCompactionMetric(TOTAL_COMPACTIONS_COMPLETED.metricName(), TOTAL_COMPACTIONS_COMPLETED.type))
        .thenReturn(100L);
        when(mockMetricsOperations.getCompactionMetric(BYTES_COMPACTED.metricName(), BYTES_COMPACTED.type))
        .thenReturn(2048000L);
        when(mockMetricsOperations.getCompactionMetric(COMPACTIONS_ABORTED.metricName(), COMPACTIONS_ABORTED.type))
        .thenReturn(5L);
        when(mockMetricsOperations.getCompactionMetric(COMPACTIONS_REDUCED.metricName(), COMPACTIONS_REDUCED.type))
        .thenReturn(1L);
        when(mockMetricsOperations.getCompactionMetric(SSTABLES_DROPPED_FROM_COMPACTION.metricName(), SSTABLES_DROPPED_FROM_COMPACTION.type))
        .thenReturn(0L);

        CompletedCompactionsRateData rateData = CompletedCompactionsRateData.builder()
                                                                            .meanRate(1800.00)
                                                                            .fifteenMinuteRate(6.00)
                                                                            .build();
        when(mockMetricsOperations.getCompletedCompactionsRate()).thenReturn(rateData);

        // Mock compaction manager operations
        List<Map<String, String>> activeCompactions = List.of(
        Map.of(
        "compactionId", "comp-1",
        "keyspace", "test_keyspace",
        "columnfamily", "test_table",
        "taskType", "COMPACTION",
        "completed", "1024000",
        "total", "2048000",
        "sstables", "",
        "targetDirectory", "/var/lib/cassandra/data"
        )
        );
        when(mockCompactionManagerOperations.getCompactions()).thenReturn(activeCompactions);
        when(mockStorageOperations.getCompactionThroughputBytesPerSec()).thenReturn(170666L);

        CompactionStatsData response = compactionStatsOperations.compactionStats();

        assertThat(response.concurrentCompactors).isEqualTo(4L);
        assertThat(response.pendingTasks).hasSize(1);
        assertThat(response.totalPendingTasks).isEqualTo(2L);
        assertThat(response.completedCompactions).isEqualTo(100L);
        assertThat(response.dataCompacted).isEqualTo(2048000L);
        assertThat(response.abortedCompactions).isEqualTo(5L);
        assertThat(response.reducedCompactions).isEqualTo(1L);
        assertThat(response.sstablesDroppedFromCompaction).isEqualTo(0L);
        assertThat(response.completedCompactionsRate.meanRate).isEqualTo(1800.00);
        assertThat(response.completedCompactionsRate.fifteenMinuteRate).isEqualTo(6.00);
        assertThat(response.activeCompactions).hasSize(1);
        assertThat(response.activeCompactionsCount).isEqualTo(1L);
        assertThat(response.activeCompactionsRemainingTime).isEqualTo(6L);
    }

    @Test
    void testCompactionStatsEmptyActiveCompactions()
    {
        // Mock storage operations
        when(mockStorageOperations.getConcurrentCompactors()).thenReturn(4);
        when(mockStorageOperations.getCompactionThroughputBytesPerSec()).thenReturn(0L);

        // Mock metrics operations with empty data
        when(mockMetricsOperations.getCompactionMetric(PENDING_TASKS_BY_TABLE_NAME.metricName(), PENDING_TASKS_BY_TABLE_NAME.type))
        .thenReturn(Collections.emptyMap());
        when(mockMetricsOperations.getCompactionMetric(TOTAL_COMPACTIONS_COMPLETED.metricName(), TOTAL_COMPACTIONS_COMPLETED.type))
        .thenReturn(0L);
        when(mockMetricsOperations.getCompactionMetric(BYTES_COMPACTED.metricName(), BYTES_COMPACTED.type))
        .thenReturn(0L);
        when(mockMetricsOperations.getCompactionMetric(COMPACTIONS_ABORTED.metricName(), COMPACTIONS_ABORTED.type))
        .thenReturn(0L);
        when(mockMetricsOperations.getCompactionMetric(COMPACTIONS_REDUCED.metricName(), COMPACTIONS_REDUCED.type))
        .thenReturn(0L);
        when(mockMetricsOperations.getCompactionMetric(SSTABLES_DROPPED_FROM_COMPACTION.metricName(), SSTABLES_DROPPED_FROM_COMPACTION.type))
        .thenReturn(0L);

        CompletedCompactionsRateData rateData = CompletedCompactionsRateData.builder()
                                                                            .meanRate(0.0)
                                                                            .fifteenMinuteRate(0.0)
                                                                            .build();
        when(mockMetricsOperations.getCompletedCompactionsRate()).thenReturn(rateData);

        // Mock empty active compactions
        when(mockCompactionManagerOperations.getCompactions()).thenReturn(Collections.emptyList());

        CompactionStatsData response = compactionStatsOperations.compactionStats();

        assertThat(response.concurrentCompactors).isEqualTo(4L);
        assertThat(response.pendingTasks).isEmpty();
        assertThat(response.totalPendingTasks).isEqualTo(0L);
        assertThat(response.activeCompactions).isEmpty();
        assertThat(response.activeCompactionsCount).isEqualTo(0L);
        assertThat(response.activeCompactionsRemainingTime).isEqualTo(0L);
    }

    @Test
    void testCompactionStatsWithEmptySSTablesList()
    {
        // Test that empty sstables string is handled correctly
        when(mockStorageOperations.getConcurrentCompactors()).thenReturn(4);
        when(mockMetricsOperations.getCompactionMetric(PENDING_TASKS_BY_TABLE_NAME.metricName(), PENDING_TASKS_BY_TABLE_NAME.type))
        .thenReturn(Collections.emptyMap());
        when(mockMetricsOperations.getCompactionMetric(TOTAL_COMPACTIONS_COMPLETED.metricName(), TOTAL_COMPACTIONS_COMPLETED.type))
        .thenReturn(0L);
        when(mockMetricsOperations.getCompactionMetric(BYTES_COMPACTED.metricName(), BYTES_COMPACTED.type))
        .thenReturn(0L);
        when(mockMetricsOperations.getCompactionMetric(COMPACTIONS_ABORTED.metricName(), COMPACTIONS_ABORTED.type))
        .thenReturn(0L);
        when(mockMetricsOperations.getCompactionMetric(COMPACTIONS_REDUCED.metricName(), COMPACTIONS_REDUCED.type))
        .thenReturn(0L);
        when(mockMetricsOperations.getCompactionMetric(SSTABLES_DROPPED_FROM_COMPACTION.metricName(), SSTABLES_DROPPED_FROM_COMPACTION.type))
        .thenReturn(0L);

        CompletedCompactionsRateData rateData = CompletedCompactionsRateData.builder()
                                                                            .meanRate(0.0)
                                                                            .fifteenMinuteRate(0.0)
                                                                            .build();
        when(mockMetricsOperations.getCompletedCompactionsRate()).thenReturn(rateData);

        List<Map<String, String>> activeCompactions = List.of(
        Map.of(
        "compactionId", "comp-1",
        "keyspace", "test_keyspace",
        "columnfamily", "test_table",
        "taskType", "COMPACTION",
        "completed", "1024000",
        "total", "2048000",
        "sstables", "",
        "targetDirectory", "/var/lib/cassandra/data"
        )
        );
        when(mockCompactionManagerOperations.getCompactions()).thenReturn(activeCompactions);

        CompactionStatsData response = compactionStatsOperations.compactionStats();

        assertThat(response.activeCompactions).hasSize(1);
        ActiveCompactionEntryData compaction = response.activeCompactions.get(0);
        assertThat(compaction.sstables).isEmpty();
    }

    @Test
    void testCompactionStatsZeroThroughput()
    {
        // Test with zero throughput
        when(mockStorageOperations.getConcurrentCompactors()).thenReturn(4);
        when(mockStorageOperations.getCompactionThroughputBytesPerSec()).thenReturn(0L);
        when(mockMetricsOperations.getCompactionMetric(PENDING_TASKS_BY_TABLE_NAME.metricName(), PENDING_TASKS_BY_TABLE_NAME.type))
        .thenReturn(Collections.emptyMap());
        when(mockMetricsOperations.getCompactionMetric(TOTAL_COMPACTIONS_COMPLETED.metricName(), TOTAL_COMPACTIONS_COMPLETED.type))
        .thenReturn(0L);
        when(mockMetricsOperations.getCompactionMetric(BYTES_COMPACTED.metricName(), BYTES_COMPACTED.type))
        .thenReturn(0L);
        when(mockMetricsOperations.getCompactionMetric(COMPACTIONS_ABORTED.metricName(), COMPACTIONS_ABORTED.type))
        .thenReturn(0L);
        when(mockMetricsOperations.getCompactionMetric(COMPACTIONS_REDUCED.metricName(), COMPACTIONS_REDUCED.type))
        .thenReturn(0L);
        when(mockMetricsOperations.getCompactionMetric(SSTABLES_DROPPED_FROM_COMPACTION.metricName(), SSTABLES_DROPPED_FROM_COMPACTION.type))
        .thenReturn(0L);

        CompletedCompactionsRateData rateData = CompletedCompactionsRateData.builder()
                                                                            .meanRate(0.0)
                                                                            .fifteenMinuteRate(0.0)
                                                                            .build();
        when(mockMetricsOperations.getCompletedCompactionsRate()).thenReturn(rateData);

        List<Map<String, String>> activeCompactions = List.of(
        Map.of(
        "compactionId", "comp-1",
        "keyspace", "test_keyspace",
        "columnfamily", "test_table",
        "taskType", "COMPACTION",
        "completed", "1024000",
        "total", "2048000",
        "sstables", "",
        "targetDirectory", "/var/lib/cassandra/data"
        )
        );
        when(mockCompactionManagerOperations.getCompactions()).thenReturn(activeCompactions);

        CompactionStatsData response = compactionStatsOperations.compactionStats();

        // With zero throughput, remaining time should be 0
        assertThat(response.activeCompactionsRemainingTime).isEqualTo(0L);
    }
}
