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

package org.apache.cassandra.sidecar.service;

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
import org.apache.cassandra.sidecar.common.server.data.CompactionStatsMetrics;
import org.apache.cassandra.sidecar.common.server.data.CompletedCompactionsRateData;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CompactionStatsServiceTest
{
    private CompactionStatsService compactionStatsService;
    private StorageOperations mockStorageOperations;
    private MetricsOperations mockMetricsOperations;
    private CompactionManagerOperations mockCompactionManagerOperations;

    @BeforeEach
    void setUp()
    {
        mockStorageOperations = mock(StorageOperations.class);
        mockMetricsOperations = mock(MetricsOperations.class);
        mockCompactionManagerOperations = mock(CompactionManagerOperations.class);

        compactionStatsService = new CompactionStatsService(mockStorageOperations, mockMetricsOperations, mockCompactionManagerOperations);
    }

    @Test
    void testCompactionStatsHappyPath()
    {
        // Setup mocks
        when(mockStorageOperations.getConcurrentCompactors()).thenReturn(4);
        when(mockStorageOperations.getCompactionThroughputBytesPerSec()).thenReturn(167772L);

        Map<String, Map<String, Integer>> pendingTasks = new HashMap<>();
        Map<String, Integer> tableMap = new HashMap<>();
        tableMap.put("test_table", 2);
        pendingTasks.put("test_keyspace", tableMap);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.PENDING_TASKS_BY_TABLE_NAME)).thenReturn(pendingTasks);

        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.TOTAL_COMPACTIONS_COMPLETED)).thenReturn(100L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.BYTES_COMPACTED)).thenReturn(2048000L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.COMPACTIONS_ABORTED)).thenReturn(5L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.COMPACTIONS_REDUCED)).thenReturn(1L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.SSTABLES_DROPPED_FROM_COMPACTION)).thenReturn(0L);

        CompletedCompactionsRateData rateData = CompletedCompactionsRateData.builder()
            .meanRate(1800.00)
            .fifteenMinuteRate(6.00)
            .build();
        when(mockMetricsOperations.getCompletedCompactionsRate()).thenReturn(rateData);

        List<Map<String, String>> activeCompactions = List.of(
            Map.of("compactionId", "comp-1", "keyspace", "test_keyspace", "columnfamily", "test_table",
                   "taskType", "COMPACTION", "completed", "1024000", "total", "2048000",
                   "sstables", "sstable1.db,sstable2.db", "targetDirectory", "/var/lib/cassandra/data")
        );
        when(mockCompactionManagerOperations.getCompactions()).thenReturn(activeCompactions);

        CompactionStatsData response = compactionStatsService.compactionStats();

        assertThat(response.concurrentCompactors()).isEqualTo(4L);
        assertThat(response.pendingTasks()).hasSize(1);
        assertThat(response.totalPendingTasks()).isEqualTo(2L);
        assertThat(response.completedCompactions()).isEqualTo(100L);
        assertThat(response.dataCompacted()).isEqualTo(2048000L);
        assertThat(response.abortedCompactions()).isEqualTo(5L);
        assertThat(response.reducedCompactions()).isEqualTo(1L);
        assertThat(response.sstablesDroppedFromCompaction()).isEqualTo(0L);
        assertThat(response.completedCompactionsRate().meanRate()).isEqualTo(1800.00);
        assertThat(response.completedCompactionsRate().fifteenMinuteRate()).isEqualTo(6.00);
        assertThat(response.activeCompactions()).hasSize(1);
        assertThat(response.activeCompactionsCount()).isEqualTo(1L);
        assertThat(response.activeCompactionsRemainingTime()).isEqualTo(6L);
    }

    @Test
    void testCompactionStatsEmptyActiveCompactions()
    {
        // Setup mocks with no active compactions
        when(mockStorageOperations.getConcurrentCompactors()).thenReturn(4);
        when(mockStorageOperations.getCompactionThroughputBytesPerSec()).thenReturn(167772L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.PENDING_TASKS_BY_TABLE_NAME)).thenReturn(Collections.emptyMap());
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.TOTAL_COMPACTIONS_COMPLETED)).thenReturn(100L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.BYTES_COMPACTED)).thenReturn(2048000L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.COMPACTIONS_ABORTED)).thenReturn(5L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.COMPACTIONS_REDUCED)).thenReturn(1L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.SSTABLES_DROPPED_FROM_COMPACTION)).thenReturn(0L);

        CompletedCompactionsRateData rateData = CompletedCompactionsRateData.builder()
            .meanRate(1800.00)
            .fifteenMinuteRate(6.00)
            .build();
        when(mockMetricsOperations.getCompletedCompactionsRate()).thenReturn(rateData);
        when(mockCompactionManagerOperations.getCompactions()).thenReturn(Collections.emptyList());

        CompactionStatsData response = compactionStatsService.compactionStats();

        assertThat(response.concurrentCompactors()).isEqualTo(4L);
        assertThat(response.pendingTasks()).isEmpty();
        assertThat(response.totalPendingTasks()).isEqualTo(0L);
        assertThat(response.activeCompactions()).isEmpty();
        assertThat(response.activeCompactionsCount()).isEqualTo(0L);
        assertThat(response.activeCompactionsRemainingTime()).isEqualTo(0L);
    }

    @Test
    void testCompactionStatsWithInvalidPendingTasks()
    {
        // Test with invalid pending tasks data type
        when(mockStorageOperations.getConcurrentCompactors()).thenReturn(4);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.PENDING_TASKS_BY_TABLE_NAME)).thenReturn("invalid_type");

        assertThatThrownBy(() -> compactionStatsService.compactionStats())
            .isInstanceOf(ClassCastException.class)
            .hasMessageContaining("Expected Map for pending tasks but got: String");
    }

    @Test
    void testCompactionStatsWithEmptySSTablesString()
    {
        // Test with empty SSTables string
        when(mockStorageOperations.getConcurrentCompactors()).thenReturn(4);
        when(mockStorageOperations.getCompactionThroughputBytesPerSec()).thenReturn(167772L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.PENDING_TASKS_BY_TABLE_NAME)).thenReturn(Collections.emptyMap());
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.TOTAL_COMPACTIONS_COMPLETED)).thenReturn(100L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.BYTES_COMPACTED)).thenReturn(2048000L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.COMPACTIONS_ABORTED)).thenReturn(5L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.COMPACTIONS_REDUCED)).thenReturn(1L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.SSTABLES_DROPPED_FROM_COMPACTION)).thenReturn(0L);

        CompletedCompactionsRateData rateData = CompletedCompactionsRateData.builder()
            .meanRate(1800.00)
            .fifteenMinuteRate(6.00)
            .build();
        when(mockMetricsOperations.getCompletedCompactionsRate()).thenReturn(rateData);

        List<Map<String, String>> activeCompactions = List.of(
            Map.of("compactionId", "comp-1", "keyspace", "test_keyspace", "columnfamily", "test_table",
                   "taskType", "COMPACTION", "completed", "1024000", "total", "2048000", "sstables", "")
        );
        when(mockCompactionManagerOperations.getCompactions()).thenReturn(activeCompactions);

        CompactionStatsData response = compactionStatsService.compactionStats();

        assertThat(response.activeCompactions()).hasSize(1);
        ActiveCompactionEntryData compaction = response.activeCompactions().get(0);
        assertThat(compaction.ssTables()).isEmpty();
    }

    @Test
    void testCompactionStatsZeroThroughput()
    {
        // Test with zero throughput
        when(mockStorageOperations.getConcurrentCompactors()).thenReturn(4);
        when(mockStorageOperations.getCompactionThroughputBytesPerSec()).thenReturn(0L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.PENDING_TASKS_BY_TABLE_NAME)).thenReturn(Collections.emptyMap());
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.TOTAL_COMPACTIONS_COMPLETED)).thenReturn(100L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.BYTES_COMPACTED)).thenReturn(2048000L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.COMPACTIONS_ABORTED)).thenReturn(5L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.COMPACTIONS_REDUCED)).thenReturn(1L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.SSTABLES_DROPPED_FROM_COMPACTION)).thenReturn(0L);

        CompletedCompactionsRateData rateData = CompletedCompactionsRateData.builder()
            .meanRate(1800.00)
            .fifteenMinuteRate(6.00)
            .build();
        when(mockMetricsOperations.getCompletedCompactionsRate()).thenReturn(rateData);

        List<Map<String, String>> activeCompactions = List.of(
            Map.of("compactionId", "comp-1", "keyspace", "test_keyspace", "columnfamily", "test_table",
                   "taskType", "COMPACTION", "completed", "1024000", "total", "2048000")
        );
        when(mockCompactionManagerOperations.getCompactions()).thenReturn(activeCompactions);

        CompactionStatsData response = compactionStatsService.compactionStats();

        assertThat(response.activeCompactionsRemainingTime()).isEqualTo(0L);
    }

    @Test
    void testCompactionStatsInvalidNumberFormat()
    {
        // Test with invalid number format for compaction data
        when(mockStorageOperations.getConcurrentCompactors()).thenReturn(4);
        when(mockStorageOperations.getCompactionThroughputBytesPerSec()).thenReturn(167772L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.PENDING_TASKS_BY_TABLE_NAME)).thenReturn(Collections.emptyMap());
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.TOTAL_COMPACTIONS_COMPLETED)).thenReturn(100L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.BYTES_COMPACTED)).thenReturn(2048000L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.COMPACTIONS_ABORTED)).thenReturn(5L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.COMPACTIONS_REDUCED)).thenReturn(1L);
        when(mockMetricsOperations.getCompactionMetric(CompactionStatsMetrics.SSTABLES_DROPPED_FROM_COMPACTION)).thenReturn(0L);

        CompletedCompactionsRateData rateData = CompletedCompactionsRateData.builder()
            .meanRate(1800.00)
            .fifteenMinuteRate(6.00)
            .build();
        when(mockMetricsOperations.getCompletedCompactionsRate()).thenReturn(rateData);

        List<Map<String, String>> activeCompactions = List.of(
            Map.of("compactionId", "comp-1", "keyspace", "test_keyspace", "columnfamily", "test_table",
                   "taskType", "COMPACTION", "completed", "invalid_number", "total", "2048000")
        );
        when(mockCompactionManagerOperations.getCompactions()).thenReturn(activeCompactions);

        assertThatThrownBy(() -> compactionStatsService.compactionStats())
            .isInstanceOf(NumberFormatException.class)
            .hasMessageContaining("For input string: \"invalid_number\"");
    }
}
