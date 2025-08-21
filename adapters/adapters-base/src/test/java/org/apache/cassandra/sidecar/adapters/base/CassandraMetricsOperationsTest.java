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

import org.apache.cassandra.sidecar.adapters.base.jmx.CompactionManagerJmxOperations;
import org.apache.cassandra.sidecar.adapters.base.jmx.CounterMetricsJmxOperations;
import org.apache.cassandra.sidecar.adapters.base.jmx.GaugeMetricsJmxOperations;
import org.apache.cassandra.sidecar.adapters.base.jmx.MeterMetricsJmxOperations;
import org.apache.cassandra.sidecar.adapters.base.jmx.StorageJmxOperations;
import org.apache.cassandra.sidecar.common.server.ICassandraAdapter;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.data.ActiveCompactionEntryData;
import org.apache.cassandra.sidecar.common.server.data.CompactionStatsData;
import org.apache.cassandra.sidecar.db.schema.TableSchemaFetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CassandraMetricsOperationsTest
{
    private CassandraMetricsOperations metricsOperations;
    private JmxClient mockJmxClient;
    private TableSchemaFetcher mockTableSchemaFetcher;
    private ICassandraAdapter mockCassandraAdapter;
    private CompactionManagerJmxOperations mockCompactionManager;
    private StorageJmxOperations mockStorageService;
    private GaugeMetricsJmxOperations mockGaugeMetrics;
    private CounterMetricsJmxOperations mockCounterMetrics;
    private MeterMetricsJmxOperations mockMeterMetrics;

    @BeforeEach
    void setUp()
    {
        mockJmxClient = mock(JmxClient.class);
        mockTableSchemaFetcher = mock(TableSchemaFetcher.class);
        mockCassandraAdapter = mock(ICassandraAdapter.class);
        mockCompactionManager = mock(CompactionManagerJmxOperations.class);
        mockStorageService = mock(StorageJmxOperations.class);
        mockGaugeMetrics = mock(GaugeMetricsJmxOperations.class);
        mockCounterMetrics = mock(CounterMetricsJmxOperations.class);
        mockMeterMetrics = mock(MeterMetricsJmxOperations.class);

        metricsOperations = new CassandraMetricsOperations(mockJmxClient, mockTableSchemaFetcher, mockCassandraAdapter);
    }

    @Test
    void testCompactionStatsHappyPath()
    {
        // Happy path with all data present
        when(mockJmxClient.proxy(CompactionManagerJmxOperations.class, "org.apache.cassandra.db:type=CompactionManager")).thenReturn(mockCompactionManager);
        when(mockJmxClient.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService")).thenReturn(mockStorageService);
        when(mockJmxClient.proxy(eq(GaugeMetricsJmxOperations.class), anyString())).thenReturn(mockGaugeMetrics);
        when(mockJmxClient.proxy(eq(CounterMetricsJmxOperations.class), anyString())).thenReturn(mockCounterMetrics);
        when(mockJmxClient.proxy(eq(MeterMetricsJmxOperations.class), anyString())).thenReturn(mockMeterMetrics);

        when(mockStorageService.getConcurrentCompactors()).thenReturn(4);
        when(mockStorageService.getCompactionThroughtputBytesPerSec()).thenReturn(167772L);

        Map<String, Map<String, Integer>> pendingTasks = new HashMap<>();
        Map<String, Integer> tableMap = new HashMap<>();
        tableMap.put("test_table", 2);
        pendingTasks.put("test_keyspace", tableMap);
        when(mockGaugeMetrics.getValue()).thenReturn(pendingTasks);

        when(mockCounterMetrics.getCount()).thenReturn(100L, 2048000L, 5L, 1L, 0L);

        when(mockMeterMetrics.getMeanRate()).thenReturn(0.5);
        when(mockMeterMetrics.getFifteenMinuteRate()).thenReturn(0.1);

        List<Map<String, String>> activeCompactions = List.of(
            Map.of("compactionId", "comp-1", "keyspace", "test_keyspace", "columnfamily", "test_table",
                   "taskType", "COMPACTION", "completed", "1024000", "total", "2048000",
                   "sstables", "sstable1.db,sstable2.db", "targetDirectory", "/var/lib/cassandra/data")
        );
        when(mockCompactionManager.getCompactions()).thenReturn(activeCompactions);

        CompactionStatsData response = metricsOperations.compactionStats();

        assertThat(response.concurrentCompactors()).isEqualTo(4L);
        assertThat(response.pendingTasks()).hasSize(1);
        assertThat(response.totalPendingTasks()).isEqualTo(2L);
        assertThat(response.completedCompactions()).isEqualTo(100L);
        assertThat(response.dataCompacted()).isEqualTo(2048000L);
        assertThat(response.abortedCompactions()).isEqualTo(5L);
        assertThat(response.reducedCompactions()).isEqualTo(1L);
        assertThat(response.sstablesDroppedFromCompaction()).isEqualTo(0L);
        assertThat(response.completedCompactionsRate().meanRate()).isEqualTo("1800.00/hour");
        assertThat(response.completedCompactionsRate().fifteenMinuteRate()).isEqualTo("6.00/minute");
        assertThat(response.activeCompactions()).hasSize(1);
        assertThat(response.activeCompactionsCount()).isEqualTo(1L);
        assertThat(response.activeCompactionsRemainingTime()).isEqualTo(6L);
    }

    @Test
    void testCompactionStatsStorageServiceException()
    {
        // storageService.getConcurrentCompactors() throws exception
        when(mockJmxClient.proxy(CompactionManagerJmxOperations.class, "org.apache.cassandra.db:type=CompactionManager")).thenReturn(mockCompactionManager);
        when(mockJmxClient.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService")).thenReturn(mockStorageService);
        
        when(mockStorageService.getConcurrentCompactors()).thenThrow(new RuntimeException("JMX connection failed"));

        assertThatThrownBy(() -> metricsOperations.compactionStats())
            .isInstanceOf(RuntimeException.class)
            .hasMessage("JMX connection failed");
    }

    @Test
    void testCompactionStatsParsePendingTasksMapFailure()
    {
        // Parsing and casting failures in parsePendingTasksMap
        when(mockJmxClient.proxy(CompactionManagerJmxOperations.class, "org.apache.cassandra.db:type=CompactionManager")).thenReturn(mockCompactionManager);
        when(mockJmxClient.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService")).thenReturn(mockStorageService);
        when(mockJmxClient.proxy(eq(GaugeMetricsJmxOperations.class), anyString())).thenReturn(mockGaugeMetrics);

        when(mockStorageService.getConcurrentCompactors()).thenReturn(4);
        when(mockGaugeMetrics.getValue()).thenReturn("invalid_type");

        assertThatThrownBy(() -> metricsOperations.compactionStats())
            .isInstanceOf(ClassCastException.class)
            .hasMessageContaining("Expected Map for pending tasks but got: String");
    }

    @Test
    void testCompactionStatsEmptyPendingTasks()
    {
        // Empty pendingTasks
        when(mockJmxClient.proxy(CompactionManagerJmxOperations.class, "org.apache.cassandra.db:type=CompactionManager")).thenReturn(mockCompactionManager);
        when(mockJmxClient.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService")).thenReturn(mockStorageService);
        when(mockJmxClient.proxy(eq(GaugeMetricsJmxOperations.class), anyString())).thenReturn(mockGaugeMetrics);
        when(mockJmxClient.proxy(eq(CounterMetricsJmxOperations.class), anyString())).thenReturn(mockCounterMetrics);
        when(mockJmxClient.proxy(eq(MeterMetricsJmxOperations.class), anyString())).thenReturn(mockMeterMetrics);

        when(mockStorageService.getConcurrentCompactors()).thenReturn(4);
        when(mockGaugeMetrics.getValue()).thenReturn(Collections.emptyMap());
        when(mockCounterMetrics.getCount()).thenReturn(0L);
        when(mockMeterMetrics.getMeanRate()).thenReturn(0.0);
        when(mockMeterMetrics.getFifteenMinuteRate()).thenReturn(0.0);
        when(mockCompactionManager.getCompactions()).thenReturn(Collections.emptyList());

        CompactionStatsData response = metricsOperations.compactionStats();

        assertThat(response.pendingTasks()).isEmpty();
        assertThat(response.totalPendingTasks()).isEqualTo(0L);
        assertThat(response.activeCompactions()).isEmpty();
        assertThat(response.activeCompactionsCount()).isEqualTo(0L);
        assertThat(response.activeCompactionsRemainingTime()).isEqualTo(0L);
    }

    @Test
    void testCompactionStatsGetCompletedCompactionsRateException()
    {
        // Exception in getCompletedCompactionsRate JMX
        when(mockJmxClient.proxy(CompactionManagerJmxOperations.class, "org.apache.cassandra.db:type=CompactionManager")).thenReturn(mockCompactionManager);
        when(mockJmxClient.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService")).thenReturn(mockStorageService);
        when(mockJmxClient.proxy(eq(GaugeMetricsJmxOperations.class), anyString())).thenReturn(mockGaugeMetrics);
        when(mockJmxClient.proxy(eq(CounterMetricsJmxOperations.class), anyString())).thenReturn(mockCounterMetrics);
        when(mockJmxClient.proxy(eq(MeterMetricsJmxOperations.class), anyString())).thenReturn(mockMeterMetrics);

        when(mockStorageService.getConcurrentCompactors()).thenReturn(4);
        when(mockGaugeMetrics.getValue()).thenReturn(Collections.emptyMap());
        when(mockCounterMetrics.getCount()).thenReturn(0L);
        when(mockMeterMetrics.getMeanRate()).thenThrow(new RuntimeException("Meter metric unavailable"));

        assertThatThrownBy(() -> metricsOperations.compactionStats())
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Meter metric unavailable");
    }

    @Test
    void testCompactionStatsGetActiveCompactionsEmptyMap()
    {
        // getActiveCompactions returns empty map
        when(mockJmxClient.proxy(CompactionManagerJmxOperations.class, "org.apache.cassandra.db:type=CompactionManager")).thenReturn(mockCompactionManager);
        when(mockJmxClient.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService")).thenReturn(mockStorageService);
        when(mockJmxClient.proxy(eq(GaugeMetricsJmxOperations.class), anyString())).thenReturn(mockGaugeMetrics);
        when(mockJmxClient.proxy(eq(CounterMetricsJmxOperations.class), anyString())).thenReturn(mockCounterMetrics);
        when(mockJmxClient.proxy(eq(MeterMetricsJmxOperations.class), anyString())).thenReturn(mockMeterMetrics);

        when(mockStorageService.getConcurrentCompactors()).thenReturn(4);
        when(mockGaugeMetrics.getValue()).thenReturn(Collections.emptyMap());
        when(mockCounterMetrics.getCount()).thenReturn(0L);
        when(mockMeterMetrics.getMeanRate()).thenReturn(0.0);
        when(mockMeterMetrics.getFifteenMinuteRate()).thenReturn(0.0);
        when(mockCompactionManager.getCompactions()).thenReturn(Collections.emptyList());

        CompactionStatsData response = metricsOperations.compactionStats();

        assertThat(response.activeCompactions()).isEmpty();
        assertThat(response.activeCompactionsCount()).isEqualTo(0L);
        assertThat(response.activeCompactionsRemainingTime()).isEqualTo(0L);
    }

    @Test
    void testCompactionStatsSafeParseLongFailure()
    {
        // safeParseLong failure due to invalid value in compactionInfo
        when(mockJmxClient.proxy(CompactionManagerJmxOperations.class, "org.apache.cassandra.db:type=CompactionManager")).thenReturn(mockCompactionManager);
        when(mockJmxClient.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService")).thenReturn(mockStorageService);
        when(mockJmxClient.proxy(eq(GaugeMetricsJmxOperations.class), anyString())).thenReturn(mockGaugeMetrics);
        when(mockJmxClient.proxy(eq(CounterMetricsJmxOperations.class), anyString())).thenReturn(mockCounterMetrics);
        when(mockJmxClient.proxy(eq(MeterMetricsJmxOperations.class), anyString())).thenReturn(mockMeterMetrics);

        when(mockStorageService.getConcurrentCompactors()).thenReturn(4);
        when(mockGaugeMetrics.getValue()).thenReturn(Collections.emptyMap());
        when(mockCounterMetrics.getCount()).thenReturn(0L);
        when(mockMeterMetrics.getMeanRate()).thenReturn(0.0);
        when(mockMeterMetrics.getFifteenMinuteRate()).thenReturn(0.0);

        List<Map<String, String>> activeCompactions = List.of(
            Map.of("compactionId", "comp-1", "keyspace", "test_keyspace", "columnfamily", "test_table",
                   "taskType", "COMPACTION", "completed", "invalid_number", "total", "2048000")
        );
        when(mockCompactionManager.getCompactions()).thenReturn(activeCompactions);

        assertThatThrownBy(() -> metricsOperations.compactionStats())
            .isInstanceOf(NumberFormatException.class)
            .hasMessageContaining("completed bytes");
    }

    @Test
    void testCompactionStatsTotalBytesMinusOne()
    {
        // totalBytes is -1
        when(mockJmxClient.proxy(CompactionManagerJmxOperations.class, "org.apache.cassandra.db:type=CompactionManager")).thenReturn(mockCompactionManager);
        when(mockJmxClient.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService")).thenReturn(mockStorageService);
        when(mockJmxClient.proxy(eq(GaugeMetricsJmxOperations.class), anyString())).thenReturn(mockGaugeMetrics);
        when(mockJmxClient.proxy(eq(CounterMetricsJmxOperations.class), anyString())).thenReturn(mockCounterMetrics);
        when(mockJmxClient.proxy(eq(MeterMetricsJmxOperations.class), anyString())).thenReturn(mockMeterMetrics);

        when(mockStorageService.getConcurrentCompactors()).thenReturn(4);
        when(mockGaugeMetrics.getValue()).thenReturn(Collections.emptyMap());
        when(mockCounterMetrics.getCount()).thenReturn(0L);
        when(mockMeterMetrics.getMeanRate()).thenReturn(0.0);
        when(mockMeterMetrics.getFifteenMinuteRate()).thenReturn(0.0);

        List<Map<String, String>> activeCompactions = List.of(
            Map.of("compactionId", "comp-1", "keyspace", "test_keyspace", "columnfamily", "test_table",
                   "taskType", "COMPACTION", "completed", "1024000", "total", "-1")
        );
        when(mockCompactionManager.getCompactions()).thenReturn(activeCompactions);

        CompactionStatsData response = metricsOperations.compactionStats();

        assertThat(response.activeCompactions()).hasSize(1);
        ActiveCompactionEntryData compaction = response.activeCompactions().get(0);
        assertThat(compaction.totalBytes()).isEqualTo(-1L);
        assertThat(compaction.percentCompleted()).isEqualTo(0.0);
    }

    @Test
    void testCompactionStatsSsTablesStrEmpty()
    {
        // ssTablesStr empty
        when(mockJmxClient.proxy(CompactionManagerJmxOperations.class, "org.apache.cassandra.db:type=CompactionManager")).thenReturn(mockCompactionManager);
        when(mockJmxClient.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService")).thenReturn(mockStorageService);
        when(mockJmxClient.proxy(eq(GaugeMetricsJmxOperations.class), anyString())).thenReturn(mockGaugeMetrics);
        when(mockJmxClient.proxy(eq(CounterMetricsJmxOperations.class), anyString())).thenReturn(mockCounterMetrics);
        when(mockJmxClient.proxy(eq(MeterMetricsJmxOperations.class), anyString())).thenReturn(mockMeterMetrics);

        when(mockStorageService.getConcurrentCompactors()).thenReturn(4);
        when(mockGaugeMetrics.getValue()).thenReturn(Collections.emptyMap());
        when(mockCounterMetrics.getCount()).thenReturn(0L);
        when(mockMeterMetrics.getMeanRate()).thenReturn(0.0);
        when(mockMeterMetrics.getFifteenMinuteRate()).thenReturn(0.0);

        List<Map<String, String>> activeCompactions = List.of(
            Map.of("compactionId", "comp-1", "keyspace", "test_keyspace", "columnfamily", "test_table",
                   "taskType", "COMPACTION", "completed", "1024000", "total", "2048000", "sstables", "")
        );
        when(mockCompactionManager.getCompactions()).thenReturn(activeCompactions);

        CompactionStatsData response = metricsOperations.compactionStats();

        assertThat(response.activeCompactions()).hasSize(1);
        ActiveCompactionEntryData compaction = response.activeCompactions().get(0);
        assertThat(compaction.ssTables()).isEmpty();
    }

    @Test
    void testCompactionStatsSsTablesStrSingleString()
    {
        // ssTablesStr with single string (no commas)
        when(mockJmxClient.proxy(CompactionManagerJmxOperations.class, "org.apache.cassandra.db:type=CompactionManager")).thenReturn(mockCompactionManager);
        when(mockJmxClient.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService")).thenReturn(mockStorageService);
        when(mockJmxClient.proxy(eq(GaugeMetricsJmxOperations.class), anyString())).thenReturn(mockGaugeMetrics);
        when(mockJmxClient.proxy(eq(CounterMetricsJmxOperations.class), anyString())).thenReturn(mockCounterMetrics);
        when(mockJmxClient.proxy(eq(MeterMetricsJmxOperations.class), anyString())).thenReturn(mockMeterMetrics);

        when(mockStorageService.getConcurrentCompactors()).thenReturn(4);
        when(mockGaugeMetrics.getValue()).thenReturn(Collections.emptyMap());
        when(mockCounterMetrics.getCount()).thenReturn(0L);
        when(mockMeterMetrics.getMeanRate()).thenReturn(0.0);
        when(mockMeterMetrics.getFifteenMinuteRate()).thenReturn(0.0);

        List<Map<String, String>> activeCompactions = List.of(
            Map.of("compactionId", "comp-1", "keyspace", "test_keyspace", "columnfamily", "test_table",
                   "taskType", "COMPACTION", "completed", "1024000", "total", "2048000", "sstables", "single_sstable.db")
        );
        when(mockCompactionManager.getCompactions()).thenReturn(activeCompactions);

        CompactionStatsData response = metricsOperations.compactionStats();

        assertThat(response.activeCompactions()).hasSize(1);
        ActiveCompactionEntryData compaction = response.activeCompactions().get(0);
        assertThat(compaction.ssTables()).hasSize(1);
        assertThat(compaction.ssTables().get(0)).isEqualTo("single_sstable.db");
    }

    @Test
    void testCompactionStatsCompletedBytesZero()
    {
        // completedBytes is 0
        when(mockJmxClient.proxy(CompactionManagerJmxOperations.class, "org.apache.cassandra.db:type=CompactionManager")).thenReturn(mockCompactionManager);
        when(mockJmxClient.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService")).thenReturn(mockStorageService);
        when(mockJmxClient.proxy(eq(GaugeMetricsJmxOperations.class), anyString())).thenReturn(mockGaugeMetrics);
        when(mockJmxClient.proxy(eq(CounterMetricsJmxOperations.class), anyString())).thenReturn(mockCounterMetrics);
        when(mockJmxClient.proxy(eq(MeterMetricsJmxOperations.class), anyString())).thenReturn(mockMeterMetrics);

        when(mockStorageService.getConcurrentCompactors()).thenReturn(4);
        when(mockStorageService.getCompactionThroughtputBytesPerSec()).thenReturn(16777216L);
        when(mockGaugeMetrics.getValue()).thenReturn(Collections.emptyMap());
        when(mockCounterMetrics.getCount()).thenReturn(0L);
        when(mockMeterMetrics.getMeanRate()).thenReturn(0.0);
        when(mockMeterMetrics.getFifteenMinuteRate()).thenReturn(0.0);

        List<Map<String, String>> activeCompactions = List.of(
            Map.of("compactionId", "comp-1", "keyspace", "test_keyspace", "columnfamily", "test_table",
                   "taskType", "COMPACTION", "completed", "0", "total", "2048000")
        );
        when(mockCompactionManager.getCompactions()).thenReturn(activeCompactions);

        CompactionStatsData response = metricsOperations.compactionStats();

        assertThat(response.activeCompactions()).hasSize(1);
        ActiveCompactionEntryData compaction = response.activeCompactions().get(0);
        assertThat(compaction.completedBytes()).isEqualTo(0L);
        assertThat(compaction.totalBytes()).isEqualTo(2048000L);
        assertThat(compaction.percentCompleted()).isEqualTo(0.0);
        assertThat(response.activeCompactionsRemainingTime()).isGreaterThanOrEqualTo(0L);
    }

    @Test
    void testCompactionStatsTotalRemainingBytesNegative()
    {
        // totalRemainingBytes < 0 (completedBytes > totalBytes)
        when(mockJmxClient.proxy(CompactionManagerJmxOperations.class, "org.apache.cassandra.db:type=CompactionManager")).thenReturn(mockCompactionManager);
        when(mockJmxClient.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService")).thenReturn(mockStorageService);
        when(mockJmxClient.proxy(eq(GaugeMetricsJmxOperations.class), anyString())).thenReturn(mockGaugeMetrics);
        when(mockJmxClient.proxy(eq(CounterMetricsJmxOperations.class), anyString())).thenReturn(mockCounterMetrics);
        when(mockJmxClient.proxy(eq(MeterMetricsJmxOperations.class), anyString())).thenReturn(mockMeterMetrics);

        when(mockStorageService.getConcurrentCompactors()).thenReturn(4);
        when(mockStorageService.getCompactionThroughtputBytesPerSec()).thenReturn(16777216L);
        when(mockGaugeMetrics.getValue()).thenReturn(Collections.emptyMap());
        when(mockCounterMetrics.getCount()).thenReturn(0L);
        when(mockMeterMetrics.getMeanRate()).thenReturn(0.0);
        when(mockMeterMetrics.getFifteenMinuteRate()).thenReturn(0.0);

        List<Map<String, String>> activeCompactions = List.of(
            Map.of("compactionId", "comp-1", "keyspace", "test_keyspace", "columnfamily", "test_table",
                   "taskType", "COMPACTION", "completed", "3000000", "total", "2048000")
        );
        when(mockCompactionManager.getCompactions()).thenReturn(activeCompactions);

        CompactionStatsData response = metricsOperations.compactionStats();

        assertThat(response.activeCompactions()).hasSize(1);
        ActiveCompactionEntryData compaction = response.activeCompactions().get(0);
        assertThat(compaction.completedBytes()).isEqualTo(3000000L);
        assertThat(compaction.totalBytes()).isEqualTo(2048000L);
        assertThat(response.activeCompactionsRemainingTime()).isEqualTo(0L);
    }

    @Test
    void testCompactionStatsThroughputBytesPerSecZero()
    {
        // throughputBytesPerSec is 0
        when(mockJmxClient.proxy(CompactionManagerJmxOperations.class, "org.apache.cassandra.db:type=CompactionManager")).thenReturn(mockCompactionManager);
        when(mockJmxClient.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService")).thenReturn(mockStorageService);
        when(mockJmxClient.proxy(eq(GaugeMetricsJmxOperations.class), anyString())).thenReturn(mockGaugeMetrics);
        when(mockJmxClient.proxy(eq(CounterMetricsJmxOperations.class), anyString())).thenReturn(mockCounterMetrics);
        when(mockJmxClient.proxy(eq(MeterMetricsJmxOperations.class), anyString())).thenReturn(mockMeterMetrics);

        when(mockStorageService.getConcurrentCompactors()).thenReturn(4);
        when(mockStorageService.getCompactionThroughtputBytesPerSec()).thenReturn(0L);
        when(mockGaugeMetrics.getValue()).thenReturn(Collections.emptyMap());
        when(mockCounterMetrics.getCount()).thenReturn(0L);
        when(mockMeterMetrics.getMeanRate()).thenReturn(0.0);
        when(mockMeterMetrics.getFifteenMinuteRate()).thenReturn(0.0);

        List<Map<String, String>> activeCompactions = List.of(
            Map.of("compactionId", "comp-1", "keyspace", "test_keyspace", "columnfamily", "test_table",
                   "taskType", "COMPACTION", "completed", "1024000", "total", "2048000")
        );
        when(mockCompactionManager.getCompactions()).thenReturn(activeCompactions);

        CompactionStatsData response = metricsOperations.compactionStats();

        assertThat(response.activeCompactions()).hasSize(1);
        assertThat(response.activeCompactionsRemainingTime()).isEqualTo(0L);
    }
}
