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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.adapters.base.jmx.CounterMetricsJmxOperations;
import org.apache.cassandra.sidecar.adapters.base.jmx.GaugeMetricsJmxOperations;
import org.apache.cassandra.sidecar.adapters.base.jmx.MeterMetricsJmxOperations;
import org.apache.cassandra.sidecar.common.server.ICassandraAdapter;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.data.CompletedCompactionsRateData;
import org.apache.cassandra.sidecar.db.schema.TableSchemaFetcher;

import static org.apache.cassandra.sidecar.adapters.base.data.CompactionStatsMetrics.PENDING_TASKS_BY_TABLE_NAME;
import static org.apache.cassandra.sidecar.adapters.base.data.CompactionStatsMetrics.TOTAL_COMPACTIONS_COMPLETED;
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
    private CounterMetricsJmxOperations mockCounterMetrics;
    private GaugeMetricsJmxOperations mockGaugeMetrics;
    private MeterMetricsJmxOperations mockMeterMetrics;

    @BeforeEach
    void setUp()
    {
        mockJmxClient = mock(JmxClient.class);
        TableSchemaFetcher mockTableSchemaFetcher = mock(TableSchemaFetcher.class);
        ICassandraAdapter mockCassandraAdapter = mock(ICassandraAdapter.class);
        mockCounterMetrics = mock(CounterMetricsJmxOperations.class);
        mockGaugeMetrics = mock(GaugeMetricsJmxOperations.class);
        mockMeterMetrics = mock(MeterMetricsJmxOperations.class);

        metricsOperations = new CassandraMetricsOperations(mockJmxClient, mockTableSchemaFetcher, mockCassandraAdapter);
    }

    @Test
    void testGetCompactionMetricHappyPathCounter()
    {
        // Test getCompactionMetric method
        when(mockJmxClient.proxy(eq(CounterMetricsJmxOperations.class), anyString())).thenReturn(mockCounterMetrics);
        when(mockCounterMetrics.getCount()).thenReturn(100L);

        Object result = metricsOperations.getCompactionMetric(
        TOTAL_COMPACTIONS_COMPLETED.metricName(), TOTAL_COMPACTIONS_COMPLETED.type);

        assertThat(result).isEqualTo(100L);
    }

    @Test
    void testGetCompactionMetricHappyPathGauge()
    {
        // Test getCompactionMetric method
        when(mockJmxClient.proxy(eq(GaugeMetricsJmxOperations.class), anyString())).thenReturn(mockGaugeMetrics);
        when(mockGaugeMetrics.getValue()).thenReturn(100L);

        Object result = metricsOperations.getCompactionMetric(
        PENDING_TASKS_BY_TABLE_NAME.metricName(), PENDING_TASKS_BY_TABLE_NAME.type);

        assertThat(result).isEqualTo(100L);
    }

    @Test
    void testGetCompactionMetricFailureJmxException()
    {
        // Test getCompactionMetric method with JMX exception
        when(mockJmxClient.proxy(eq(CounterMetricsJmxOperations.class), anyString()))
        .thenThrow(new RuntimeException("JMX connection failed"));

        assertThatThrownBy(() -> metricsOperations.getCompactionMetric(
        TOTAL_COMPACTIONS_COMPLETED.metricName(), TOTAL_COMPACTIONS_COMPLETED.type))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("JMX connection failed");
    }

    @Test
    void testGetCompletedCompactionsRateHappyPath()
    {
        // Test getCompletedCompactionsRate method
        when(mockJmxClient.proxy(eq(MeterMetricsJmxOperations.class), anyString())).thenReturn(mockMeterMetrics);
        when(mockMeterMetrics.getMeanRate()).thenReturn(0.5);
        when(mockMeterMetrics.getFifteenMinuteRate()).thenReturn(0.1);

        CompletedCompactionsRateData result = metricsOperations.getCompletedCompactionsRate();

        assertThat(result.meanRate).isEqualTo(0.5);
        assertThat(result.fifteenMinuteRate).isEqualTo(0.1);
    }

    @Test
    void testGetCompletedCompactionsRateFailureJmxException()
    {
        // Test getCompletedCompactionsRate method with JMX exception
        when(mockJmxClient.proxy(eq(MeterMetricsJmxOperations.class), anyString()))
        .thenThrow(new RuntimeException("JMX connection failed"));

        assertThatThrownBy(() -> metricsOperations.getCompletedCompactionsRate())
        .isInstanceOf(RuntimeException.class)
        .hasMessage("JMX connection failed");
    }
}
