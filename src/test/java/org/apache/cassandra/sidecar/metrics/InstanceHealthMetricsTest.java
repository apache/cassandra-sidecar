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

package org.apache.cassandra.sidecar.metrics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.utils.DriverUtils;
import org.apache.cassandra.sidecar.metrics.instance.InstanceHealthMetrics;
import org.apache.cassandra.sidecar.utils.CassandraVersionProvider;

import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verify health metrics are captured during cassandra instance specific health check failures
 */
public class InstanceHealthMetricsTest
{
    InstanceHealthMetrics metrics;
    JmxClient jmxClient;
    CassandraAdapterDelegate delegate;

    @BeforeEach
    void setup()
    {
        Vertx vertx = Vertx.vertx();
        CassandraVersionProvider mockCassandraVersionProvider = mock(CassandraVersionProvider.class);
        CQLSessionProvider mockCqlSessionProvider = mock(CQLSessionProvider.class);
        jmxClient = mock(JmxClient.class);
        metrics = new InstanceHealthMetrics(registry(1));
        delegate = new CassandraAdapterDelegate(vertx, 1, mockCassandraVersionProvider,
                                                mockCqlSessionProvider, jmxClient, new DriverUtils(), null,
                                                "localhost1", 9042, metrics);
    }

    @AfterEach
    void clear()
    {
        registry().removeMatching((name, metric) -> true);
        registry(1).removeMatching((name, metric) -> true);
    }

    @Test
    void testRecorded()
    {
        when(jmxClient.proxy(any(), anyString())).thenThrow(new RuntimeException("Expected exception"));

        delegate.healthCheck();
        assertThat(metrics.jmxDown.metric.getCount()).isOne();
    }
}
