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

package org.apache.cassandra.sidecar.handlers;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.CompactionStatsResponse;
import org.apache.cassandra.sidecar.common.server.CompactionManagerOperations;
import org.apache.cassandra.sidecar.common.server.CompactionStatsOperations;
import org.apache.cassandra.sidecar.common.server.MetricsOperations;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.data.ActiveCompactionEntryData;
import org.apache.cassandra.sidecar.common.server.data.CompactionStatsData;
import org.apache.cassandra.sidecar.common.server.data.CompletedCompactionsRateData;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.cassandra.sidecar.adapters.base.data.CompactionStatsMetrics.BYTES_COMPACTED;
import static org.apache.cassandra.sidecar.adapters.base.data.CompactionStatsMetrics.COMPACTIONS_ABORTED;
import static org.apache.cassandra.sidecar.adapters.base.data.CompactionStatsMetrics.COMPACTIONS_REDUCED;
import static org.apache.cassandra.sidecar.adapters.base.data.CompactionStatsMetrics.PENDING_TASKS_BY_TABLE_NAME;
import static org.apache.cassandra.sidecar.adapters.base.data.CompactionStatsMetrics.SSTABLES_DROPPED_FROM_COMPACTION;
import static org.apache.cassandra.sidecar.adapters.base.data.CompactionStatsMetrics.TOTAL_COMPACTIONS_COMPLETED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Tests for the {@link CompactionStatsHandler} class
 */
@ExtendWith(VertxExtension.class)
public class CompactionStatsHandlerTest
{
    private static final int EXPECTED_CONCURRENT_COMPACTORS = 4;
    private static final long EXPECTED_COMPLETED_COMPACTIONS = 100;
    private static final long EXPECTED_DATA_COMPACTED = 2048000;
    private static final double EXPECTED_MEAN_RATE = 1800.00;
    private static final double EXPECTED_FIFTEEN_MINUTE_RATE = 6.00;
    private static final long EXPECTED_REMAINING_TIME = 61;

    private static final ActiveCompactionEntryData EXPECTED_ACTIVE_COMPACTION = ActiveCompactionEntryData.builder()
                                                                                                         .id("comp-1")
                                                                                                         .keyspace("test_keyspace")
                                                                                                         .table("test_table")
                                                                                                         .taskType("COMPACTION")
                                                                                                         .completedBytes(1024000L)
                                                                                                         .totalBytes(2048000L)
                                                                                                         .percentCompleted(50.0)
                                                                                                         .sstables(List.of("sstable1.db", "sstable2.db"))
                                                                                                         .targetDirectory("/var/lib/cassandra/data")
                                                                                                         .build();

    static final Logger LOGGER = LoggerFactory.getLogger(CompactionStatsHandlerTest.class);
    Vertx vertx;
    Server server;

    @BeforeEach
    void before() throws InterruptedException
    {
        Module testOverride = Modules.override(new TestModule())
                                     .with(new CompactionStatsTestModule());
        Injector injector = Guice.createInjector(Modules.override(SidecarModules.all())
                                                        .with(testOverride));

        server = injector.getInstance(Server.class);
        vertx = injector.getInstance(Vertx.class);
        VertxTestContext context = new VertxTestContext();
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void after() throws InterruptedException
    {
        CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    @Test
    void testCompactionStatsHandlerHappyPath(VertxTestContext context)
    {
        // Happy path success response
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/stats/compaction";
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  CompactionStatsResponse stats = response.bodyAsJson(CompactionStatsResponse.class);
                  assertThat(stats).isNotNull();
                  assertThat(stats.concurrentCompactors()).isEqualTo(EXPECTED_CONCURRENT_COMPACTORS);
                  assertThat(stats.completedCompactions()).isEqualTo(EXPECTED_COMPLETED_COMPACTIONS);
                  assertThat(stats.dataCompacted()).isEqualTo(EXPECTED_DATA_COMPACTED);
                  assertThat(stats.activeCompactions()).hasSize(1);
                  assertThat(stats.activeCompactionsCount()).isEqualTo(1);
                  assertThat(stats.completedCompactionsRate().meanRate()).isEqualTo(EXPECTED_MEAN_RATE);
                  assertThat(stats.completedCompactionsRate().fifteenMinuteRate()).isEqualTo(EXPECTED_FIFTEEN_MINUTE_RATE);
                  assertThat(stats.activeCompactionsRemainingTime()).isEqualTo(EXPECTED_REMAINING_TIME);
                  context.completeNow();
              }));
    }

    static class CompactionStatsTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public InstancesMetadata instanceConfig()
        {
            CompletedCompactionsRateData rate =
            CompletedCompactionsRateData.builder()
                                        .meanRate(EXPECTED_MEAN_RATE)
                                        .fifteenMinuteRate(EXPECTED_FIFTEEN_MINUTE_RATE)
                                        .build();

            CompactionStatsData mockResponse = CompactionStatsData.builder()
                                                                  .concurrentCompactors(EXPECTED_CONCURRENT_COMPACTORS)
                                                                  .pendingTasks(Collections.emptyMap())
                                                                  .totalPendingTasks(0)
                                                                  .completedCompactions(EXPECTED_COMPLETED_COMPACTIONS)
                                                                  .dataCompacted(EXPECTED_DATA_COMPACTED)
                                                                  .abortedCompactions(0)
                                                                  .reducedCompactions(0)
                                                                  .sstablesDroppedFromCompaction(0)
                                                                  .completedCompactionsRate(rate)
                                                                  .activeCompactions(List.of(EXPECTED_ACTIVE_COMPACTION))
                                                                  .activeCompactionsCount(1)
                                                                  .activeCompactionsRemainingTime(EXPECTED_REMAINING_TIME)
                                                                  .build();

            final int instanceId = 100;
            final String host = "127.0.0.1";
            final InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
            when(instanceMetadata.host()).thenReturn(host);
            when(instanceMetadata.port()).thenReturn(9042);
            when(instanceMetadata.id()).thenReturn(instanceId);
            when(instanceMetadata.stagingDir()).thenReturn("");

            CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);

            // Mock StorageOperations
            StorageOperations mockStorageOperations = mock(StorageOperations.class);
            when(mockStorageOperations.getConcurrentCompactors()).thenReturn(EXPECTED_CONCURRENT_COMPACTORS);
            when(mockStorageOperations.getCompactionThroughputBytesPerSec()).thenReturn(16777L); // 1024000 bytes / 62 seconds

            // Mock MetricsOperations
            MetricsOperations mockMetricsOperations = mock(MetricsOperations.class);
            when(mockMetricsOperations.getCompactionMetric(PENDING_TASKS_BY_TABLE_NAME.metricName(), PENDING_TASKS_BY_TABLE_NAME.type))
            .thenReturn(Collections.emptyMap());
            when(mockMetricsOperations.getCompactionMetric(TOTAL_COMPACTIONS_COMPLETED.metricName(), TOTAL_COMPACTIONS_COMPLETED.type))
            .thenReturn(EXPECTED_COMPLETED_COMPACTIONS);
            when(mockMetricsOperations.getCompactionMetric(BYTES_COMPACTED.metricName(), BYTES_COMPACTED.type))
            .thenReturn(EXPECTED_DATA_COMPACTED);
            when(mockMetricsOperations.getCompactionMetric(COMPACTIONS_ABORTED.metricName(), COMPACTIONS_ABORTED.type))
            .thenReturn(0L);
            when(mockMetricsOperations.getCompactionMetric(COMPACTIONS_REDUCED.metricName(), COMPACTIONS_REDUCED.type))
            .thenReturn(0L);
            when(mockMetricsOperations.getCompactionMetric(SSTABLES_DROPPED_FROM_COMPACTION.metricName(), SSTABLES_DROPPED_FROM_COMPACTION.type))
            .thenReturn(0L);
            when(mockMetricsOperations.getCompletedCompactionsRate()).thenReturn(rate);

            // Mock CompactionManagerOperations
            CompactionManagerOperations mockCompactionManagerOperations = mock(CompactionManagerOperations.class);
            when(mockCompactionManagerOperations.getCompactions()).thenReturn(List.of(
            java.util.Map.of(
            "compactionId", "comp-1",
            "keyspace", "test_keyspace",
            "columnfamily", "test_table",
            "taskType", "COMPACTION",
            "completed", "1024000",
            "total", "2048000",
            "sstables", "sstable1.db,sstable2.db",
            "targetDirectory", "/var/lib/cassandra/data"
            )
            ));

            // Mock CompactionStatsOperations
            CompactionStatsOperations mockCompactionStatsOperations = mock(CompactionStatsOperations.class);
            when(mockCompactionStatsOperations.compactionStats()).thenReturn(mockResponse);

            when(delegate.storageOperations()).thenReturn(mockStorageOperations);
            when(delegate.metricsOperations()).thenReturn(mockMetricsOperations);
            when(delegate.compactionManagerOperations()).thenReturn(mockCompactionManagerOperations);
            when(delegate.compactionStatsOperations()).thenReturn(mockCompactionStatsOperations);
            when(instanceMetadata.delegate()).thenReturn(delegate);

            InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
            when(mockInstancesMetadata.instances()).thenReturn(Collections.singletonList(instanceMetadata));
            when(mockInstancesMetadata.instanceFromId(instanceId)).thenReturn(instanceMetadata);
            when(mockInstancesMetadata.instanceFromHost(host)).thenReturn(instanceMetadata);

            return mockInstancesMetadata;
        }
    }
}
