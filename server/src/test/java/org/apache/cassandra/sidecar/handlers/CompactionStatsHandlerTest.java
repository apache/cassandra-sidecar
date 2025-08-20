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
import org.apache.cassandra.sidecar.common.response.data.ActiveCompactionEntry;
import org.apache.cassandra.sidecar.common.server.MetricsOperations;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
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
    private static final String EXPECTED_MEAN_RATE = "1800.00/hour";
    private static final String EXPECTED_FIFTEEN_MINUTE_RATE = "6.00/minute";
    private static final String EXPECTED_REMAINING_TIME = "0h01m02s";
    
    private static final ActiveCompactionEntry EXPECTED_ACTIVE_COMPACTION = new ActiveCompactionEntry(
        "comp-1", "test_keyspace", "test_table", "COMPACTION",
        1024000L, 2048000L, 50.0,
        List.of("sstable1.db", "sstable2.db"), "/var/lib/cassandra/data"
    );
    
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
            CompactionStatsResponse.CompletedCompactionsRate rate = 
                new CompactionStatsResponse.CompletedCompactionsRate(EXPECTED_MEAN_RATE, EXPECTED_FIFTEEN_MINUTE_RATE);

            CompactionStatsResponse mockResponse = new CompactionStatsResponse(
                EXPECTED_CONCURRENT_COMPACTORS, Collections.emptyMap(), 0, 
                EXPECTED_COMPLETED_COMPACTIONS, EXPECTED_DATA_COMPACTED, 0, 0, 0,
                rate, List.of(EXPECTED_ACTIVE_COMPACTION), 1, EXPECTED_REMAINING_TIME
            );

            final int instanceId = 100;
            final String host = "127.0.0.1";
            final InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
            when(instanceMetadata.host()).thenReturn(host);
            when(instanceMetadata.port()).thenReturn(9042);
            when(instanceMetadata.id()).thenReturn(instanceId);
            when(instanceMetadata.stagingDir()).thenReturn("");

            CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
            MetricsOperations mockMetricsOperations = mock(MetricsOperations.class);
            when(mockMetricsOperations.compactionStats()).thenReturn(mockResponse);
            when(delegate.metricsOperations()).thenReturn(mockMetricsOperations);
            when(instanceMetadata.delegate()).thenReturn(delegate);

            InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
            when(mockInstancesMetadata.instances()).thenReturn(Collections.singletonList(instanceMetadata));
            when(mockInstancesMetadata.instanceFromId(instanceId)).thenReturn(instanceMetadata);
            when(mockInstancesMetadata.instanceFromHost(host)).thenReturn(instanceMetadata);

            return mockInstancesMetadata;
        }
    }
}
