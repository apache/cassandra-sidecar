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

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
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
import org.apache.cassandra.sidecar.common.response.TableStatsResponse;
import org.apache.cassandra.sidecar.common.server.MetricsOperations;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link TableStatsHandler} class
 */
@ExtendWith(VertxExtension.class)
public class TableStatsHandlerTest
{

    private static final int EXPECTED_SSTABLES = 10;
    private static final long EXPECTED_SIZE = 1024;
    private static final long EXPECTED_TOTAL_SIZE = 2048;
    private static final long EXPECTED_SNAPSHOT_SIZE = 100;
    private static final String KEYSPACE = "testkeyspace";
    private static final String TABLE = "testtable";
    static final Logger LOGGER = LoggerFactory.getLogger(TableStatsHandlerTest.class);
    Vertx vertx;
    Server server;

    @BeforeEach
    void before() throws InterruptedException
    {
        Module testOverride = Modules.override(new TestModule())
                                      .with(new TableStatsTestModule());
        Injector injector = Guice.createInjector(Modules.override(new MainModule())
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
    void testHandlerStats(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/keyspaces/testkeyspace/tables/testtable/stats";
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  TableStatsResponse statsResponse = response.bodyAsJson(TableStatsResponse.class);
                  assertThat(statsResponse).isNotNull();
                  assertThat(statsResponse.keyspace()).isEqualTo(KEYSPACE);
                  assertThat(statsResponse.table()).isEqualTo(TABLE);
                  assertThat(statsResponse.sstableCount()).isEqualTo(EXPECTED_SSTABLES);
                  assertThat(statsResponse.diskSpaceUsedBytes()).isEqualTo(EXPECTED_SIZE);
                  assertThat(statsResponse.totalDiskSpaceUsedBytes()).isEqualTo(EXPECTED_TOTAL_SIZE);
                  assertThat(statsResponse.snapshotsSizeBytes()).isEqualTo(EXPECTED_SNAPSHOT_SIZE);
                  context.completeNow();
              }));
    }

    @Test
    void testHandlerStatsNoTable(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/keyspaces/testkeyspace/stats";
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  context.completeNow();
              }));
    }

    @Test
    void testHandlerStatsOnlyTable(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/tables/testtable/stats";
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  context.completeNow();
              }));
    }

    @Test
    void testHandlerStatsNoKeyspace(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/stats";
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  context.completeNow();
              }));
    }


    static class TableStatsTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public InstancesMetadata instanceConfig()
        {
            TableStatsResponse response = new TableStatsResponse(KEYSPACE, TABLE, EXPECTED_SSTABLES, EXPECTED_SIZE,
                                                                 EXPECTED_TOTAL_SIZE, EXPECTED_SNAPSHOT_SIZE);
            final int instanceId = 100;
            final String host = "127.0.0.1";
            final InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
            when(instanceMetadata.host()).thenReturn(host);
            when(instanceMetadata.port()).thenReturn(9042);
            when(instanceMetadata.id()).thenReturn(instanceId);
            when(instanceMetadata.stagingDir()).thenReturn("");

            CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
            MetricsOperations mockMetricsOperations = mock(MetricsOperations.class);
            when(mockMetricsOperations.tableStats(any())).thenReturn(response);
            when(delegate.metricsOperations()).thenReturn(mockMetricsOperations);
            when(instanceMetadata.delegate()).thenReturn(delegate);

            KeyspaceMetadata mockKeyspaceMetadata = mock(KeyspaceMetadata.class);
            Metadata mockMetadata = mock(Metadata.class);
            when(mockMetadata.getKeyspace(KEYSPACE)).thenReturn(mockKeyspaceMetadata);
            TableMetadata table = mock(TableMetadata.class);
            when(table.getKeyspace()).thenReturn(mockKeyspaceMetadata);
            when(table.getName()).thenReturn(TABLE);
            when(mockKeyspaceMetadata.getTable(TABLE)).thenReturn(table);
            when(delegate.metadata()).thenReturn(mockMetadata);

            InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
            when(mockInstancesMetadata.instances()).thenReturn(Collections.singletonList(instanceMetadata));
            when(mockInstancesMetadata.instanceFromId(instanceId)).thenReturn(instanceMetadata);
            when(mockInstancesMetadata.instanceFromHost(host)).thenReturn(instanceMetadata);

            return mockInstancesMetadata;
        }
    }
}
