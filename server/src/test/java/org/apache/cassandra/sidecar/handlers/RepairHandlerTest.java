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
import java.util.Map;
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
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.request.data.RepairPayload;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentCaptor;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.FAILED;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.RUNNING;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.SUCCEEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link RepairHandler}
 */
@ExtendWith(VertxExtension.class)
public class RepairHandlerTest
{
    static final Logger LOGGER = LoggerFactory.getLogger(RepairHandlerTest.class);
    Vertx vertx;
    Server server;
    StorageOperations mockStorageOperations = mock(StorageOperations.class);

    @BeforeEach
    void before() throws InterruptedException
    {
        Injector injector;
        Module testOverride = Modules.override(new TestModule())
                                     .with(new RepairTestModule());
        injector = Guice.createInjector(Modules.override(SidecarModules.all())
                                               .with(testOverride));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
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
    void testRepairHandler(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/keyspaces/testkeyspace/repair";

        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .repairType(RepairPayload.RepairType.INCREMENTAL)
                                             .tables(List.of("test_table"))
                                             .build();

        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .putHeader("Content-Type", "application/json")
              .sendJson(payload, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  LOGGER.info("Repair Response: {}", response.bodyAsString());

                  OperationalJobResponse repairResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(repairResponse).isNotNull();
                  assertThat(repairResponse.status()).isEqualTo(SUCCEEDED);
                  context.completeNow();
              }));
    }

    @Test
    void testRepairHandlerIR(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/keyspaces/testkeyspace/repair";

        ArgumentCaptor<Map<String, String>> jobCapture = ArgumentCaptor.forClass(Map.class);
        RepairPayload payload = RepairPayload.builder()
                                             .repairType(RepairPayload.RepairType.INCREMENTAL)
                                             .tables(List.of("test_table"))
                                             .build();

        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .putHeader("Content-Type", "application/json")
              .sendJson(payload, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  LOGGER.info("Repair Response: {}", response.bodyAsString());

                  OperationalJobResponse repairResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(repairResponse).isNotNull();
                  assertThat(repairResponse.status()).isEqualTo(SUCCEEDED);
                  verify(mockStorageOperations).repair(anyString(), jobCapture.capture());
                  assertThat(jobCapture.getValue()).containsKey("incremental");
                  assertThat(jobCapture.getValue().get("incremental")).isEqualTo("true");
                  context.completeNow();
              }));
    }

    @Test
    void testRepairHandlerWithRanges(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/keyspaces/testkeyspace/repair";

        String expectedRanges = 0L + ":" + Integer.MAX_VALUE;
        ArgumentCaptor<Map<String, String>> jobCapture = ArgumentCaptor.forClass(Map.class);
        RepairPayload payload = RepairPayload.builder()
                                             .startToken(0L)
                                             .endToken(Integer.MAX_VALUE)
                                             .tables(List.of("test_table"))
                                             .build();

        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .putHeader("Content-Type", "application/json")
              .sendJson(payload, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  LOGGER.info("Repair Response: {}", response.bodyAsString());

                  OperationalJobResponse repairResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(repairResponse).isNotNull();
                  assertThat(repairResponse.status()).isEqualTo(SUCCEEDED);
                  verify(mockStorageOperations).repair(anyString(), jobCapture.capture());
                  assertThat(jobCapture.getValue()).containsKey("ranges");
                  assertThat(jobCapture.getValue().get("ranges")).isEqualTo(expectedRanges);
                  context.completeNow();
              }));
    }

    @Test
    void testRepairHandlerLongRunning(VertxTestContext context)
    {
        doAnswer(AdditionalAnswers.answersWithDelay(6000, invocation -> null))
        .when(mockStorageOperations).repair(anyString(), any());

        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/keyspaces/testkeyspace/repair";

        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of("test_table"))
                                             .build();

        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .putHeader("Content-Type", "application/json")
              .sendJson(payload, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(ACCEPTED.code());
                  LOGGER.info("Repair Response: {}", response.bodyAsString());

                  OperationalJobResponse repairResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(repairResponse).isNotNull();
                  assertThat(repairResponse.status()).isEqualTo(RUNNING);
                  context.completeNow();
              }));
    }

    @Test
    void testRepairHandlerBadRequest(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/keyspaces/testkeyspace/repair";

        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .putHeader("Content-Type", "application/json")
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  context.completeNow();
              }));
    }

    @Test
    void testRepairHandlerFailed(VertxTestContext context)
    {
        doThrow(new RuntimeException("Simulated failure")).when(mockStorageOperations).repair(anyString(), any());
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/keyspaces/testkeyspace/repair";
        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of("test_table"))
                                             .build();


        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .putHeader("Content-Type", "application/json")
              .sendJson(payload, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  OperationalJobResponse repairResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(repairResponse).isNotNull();
                  assertThat(repairResponse.jobId()).isNotNull();
                  assertThat(repairResponse.status()).isEqualTo(FAILED);
                  context.completeNow();
              }));
    }

    /**
     * Test guice module for Node Decommission handler tests
     */
    class RepairTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public InstancesMetadata instanceMetadata()
        {
            final int instanceId = 100;
            final String host = "127.0.0.1";
            final InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
            when(instanceMetadata.host()).thenReturn(host);
            when(instanceMetadata.port()).thenReturn(9042);
            when(instanceMetadata.id()).thenReturn(instanceId);
            when(instanceMetadata.stagingDir()).thenReturn("");

            CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);

            when(delegate.storageOperations()).thenReturn(mockStorageOperations);
            when(instanceMetadata.delegate()).thenReturn(delegate);

            InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
            when(mockInstancesMetadata.instances()).thenReturn(Collections.singletonList(instanceMetadata));
            when(mockInstancesMetadata.instanceFromId(instanceId)).thenReturn(instanceMetadata);
            when(mockInstancesMetadata.instanceFromHost(host)).thenReturn(instanceMetadata);

            return mockInstancesMetadata;
        }
    }
}
