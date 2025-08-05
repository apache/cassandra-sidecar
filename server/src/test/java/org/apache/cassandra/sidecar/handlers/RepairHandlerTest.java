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

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.request.data.RepairPayload;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
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
    private static final String REPAIR_ROUTE = "/api/v1/cassandra/keyspaces/testkeyspace/repair";
    Vertx vertx;
    Server server;
    StorageOperations mockStorageOperations = mock(StorageOperations.class);
    InstanceMetadataFetcher mockMetadataFetcher = mock(InstanceMetadataFetcher.class);

    @BeforeEach
    void before() throws InterruptedException
    {
        // Set up the mock metadata chain
        InstanceMetadata mockInstanceMetadata = mock(InstanceMetadata.class);
        CassandraAdapterDelegate mockDelegate = mock(CassandraAdapterDelegate.class);
        Metadata mockMetadata = mock(Metadata.class);
        KeyspaceMetadata mockKeyspaceMetadata = mock(KeyspaceMetadata.class);
        TableMetadata mockTableMetadata = mock(TableMetadata.class);
        
        // Configure the mock chain
        when(mockMetadataFetcher.instance(anyString())).thenReturn(mockInstanceMetadata);
        when(mockMetadataFetcher.delegate(anyString())).thenReturn(mockDelegate); // Add this line to fix the NPE
        when(mockInstanceMetadata.delegate()).thenReturn(mockDelegate);
        when(mockDelegate.metadata()).thenReturn(mockMetadata);
        when(mockDelegate.storageOperations()).thenReturn(mockStorageOperations);
        when(mockMetadata.getKeyspace(anyString())).thenReturn(mockKeyspaceMetadata);
        when(mockKeyspaceMetadata.getTable(anyString())).thenReturn(mockTableMetadata);
        
        AbstractModule repairTestModule = new AbstractModule()
        {
            @Override
            protected void configure()
            {
                // Bind the mocks needed for the test
                bind(StorageOperations.class).toInstance(mockStorageOperations);
                bind(InstanceMetadataFetcher.class).toInstance(mockMetadataFetcher);
            }
        };
        
        // Create the injector with the proper module overrides
        Injector injector = Guice.createInjector(
            Modules.override(SidecarModules.all())
                  .with(Modules.override(new TestModule())
                              .with(new CommonTest.CommonTestModule(mockStorageOperations), repairTestModule))
        );
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
        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .repairType(RepairPayload.RepairType.INCREMENTAL)
                                             .tables(List.of("test_table"))
                                             .build();
        client.put(server.actualPort(), "127.0.0.1", REPAIR_ROUTE)
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
        ArgumentCaptor<Map<String, String>> jobCapture = ArgumentCaptor.forClass(Map.class);
        RepairPayload payload = RepairPayload.builder()
                                             .repairType(RepairPayload.RepairType.INCREMENTAL)
                                             .tables(List.of("test_table"))
                                             .build();

        client.put(server.actualPort(), "127.0.0.1", REPAIR_ROUTE)
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
        String expectedRanges = 0L + ":" + Integer.MAX_VALUE;
        ArgumentCaptor<Map<String, String>> jobCapture = ArgumentCaptor.forClass(Map.class);
        RepairPayload payload = RepairPayload.builder()
                                             .startToken("0")
                                             .endToken(Integer.toString(Integer.MAX_VALUE))
                                             .tables(List.of("test_table"))
                                             .build();

        client.put(server.actualPort(), "127.0.0.1", REPAIR_ROUTE)
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
    void testRepairHandlerWithHosts(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        ArgumentCaptor<Map<String, String>> jobCapture = ArgumentCaptor.forClass(Map.class);
        RepairPayload payload = RepairPayload.builder()
                                             .hosts(List.of("127.0.0.1"))
                                             .tables(List.of("test_table"))
                                             .build();

        client.put(server.actualPort(), "127.0.0.1", REPAIR_ROUTE)
              .putHeader("Content-Type", "application/json")
              .sendJson(payload, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  LOGGER.info("Repair Response: {}", response.bodyAsString());

                  OperationalJobResponse repairResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(repairResponse).isNotNull();
                  assertThat(repairResponse.status()).isEqualTo(SUCCEEDED);
                  verify(mockStorageOperations).repair(anyString(), jobCapture.capture());
                  assertThat(jobCapture.getValue()).containsKey("hosts");
                  assertThat(jobCapture.getValue().get("hosts")).isEqualTo("127.0.0.1");
                  context.completeNow();
              }));
    }

    @Test
    void testRepairHandlerLongRunning(VertxTestContext context)
    {
        doAnswer(AdditionalAnswers.answersWithDelay(6000, invocation -> null))
        .when(mockStorageOperations).repair(anyString(), any());

        WebClient client = WebClient.create(vertx);
        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of("test_table"))
                                             .build();

        client.put(server.actualPort(), "127.0.0.1", REPAIR_ROUTE)
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
        client.put(server.actualPort(), "127.0.0.1", REPAIR_ROUTE)
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
}
