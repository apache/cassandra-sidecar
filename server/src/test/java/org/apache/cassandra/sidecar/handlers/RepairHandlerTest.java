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
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.CREATED;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.FAILED;
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
class RepairHandlerTest
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
                  verify(mockStorageOperations).repairAsync(anyString(), jobCapture.capture());
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
                  verify(mockStorageOperations).repairAsync(anyString(), jobCapture.capture());
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
                  verify(mockStorageOperations).repairAsync(anyString(), jobCapture.capture());
                  assertThat(jobCapture.getValue()).containsKey("hosts");
                  assertThat(jobCapture.getValue().get("hosts")).isEqualTo("127.0.0.1");
                  context.completeNow();
              }));
    }

    @Test
    void testRepairHandlerLongRunning(VertxTestContext context)
    {
        // Simulate repair invocation taking 6s
        doAnswer(AdditionalAnswers.answersWithDelay(6000, invocation -> null))
        .when(mockStorageOperations).repairAsync(anyString(), any());

        WebClient client = WebClient.create(vertx);
        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of("test_table"))
                                             .build();

        // Since it was the OperationalJobExecutionTimeout that triggered the response, the job is still
        // in CREATED state, as we returned before polling for the job status.
        client.put(server.actualPort(), "127.0.0.1", REPAIR_ROUTE)
              .putHeader("Content-Type", "application/json")
              .sendJson(payload, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(ACCEPTED.code());
                  LOGGER.info("Repair Response: {}", response.bodyAsString());

                  OperationalJobResponse repairResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(repairResponse).isNotNull();
                  assertThat(repairResponse.status()).isEqualTo(CREATED);
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
        doThrow(new RuntimeException("Simulated failure")).when(mockStorageOperations).repairAsync(anyString(), any());
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

    @Test
    void testRepairHandlerInvalidTokenRange_StartGreaterThanEnd(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        RepairPayload payload = RepairPayload.builder()
                                             .startToken("100")
                                             .endToken("50")  // End token less than start token
                                             .tables(List.of("test_table"))
                                             .build();

        client.put(server.actualPort(), "127.0.0.1", REPAIR_ROUTE)
              .putHeader("Content-Type", "application/json")
              .sendJson(payload, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.bodyAsString()).contains("Start token must be less than end token");
                  context.completeNow();
              }));
    }

    @Test
    void testRepairHandlerInvalidTokenRange_StartEqualToEnd(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        RepairPayload payload = RepairPayload.builder()
                                             .startToken("100")
                                             .endToken("100")  // End token equal to start token
                                             .tables(List.of("test_table"))
                                             .build();

        client.put(server.actualPort(), "127.0.0.1", REPAIR_ROUTE)
              .putHeader("Content-Type", "application/json")
              .sendJson(payload, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.bodyAsString()).contains("Start token must be less than end token");
                  context.completeNow();
              }));
    }

    @Test
    void testRepairHandlerInvalidTokenFormat_NonNumericStartToken(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        RepairPayload payload = RepairPayload.builder()
                                             .startToken("invalid_token")
                                             .endToken("100")
                                             .tables(List.of("test_table"))
                                             .build();

        client.put(server.actualPort(), "127.0.0.1", REPAIR_ROUTE)
              .putHeader("Content-Type", "application/json")
              .sendJson(payload, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.bodyAsString()).contains("Invalid token format");
                  context.completeNow();
              }));
    }

    @Test
    void testRepairHandlerInvalidTokenFormat_NonNumericEndToken(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        RepairPayload payload = RepairPayload.builder()
                                             .startToken("50")
                                             .endToken("invalid_token")
                                             .tables(List.of("test_table"))
                                             .build();

        client.put(server.actualPort(), "127.0.0.1", REPAIR_ROUTE)
              .putHeader("Content-Type", "application/json")
              .sendJson(payload, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.bodyAsString()).contains("Invalid token format");
                  context.completeNow();
              }));
    }

    @Test
    void testRepairHandlerValidLargeTokenRange(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        ArgumentCaptor<Map<String, String>> jobCapture = ArgumentCaptor.forClass(Map.class);
        // Test with very large token values (BigInteger range)
        String startToken = "123456789012345678901234567890";
        String endToken = "987654321098765432109876543210";
        RepairPayload payload = RepairPayload.builder()
                                             .startToken(startToken)
                                             .endToken(endToken)
                                             .tables(List.of("test_table"))
                                             .build();

        client.put(server.actualPort(), "127.0.0.1", REPAIR_ROUTE)
              .putHeader("Content-Type", "application/json")
              .sendJson(payload, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  OperationalJobResponse repairResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(repairResponse).isNotNull();
                  assertThat(repairResponse.status()).isEqualTo(SUCCEEDED);
                  verify(mockStorageOperations).repairAsync(anyString(), jobCapture.capture());
                  assertThat(jobCapture.getValue()).containsKey("ranges");
                  assertThat(jobCapture.getValue().get("ranges")).isEqualTo(startToken + ":" + endToken);
                  context.completeNow();
              }));
    }

    @Test
    void testRepairHandlerValidNegativeTokenRange(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        ArgumentCaptor<Map<String, String>> jobCapture = ArgumentCaptor.forClass(Map.class);
        // Test with negative token values
        String startToken = "-1000";
        String endToken = "1000";
        RepairPayload payload = RepairPayload.builder()
                                             .startToken(startToken)
                                             .endToken(endToken)
                                             .tables(List.of("test_table"))
                                             .build();

        client.put(server.actualPort(), "127.0.0.1", REPAIR_ROUTE)
              .putHeader("Content-Type", "application/json")
              .sendJson(payload, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  OperationalJobResponse repairResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(repairResponse).isNotNull();
                  assertThat(repairResponse.status()).isEqualTo(SUCCEEDED);
                  verify(mockStorageOperations).repairAsync(anyString(), jobCapture.capture());
                  assertThat(jobCapture.getValue()).containsKey("ranges");
                  assertThat(jobCapture.getValue().get("ranges")).isEqualTo(startToken + ":" + endToken);
                  context.completeNow();
              }));
    }

    @Test
    void testRepairHandlerOnlyStartTokenProvided(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        ArgumentCaptor<Map<String, String>> jobCapture = ArgumentCaptor.forClass(Map.class);
        // Test with only start token (should not add ranges option)
        RepairPayload payload = RepairPayload.builder()
                                             .startToken("100")
                                             // No end token
                                             .tables(List.of("test_table"))
                                             .build();

        client.put(server.actualPort(), "127.0.0.1", REPAIR_ROUTE)
              .putHeader("Content-Type", "application/json")
              .sendJson(payload, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  OperationalJobResponse repairResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(repairResponse).isNotNull();
                  assertThat(repairResponse.status()).isEqualTo(SUCCEEDED);
                  verify(mockStorageOperations).repairAsync(anyString(), jobCapture.capture());
                  // Should not contain ranges since only start token was provided
                  assertThat(jobCapture.getValue()).doesNotContainKey("ranges");
                  context.completeNow();
              }));
    }
}
