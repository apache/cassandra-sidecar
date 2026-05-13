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

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
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
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;
import org.mockito.AdditionalAnswers;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.FAILED;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.RUNNING;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.SUCCEEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link NodeMoveHandler}
 */
@ExtendWith(VertxExtension.class)
public class NodeMoveHandlerTest
{
    static final Logger LOGGER = LoggerFactory.getLogger(NodeMoveHandlerTest.class);
    public static final String MOVE_ROUTE = "/api/v1/cassandra/operations/move";
    public static final String LOCAL_HOST = "127.0.0.1";
    public static final String OPERATION_MODE_MOVING = "MOVING";
    public static final String OPERATION_MODE_NORMAL = "NORMAL";
    Vertx vertx;
    Server server;
    StorageOperations mockStorageOperations = mock(StorageOperations.class);

    @BeforeEach
    void before() throws InterruptedException
    {
        Injector injector;
        Module testOverride = Modules.override(new TestModule())
                                     .with(new NodeMoveHandlerTest.NodeMoveTestModule());
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
    void testMoveLongRunning(VertxTestContext context) throws IOException
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);
        doAnswer(AdditionalAnswers.answersWithDelay(6000, invocation -> null))
        .when(mockStorageOperations).move(anyString());

        WebClient client = WebClient.create(vertx);
        String requestBody = "{\"newToken\":\"123456789\"}";
        client.put(server.actualPort(), LOCAL_HOST, MOVE_ROUTE)
              .expect(ResponsePredicate.SC_ACCEPTED)
              .putHeader("content-type", "application/json")
              .sendBuffer(io.vertx.core.buffer.Buffer.buffer(requestBody), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(ACCEPTED.code());
                  OperationalJobResponse moveResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(moveResponse).isNotNull();
                  assertThat(moveResponse.status()).isEqualTo(RUNNING);
                  assertThat(moveResponse.operation()).isEqualTo("move");
                  context.completeNow();
              }));
    }

    @Test
    void testMoveCompleted(VertxTestContext context)
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);
        WebClient client = WebClient.create(vertx);
        String requestBody = "{\"newToken\":\"123456789\"}";
        client.put(server.actualPort(), LOCAL_HOST, MOVE_ROUTE)
              .putHeader("content-type", "application/json")
              .sendBuffer(io.vertx.core.buffer.Buffer.buffer(requestBody), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  LOGGER.info("Move Response: {}", response.bodyAsString());

                  OperationalJobResponse moveResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(moveResponse).isNotNull();
                  assertThat(moveResponse.status()).isEqualTo(SUCCEEDED);
                  assertThat(moveResponse.operation()).isEqualTo("move");
                  try
                  {
                      verify(mockStorageOperations).move("123456789");
                  }
                  catch (IOException e)
                  {
                      throw new RuntimeException(e);
                  }
                  context.completeNow();
              }));
    }

    @Test
    void testMoveFailed(VertxTestContext context) throws IOException
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);
        doThrow(new RuntimeException("Simulated failure")).when(mockStorageOperations).move(anyString());
        WebClient client = WebClient.create(vertx);
        String requestBody = "{\"newToken\":\"123456789\"}";
        client.put(server.actualPort(), LOCAL_HOST, MOVE_ROUTE)
              .expect(ResponsePredicate.SC_OK)
              .putHeader("content-type", "application/json")
              .sendBuffer(io.vertx.core.buffer.Buffer.buffer(requestBody), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  OperationalJobResponse moveResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(moveResponse).isNotNull();
                  assertThat(moveResponse.status()).isEqualTo(FAILED);
                  assertThat(moveResponse.operation()).isEqualTo("move");
                  assertThat(moveResponse.reason()).isEqualTo("Simulated failure");
                  context.completeNow();
              }));
    }

    @Test
    void testMoveConflictAlreadyMoving(VertxTestContext context)
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_MOVING);
        WebClient client = WebClient.create(vertx);
        String requestBody = "{\"newToken\":\"123456789\"}";
        client.put(server.actualPort(), LOCAL_HOST, MOVE_ROUTE)
              .expect(ResponsePredicate.SC_CONFLICT)
              .putHeader("content-type", "application/json")
              .sendBuffer(io.vertx.core.buffer.Buffer.buffer(requestBody), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(CONFLICT.code());
                  LOGGER.info("Move Response: {}", response.bodyAsString());
                  OperationalJobResponse moveResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(moveResponse).isNotNull();
                  assertThat(moveResponse.jobId()).isNotNull();
                  try
                  {
                      verify(mockStorageOperations, never()).move(anyString()); // Should not call move when already moving
                  }
                  catch (IOException e)
                  {
                      throw new RuntimeException(e);
                  }
                  context.completeNow();
              }));
    }

    @Test
    void testMoveWithMissingToken(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String requestBody = "{}"; // Empty JSON body
        client.put(server.actualPort(), LOCAL_HOST, MOVE_ROUTE)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .putHeader("content-type", "application/json")
              .sendBuffer(io.vertx.core.buffer.Buffer.buffer(requestBody), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  try
                  {
                      verify(mockStorageOperations, never()).move(anyString());
                  }
                  catch (IOException e)
                  {
                      throw new RuntimeException(e);
                  }
                  context.completeNow();
              }));
    }

    @Test
    void testMoveWithEmptyToken(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String requestBody = "{\"newToken\":\"\"}"; // Empty token in JSON
        client.put(server.actualPort(), LOCAL_HOST, MOVE_ROUTE)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .putHeader("content-type", "application/json")
              .sendBuffer(io.vertx.core.buffer.Buffer.buffer(requestBody), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  try
                  {
                      verify(mockStorageOperations, never()).move(anyString());
                  }
                  catch (IOException e)
                  {
                      throw new RuntimeException(e);
                  }
                  context.completeNow();
              }));
    }

    @Test
    void testMoveWithInvalidTokenTooLong(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        // Create a token string that is exactly 128 characters (should fail)
        String longToken = "a".repeat(128);
        String requestBody = "{\"newToken\":\"" + longToken + "\"}";
        client.put(server.actualPort(), LOCAL_HOST, MOVE_ROUTE)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .putHeader("content-type", "application/json")
              .sendBuffer(io.vertx.core.buffer.Buffer.buffer(requestBody), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  try
                  {
                      verify(mockStorageOperations, never()).move(anyString());
                  }
                  catch (IOException e)
                  {
                      throw new RuntimeException(e);
                  }
                  context.completeNow();
              }));
    }

    @Test
    void testMoveWithValidAlphanumericToken(VertxTestContext context)
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);
        WebClient client = WebClient.create(vertx);
        String requestBody = "{\"newToken\":\"validtoken123\"}"; // Valid alphanumeric token
        client.put(server.actualPort(), LOCAL_HOST, MOVE_ROUTE)
              .putHeader("content-type", "application/json")
              .sendBuffer(io.vertx.core.buffer.Buffer.buffer(requestBody), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  OperationalJobResponse moveResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(moveResponse).isNotNull();
                  assertThat(moveResponse.status()).isEqualTo(SUCCEEDED);
                  try
                  {
                      verify(mockStorageOperations).move("validtoken123");
                  }
                  catch (IOException e)
                  {
                      throw new RuntimeException(e);
                  }
                  context.completeNow();
              }));
    }

    @Test
    void testMoveWithTokenAtMaxLength(VertxTestContext context)
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);
        WebClient client = WebClient.create(vertx);
        // Create a token string that is 127 characters (should pass)
        String maxLengthToken = "a".repeat(127);
        String requestBody = "{\"newToken\":\"" + maxLengthToken + "\"}";
        client.put(server.actualPort(), LOCAL_HOST, MOVE_ROUTE)
              .putHeader("content-type", "application/json")
              .sendBuffer(io.vertx.core.buffer.Buffer.buffer(requestBody), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  OperationalJobResponse moveResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(moveResponse).isNotNull();
                  assertThat(moveResponse.status()).isEqualTo(SUCCEEDED);
                  try
                  {
                      verify(mockStorageOperations).move(maxLengthToken);
                  }
                  catch (IOException e)
                  {
                      throw new RuntimeException(e);
                  }
                  context.completeNow();
              }));
    }

    @Test
    void testMoveWithNegativeToken(VertxTestContext context)
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);
        WebClient client = WebClient.create(vertx);
        String requestBody = "{\"newToken\":\"-9223372036854775808\"}"; // Negative token (valid for Murmur3)
        client.put(server.actualPort(), LOCAL_HOST, MOVE_ROUTE)
              .putHeader("content-type", "application/json")
              .sendBuffer(io.vertx.core.buffer.Buffer.buffer(requestBody), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  OperationalJobResponse moveResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(moveResponse).isNotNull();
                  assertThat(moveResponse.status()).isEqualTo(SUCCEEDED);
                  try
                  {
                      verify(mockStorageOperations).move("-9223372036854775808");
                  }
                  catch (IOException e)
                  {
                      throw new RuntimeException(e);
                  }
                  context.completeNow();
              }));
    }

    @Test
    void testMoveWithZeroToken(VertxTestContext context)
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);
        WebClient client = WebClient.create(vertx);
        String requestBody = "{\"newToken\":\"0\"}"; // Zero token
        client.put(server.actualPort(), LOCAL_HOST, MOVE_ROUTE)
              .putHeader("content-type", "application/json")
              .sendBuffer(io.vertx.core.buffer.Buffer.buffer(requestBody), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  OperationalJobResponse moveResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(moveResponse).isNotNull();
                  assertThat(moveResponse.status()).isEqualTo(SUCCEEDED);
                  try
                  {
                      verify(mockStorageOperations).move("0");
                  }
                  catch (IOException e)
                  {
                      throw new RuntimeException(e);
                  }
                  context.completeNow();
              }));
    }

    /**
     * Test guice module for Node Move handler tests
     */
    class NodeMoveTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public InstancesMetadata instanceMetadata()
        {
            final int instanceId = 100;
            final String host = LOCAL_HOST;
            final InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
            when(instanceMetadata.host()).thenReturn(host);
            when(instanceMetadata.port()).thenReturn(9042);
            when(instanceMetadata.id()).thenReturn(instanceId);
            when(instanceMetadata.stagingDir()).thenReturn("");

            CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);

            NodeSettings mockNodeSettings = NodeSettings.builder()
                                                        .hostId(UUID.randomUUID())
                                                        .build();
            when(delegate.storageOperations()).thenReturn(mockStorageOperations);
            when(delegate.nodeSettings()).thenReturn(mockNodeSettings);
            when(instanceMetadata.delegate()).thenReturn(delegate);

            InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
            when(mockInstancesMetadata.instances()).thenReturn(Collections.singletonList(instanceMetadata));
            when(mockInstancesMetadata.instanceFromId(instanceId)).thenReturn(instanceMetadata);
            when(mockInstancesMetadata.instanceFromHost(host)).thenReturn(instanceMetadata);

            return mockInstancesMetadata;
        }
    }
}
