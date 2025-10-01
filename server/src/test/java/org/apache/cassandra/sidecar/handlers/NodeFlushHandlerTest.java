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
import java.util.Arrays;
import java.util.Collections;
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
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;
import org.mockito.AdditionalAnswers;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
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
 * Tests for the {@link NodeFlushHandler}
 */
@ExtendWith(VertxExtension.class)
public class NodeFlushHandlerTest
{
    static final Logger LOGGER = LoggerFactory.getLogger(NodeFlushHandlerTest.class);
    public static final String LOCAL_HOST = "127.0.0.1";
    public static final String TEST_ROUTE = "/api/v1/cassandra/keyspaces/testkeyspace/flush";
    Vertx vertx;
    Server server;
    StorageOperations mockStorageOperations = mock(StorageOperations.class);

    @BeforeEach
    void before() throws InterruptedException
    {
        // Reset mock before each test
        org.mockito.Mockito.reset(mockStorageOperations);

        Injector injector;
        Module testOverride = Modules.override(new TestModule())
                                     .with(new NodeFlushHandlerTest.NodeFlushTestModule());
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
    void testFlushLongRunning(VertxTestContext context) throws IOException
    {
        doAnswer(AdditionalAnswers.answersWithDelay(6000, invocation -> null))
        .when(mockStorageOperations).flush("testkeyspace", "table1", "table2");

        WebClient client = WebClient.create(vertx);

        JsonObject requestBody = new JsonObject();
        requestBody.put("tableNames", Arrays.asList("table1", "table2"));

        client.post(server.actualPort(), LOCAL_HOST, TEST_ROUTE)
              .sendJsonObject(requestBody, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(ACCEPTED.code());
                  OperationalJobResponse flushResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(flushResponse).isNotNull();
                  assertThat(flushResponse.status()).isEqualTo(RUNNING);
                  assertThat(flushResponse.operation()).isEqualTo("flush");
                  context.completeNow();
              }));
    }

    @Test
    void testFlushCompleted(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        JsonObject requestBody = new JsonObject();
        requestBody.put("tableNames", Collections.singletonList("table1"));

        client.post(server.actualPort(), LOCAL_HOST, TEST_ROUTE)
              .sendJsonObject(requestBody, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  LOGGER.info("Flush Response: {}", response.bodyAsString());

                  OperationalJobResponse flushResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(flushResponse).isNotNull();
                  assertThat(flushResponse.status()).isEqualTo(SUCCEEDED);
                  assertThat(flushResponse.operation()).isEqualTo("flush");

                  try
                  {
                      verify(mockStorageOperations).flush("testkeyspace", "table1");
                  }
                  catch (IOException e)
                  {
                      throw new RuntimeException(e);
                  }
                  context.completeNow();
              }));
    }

    @Test
    void testFlushEmptyTableList(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        JsonObject requestBody = new JsonObject();
        requestBody.put("tableNames", Collections.emptyList());

        client.post(server.actualPort(), LOCAL_HOST, TEST_ROUTE)
              .sendJsonObject(requestBody, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  OperationalJobResponse flushResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(flushResponse).isNotNull();
                  assertThat(flushResponse.status()).isEqualTo(SUCCEEDED);
                  assertThat(flushResponse.operation()).isEqualTo("flush");

                  try
                  {
                      verify(mockStorageOperations).flush("testkeyspace");
                  }
                  catch (IOException e)
                  {
                      throw new RuntimeException(e);
                  }
                  context.completeNow();
              }));
    }

    @Test
    void testFlushNoPayload(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        client.post(server.actualPort(), LOCAL_HOST, TEST_ROUTE)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  OperationalJobResponse flushResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(flushResponse).isNotNull();
                  assertThat(flushResponse.status()).isEqualTo(SUCCEEDED);
                  assertThat(flushResponse.operation()).isEqualTo("flush");

                  try
                  {
                      verify(mockStorageOperations).flush("testkeyspace");
                  }
                  catch (IOException e)
                  {
                      throw new RuntimeException(e);
                  }
                  context.completeNow();
              }));
    }

    @Test
    void testFlushFailed(VertxTestContext context) throws IOException
    {
        doThrow(new RuntimeException("Flush failed")).when(mockStorageOperations).flush(anyString(), any(String[].class));

        WebClient client = WebClient.create(vertx);

        JsonObject requestBody = new JsonObject();
        requestBody.put("tableNames", Collections.singletonList("table1"));

        client.post(server.actualPort(), LOCAL_HOST, TEST_ROUTE)
              .sendJsonObject(requestBody, context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  context.completeNow();
              }));
    }

    @Test
    void testFlushInvalidPayload(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        String invalidJson = "{ invalid json }";

        client.post(server.actualPort(), LOCAL_HOST, TEST_ROUTE)
              .sendBuffer(io.vertx.core.buffer.Buffer.buffer(invalidJson), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  context.completeNow();
              }));
    }

    /**
     * Test guice module for Node Flush handler tests
     */
    class NodeFlushTestModule extends AbstractModule
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
