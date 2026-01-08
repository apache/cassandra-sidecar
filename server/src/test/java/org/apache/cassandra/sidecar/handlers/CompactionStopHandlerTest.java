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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

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
import org.apache.cassandra.sidecar.common.data.CompactionStopStatus;
import org.apache.cassandra.sidecar.common.response.CompactionStopResponse;
import org.apache.cassandra.sidecar.common.server.CompactionManagerOperations;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.vertx.core.buffer.Buffer.buffer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link CompactionStopHandler} class
 */
@ExtendWith(VertxExtension.class)
public class CompactionStopHandlerTest
{
    private static final String TEST_ROUTE = "/api/v1/cassandra/operations/compaction/stop";

    Vertx vertx;
    Server server;
    CompactionManagerOperations mockCompactionManagerOperations = mock(CompactionManagerOperations.class);

    @BeforeEach
    void before() throws InterruptedException
    {
        Injector injector;
        Module testOverride = Modules.override(new TestModule()).with(new CompactionStopHandlerTestModule());
        injector = Guice.createInjector(Modules.override(SidecarModules.all()).with(testOverride));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        VertxTestContext context = new VertxTestContext();
        server.start().onSuccess(s -> context.completeNow()).onFailure(context::failNow);
        context.awaitCompletion(5, TimeUnit.SECONDS);

        // Mock supportedCompactionTypes to return all types for testing
        when(mockCompactionManagerOperations.supportedCompactionTypes()).thenReturn(
            java.util.Arrays.asList("COMPACTION", "VALIDATION", "KEY_CACHE_SAVE", "ROW_CACHE_SAVE",
                "COUNTER_CACHE_SAVE", "CLEANUP", "SCRUB", "UPGRADE_SSTABLES",
                "INDEX_BUILD", "TOMBSTONE_COMPACTION", "ANTICOMPACTION",
                "VERIFY", "VIEW_BUILD", "INDEX_SUMMARY", "RELOCATE",
                "GARBAGE_COLLECT", "MAJOR_COMPACTION"));
    }

    @AfterEach
    void after() throws InterruptedException
    {
        if (server != null)
        {
            VertxTestContext closeContext = new VertxTestContext();
            server.close()
                  .onComplete(res -> closeContext.completeNow());
            closeContext.awaitCompletion(60, TimeUnit.SECONDS);
        }
    }

    @Test
    void testStopCompactionByTypeHappyPath(VertxTestContext ctx)
    {
        WebClient client = WebClient.create(vertx);
        String payload = "{\"compactionType\":\"COMPACTION\"}";
        client.put(server.actualPort(), "127.0.0.1", TEST_ROUTE)
              .sendBuffer(buffer(payload), ctx.succeeding(resp -> {
                  ctx.verify(() -> {
                      verify(mockCompactionManagerOperations, times(1)).stopCompaction(eq("COMPACTION"));

                      assertThat(resp.statusCode()).isEqualTo(OK.code());
                      CompactionStopResponse response = resp.bodyAsJson(CompactionStopResponse.class);
                      assertThat(response.status()).isEqualTo(CompactionStopStatus.SUBMITTED);
                      assertThat(response.compactionType()).isEqualTo("COMPACTION");
                  });
                  ctx.completeNow();
              }));
    }

    @Test
    void testStopCompactionByIdHappyPath(VertxTestContext ctx)
    {
        WebClient client = WebClient.create(vertx);
        String payload = "{\"compactionId\":\"abc-123\"}";
        client.put(server.actualPort(), "127.0.0.1", TEST_ROUTE)
              .sendBuffer(buffer(payload), ctx.succeeding(resp -> {
                  ctx.verify(() -> {
                      verify(mockCompactionManagerOperations, times(1)).stopCompactionById(eq("abc-123"));

                      assertThat(resp.statusCode()).isEqualTo(OK.code());
                      CompactionStopResponse response = resp.bodyAsJson(CompactionStopResponse.class);
                      assertThat(response.status()).isEqualTo(CompactionStopStatus.SUBMITTED);
                      assertThat(response.compactionId()).isEqualTo("abc-123");
                  });
                  ctx.completeNow();
              }));
    }

    @Test
    void testStopCompactionByBothFieldsHappyPath(VertxTestContext ctx)
    {
        WebClient client = WebClient.create(vertx);
        String payload = "{\"compactionType\":\"VALIDATION\",\"compactionId\":\"xyz-456\"}";
        client.put(server.actualPort(), "127.0.0.1", TEST_ROUTE)
              .sendBuffer(buffer(payload), ctx.succeeding(resp -> {
                  ctx.verify(() -> {
                      verify(mockCompactionManagerOperations, times(1))
                      .stopCompactionById(eq("xyz-456"));

                      assertThat(resp.statusCode()).isEqualTo(OK.code());
                      CompactionStopResponse response = resp.bodyAsJson(CompactionStopResponse.class);
                      assertThat(response.status()).isEqualTo(CompactionStopStatus.SUBMITTED);
                      assertThat(response.compactionType()).isEqualTo("VALIDATION");
                      assertThat(response.compactionId()).isEqualTo("xyz-456");
                  });
                  ctx.completeNow();
              }));
    }

    @Test
    void testMissingBothFields(VertxTestContext ctx)
    {
        WebClient client = WebClient.create(vertx);
        String payload = "{}";
        client.put(server.actualPort(), "127.0.0.1", TEST_ROUTE)
              .sendBuffer(buffer(payload), ctx.succeeding(resp -> {
                  ctx.verify(() -> {
                      assertThat(resp.statusCode()).isEqualTo(BAD_REQUEST.code());
                      verify(mockCompactionManagerOperations, times(0))
                      .stopCompaction(anyString());
                  });
                  ctx.completeNow();
              }));
    }

    @Test
    void testBothFieldsEmpty(VertxTestContext ctx)
    {
        WebClient client = WebClient.create(vertx);
        String payload = "{\"compactionType\":\"\",\"compactionId\":\"\"}";
        client.put(server.actualPort(), "127.0.0.1", TEST_ROUTE)
              .sendBuffer(buffer(payload), ctx.succeeding(resp -> {
                  ctx.verify(() -> {
                      assertThat(resp.statusCode()).isEqualTo(BAD_REQUEST.code());
                      verify(mockCompactionManagerOperations, times(0))
                      .stopCompaction(anyString());
                  });
                  ctx.completeNow();
              }));
    }

    @Test
    void testInvalidCompactionType(VertxTestContext ctx)
    {
        // Configure mock to throw exception for invalid compaction type
        doThrow(new IllegalArgumentException("compaction type INVALID_TYPE is not supported"))
            .when(mockCompactionManagerOperations).stopCompaction("INVALID_TYPE");

        WebClient client = WebClient.create(vertx);
        String payload = "{\"compactionType\":\"INVALID_TYPE\"}";
        client.put(server.actualPort(), "127.0.0.1", TEST_ROUTE)
              .sendBuffer(buffer(payload), ctx.succeeding(resp -> {
                  ctx.verify(() -> {
                      assertThat(resp.statusCode()).isEqualTo(BAD_REQUEST.code());
                      verify(mockCompactionManagerOperations, times(1))
                      .stopCompaction(eq("INVALID_TYPE"));
                  });
                  ctx.completeNow();
              }));
    }

    @Test
    void testMalformedJson(VertxTestContext ctx)
    {
        WebClient client = WebClient.create(vertx);
        String payload = "{invalid json";
        client.put(server.actualPort(), "127.0.0.1", TEST_ROUTE)
              .sendBuffer(buffer(payload), ctx.succeeding(resp -> {
                  ctx.verify(() -> {
                      assertThat(resp.statusCode()).isEqualTo(BAD_REQUEST.code());
                      verify(mockCompactionManagerOperations, times(0))
                      .stopCompaction(anyString());
                  });
                  ctx.completeNow();
              }));
    }

    @Test
    void testTrimWhitespace(VertxTestContext ctx)
    {
        WebClient client = WebClient.create(vertx);
        String payload = "{\"compactionType\":\"  COMPACTION  \"}";
        client.put(server.actualPort(), "127.0.0.1", TEST_ROUTE)
              .sendBuffer(buffer(payload), ctx.succeeding(resp -> {
                  ctx.verify(() -> {
                      // Should trim and pass as uppercase enum name
                      verify(mockCompactionManagerOperations, times(1))
                      .stopCompaction(eq("COMPACTION"));

                      assertThat(resp.statusCode()).isEqualTo(OK.code());
                  });
                  ctx.completeNow();
              }));
    }

    @Test
    void testCaseInsensitiveCompactionType(VertxTestContext ctx)
    {
        WebClient client = WebClient.create(vertx);
        String payload = "{\"compactionType\":\"compaction\"}";
        client.put(server.actualPort(), "127.0.0.1", TEST_ROUTE)
              .sendBuffer(buffer(payload), ctx.succeeding(resp -> {
                  ctx.verify(() -> {
                      // Should accept lowercase input and send uppercase to Cassandra
                      verify(mockCompactionManagerOperations, times(1))
                      .stopCompaction(eq("COMPACTION"));

                      assertThat(resp.statusCode()).isEqualTo(OK.code());
                  });
                  ctx.completeNow();
              }));
    }

    @Test
    void testAllSupportedCompactionTypes(VertxTestContext ctx) throws InterruptedException
    {
        String[] supportedTypes = {
            "COMPACTION", "VALIDATION", "KEY_CACHE_SAVE", "ROW_CACHE_SAVE",
            "COUNTER_CACHE_SAVE", "CLEANUP", "SCRUB", "UPGRADE_SSTABLES",
            "INDEX_BUILD", "TOMBSTONE_COMPACTION", "ANTICOMPACTION",
            "VERIFY", "VIEW_BUILD", "INDEX_SUMMARY", "RELOCATE",
            "GARBAGE_COLLECT", "MAJOR_COMPACTION"
        };

        WebClient client = WebClient.create(vertx);

        CountDownLatch expectedCalls = new CountDownLatch(supportedTypes.length);
        for (String type : supportedTypes)
        {
            String payload = "{\"compactionType\":\"" + type + "\"}";
            client.put(server.actualPort(), "127.0.0.1", TEST_ROUTE)
                  .sendBuffer(buffer(payload), ctx.succeeding(resp -> {
                      ctx.verify(() -> {
                          assertThat(resp.statusCode()).isEqualTo(OK.code());
                          CompactionStopResponse response = resp.bodyAsJson(CompactionStopResponse.class);
                          assertThat(response.status()).isEqualTo(CompactionStopStatus.SUBMITTED);
                          expectedCalls.countDown();
                      });
                  }));
        }
        expectedCalls.await(30, TimeUnit.SECONDS);
        ctx.completeNow();
    }

    /**
     * Test guice module for {@link CompactionStopHandler} tests
     */
    class CompactionStopHandlerTestModule extends AbstractModule
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
            when(delegate.compactionManagerOperations()).thenReturn(mockCompactionManagerOperations);
            when(instanceMetadata.delegate()).thenReturn(delegate);

            InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
            when(mockInstancesMetadata.instances()).thenReturn(Collections.singletonList(instanceMetadata));
            when(mockInstancesMetadata.instanceFromId(instanceId)).thenReturn(instanceMetadata);
            when(mockInstancesMetadata.instanceFromHost(host)).thenReturn(instanceMetadata);

            return mockInstancesMetadata;
        }
    }
}
