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
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.common.data.LifecycleCassandraState;
import org.apache.cassandra.sidecar.common.data.LifecycleStatus;
import org.apache.cassandra.sidecar.common.response.LifecycleInfoResponse;
import org.apache.cassandra.sidecar.exceptions.LifecycleTaskConflictException;
import org.apache.cassandra.sidecar.lifecycle.LifecycleManager;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.vertx.core.buffer.Buffer.buffer;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for {@link LifecycleUpdateHandler}
 */
@ExtendWith(VertxExtension.class)
public class LifecycleUpdateHandlerTest
{
    Vertx vertx;
    Server server;
    LifecycleManager mockLifecycleManager = mock(LifecycleManager.class);

    @BeforeEach
    void before() throws InterruptedException
    {
        Injector injector;
        Module testOverride = Modules.override(new TestModule()).with(new LifecycleUpdateHandlerTestModule());
        injector = Guice.createInjector(Modules.override(SidecarModules.all()).with(testOverride));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        VertxTestContext context = new VertxTestContext();
        server.start().onSuccess(s -> context.completeNow()).onFailure(context::failNow);
        context.awaitCompletion(5, TimeUnit.SECONDS);
        reset(mockLifecycleManager);
    }

    @AfterEach
    void after() throws InterruptedException
    {
        getBlocking(TestResourceReaper.create().with(server).close(), 60, TimeUnit.SECONDS, "Closing server");
    }

    @Test
    void testSuccessfulPutWithAcceptedResponse(VertxTestContext ctx) throws LifecycleTaskConflictException
    {
        WebClient client = WebClient.create(vertx);
        String payload = "{\"state\":\"start\"}";
        LifecycleInfoResponse expectedResponse = new LifecycleInfoResponse(LifecycleCassandraState.STOPPED,
                                                                           LifecycleCassandraState.RUNNING,
                                                                           LifecycleStatus.CONVERGING,
                                                                           "Submitted task to start instance");
        when(mockLifecycleManager.updateDesiredState("127.0.0.1", LifecycleCassandraState.RUNNING))
                                                                                .thenReturn(expectedResponse);
        client.put(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/lifecycle")
              .sendBuffer(buffer(payload), ctx.succeeding(resp -> {
                  ctx.verify(() -> {
                      verify(mockLifecycleManager, times(1)).updateDesiredState("127.0.0.1", LifecycleCassandraState.RUNNING);
                      assertThat(resp.bodyAsJson(LifecycleInfoResponse.class)).isEqualTo(expectedResponse);
                      assertThat(resp.statusCode()).isEqualTo(ACCEPTED.code());
                  });
                  ctx.completeNow();
              }));
    }

    @Test
    void testSuccessfulPutWithOKResponse(VertxTestContext ctx) throws LifecycleTaskConflictException
    {
        WebClient client = WebClient.create(vertx);
        String payload = "{\"state\":\"STOP\"}";
        LifecycleInfoResponse expectedResponse = new LifecycleInfoResponse(LifecycleCassandraState.STOPPED,
                                                                           LifecycleCassandraState.STOPPED,
                                                                           LifecycleStatus.CONVERGED,
                                                                           "Submitted task to stop instance");
        when(mockLifecycleManager.updateDesiredState("127.0.0.1", LifecycleCassandraState.STOPPED)).thenReturn(expectedResponse);
        client.put(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/lifecycle")
              .sendBuffer(buffer(payload), ctx.succeeding(resp -> {
                  ctx.verify(() -> {
                      verify(mockLifecycleManager, times(1)).updateDesiredState("127.0.0.1", LifecycleCassandraState.STOPPED);
                      assertThat(resp.bodyAsJson(LifecycleInfoResponse.class)).isEqualTo(expectedResponse);
                      assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  });
                  ctx.completeNow();
              }));
    }

    @Test
    void testSuccessfulPutWithFailedResponse(VertxTestContext ctx) throws LifecycleTaskConflictException
    {
        WebClient client = WebClient.create(vertx);
        String payload = "{\"state\":\"STOP\"}";
        LifecycleInfoResponse expectedResponse = new LifecycleInfoResponse(LifecycleCassandraState.RUNNING,
                                                                           LifecycleCassandraState.STOPPED,
                                                                           LifecycleStatus.DIVERGED,
                                                                           "Error while stopping instance");
        when(mockLifecycleManager.updateDesiredState("127.0.0.1", LifecycleCassandraState.STOPPED)).thenReturn(expectedResponse);
        client.put(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/lifecycle")
              .sendBuffer(buffer(payload), ctx.succeeding(resp -> {
                  ctx.verify(() -> {
                      verify(mockLifecycleManager, times(1)).updateDesiredState("127.0.0.1", LifecycleCassandraState.STOPPED);
                      assertThat(resp.bodyAsJson(LifecycleInfoResponse.class)).isEqualTo(expectedResponse);
                      assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                  });
                  ctx.completeNow();
              }));
    }

    @Test
    void testInvalidState(VertxTestContext ctx)
    {
        WebClient client = WebClient.create(vertx);
        String payload = "{\"state\":\"invalid\"}";
        client.put(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/lifecycle")
              .sendBuffer(buffer(payload), ctx.succeeding(resp -> {
                  ctx.verify(() -> {
                      assertThat(resp.statusCode()).isEqualTo(BAD_REQUEST.code());
                      verify(mockLifecycleManager, times(0)).updateDesiredState("127.0.0.1", LifecycleCassandraState.RUNNING);
                      verify(mockLifecycleManager, times(0)).updateDesiredState("127.0.0.1", LifecycleCassandraState.STOPPED);
                  });
                  ctx.completeNow();
              }));
    }

    @Test
    void testSubmitTaskAlreadyInProgress(VertxTestContext ctx) throws LifecycleTaskConflictException
    {
        // Setup mock to throw conflict exception
        doThrow(new LifecycleTaskConflictException("Cannot start host 127.0.0.1. Task already in progress for this host."))
        .when(mockLifecycleManager).updateDesiredState("127.0.0.1", LifecycleCassandraState.RUNNING);

        WebClient client = WebClient.create(vertx);
        String payload = "{\"state\":\"start\"}";
        client.put(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/lifecycle")
              .sendBuffer(buffer(payload), ctx.succeeding(resp -> {
                  ctx.verify(() -> {
                      assertThat(resp.statusCode()).isEqualTo(CONFLICT.code());
                      verify(mockLifecycleManager, times(1)).updateDesiredState("127.0.0.1", LifecycleCassandraState.RUNNING);
                  });
                  ctx.completeNow();
              }));
    }

    /**
     * Test guice module for {@link LifecycleUpdateHandler} tests
     */
    class LifecycleUpdateHandlerTestModule extends AbstractModule
    {

        @Provides
        @Singleton
        public LifecycleManager intentManager()
        {
            return mockLifecycleManager;
        }
    }
}
