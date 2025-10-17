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
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.common.data.Lifecycle.CassandraState;
import org.apache.cassandra.sidecar.common.data.Lifecycle.OperationStatus;
import org.apache.cassandra.sidecar.common.response.LifecycleInfoResponse;
import org.apache.cassandra.sidecar.lifecycle.LifecycleManager;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link LifecycleInfoHandler}
 */
@ExtendWith(VertxExtension.class)
public class LifecycleInfoHandlerTest
{

    Vertx vertx;
    Server server;
    LifecycleManager mockLifecycleManager = mock(LifecycleManager.class);

    @BeforeEach
    void before() throws InterruptedException
    {
        Injector injector;
        Module testOverride = Modules.override(new TestModule()).with(new LifecycleInfoHandlerTestModule());
        injector = Guice.createInjector(Modules.override(SidecarModules.all()).with(testOverride));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        VertxTestContext context = new VertxTestContext();
        server.start().onSuccess(s -> context.completeNow()).onFailure(context::failNow);
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void after() throws InterruptedException
    {
        getBlocking(TestResourceReaper.create().with(server).close(), 60, TimeUnit.SECONDS, "Closing server");
    }

    @Test
    void testGetLifecycleInfoNoTask(VertxTestContext context)
    {
        // Mock response for initial state with no task submitted
        LifecycleInfoResponse expectedResponse = new LifecycleInfoResponse(CassandraState.RUNNING, CassandraState.RUNNING,
                OperationStatus.CONVERGED, "All good");
        when(mockLifecycleManager.getLifecycleInfo(anyString())).thenReturn(expectedResponse);

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/lifecycle")
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  LifecycleInfoResponse lifecycleResponse = response.bodyAsJson(LifecycleInfoResponse.class);
                  assertThat(lifecycleResponse).isEqualTo(expectedResponse);
                  context.completeNow();
              }));
    }


    @Test
    void testGetLifecycleInfoTaskInProgress(VertxTestContext context)
    {
        // Mock response for a task in progress
        LifecycleInfoResponse mockResponse = new LifecycleInfoResponse(
        CassandraState.STOPPED, CassandraState.RUNNING, OperationStatus.CONVERGING, null);
        when(mockLifecycleManager.getLifecycleInfo("127.0.0.1")).thenReturn(mockResponse);

        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/lifecycle";
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  LifecycleInfoResponse lifecycleResponse = response.bodyAsJson(LifecycleInfoResponse.class);
                  assertThat(lifecycleResponse).isNotNull();
                  assertThat(lifecycleResponse.currentState()).isEqualTo(CassandraState.STOPPED);
                  assertThat(lifecycleResponse.desiredState()).isEqualTo(CassandraState.RUNNING);
                  assertThat(lifecycleResponse.status()).isEqualTo(OperationStatus.CONVERGING);
                  assertThat(lifecycleResponse.lastUpdate()).isNull();
                  context.completeNow();
              }));
    }

    @Test
    void testGetLifecycleInfoFinished(VertxTestContext context)
    {
        // Mock response for a finished task
        LifecycleInfoResponse mockResponse = new LifecycleInfoResponse(
        CassandraState.RUNNING, CassandraState.RUNNING, OperationStatus.CONVERGED, "Host has started");
        when(mockLifecycleManager.getLifecycleInfo("127.0.0.1")).thenReturn(mockResponse);

        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/lifecycle";
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  LifecycleInfoResponse lifecycleResponse = response.bodyAsJson(LifecycleInfoResponse.class);
                  assertThat(lifecycleResponse).isNotNull();
                  assertThat(lifecycleResponse.currentState()).isEqualTo(CassandraState.RUNNING);
                  assertThat(lifecycleResponse.desiredState()).isEqualTo(CassandraState.RUNNING);
                  assertThat(lifecycleResponse.status()).isEqualTo(OperationStatus.CONVERGED);
                  assertThat(lifecycleResponse.lastUpdate()).isEqualTo("Host has started");
                  context.completeNow();
              }));
    }

    /**
     * Test guice module for {@link LifecycleInfoHandler} tests
     */
    class LifecycleInfoHandlerTestModule extends AbstractModule
    {

        @Provides
        @Singleton
        public LifecycleManager intentManager()
        {
            return mockLifecycleManager;
        }
    }
}
