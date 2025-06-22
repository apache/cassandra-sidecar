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
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.vertx.core.buffer.Buffer.buffer;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for {@link NativeUpdateHandler}
 */
@ExtendWith(VertxExtension.class)
public class NativeUpdateHandlerTest
{
    Vertx vertx;
    Server server;
    StorageOperations mockStorageOperations = mock(StorageOperations.class);

    @BeforeEach
    void before() throws InterruptedException
    {
        Injector injector;
        Module testOverride = Modules.override(new TestModule()).with(new NativeUpdateHandlerTest.NativeUpdateHandlerTestModule());
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
    void testStartNative(VertxTestContext ctx)
    {
        WebClient client = WebClient.create(vertx);
        String payload = "{\"state\":\"start\"}";
        client.put(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/native").sendBuffer(buffer(payload), ctx.succeeding(resp -> {
            ctx.verify(() -> {
                verify(mockStorageOperations, times(1)).startNativeTransport();

                assertThat(resp.statusCode()).isEqualTo(OK.code());
                JsonObject json = resp.bodyAsJsonObject();
                assertThat(json.getMap().get("status")).isEqualTo("OK");
            });
            ctx.completeNow();
        }));
    }

    @Test
    void testStopNative(VertxTestContext ctx)
    {
        WebClient client = WebClient.create(vertx);
        String payload = "{\"state\":\"stop\"}";
        client.put(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/native").sendBuffer(buffer(payload), ctx.succeeding(resp -> {
            ctx.verify(() -> {
                verify(mockStorageOperations, times(1)).stopNativeTransport();

                assertThat(resp.statusCode()).isEqualTo(OK.code());
                JsonObject json = resp.bodyAsJsonObject();
                assertThat(json.getMap().get("status")).isEqualTo("OK");
            });
            ctx.completeNow();
        }));
    }

    @Test
    void testInvalidState(VertxTestContext ctx)
    {
        WebClient client = WebClient.create(vertx);
        String payload = "{\"state\":\"foo\"}";
        client.put(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/native").sendBuffer(buffer(payload), ctx.succeeding(resp -> {
            ctx.verify(() -> {
                assertThat(resp.statusCode()).isEqualTo(BAD_REQUEST.code());
                verify(mockStorageOperations, times(0)).startNativeTransport();
                verify(mockStorageOperations, times(0)).stopNativeTransport();
            });
            ctx.completeNow();
        }));
    }


    /**
     * Test guice module for {@link NativeUpdateHandler}
     */
    class NativeUpdateHandlerTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public InstancesMetadata instanceMetadata()
        {
            int instanceId = 100;
            String host = "127.0.0.1";
            InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
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
