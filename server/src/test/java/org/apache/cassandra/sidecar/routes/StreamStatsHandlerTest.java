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

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.StreamStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.StreamProgressStats;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.StreamManagerOperations;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.mockito.stubbing.Answer;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link StreamStatsHandler}
 */
@ExtendWith(VertxExtension.class)
public class StreamStatsHandlerTest
{

    static final Logger LOGGER = LoggerFactory.getLogger(StreamStatsHandlerTest.class);
    Vertx vertx;
    Server server;

    @BeforeEach
    void before() throws InterruptedException
    {
        Module testOverride = Modules.override(new TestModule())
                                     .with(new StreamingStatsTestModule());
        Injector injector = Guice.createInjector(Modules.override(new MainModule())
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
    void testStreamingStatsHandler(VertxTestContext context)
    {
        StreamingStatsTestModule.streamingStatsSupplier = () -> {
            return new StreamStatsResponse("NORMAL",
                                                                   new StreamProgressStats(7, 7, 1024, 1024, 0, 0, 0, 0));
        };


        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/stats/stream";
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  StreamStatsResponse statsResponse = response.bodyAsJson(StreamStatsResponse.class);
                  assertThat(statsResponse).isNotNull();
                  assertThat(statsResponse.operationMode()).isEqualTo("NORMAL");
                  context.completeNow();
              }));
    }

    @Test
    void testStreamingStatsHandlerFailure(VertxTestContext context)
    {
        StreamingStatsTestModule.streamingStatsSupplier = () -> {
            StreamStatsResponse response = new StreamStatsResponse("NORMAL",
                                                                   new StreamProgressStats(7, 7, 1024, 1024, 0, 0, 0, 0));
            return response;
        };


        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/stats/stream";
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  StreamStatsResponse statsResponse = response.bodyAsJson(StreamStatsResponse.class);
                  assertThat(statsResponse).isNotNull();
                  assertThat(statsResponse.operationMode()).isEqualTo("NORMAL");
                  context.completeNow();
              }));
    }


    static class StreamingStatsTestModule extends AbstractModule
    {
        static Supplier<StreamStatsResponse> streamingStatsSupplier;

        @Provides
        @Singleton
        public InstancesConfig instanceConfig()
        {
            int instanceId = 100;
            String host = "127.0.0.1";
            InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
            when(instanceMetadata.host()).thenReturn(host);
            when(instanceMetadata.port()).thenReturn(9042);
            when(instanceMetadata.id()).thenReturn(instanceId);
            when(instanceMetadata.stagingDir()).thenReturn("");
            CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
            StorageOperations ops = mock(StorageOperations.class);
            when(ops.getOperationMode()).thenAnswer((Answer<String>) invocation -> streamingStatsSupplier.get().operationMode());
            when(delegate.storageOperations()).thenReturn(ops);
            StreamManagerOperations streamMgrOps = mock(StreamManagerOperations.class);
            when(streamMgrOps.getStreamProgressStats())
            .thenAnswer((Answer<StreamProgressStats>) invocation -> streamingStatsSupplier.get().streamProgressStats());
            when(delegate.streamManagerOperations()).thenReturn(streamMgrOps);

            when(instanceMetadata.delegate()).thenReturn(delegate);

            InstancesConfig mockInstancesConfig = mock(InstancesConfig.class);
            when(mockInstancesConfig.instances()).thenReturn(Collections.singletonList(instanceMetadata));
            when(mockInstancesConfig.instanceFromId(instanceId)).thenReturn(instanceMetadata);
            when(mockInstancesConfig.instanceFromHost(host)).thenReturn(instanceMetadata);

            return mockInstancesConfig;
        }
    }
}
