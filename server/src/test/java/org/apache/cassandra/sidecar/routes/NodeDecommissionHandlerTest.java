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

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
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
import org.apache.cassandra.sidecar.common.response.NodeDecommissionResponse;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.RUNNING;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.SUCCEEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link NodeDecommissionHandler}
 */
@ExtendWith(VertxExtension.class)
public class NodeDecommissionHandlerTest
{
    static final Logger LOGGER = LoggerFactory.getLogger(NodeDecommissionHandlerTest.class);
    Vertx vertx;
    Server server;

    @Mock
    static
    StorageOperations mockStorageOperations;

    @BeforeEach
    void before() throws InterruptedException
    {
        Injector injector;
        MockitoAnnotations.openMocks(this);
        Module testOverride = Modules.override(new TestModule())
                                     .with(new NodeDecommissionHandlerTest.NodeDecommissionTestModule());
                                     //mockJobManager));
        injector = Guice.createInjector(Modules.override(new MainModule())
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
    void testDecommissionLongRunning(VertxTestContext context)
    {
        when(mockStorageOperations.decommission(anyBoolean())).thenAnswer(i -> {
            Uninterruptibles.sleepUninterruptibly(6000, TimeUnit.SECONDS);
            return SUCCEEDED;
        });

        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/node/decommission";
        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_ACCEPTED)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(ACCEPTED.code());
                  NodeDecommissionResponse decommissionResponse = response.bodyAsJson(NodeDecommissionResponse.class);
                  assertThat(decommissionResponse).isNotNull();
                  assertThat(decommissionResponse.status()).isEqualTo(RUNNING);
                  context.completeNow();
              }));
    }

    @Test
    void testDecommissionCompleted(VertxTestContext context)
    {
        when(mockStorageOperations.decommission(anyBoolean())).thenReturn(SUCCEEDED);
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/node/decommission";
        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  LOGGER.info("Decommission Response: {}", response.bodyAsString());

                  NodeDecommissionResponse decommissionResponse = response.bodyAsJson(NodeDecommissionResponse.class);
                  assertThat(decommissionResponse).isNotNull();
                  assertThat(decommissionResponse.status()).isEqualTo(SUCCEEDED);
                  assertThat(decommissionResponse.instance()).isEqualTo("127.0.0.1");
                  context.completeNow();
              }));
    }

    @Test
    void testDecommissionFailed(VertxTestContext context)
    {
        when(mockStorageOperations.decommission(anyBoolean())).thenThrow(new RuntimeException("Simulated failure"));
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/node/decommission";
        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_INTERNAL_SERVER_ERROR)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(INTERNAL_SERVER_ERROR.code());
                  context.completeNow();
              }));
    }

    /**
     * Test guice module for Node Decommission handler tests
     */
    static class NodeDecommissionTestModule extends AbstractModule
    {

        public NodeDecommissionTestModule()
        {
            MockitoAnnotations.openMocks(this);
        }

        @Provides
        @Singleton
        public InstancesConfig instanceConfig()
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

            InstancesConfig mockInstancesConfig = mock(InstancesConfig.class);
            when(mockInstancesConfig.instances()).thenReturn(Collections.singletonList(instanceMetadata));
            when(mockInstancesConfig.instanceFromId(instanceId)).thenReturn(instanceMetadata);
            when(mockInstancesConfig.instanceFromHost(host)).thenReturn(instanceMetadata);

            return mockInstancesConfig;
        }
    }

}
