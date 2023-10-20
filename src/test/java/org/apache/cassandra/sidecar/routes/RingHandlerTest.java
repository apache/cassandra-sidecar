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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.StorageOperations;
import org.apache.cassandra.sidecar.common.data.RingEntry;
import org.apache.cassandra.sidecar.common.data.RingResponse;
import org.apache.cassandra.sidecar.common.exceptions.JmxAuthenticationException;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.mockito.stubbing.Answer;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link RingHandler}
 */
@ExtendWith(VertxExtension.class)
class RingHandlerTest
{
    static final Logger LOGGER = LoggerFactory.getLogger(RingHandlerTest.class);
    Vertx vertx;
    Server server;

    @BeforeEach
    void before() throws InterruptedException
    {
        Module testOverride = Modules.override(new TestModule())
                                     .with(new RingHandlerTestModule());
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

    @ParameterizedTest
    @ValueSource(strings = { "i_❤_u", "invalid with spaces", "... is not allowed" })
    void testInvalidKeyspace(String keyspace)
    {
        VertxTestContext context = new VertxTestContext();
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/ring/keyspaces/" + keyspace;
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .send(context.succeedingThenComplete());
    }

    @Test
    void testGetRing(VertxTestContext context)
    {
        int ringSize = 10;
        RingHandlerTestModule.ringSupplier = () -> {
            RingResponse response = new RingResponse(ringSize);
            for (int i = 1; i <= ringSize; i++)
            {
                RingEntry.Builder builder = new RingEntry.Builder()
                                            .address("127.0.0." + i)
                                            .hostId("hostId" + i)
                                            .datacenter("dc1")
                                            .state("Normal")
                                            .status("Up")
                                            .token(String.valueOf(i));
                response.add(builder.build());
            }
            return response;
        };
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/ring";
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  RingResponse ringResponse = response.bodyAsJson(RingResponse.class);
                  assertThat(ringResponse).isNotNull()
                                          .hasSize(ringSize);
                  // verify the json is ordered and verify each of the entry
                  for (int i = 1; i <= ringSize; i++)
                  {
                      RingEntry entry = ringResponse.poll();
                      assertThat(entry).isNotNull();
                      assertThat(entry.address()).isEqualTo("127.0.0." + i);
                      assertThat(entry.hostId()).isEqualTo("hostId" + i);
                      assertThat(entry.datacenter()).isEqualTo("dc1");
                      assertThat(entry.state()).isEqualTo("Normal");
                      assertThat(entry.status()).isEqualTo("Up");
                      assertThat(entry.token()).isEqualTo(String.valueOf(i));
                  }

                  context.completeNow();
              }));
    }

    @Test
    void testGetRingFails(VertxTestContext context)
    {
        String errorMessage = "Ring is gone";
        RingHandlerTestModule.ringSupplier = () -> {
            throw new RuntimeException(errorMessage);
        };
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/ring";
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_INTERNAL_SERVER_ERROR)
              .send(context.succeeding(response -> {
                  JsonObject error = response.bodyAsJsonObject();
                  assertThat(error.getInteger("code")).isEqualTo(500);
                  assertThat(error.getString("message").contains(errorMessage));

                  context.completeNow();
              }));
    }

    @Test
    void testGetRingFailsOnUnavailableJmxConnectivity(VertxTestContext context)
    {
        String errorMessage = "Authentication failed! Invalid username or password";
        RingHandlerTestModule.ringSupplier = () -> {
            throw new JmxAuthenticationException(errorMessage);
        };
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/ring";
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_SERVICE_UNAVAILABLE)
              .send(context.succeeding(response -> {
                  JsonObject error = response.bodyAsJsonObject();
                  assertThat(error.getInteger("code")).isEqualTo(503);
                  assertThat(error.getString("status")).isEqualTo("Service Unavailable");
                  assertThat(error.getString("message").contains(errorMessage));

                  context.completeNow();
              }));
    }

    static class RingHandlerTestModule extends AbstractModule
    {
        static Supplier<RingResponse> ringSupplier;

        @Provides
        @Singleton
        public InstancesConfig instanceConfig() throws IOException
        {
            int instanceId = 100;
            String host = "127.0.0.1";
            InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
            when(instanceMetadata.host()).thenReturn(host);
            when(instanceMetadata.port()).thenReturn(9042);
            when(instanceMetadata.id()).thenReturn(instanceId);
            CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
            StorageOperations ops = mock(StorageOperations.class);
            when(ops.ring(any())).thenAnswer((Answer<RingResponse>) invocation -> ringSupplier.get());
            when(delegate.storageOperations()).thenReturn(ops);
            when(instanceMetadata.delegate()).thenReturn(delegate);

            InstancesConfig mockInstancesConfig = mock(InstancesConfig.class);
            when(mockInstancesConfig.instances()).thenReturn(Collections.singletonList(instanceMetadata));
            when(mockInstancesConfig.instanceFromId(instanceId)).thenReturn(instanceMetadata);
            when(mockInstancesConfig.instanceFromHost(host)).thenReturn(instanceMetadata);

            return mockInstancesConfig;
        }
    }
}
