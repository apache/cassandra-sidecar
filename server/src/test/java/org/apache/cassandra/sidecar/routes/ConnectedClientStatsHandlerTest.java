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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.cassandra.sidecar.common.response.ConnectedClientStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.ClientConnectionEntry;
import org.apache.cassandra.sidecar.common.server.MetricsOperations;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link ConnectedClientStatsHandler} class
 */
@ExtendWith(VertxExtension.class)
public class ConnectedClientStatsHandlerTest
{

    private static final int EXPECTED_TOTAL_CLIENTS = 10;
    private static final Map<String, Long> EXPECTED_CONNECTIONS_BY_USER = new HashMap<String, Long>()
    {
        {
        put("u1", 5L);
        put("u2", 5L);
        }
    };
    private static final List<ClientConnectionEntry> EXPECTED_STATS = Arrays.asList(
    new ClientConnectionEntry("1", 0, false, null, null, "5", "u1", 5,
                              "name", "version", "keyspace1", Collections.emptyMap(), null, Collections.emptyMap()),
    new ClientConnectionEntry("1", 0, false, null, null, "5", "u1", 5,
                              "name", "version", "keyspace1", Collections.emptyMap(), null, Collections.emptyMap()));
    static final Logger LOGGER = LoggerFactory.getLogger(ConnectedClientStatsHandlerTest.class);
    Vertx vertx;
    Server server;

    @BeforeEach
    void before() throws InterruptedException
    {
        Module testOverride = Modules.override(new TestModule())
                                     .with(new ConnectedClientStatsTestModule());
        Injector injector = Guice.createInjector(Modules.override(new MainModule())
                                                        .with(testOverride));

        server = injector.getInstance(Server.class);
        vertx = injector.getInstance(Vertx.class);
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
    void testHandlerStats(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/stats/connected-clients";
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  ConnectedClientStatsResponse statsResponse = response.bodyAsJson(ConnectedClientStatsResponse.class);
                  assertThat(statsResponse).isNotNull();
                  assertThat(statsResponse.totalConnectedClients()).isEqualTo(EXPECTED_TOTAL_CLIENTS);
                  assertThat(statsResponse.connectionsByUser()).hasSize(EXPECTED_CONNECTIONS_BY_USER.size());
                  context.completeNow();
              }));
    }

    static class ConnectedClientStatsTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public InstancesConfig instanceConfig()
        {
            ConnectedClientStatsResponse summaryResponse = new ConnectedClientStatsResponse(Collections.emptyList(),
                                                                                            EXPECTED_TOTAL_CLIENTS,
                                                                                            EXPECTED_CONNECTIONS_BY_USER);
            ConnectedClientStatsResponse statsResponse = new ConnectedClientStatsResponse(EXPECTED_STATS,
                                                                                          EXPECTED_TOTAL_CLIENTS,
                                                                                          EXPECTED_CONNECTIONS_BY_USER);

            final int instanceId = 100;
            final String host = "127.0.0.1";
            final InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
            when(instanceMetadata.host()).thenReturn(host);
            when(instanceMetadata.port()).thenReturn(9042);
            when(instanceMetadata.id()).thenReturn(instanceId);
            when(instanceMetadata.stagingDir()).thenReturn("");

            CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
            MetricsOperations mockMetricsOperations = mock(MetricsOperations.class);
            when(mockMetricsOperations.connectedClientStats(true)).thenReturn(summaryResponse);
            when(mockMetricsOperations.connectedClientStats(false)).thenReturn(statsResponse);
            when(delegate.metricsOperations()).thenReturn(mockMetricsOperations);
            when(instanceMetadata.delegate()).thenReturn(delegate);

            InstancesConfig mockInstancesConfig = mock(InstancesConfig.class);
            when(mockInstancesConfig.instances()).thenReturn(Collections.singletonList(instanceMetadata));
            when(mockInstancesConfig.instanceFromId(instanceId)).thenReturn(instanceMetadata);
            when(mockInstancesConfig.instanceFromHost(host)).thenReturn(instanceMetadata);

            return mockInstancesConfig;
        }
    }
}
