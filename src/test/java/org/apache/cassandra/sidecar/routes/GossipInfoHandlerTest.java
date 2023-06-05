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
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.Configuration;
import org.apache.cassandra.sidecar.MainModule;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.ClusterMembershipOperations;
import org.apache.cassandra.sidecar.common.data.GossipInfoResponse;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link GossipInfoHandler}
 */
@ExtendWith(VertxExtension.class)
public class GossipInfoHandlerTest
{
    static final Logger LOGGER = LoggerFactory.getLogger(GossipInfoHandlerTest.class);
    Vertx vertx;
    Configuration config;
    HttpServer server;

    @SuppressWarnings("DataFlowIssue")
    @BeforeEach
    void before() throws InterruptedException
    {
        Injector injector;
        Module testOverride = Modules.override(new TestModule())
                                     .with(new GossipInfoHandlerTestModule());
        injector = Guice.createInjector(Modules.override(new MainModule())
                                               .with(testOverride));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(HttpServer.class);
        config = injector.getInstance(Configuration.class);
        VertxTestContext context = new VertxTestContext();
        server.listen(config.getPort(), config.getHost(), context.succeedingThenComplete());
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void after() throws InterruptedException
    {
        final CountDownLatch closeLatch = new CountDownLatch(1);
        server.close(res -> closeLatch.countDown());
        vertx.close();
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    @Test
    void testGetGossipInfo(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/gossip";
        client.get(config.getPort(), config.getHost(), testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  GossipInfoResponse gossipResponse = response.bodyAsJson(GossipInfoResponse.class);
                  assertThat(gossipResponse).isNotNull();
                  GossipInfoResponse.GossipInfo gossipInfo = gossipResponse.get("/127.0.0.1:7000");
                  assertThat(gossipInfo).isNotNull().hasSize(6);
                  assertThat(gossipInfo.generation()).isEqualTo("1668100877");
                  assertThat(gossipInfo.heartbeat()).isEqualTo("242");
                  assertThat(gossipInfo.load()).isEqualTo("88971.0");
                  assertThat(gossipInfo.statusWithPort())
                      .isEqualTo("NORMAL,-9223372036854775808");
                  assertThat(gossipInfo.sstableVersions())
                            .isEqualTo(Collections.singletonList("big-nb"));
                  assertThat(gossipInfo.tokens()).isEqualTo("<hidden>");
                  context.completeNow();
              }));
    }

    static class GossipInfoHandlerTestModule extends AbstractModule
    {
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
            CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
            ClusterMembershipOperations ops = mock(ClusterMembershipOperations.class);
            when(ops.gossipInfo()).thenReturn(SAMPLE_GOSSIP_INFO);
            when(delegate.clusterMembershipOperations()).thenReturn(ops);
            when(instanceMetadata.delegate()).thenReturn(delegate);

            InstancesConfig mockInstancesConfig = mock(InstancesConfig.class);
            when(mockInstancesConfig.instances()).thenReturn(Collections.singletonList(instanceMetadata));
            when(mockInstancesConfig.instanceFromId(instanceId)).thenReturn(instanceMetadata);
            when(mockInstancesConfig.instanceFromHost(host)).thenReturn(instanceMetadata);

            return mockInstancesConfig;
        }
    }

    private static final String SAMPLE_GOSSIP_INFO =
        "/127.0.0.1:7000\n" +
        "  generation:1668100877\n" +
        "  heartbeat:242\n" +
        "  LOAD:211:88971.0\n" +
        "  STATUS_WITH_PORT:19:NORMAL,-9223372036854775808\n" +
        "  SSTABLE_VERSIONS:6:big-nb\n" +
        "  TOKENS:18:<hidden>";
}
