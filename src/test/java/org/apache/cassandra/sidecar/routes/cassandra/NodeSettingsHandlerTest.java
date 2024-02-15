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
package org.apache.cassandra.sidecar.routes.cassandra;

import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.common.NodeSettings;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.NODE_SETTINGS_ROUTE;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class NodeSettingsHandlerTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeSettingsHandlerTest.class);
    private static final String URI_WITH_INSTANCE_ID = NODE_SETTINGS_ROUTE + "?instanceId=%s";

    private Vertx vertx;
    private Server server;

    @BeforeEach
    void setUp() throws InterruptedException
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(new TestModule()));
        server = injector.getInstance(Server.class);
        vertx = injector.getInstance(Vertx.class);

        VertxTestContext context = new VertxTestContext();
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);

        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() throws InterruptedException
    {
        CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    @Test
    void validRequestWithoutInstanceId(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "localhost", NODE_SETTINGS_ROUTE)
              .as(BodyCodec.buffer())
              .send(resp -> {
                  assertThat(resp.result().statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  validateNodeSettings(resp);
                  context.completeNow();
              });
    }

    @Test
    void validRequestWithInstanceId(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "localhost", String.format(URI_WITH_INSTANCE_ID, "1"))
              .as(BodyCodec.buffer())
              .send(resp -> {
                  assertThat(resp.result().statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  validateNodeSettings(resp);
                  context.completeNow();
              });
    }

    @Test
    void validRequestWithInvalidInstanceId(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "localhost", String.format(URI_WITH_INSTANCE_ID, "10"))
              .as(BodyCodec.buffer())
              .send(resp -> {
                  assertThat(resp.result().statusCode()).isEqualTo(HttpResponseStatus.NOT_FOUND.code());
                  JsonObject error = resp.result().bodyAsJsonObject();
                  assertThat(error.getString("status")).isEqualTo("Not Found");
                  assertThat(error.getString("message")).isEqualTo("Instance id '10' not found");
                  context.completeNow();
              });
    }

    static void validateNodeSettings(AsyncResult<HttpResponse<Buffer>> resp)
    {
        NodeSettings status = resp.result().bodyAsJson(NodeSettings.class);
        assertThat(status.partitioner()).isEqualTo("testPartitioner");
        assertThat(status.releaseVersion()).isEqualTo("testVersion");
        assertThat(status.sidecarVersion()).isEqualTo("testSidecar");
        assertThat(status.datacenter()).isEqualTo("testDC");
        assertThat(status.rpcAddress()).isEqualTo(InetAddress.getLoopbackAddress());
        assertThat(status.rpcPort()).isEqualTo(6475);
        assertThat(status.tokens()).isNotNull()
                                   .isNotEmpty()
                                   .containsExactly("testToken");
    }
}
