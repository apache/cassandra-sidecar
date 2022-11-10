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
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import org.apache.cassandra.sidecar.Configuration;
import org.apache.cassandra.sidecar.MainModule;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.common.NodeSettings;
import org.apache.http.HttpStatus;


import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class CassandraSettingsServiceTest
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraSettingsService.class);
    private static final String URI = "/api/v1/cassandra/settings";
    private static final String URI_WITH_INSTANCE_ID = URI + "?instanceId=%s";

    private Vertx vertx;
    private Configuration config;
    private HttpServer server;

    @BeforeEach
    void setUp() throws InterruptedException
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(new TestModule()));
        server = injector.getInstance(HttpServer.class);
        vertx = injector.getInstance(Vertx.class);
        config = injector.getInstance(Configuration.class);

        VertxTestContext context = new VertxTestContext();
        server.listen(config.getPort(), config.getHost(), context.succeedingThenComplete());

        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() throws InterruptedException
    {
        final CountDownLatch closeLatch = new CountDownLatch(1);
        server.close(res -> closeLatch.countDown());
        vertx.close();
        if (closeLatch.await(60, TimeUnit.SECONDS))
            logger.info("Close event received before timeout.");
        else
            logger.error("Close event timed out.");
    }


    @Test
    public void validRequestWithoutInstanceId(VertxTestContext context) throws InterruptedException
    {
        WebClient client = WebClient.create(vertx);
        client.get(config.getPort(), "localhost", URI)
                .as(BodyCodec.buffer())
                .send(resp -> {
                    assertThat(resp.result().statusCode()).isEqualTo(HttpStatus.SC_OK);
                    NodeSettings status = resp.result().bodyAsJson(NodeSettings.class);
                    assertThat(status.partitioner()).isEqualTo("testPartitioner");
                    assertThat(status.releaseVersion()).isEqualTo("testVersion");
                    context.completeNow();
                });
        context.awaitCompletion(1, TimeUnit.MINUTES);
        client.close();
    }

    @Test
    public void validRequestWithInstanceId(VertxTestContext context) throws InterruptedException
    {
        WebClient client = WebClient.create(vertx);
        client.get(config.getPort(), "localhost", String.format(URI_WITH_INSTANCE_ID, "1"))
                .as(BodyCodec.buffer())
                .send(resp -> {
                    assertThat(resp.result().statusCode()).isEqualTo(HttpStatus.SC_OK);
                    NodeSettings status = resp.result().bodyAsJson(NodeSettings.class);
                    assertThat(status.partitioner()).isEqualTo("testPartitioner");
                    assertThat(status.releaseVersion()).isEqualTo("testVersion");
                    context.completeNow();
                });
        context.awaitCompletion(1, TimeUnit.MINUTES);
        client.close();
    }

    @Test
    public void validRequestWithInvalidInstanceId(VertxTestContext context) throws InterruptedException
    {
        WebClient client = WebClient.create(vertx);
        client.get(config.getPort(), "localhost", String.format(URI_WITH_INSTANCE_ID, "10"))
                .as(BodyCodec.buffer())
                .send(resp -> {
                    assertThat(resp.result().statusCode()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
                    assertThat(resp.result().bodyAsString()).isEqualTo("Instance id 10 not found");
                    context.completeNow();
                });
        context.awaitCompletion(1, TimeUnit.MINUTES);
        client.close();
    }
}
