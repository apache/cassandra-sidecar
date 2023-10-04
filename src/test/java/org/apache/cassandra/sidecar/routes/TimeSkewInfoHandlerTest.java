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
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.common.data.TimeSkewResponse;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.utils.TimeProvider;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test timeskew info handler.
 */
@ExtendWith(VertxExtension.class)
class TimeSkewInfoHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(StreamSSTableComponentHandlerTest.class);
    private static final long TEST_TIMESTAMP = 12345L;
    private Vertx vertx;
    private Server server;

    @BeforeEach
    public void setUp() throws InterruptedException
    {
        Module customTimeProvider = binder -> binder.bind(TimeProvider.class).toInstance(() -> TEST_TIMESTAMP);
        Injector injector = Guice.createInjector(Modules.override(new MainModule())
                                                        .with(new TestModule(), customTimeProvider));
        this.vertx = injector.getInstance(Vertx.class);
        this.server = injector.getInstance(Server.class);
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
            logger.info("Close event received before timeout.");
        else
            logger.error("Close event timed out.");
    }

    @Test
    void testReturnsTimeSkew(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/time-skew";
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .as(BodyCodec.buffer())
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  TimeSkewResponse timeSkewRes = response.bodyAsJson(TimeSkewResponse.class);
                  assertThat(timeSkewRes.allowableSkewInMinutes).isEqualTo(60);
                  assertThat(timeSkewRes.currentTime).isEqualTo(TEST_TIMESTAMP);
                  context.completeNow();
              })));
    }
}
