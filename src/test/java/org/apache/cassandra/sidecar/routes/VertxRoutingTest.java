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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class VertxRoutingTest
{
    private Vertx vertx;
    private HttpServer server;
    private WebClient client;

    @BeforeEach
    void setup(VertxTestContext context)
    {
        vertx = Vertx.vertx();
        server = vertx.createHttpServer();
        client = WebClient.create(vertx, new WebClientOptions());
        context.completeNow();
    }

    @AfterEach
    void teardown(VertxTestContext context) throws Exception
    {
        if (vertx == null)
        {
            return;
        }
        if (client != null)
        {
            client.close();
        }
        vertx.close(result -> context.completeNow());
        assertThat(context.awaitCompletion(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void testRoutesWithSameOrder(VertxTestContext context) throws Exception
    {
        /*
         * For routes declared with same order, they should continue serving requests
         */
        CountDownLatch serverReady = new CountDownLatch(1);
        Router router = Router.router(vertx);
        router.get("/endpoint1").order(1).handler(ctx -> {
            ctx.response().end("Endpoint 1 OK");
        });
        router.get("/endpoint2").order(1).handler(ctx -> {
            ctx.response().end("Endpoint 2 OK");
        });

        server.requestHandler(router).listen(0, result -> serverReady.countDown());
        assertThat(serverReady.await(10, TimeUnit.SECONDS))
        .isTrue()
        .describedAs("Server should be up");

        asyncVerifyRequest("/endpoint1", context, response -> {
            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
            assertThat(response.bodyAsString()).isEqualTo("Endpoint 1 OK");
        });

        asyncVerifyRequest("/endpoint2", context, response -> {
            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
            assertThat(response.bodyAsString()).isEqualTo("Endpoint 2 OK");
        });
    }

    @Test
    void testHandlersWithSameRouteSameOrder(VertxTestContext context) throws Exception
    {
        /*
         * For handlers added to the routes (but same path) with the same order, it is evaluated as the adding order.
         */
        CountDownLatch serverReady = new CountDownLatch(1);
        Router router = Router.router(vertx);
        router.get("/endpoint").order(1).handler(ctx -> {
            ctx.response()
               .setChunked(true) // required to be `true` for adding data from multiple handlers
               .write("handler 1\n");
            ctx.next();
        });
        router.get("/endpoint").order(1).handler(ctx -> {
            ctx.response().end("handler 2");
        });

        server.requestHandler(router).listen(0, result -> serverReady.countDown());
        assertThat(serverReady.await(10, TimeUnit.SECONDS))
        .isTrue()
        .describedAs("Server should be up");

        asyncVerifyRequest("/endpoint", context, response -> {
            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
            assertThat(response.bodyAsString()).isEqualTo("handler 1\n" +
                                                          "handler 2");
        });
    }

    @Test
    void testLeveledRoutesWithSameOrder(VertxTestContext context) throws Exception
    {
        /*
         * For the leveled routes that is declared with the same order, the effective evaluation order is the adding order
         */
        CountDownLatch serverReady = new CountDownLatch(1);
        Router router = Router.router(vertx);
        router.route().order(1).handler(ctx -> {
            ctx.response()
               .setChunked(true) // required to be `true` for adding data from multiple handlers
               .write("root\n");
            ctx.next();
        });
        router.get("/endpoint").order(1).handler(ctx -> {
            ctx.response().end("endpoint");
        });

        server.requestHandler(router).listen(0, result -> serverReady.countDown());
        assertThat(serverReady.await(10, TimeUnit.SECONDS))
        .isTrue()
        .describedAs("Server should be up");

        asyncVerifyRequest("/endpoint", context, response -> {
            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
            assertThat(response.bodyAsString()).isEqualTo("root\n" +
                                                          "endpoint");
        });
    }

    @Test
    void testLeveledRoutesWithSameOrderReversedDeclaration(VertxTestContext context) throws Exception
    {
        /*
         * Similar to testLeveledRoutesWithSameOrder, but the root route is declared after `/endpoint`.
         * Since `/endpoint` ends the response and does not forward the evaluation, the root route handler is not called.
         */
        AtomicBoolean rootEvaluated = new AtomicBoolean(false);
        CountDownLatch serverReady = new CountDownLatch(1);
        Router router = Router.router(vertx);
        // NOTE: the routes declaration is swapped
        router.get("/endpoint").order(1).handler(ctx -> {
            ctx.response().end("endpoint");
        });
        router.route().order(1).handler(ctx -> {
            rootEvaluated.set(true);
            ctx.response()
               .setChunked(true) // required to be `true` for adding data from multiple handlers
               .write("root\n");
            ctx.next();
        });

        server.requestHandler(router).listen(0, result -> serverReady.countDown());
        assertThat(serverReady.await(10, TimeUnit.SECONDS))
        .isTrue()
        .describedAs("Server should be up");

        asyncVerifyRequest("/endpoint", context, response -> {
            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
            assertThat(response.bodyAsString()).isEqualTo("endpoint");
            assertThat(rootEvaluated.get()).isFalse().describedAs("Root route handler is not reached");
        });
    }

    private void asyncVerifyRequest(String requestURI,
                                    VertxTestContext context,
                                    Consumer<HttpResponse<Buffer>> responseConsumer)
    {
        client.get(server.actualPort(), "localhost", requestURI)
              .send(context.succeeding(response -> {
                  context.verify(() -> responseConsumer.accept(response))
                         .completeNow();
              }));
    }
}

