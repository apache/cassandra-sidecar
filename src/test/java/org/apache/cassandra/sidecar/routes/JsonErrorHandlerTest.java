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

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.handler.TimeoutHandler;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link JsonErrorHandler} class
 */
@ExtendWith(VertxExtension.class)
class JsonErrorHandlerTest
{
    private Vertx vertx;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
    }

    @AfterEach
    void tearDown()
    {
        vertx.close();
    }

    @Test
    public void testHttpExceptionHandling() throws InterruptedException
    {
        testHelper("/http-exception", result ->
        {
            assertThat(result.statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
            JsonObject response = result.bodyAsJsonObject();
            assertThat(response.getString("status")).isEqualTo("Fail");
            assertThat(response.getString("message")).isEqualTo("Payload is written to JSON");
        }, false);
    }

    @Test
    public void testRequestTimeoutHandling() throws InterruptedException
    {
        testHelper("/timeout", result ->
        {
            assertThat(result.statusCode()).isEqualTo(HttpResponseStatus.REQUEST_TIMEOUT.code());
            assertThat(result.bodyAsJsonObject().getString("status")).isEqualTo("Request Timeout");
        }, false);
    }

    @ParameterizedTest(name = "unhandled throwable displayExceptionDetails={0}")
    @ValueSource(booleans = { false, true })
    public void testUnhandledThrowable(boolean displayExceptionDetails)
    throws InterruptedException
    {
        testHelper("/RuntimeException", result ->
        {
            assertThat(result.statusCode()).isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
            JsonObject jsonResponse = result.bodyAsJsonObject();
            assertThat(jsonResponse.getString("status")).isEqualTo("Internal Server Error");
            if (displayExceptionDetails)
            {
                assertThat(jsonResponse.getInteger("code")).isEqualTo(500);
                assertThat(jsonResponse.getString("message")).isEqualTo("oops");
                assertThat(jsonResponse.getJsonArray("stack")).isNotEmpty();
            }
            else
            {
                assertThat(jsonResponse.size()).isEqualTo(1);
            }
        }, displayExceptionDetails);
    }

    private void testHelper(String requestURI,
                            Consumer<HttpResponse<Buffer>> consumer,
                            boolean displayExceptionDetails) throws InterruptedException
    {
        VertxTestContext context = new VertxTestContext();

        Router router = getRouter(vertx, displayExceptionDetails);

        HttpServer server = vertx.createHttpServer()
                                 .requestHandler(router)
                                 .listen(0, context.succeedingThenComplete());

        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();

        VertxTestContext testContext = new VertxTestContext();
        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "localhost", requestURI)
              .as(BodyCodec.buffer())
              .send(testContext.succeeding(response -> testContext.verify(() -> {
                  assertThat(response.getHeader(HttpHeaders.CONTENT_TYPE.toString()))
                  .isEqualTo("application/json");
                  consumer.accept(response);
                  testContext.completeNow();
                  server.close();
              })));
        assertThat(testContext.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    private Router getRouter(Vertx vertx, boolean displayExceptionDetails)
    {
        Router router = Router.router(vertx);
        router.route().failureHandler(new JsonErrorHandler(displayExceptionDetails))
              .handler(TimeoutHandler.create(250, HttpResponseStatus.REQUEST_TIMEOUT.code()));
        router.get("/http-exception").handler(ctx -> {
            throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(), "Payload is written to JSON");
        });
        router.get("/timeout").handler(ctx -> {
            // wait for the timeout
        });
        router.get("/RuntimeException").handler(ctx -> {
            throw new RuntimeException("oops");
        });
        return router;
    }
}
