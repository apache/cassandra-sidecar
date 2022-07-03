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

package org.apache.cassandra.sidecar;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.handler.LoggerFormatter;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for the {@link LoggerHandler} injection tests
 */
@DisplayName("LoggerHandler Injection Test")
@ExtendWith(VertxExtension.class)
public class LoggerHandlerInjectionTest
{
    private Vertx vertx;
    private Configuration config;
    private final Logger logger = mock(Logger.class);
    private HttpServer server;

    @BeforeEach
    void setUp() throws InterruptedException
    {
        FakeLoggerHandler loggerHandler = new FakeLoggerHandler(logger);
        Injector injector = Guice.createInjector(Modules.override(Modules.override(new MainModule())
                                                                         .with(new TestModule()))
                                                        .with(binder -> binder.bind(LoggerHandler.class)
                                                                              .toInstance(loggerHandler)));
        vertx = injector.getInstance(Vertx.class);
        config = injector.getInstance(Configuration.class);
        Router router = injector.getInstance(Router.class);

        router.get("/500-route").handler(p ->
                                         {
                                             throw new RuntimeException("Fails with 500");
                                         });

        router.get("/404-route").handler(p ->
                                         {
                                             throw new HttpException(NOT_FOUND.code(), "Sorry, it's not here");
                                         });

        router.get("/204-route").handler(p ->
                                         {
                                             throw new HttpException(NO_CONTENT.code(), "Sorry, no content");
                                         });

        VertxTestContext context = new VertxTestContext();
        server = injector.getInstance(HttpServer.class);
        server.listen(config.getPort(), context.succeedingThenComplete());

        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() throws InterruptedException
    {
        final CountDownLatch closeLatch = new CountDownLatch(1);
        server.close(res -> closeLatch.countDown());
        vertx.close();
        if (closeLatch.await(60, TimeUnit.SECONDS))
        {
            logger.info("Close event received before timeout.");
        }
        else
        {
            logger.error("Close event timed out.");
        }
    }

    @DisplayName("Should log at error level when the request fails with a 500 code")
    @Test
    public void testInjectedLoggerHandlerLogsAtErrorLevel(VertxTestContext testContext)
    {
        helper("/500-route", testContext, INTERNAL_SERVER_ERROR.code(),
               "Error 500: Internal Server Error");
    }

    @DisplayName("Should log at warn level when the request fails with a 404 error")
    @Test
    public void testInjectedLoggerHandlerLogsAtWarnLevel(VertxTestContext testContext)
    {
        helper("/404-route", testContext, NOT_FOUND.code(), "Error 404: Not Found");
    }

    @DisplayName("Should log at info level when the request returns with a 204 status code")
    @Test
    public void testInjectedLoggerHandlerLogsAtInfoLevel(VertxTestContext testContext)
    {
        helper("/204-route", testContext, NO_CONTENT.code(), null);
    }

    private void helper(String requestURI, VertxTestContext testContext, int expectedStatusCode, String expectedBody)
    {
        WebClient client = WebClient.create(vertx);
        Handler<HttpResponse<String>> responseVerifier = response -> testContext.verify(() ->
        {
            assertThat(response.statusCode()).isEqualTo(expectedStatusCode);
            if (expectedBody == null)
            {
                assertThat(response.body()).isNull();
            }
            else
            {
                assertThat(response.body()).isEqualTo(expectedBody);
            }
            testContext.completeNow();
            verify(logger, times(1)).info("{}", expectedStatusCode);
        });
        client.get(config.getPort(), "localhost", requestURI)
              .as(BodyCodec.string()).ssl(false)
              .send(testContext.succeeding(responseVerifier));
    }

    private static class FakeLoggerHandler implements LoggerHandler
    {
        private final Logger logger;

        FakeLoggerHandler(Logger logger)
        {
            this.logger = logger;
        }

        @Override
        public LoggerHandler customFormatter(Function<HttpServerRequest, String> formatter)
        {
            return this;
        }

        @Override
        public LoggerHandler customFormatter(LoggerFormatter formatter)
        {
            return this;
        }

        @Override
        public void handle(RoutingContext context)
        {
            HttpServerRequest request = context.request();
            context.addBodyEndHandler(v -> logger.info("{}", request.response().getStatusCode()));
            context.next();
        }
    }
}
