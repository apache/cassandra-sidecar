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
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.handler.LoggerFormatter;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

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
    private final Logger logger = mock(Logger.class);
    private HttpServer server;
    private FakeLoggerHandler loggerHandler;

    @BeforeEach
    void setUp() throws InterruptedException
    {
        loggerHandler = new FakeLoggerHandler(logger);
        Injector injector = Guice.createInjector(Modules.override(Modules.override(new MainModule())
                                                                         .with(new TestModule()))
                                                        .with(binder -> binder.bind(LoggerHandler.class)
                                                                              .toInstance(loggerHandler)));
        vertx = injector.getInstance(Vertx.class);
        Router router = injector.getInstance(Router.class);

        router.get("/fake-route").handler(promise -> promise.json("done"));

        VertxTestContext context = new VertxTestContext();
        server = injector.getInstance(HttpServer.class);
        server.listen(0, context.succeedingThenComplete());

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

    @DisplayName("Custom log handler is invoked for every request")
    @Test
    public void testInjectedLoggerHandlerIsCalledWhenARequestIsServed(VertxTestContext testContext)
    {
        WebClient client = WebClient.create(vertx);
        Handler<HttpResponse<String>> responseVerifier = response -> testContext.verify(() -> {
            verify(logger, times(1)).info("{}", HttpResponseStatus.OK.code());
            testContext.completeNow();
        });
        client.get(server.actualPort(), "localhost", "/fake-route")
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
