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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Unit tests for {@link AbstractHandler}
 */
@ExtendWith(VertxExtension.class)
class AbstractHandlerTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHandlerTest.class);
    Vertx vertx;
    Server server;
    Injector injector;

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
    void testDoNotHandleReqWhenParsingParamsFails(VertxTestContext context) throws InterruptedException
    {
        injector = Guice.createInjector(Modules.override(new MainModule())
                                               .with(new TestModule()));
        vertx = injector.getInstance(Vertx.class);

        Router router = injector.getInstance(Router.class);
        InstanceMetadataFetcher metadataFetcher = injector.getInstance(InstanceMetadataFetcher.class);
        ExecutorPools executorPools = injector.getInstance(ExecutorPools.class);
        router.get("/fail/parsing/params")
              .handler(new FailsOnParameterParsing(context, metadataFetcher, executorPools));

        VertxTestContext serverStartContext = new VertxTestContext();
        server = injector.getInstance(Server.class);
        server.start()
              .onSuccess(s -> serverStartContext.completeNow())
              .onFailure(serverStartContext::failNow);
        serverStartContext.awaitCompletion(5, TimeUnit.SECONDS);

        WebClient client = WebClient.create(vertx);
        String testRoute = "/fail/parsing/params";
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(INTERNAL_SERVER_ERROR.code());
                  vertx.setTimer(1000, tid -> context.completeNow());
              }));
    }

    /**
     * A handler that will simulate a failure in the {@link #extractParamsOrThrow(RoutingContext)} method
     */
    static class FailsOnParameterParsing extends AbstractHandler<Object>
    {
        private final VertxTestContext testContext;

        protected FailsOnParameterParsing(VertxTestContext testContext,
                                          InstanceMetadataFetcher metadataFetcher,
                                          ExecutorPools executorPools)
        {
            super(metadataFetcher, executorPools, null);
            this.testContext = testContext;
        }

        @Override
        protected Object extractParamsOrThrow(RoutingContext context)
        {
            throw new RuntimeException("Simulated Exception");
        }

        @Override
        protected void handleInternal(RoutingContext context,
                                      HttpServerRequest httpRequest,
                                      String host,
                                      SocketAddress remoteAddress,
                                      Object request)
        {
            testContext.failNow("Should never reach here");
        }
    }
}
