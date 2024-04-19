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

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Provides basic tests shared between SSL and normal http health services
 */
public abstract class AbstractHealthServiceTest
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractHealthServiceTest.class);

    @TempDir
    private Path certPath;
    private Vertx vertx;
    private Server server;
    TestModule testModule;

    public abstract boolean isSslEnabled();

    TestModule testModule()
    {
        if (isSslEnabled())
            return new TestSslModule(certPath);

        return new TestModule();
    }

    @BeforeEach
    void setUp() throws InterruptedException
    {
        testModule = testModule();
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(testModule));
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
        final CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            logger.info("Close event received before timeout.");
        else
            logger.error("Close event timed out.");
    }

    @DisplayName("Should return HTTP 200 OK if sidecar server is running")
    @Test
    void testSidecarHealthCheckReturnsOK(VertxTestContext testContext)
    {
        WebClient client = client();

        client.get(server.actualPort(), "localhost", "/api/v1/__health")
              .as(BodyCodec.string())
              .ssl(isSslEnabled())
              .send(testContext.succeeding(response -> testContext.verify(() ->
              {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  assertThat(response.body()).isEqualTo("{\"status\":\"OK\"}");
                  testContext.completeNow();
              })));
    }

    private WebClient client()
    {
        return WebClient.create(vertx, webClientOptions());
    }

    private WebClientOptions webClientOptions()
    {
        WebClientOptions options = new WebClientOptions();
        if (isSslEnabled())
        {
            options.setTrustStoreOptions(new JksOptions().setPath("src/test/resources/certs/ca.p12")
                                                         .setPassword("password"));
        }
        return options;
    }

    @DisplayName("Should return HTTP 200 OK when cassandra instance is up")
    @Test
    void testHealthCheckReturns200OK(VertxTestContext testContext)
    {
        WebClient client = client();

        client.get(server.actualPort(), "localhost", "/api/v1/cassandra/__health")
              .as(BodyCodec.string())
              .ssl(isSslEnabled())
              .send(testContext.succeeding(response -> testContext.verify(() ->
              {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  assertThat(response.body()).isEqualTo("{\"status\":\"OK\"}");
                  testContext.completeNow();
              })));
    }

    @DisplayName("Should return HTTP 503 Failure when instance is down with query param")
    @Test
    void testHealthCheckReturns503FailureWithQueryParam(VertxTestContext testContext)
    {
        testModule.delegate.setIsNativeUp(false);
        WebClient client = client();

        client.get(server.actualPort(), "localhost", "/api/v1/cassandra/__health?instanceId=2")
              .as(BodyCodec.string())
              .ssl(isSslEnabled())
              .send(testContext.succeeding(response -> testContext.verify(() ->
              {
                  assertThat(response.statusCode()).isEqualTo(SERVICE_UNAVAILABLE.code());
                  assertThat(response.body()).isEqualTo("{\"status\":\"NOT_OK\"}");
                  testContext.completeNow();
              })));
    }

    @DisplayName("Should return HTTP 404 (NOT FOUND) when instance is not found")
    @Test
    void testHealthCheckReturns404NotFound(VertxTestContext testContext)
    {
        WebClient client = client();

        // instance with ID=400 does not exist
        client.get(server.actualPort(), "localhost", "/api/v1/cassandra/__health?instanceId=400")
              .as(BodyCodec.string())
              .ssl(isSslEnabled())
              .send(testContext.succeeding(response -> testContext.verify(() ->
              {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  assertThat(response.body())
                  .isEqualTo("{\"status\":\"Not Found\",\"code\":404,\"message\":\"Instance id '400' not found\"}");
                  testContext.completeNow();
              })));
    }
}
