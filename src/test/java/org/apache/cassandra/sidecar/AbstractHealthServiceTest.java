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

import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.routes.HealthService;
import static org.mockito.Mockito.when;

/**
 * Provides basic tests shared between SSL and normal http health services
 */
public abstract class AbstractHealthServiceTest
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractHealthServiceTest.class);
    private HealthService service;
    private Vertx vertx;
    private Configuration config;
    private HttpServer server;
    private CassandraAdapterDelegate cassandra;

    public abstract boolean isSslEnabled();

    public AbstractModule getTestModule()
    {
        if (isSslEnabled())
            return new TestSslModule();

        return new TestModule();
    }

    @BeforeEach
    void setUp() throws InterruptedException
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(getTestModule()));
        server = injector.getInstance(HttpServer.class);
        cassandra = injector.getInstance(CassandraAdapterDelegate.class);

        service = injector.getInstance(HealthService.class);
        vertx = injector.getInstance(Vertx.class);
        config = injector.getInstance(Configuration.class);

        VertxTestContext context = new VertxTestContext();
        server.listen(config.getPort(), context.completing());

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

    @DisplayName("Should return HTTP 200 OK when check=True")
    @Test
    public void testHealthCheckReturnsCassandraOK(VertxTestContext testContext)
    {
        when(cassandra.isUp()).thenReturn(true);
        WebClient client = getClient();

        client.get(config.getPort(), "localhost", "/api/v1/__health")
              .as(BodyCodec.string())
              .ssl(isSslEnabled())
              .send(testContext.succeeding(response -> testContext.verify(() ->
              {
                  Assert.assertEquals(200, response.statusCode());
                  Assert.assertEquals("{\"status\":{\"cassandra\":\"OK\"}}", response.body());
                  testContext.completeNow();
              })));
    }

    private WebClient getClient()
    {
        return WebClient.create(vertx, getWebClientOptions());
    }

    private WebClientOptions getWebClientOptions()
    {
        WebClientOptions options = new WebClientOptions();
        if (isSslEnabled())
        {
            options.setTrustStoreOptions(new JksOptions().setPath("src/test/resources/certs/ca.p12")
                                                         .setPassword("password"));
        }
        return options;
    }

    @DisplayName("Should return HTTP 200 NOT OK when check=False")
    @Test
    public void testHealthCheckReturnsCassandraNOTOK(VertxTestContext testContext)
    {

        when(cassandra.isUp()).thenReturn(false);
        WebClient client = getClient();

        client.get(config.getPort(), "localhost", "/api/v1/__health")
              .as(BodyCodec.string())
              .ssl(isSslEnabled())
              .send(testContext.succeeding(response -> testContext.verify(() ->
              {
                  Assert.assertEquals(200, response.statusCode());
                  Assert.assertEquals("{\"status\":{\"cassandra\":\"NOT_OK\"}}", response.body());
                  testContext.completeNow();
              })));
    }
}
