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

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.mocks.MockHealthCheck;
import org.apache.cassandra.sidecar.routes.HealthService;

import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@DisplayName("Health Service Test")
@ExtendWith(VertxExtension.class)
public class HealthServiceTest
{
    private MockHealthCheck check;
    private HealthService service;
    private Vertx vertx;
    private Configuration config;

    @BeforeEach
    void setUp()
    {
        Injector injector = Guice.createInjector(new TestModule(Vertx.vertx()));
        HttpServer server = injector.getInstance(HttpServer.class);
        Router router = injector.getInstance(Router.class);

        check = injector.getInstance(MockHealthCheck.class);
        service = injector.getInstance(HealthService.class);
        vertx = injector.getInstance(Vertx.class);
        config = injector.getInstance(Configuration.class);

        server.listen(config.getPort());
    }

    @AfterEach
    void tearDown()
    {
        vertx.close();
    }

    @DisplayName("Should return HTTP 200 OK when check=True")
    @Test
    public void testHealthCheckReturns200OK(VertxTestContext testContext)
    {
        check.setStatus(true);
        service.refreshNow();

        WebClient client = WebClient.create(vertx);

        client.get(config.getPort(), "localhost", "/api/v1/__health")
              .as(BodyCodec.string())
              .send(testContext.succeeding(response -> testContext.verify(() -> {
                  System.out.println(response.statusCode());
                  Assert.assertEquals(200, response.statusCode());
                  testContext.completeNow();
              })));
    }

    @DisplayName("Should return HTTP 503 Failure when check=False")
    @Test
    public void testHealthCheckReturns503Failure(VertxTestContext testContext)
    {
        check.setStatus(false);
        service.refreshNow();

        WebClient client = WebClient.create(vertx);

        client.get(config.getPort(), "localhost", "/api/v1/__health")
              .as(BodyCodec.string())
              .send(testContext.succeeding(response -> testContext.verify(() -> {
                  System.out.println(response.statusCode());
                  Assert.assertEquals(503, response.statusCode());
                  testContext.completeNow();
              })));
    }
}
