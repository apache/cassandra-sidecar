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

package org.apache.cassandra.sidecar.handlers.livemigration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_STATUS_ROUTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class LiveMigrationStatusHandlersTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationStatusHandlersTest.class);

    Server server;
    private Injector injector;
    private Vertx vertx;

    @BeforeEach
    public void before(@TempDir Path tempDir) throws InterruptedException
    {
        injector = Guice.createInjector(Modules.override(SidecarModules.all())
                                               .with(Modules.override(new TestModule())
                                                            .with(new LiveMigrationStatusTestModule(),
                                                                  new InstanceFetcherTestModule(tempDir))));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        VertxTestContext context = new VertxTestContext();
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);

        context.awaitCompletion(15, TimeUnit.SECONDS);
    }

    @AfterEach
    public void after() throws InterruptedException
    {
        CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    @Test
    void testUpdateStatusAndGetStatusSucceedsAtSource(VertxTestContext context)
    {
        updateStatusAndGetStatusSucceedsAtDestination(context, Map.of("127.0.0.1", "127.0.0.3"));
    }

    @Test
    void testUpdateStatusAndGetStatusSucceedsAtDestination(VertxTestContext context)
    {
        updateStatusAndGetStatusSucceedsAtDestination(context, Map.of("127.0.0.3", "127.0.0.1"));
    }

    void updateStatusAndGetStatusSucceedsAtDestination(VertxTestContext context, Map<String, String> liveMigrationMap)
    {
        mockLiveMigrationMap(injector, liveMigrationMap);
        WebClient client = WebClient.create(vertx);
        client.post(server.actualPort(), "127.0.0.1", LIVE_MIGRATION_STATUS_ROUTE)
              .as(BodyCodec.buffer())
              .send()
              .compose(response -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  assertThat(response.bodyAsJsonObject().getString("state")).isEqualTo("COMPLETED");
                  return Future.succeededFuture();
              })
              .compose(v -> client.get(server.actualPort(), "127.0.0.1", LIVE_MIGRATION_STATUS_ROUTE)
                                  .send()
                                  .compose(response -> {
                                      assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                                      assertThat(response.bodyAsJsonObject().getString("state")).isEqualTo("COMPLETED");
                                      return Future.succeededFuture();
                                  }))
              .onSuccess(r -> context.completeNow())
              .onFailure(context::failNow);
    }

    @Test
    void testGetStatusReturnsNotCompletedBeforeUpdatingStatus(VertxTestContext context)
    {
        // Not updating migration status as completed. Get status call should succeed, but the status
        // should be NOT_COMPLETED.
        mockLiveMigrationMap(injector, Map.of("127.0.0.1", "127.0.0.3"));
        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", LIVE_MIGRATION_STATUS_ROUTE)
              .send()
              .compose(response -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  assertThat(response.bodyAsJsonObject().getString("state")).isEqualTo("NOT_COMPLETED");
                  return Future.succeededFuture();
              })
              .onSuccess(r -> context.completeNow())
              .onFailure(context::failNow)
              .onComplete(v -> client.close());
    }

    @Test
    void testUpdatingStatusSecondTimeShouldFail(VertxTestContext context)
    {
        mockLiveMigrationMap(injector, Map.of("127.0.0.1", "127.0.0.3"));

        WebClient client = WebClient.create(vertx);
        client.post(server.actualPort(), "127.0.0.1", LIVE_MIGRATION_STATUS_ROUTE)
              .as(BodyCodec.buffer())
              .send()
              .compose(response -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  assertThat(response.bodyAsJsonObject().getString("state")).isEqualTo("COMPLETED");
                  return Future.succeededFuture();
              })
              // Updating status for the second time
              .compose(v -> client.post(server.actualPort(), "127.0.0.1", LIVE_MIGRATION_STATUS_ROUTE)
                                  .send()
                                  .compose(response -> {
                                      assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
                                      assertThat(response.bodyAsJsonObject()).isNotNull();
                                      return Future.succeededFuture();
                                  }))
              .onSuccess(r -> context.completeNow())
              .onFailure(context::failNow)
              .onComplete(v -> client.close());
    }

    @Test
    void testDeleteStatusShouldSucceedPostMigrationMapCleanup(VertxTestContext context)
    {
        mockLiveMigrationMap(injector, Map.of("127.0.0.1", "127.0.0.3"));

        WebClient client = WebClient.create(vertx);
        client.post(server.actualPort(), "127.0.0.1", LIVE_MIGRATION_STATUS_ROUTE)
              .as(BodyCodec.buffer())
              .send()
              // Updating live migration status
              .compose(response -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  assertThat(response.bodyAsJsonObject().getString("state")).isEqualTo("COMPLETED");
                  return Future.succeededFuture();
              })
              // Getting the live migration status
              .compose(v -> client.get(server.actualPort(), "127.0.0.1", LIVE_MIGRATION_STATUS_ROUTE)
                                  .send()
                                  .compose(response -> {
                                      assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                                      assertThat(response.bodyAsJsonObject().getString("state")).isEqualTo("COMPLETED");
                                      return Future.succeededFuture();
                                  }))
              // Clearing the live migration Map
              .compose(v -> {
                  mockLiveMigrationMap(injector, Map.of());
                  return Future.succeededFuture();
              })
              // Sending the delete status request
              .compose(v -> client.delete(server.actualPort(), "127.0.0.1", LIVE_MIGRATION_STATUS_ROUTE)
                                  .send()
                                  .compose(response -> {
                                      assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                                      return Future.succeededFuture();
                                  }))
              .onSuccess(r -> context.completeNow())
              .onFailure(context::failNow)
              .onComplete(v -> client.close());
    }

    @Test
    void testDeleteStatusShouldNotSucceedBeforeMigrationMapCleanup(VertxTestContext context)
    {
        mockLiveMigrationMap(injector, Map.of("127.0.0.1", "127.0.0.3"));

        WebClient client = WebClient.create(vertx);
        client.post(server.actualPort(), "127.0.0.1", LIVE_MIGRATION_STATUS_ROUTE)
              .as(BodyCodec.buffer())
              .send()
              .compose(response -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  assertThat(response.bodyAsJsonObject().getString("state")).isEqualTo("COMPLETED");
                  return Future.succeededFuture();
              })
              // Migration map is not cleared before sending delete status request,
              // so delete status request should fail.
              .compose(v -> client.delete(server.actualPort(), "127.0.0.1", LIVE_MIGRATION_STATUS_ROUTE)
                                  .as(BodyCodec.buffer())
                                  .send())
              .compose(response -> {
                  assertThat(response.statusCode()).isEqualTo(403);
                  return Future.succeededFuture();
              })
              .onSuccess(r -> context.completeNow())
              .onFailure(context::failNow);
    }

    @Test
    void testCompleteStatusShouldNotSucceedWhenFetchingMigrationMapFails(VertxTestContext context)
    {
        LiveMigrationMap migrationMap = injector.getInstance(LiveMigrationMap.class);
        when(migrationMap.getMigrationMap())
        .thenReturn(Future.failedFuture(new IOException("Failed to fetch config")));

        WebClient client = WebClient.create(vertx);
        client.post(server.actualPort(), "127.0.0.1", LIVE_MIGRATION_STATUS_ROUTE)
              .as(BodyCodec.buffer())
              .send()
              .compose(response -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
                  assertThat(response.bodyAsJsonObject()).isNotNull();
                  return Future.succeededFuture();
              })
              .onSuccess(r -> context.completeNow())
              .onFailure(context::failNow);
    }

    @Test
    void testDeleteStatusShouldNotSucceedWhenFetchingMigrationMapFails(VertxTestContext context)
    {
        mockLiveMigrationMap(injector, Map.of("127.0.0.1", "127.0.0.3"));
        WebClient client = WebClient.create(vertx);
        client.post(server.actualPort(), "127.0.0.1", LIVE_MIGRATION_STATUS_ROUTE)
              .as(BodyCodec.buffer())
              .send()
              .compose(response -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  assertThat(response.bodyAsJsonObject().getString("state")).isEqualTo("COMPLETED");
                  return Future.succeededFuture();
              })
              .compose(v -> {
                  LiveMigrationMap migrationMap = injector.getInstance(LiveMigrationMap.class);
                  when(migrationMap.getMigrationMap())
                  .thenReturn(Future.failedFuture(new IOException("Failed to fetch config")));
                  return Future.succeededFuture();
              })
              .compose(v -> client.delete(server.actualPort(), "127.0.0.1", LIVE_MIGRATION_STATUS_ROUTE)
                                  .as(BodyCodec.buffer())
                                  .send())
              .compose(response -> {
                  assertThat(response.statusCode()).isEqualTo(503);
                  return Future.succeededFuture();
              })
              .onSuccess(r -> context.completeNow())
              .onFailure(context::failNow);
    }

    @Test
    void testDeleteStatusShouldNotSucceedForInstanceNotRelatedToMigration(VertxTestContext context)
    {
        // This test simulates a scenario where delete migration status API is called for which migration
        // was not attempted but tried to delete status while another migration is in progress.
        // Since the migration status was not updated for the instance, it returns BAD_REQUEST response.

        mockLiveMigrationMap(injector, Map.of("localhost12345", "localhost54321"));
        WebClient client = WebClient.create(vertx);
        client.delete(server.actualPort(), "127.0.0.1", LIVE_MIGRATION_STATUS_ROUTE)
              .as(BodyCodec.buffer())
              .send()
              .compose(response -> {
                  assertThat(response.statusCode()).isEqualTo(400);
                  return Future.succeededFuture();
              })
              .onSuccess(r -> context.completeNow())
              .onFailure(context::failNow);
    }

    void mockLiveMigrationMap(Injector injector, Map<String, String> migrationMap)
    {
        LiveMigrationMap map = injector.getInstance(LiveMigrationMap.class);
        when(map.getMigrationMap()).thenReturn(Future.succeededFuture(migrationMap));
    }

    private static class LiveMigrationStatusTestModule extends AbstractModule
    {

        @Override
        protected void configure()
        {
            // noinspection Convert2Lambda
            LiveMigrationMap liveMigrationMap = spy(new LiveMigrationMap()
            {
                @Override
                public @NotNull Future<Map<String, String>> getMigrationMap()
                {
                    return Future.succeededFuture(Map.of());
                }
            });
            bind(LiveMigrationMap.class).toInstance(liveMigrationMap);
        }
    }
}
