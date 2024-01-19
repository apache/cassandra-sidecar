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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.SidecarRateLimiter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.stats.SidecarStats;
import org.apache.cassandra.sidecar.utils.FileStreamer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FileStreamHandler}
 */
@ExtendWith(VertxExtension.class)
class FileStreamHandlerTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStreamHandlerTest.class);
    Vertx vertx = Vertx.vertx();
    WebClient client = WebClient.create(vertx);

    HttpServer server;

    @TempDir
    Path tempDir;


    @AfterEach
    void tearDown() throws InterruptedException
    {
        client.close();
        if (server == null)
            return;
        CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    @Test
    void failsWhenFileDoesNotExist(VertxTestContext context)
    {
        serverFuture(tempDir.resolve("non-existent-file").toString())
        .onSuccess(server -> {
            client.get(server.actualPort(), "127.0.0.1", "/stream")
                  .expect(ResponsePredicate.SC_NOT_FOUND)
                  .send(context.succeeding(response -> context.verify(() -> {
                      JsonObject responseJson = response.bodyAsJsonObject();
                      assertThat(responseJson.getString("message")).isEqualTo("The requested file does not exist");
                      assertThat(responseJson.getInteger("code")).isEqualTo(404);
                      assertThat(responseJson.getString("status")).isEqualTo("Not Found");
                      context.completeNow();
                  })));
        })
        .onFailure(context::failNow);
    }

    @Test
    void failsWhenFileIsNotARegularFile(VertxTestContext context) throws IOException
    {
        Path directory = tempDir.resolve("directory");
        assertThat(Files.createDirectory(directory)).exists();
        serverFuture(directory.toString())
        .onSuccess(server -> {
            client.get(server.actualPort(), "127.0.0.1", "/stream")
                  .expect(ResponsePredicate.SC_NOT_FOUND)
                  .send(context.succeeding(response -> context.verify(() -> {
                      JsonObject responseJson = response.bodyAsJsonObject();
                      assertThat(responseJson.getString("message")).isEqualTo("The requested file does not exist");
                      assertThat(responseJson.getInteger("code")).isEqualTo(404);
                      assertThat(responseJson.getString("status")).isEqualTo("Not Found");
                      context.completeNow();
                  })));
        })
        .onFailure(context::failNow);
    }

    Future<HttpServer> serverFuture(String path)
    {
        Router router = Router.router(vertx);
        // to capture the expected payloads from the clients
        router.route().failureHandler(new JsonErrorHandler());

        router.get("/stream")
              .handler(context -> context.put(FileStreamHandler.FILE_PATH_CONTEXT_KEY, path)
                                         .next())
              .handler(fileStreamHandler());

        Future<HttpServer> future = vertx.createHttpServer()
                                         .requestHandler(router)
                                         .listen(0, "localhost");
        // keep track of the server, so we can close it on tearDown
        future.onSuccess(server -> this.server = server);
        return future;
    }

    private FileStreamHandler fileStreamHandler()
    {
        ServiceConfigurationImpl serviceConfiguration = new ServiceConfigurationImpl();
        ExecutorPools executorPools = new ExecutorPools(vertx, serviceConfiguration);
        FileStreamer fileStreamer = new FileStreamer(executorPools, serviceConfiguration,
                                                     SidecarRateLimiter.create(0), SidecarStats.INSTANCE);
        return new FileStreamHandler(null, serviceConfiguration, fileStreamer, executorPools);
    }
}
