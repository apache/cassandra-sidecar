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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.google.common.util.concurrent.SidecarRateLimiter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileProps;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.stats.SidecarStats;
import org.apache.cassandra.sidecar.utils.FileStreamer;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.PARTIAL_CONTENT;
import static org.apache.cassandra.sidecar.utils.TestFileUtils.prepareTestFile;
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
    TestFileStreamHandler fileStreamHandler;
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
        serverFuture(tempDir.resolve("non-existent-file").toString(), false)
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
        serverFuture(directory.toString(), false)
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
    void failsWhenFileIsEmpty(VertxTestContext context) throws IOException
    {
        Path emptyFile = tempDir.resolve("empty-file");
        assertThat(Files.createFile(emptyFile)).exists();
        serverFuture(emptyFile.toString(), false)
        .onSuccess(server -> {
            client.get(server.actualPort(), "127.0.0.1", "/stream")
                  .expect(ResponsePredicate.SC_REQUESTED_RANGE_NOT_SATISFIABLE)
                  .send(context.succeeding(response -> context.verify(() -> {
                      JsonObject responseJson = response.bodyAsJsonObject();
                      assertThat(responseJson.getString("message")).isEqualTo("The requested file is empty");
                      assertThat(responseJson.getInteger("code")).isEqualTo(416);
                      assertThat(responseJson.getString("status")).isEqualTo("Requested Range Not Satisfiable");
                      context.completeNow();
                  })));
        })
        .onFailure(context::failNow);
    }

    @ParameterizedTest(name = "cachingEnabled={0}")
    @ValueSource(booleans = { true, false })
    void testStreamEntireFile(boolean cachingEnabled) throws IOException
    {
        VertxTestContext context = new VertxTestContext();
        int sizeInBytes = 1024;
        Path oneKbFile = prepareTestFile(tempDir, "one-kb-file", sizeInBytes);
        assertThat(oneKbFile).exists();
        serverFuture(oneKbFile.toString(), cachingEnabled)
        .onSuccess(server -> {
            client.get(server.actualPort(), "127.0.0.1", "/stream")
                  .as(BodyCodec.buffer())
                  .send(context.succeeding(response -> context.verify(() -> {
                      assertThat(response.statusCode()).isEqualTo(OK.code());
                      assertThat(response.body().length()).isEqualTo(sizeInBytes);
                      context.completeNow();
                  })));
        })
        .onFailure(context::failNow);
    }

    @ParameterizedTest(name = "cachingEnabled={0}")
    @ValueSource(booleans = { true, false })
    void testStreamRange(boolean cachingEnabled) throws IOException
    {
        VertxTestContext context = new VertxTestContext();
        int sizeInBytes = 1024;
        Path oneKbFile = prepareTestFile(tempDir, "one-kb-file", sizeInBytes);
        assertThat(oneKbFile).exists();
        serverFuture(oneKbFile.toString(), cachingEnabled)
        .onSuccess(server -> {
            client.get(server.actualPort(), "127.0.0.1", "/stream")
                  .putHeader("Range", "bytes=512-1023")
                  .as(BodyCodec.buffer())
                  .send(context.succeeding(response -> context.verify(() -> {
                      assertThat(response.statusCode()).isEqualTo(PARTIAL_CONTENT.code());
                      assertThat(response.headers().get("Accept-Ranges")).isNotNull()
                                                                         .isEqualTo("bytes");
                      assertThat(response.headers().get("Content-Length")).isNotNull()
                                                                          .isEqualTo("512");
                      assertThat(response.body().length()).isEqualTo(512);
                      context.completeNow();
                  })));
        })
        .onFailure(context::failNow);
    }

    // Cache testing

    @Test
    void testStreamFileThatIsCreatedAfterFirstRequest(VertxTestContext context)
    {
        int sizeInBytes = 1024;
        Path oneKbFile = tempDir.resolve("one-kb-file");
        assertThat(oneKbFile).doesNotExist();
        serverFuture(oneKbFile.toString(), true)
        .onSuccess(server -> {
            client.get(server.actualPort(), "127.0.0.1", "/stream")
                  .expect(ResponsePredicate.SC_NOT_FOUND)
                  .send()
                  .compose(response -> {
                      JsonObject responseJson = response.bodyAsJsonObject();
                      assertThat(responseJson.getString("message")).isEqualTo("The requested file does not exist");
                      assertThat(responseJson.getInteger("code")).isEqualTo(404);
                      assertThat(responseJson.getString("status")).isEqualTo("Not Found");

                      try
                      {
                          prepareTestFile(tempDir, "one-kb-file", sizeInBytes);
                      }
                      catch (IOException e)
                      {
                          context.failNow(e);
                      }

                      assertThat(oneKbFile).exists();
                      return Future.succeededFuture(oneKbFile);
                  }).onSuccess(file -> {
                      client.get(server.actualPort(), "127.0.0.1", "/stream")
                            .as(BodyCodec.buffer())
                            .send(context.succeeding(response -> context.verify(() -> {
                                assertThat(response.statusCode()).isEqualTo(OK.code());
                                assertThat(response.body().length()).isEqualTo(sizeInBytes);
                                context.completeNow();
                            })));
                  });
        })
        .onFailure(context::failNow);
    }

    @Test
    void testCacheBeingUsed(VertxTestContext context) throws IOException
    {
        int sizeInBytes = 1024;
        Path oneKbFile = prepareTestFile(tempDir, "one-kb-file", sizeInBytes);
        assertThat(oneKbFile).exists();
        serverFuture(oneKbFile.toString(), true)
        .compose(server -> {
            assertThat(fileStreamHandler.cacheMisses.get()).isEqualTo(0);
            return client.get(server.actualPort(), "127.0.0.1", "/stream")
                         .as(BodyCodec.buffer())
                         .send();
        })
        .compose(response -> {
            assertThat(fileStreamHandler.cacheMisses.get()).isEqualTo(1);
            assertThat(response.statusCode()).isEqualTo(OK.code());
            assertThat(response.body().length()).isEqualTo(sizeInBytes);
            return Future.succeededFuture();
        })
        .compose(v -> client.get(server.actualPort(), "127.0.0.1", "/stream")
                            .as(BodyCodec.buffer())
                            .send())
        .compose(response -> {
            // cache was used for the second request
            assertThat(fileStreamHandler.cacheMisses.get()).isEqualTo(1);
            assertThat(response.statusCode()).isEqualTo(OK.code());
            assertThat(response.body().length()).isEqualTo(sizeInBytes);
            return Future.succeededFuture();
        })
        .compose(v -> {
            // force a cache invalidation
            assertThat(fileStreamHandler.filePropsCache).isNotNull();
            fileStreamHandler.filePropsCache.invalidateAll();
            return client.get(server.actualPort(), "127.0.0.1", "/stream")
                         .as(BodyCodec.buffer())
                         .send();
        })
        .onSuccess(response -> {
            // cache is empty, so we should have a cache miss for the third request
            assertThat(fileStreamHandler.cacheMisses.get()).isEqualTo(2);
            assertThat(response.statusCode()).isEqualTo(OK.code());
            assertThat(response.body().length()).isEqualTo(sizeInBytes);
            context.completeNow();
        })
        .onFailure(context::failNow);
    }

    @Test
    void testAttemptToStreamDeletedFileThatHadFilePropsCached(VertxTestContext context) throws IOException
    {
        int sizeInBytes = 1024;
        Path oneKbFile = prepareTestFile(tempDir, "one-kb-file", sizeInBytes);
        assertThat(oneKbFile).exists();
        serverFuture(oneKbFile.toString(), true)
        .compose(server -> {
            // First let's stream a file that we just created
            assertThat(fileStreamHandler.cacheMisses.get()).isEqualTo(0);
            return client.get(server.actualPort(), "127.0.0.1", "/stream")
                         .as(BodyCodec.buffer())
                         .send();
        })
        .compose(response -> {
            assertThat(response.statusCode()).isEqualTo(OK.code());
            assertThat(response.body().length()).isEqualTo(sizeInBytes);
            assertThat(fileStreamHandler.cacheMisses.get()).isEqualTo(1);
            try
            {
                // Delete the file, now we should see a 404 when trying to stream the file
                Files.delete(oneKbFile);
            }
            catch (IOException e)
            {
                return Future.failedFuture(e);
            }
            return Future.succeededFuture();
        }).compose(v -> client.get(server.actualPort(), "127.0.0.1", "/stream")
                              .expect(ResponsePredicate.SC_NOT_FOUND)
                              .send())
        .onSuccess(response -> {
            // A 404 is expected here because the underlying file is gone
            // even when the cache still has an entry for non-existing file
            JsonObject responseJson = response.bodyAsJsonObject();
            assertThat(responseJson.getString("message"))
            .isEqualTo("The requested file does not exist");
            assertThat(responseJson.getInteger("code")).isEqualTo(404);
            assertThat(responseJson.getString("status")).isEqualTo("Not Found");
            assertThat(fileStreamHandler.cacheMisses.get()).isEqualTo(1);
            context.completeNow();
        })
        .onFailure(context::failNow);
    }

    Future<HttpServer> serverFuture(String path, boolean cachingEnabled)
    {
        Router router = Router.router(vertx);
        // to capture the expected payloads from the clients
        router.route().failureHandler(new JsonErrorHandler());

        fileStreamHandler = fileStreamHandler(cachingEnabled);
        router.get("/stream")
              .handler(context -> context.put(FileStreamHandler.FILE_PATH_CONTEXT_KEY, path)
                                         .next())
              .handler(fileStreamHandler);

        Future<HttpServer> future = vertx.createHttpServer()
                                         .requestHandler(router)
                                         .listen(0, "localhost");
        // keep track of the server, so we can close it on tearDown
        future.onSuccess(server -> this.server = server);
        return future;
    }

    TestFileStreamHandler fileStreamHandler(boolean cachingEnabled)
    {
        ServiceConfigurationImpl serviceConfiguration = cachingEnabled
                                                        ? new ServiceConfigurationImpl()
                                                        : ServiceConfigurationImpl.builder()
                                                                                  // cache is not initialized when
                                                                                  // config is null
                                                                                  .fileStreamPropsCache(null)
                                                                                  .build();
        ExecutorPools executorPools = new ExecutorPools(vertx, serviceConfiguration);
        FileStreamer fileStreamer = new FileStreamer(executorPools, serviceConfiguration,
                                                     SidecarRateLimiter.create(0), SidecarStats.INSTANCE);
        return new TestFileStreamHandler(null, serviceConfiguration, fileStreamer, executorPools);
    }

    /**
     * Class that peeks into the {@link #ensureValidFileNonCached} method, and keeps track of non-cached access to
     * the {@link FileProps}
     */
    static class TestFileStreamHandler extends FileStreamHandler
    {
        final AtomicInteger cacheMisses = new AtomicInteger();

        public TestFileStreamHandler(InstanceMetadataFetcher metadataFetcher,
                                     ServiceConfiguration serviceConfiguration,
                                     FileStreamer fileStreamer,
                                     ExecutorPools executorPools)
        {
            super(metadataFetcher, serviceConfiguration, fileStreamer, executorPools);
        }

        @Override
        protected @NotNull Function<String, Future<FileProps>> ensureValidFileNonCached(FileSystem fs)
        {
            cacheMisses.incrementAndGet();
            return super.ensureValidFileNonCached(fs);
        }
    }
}
