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

import com.codahale.metrics.SharedMetricRegistries;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetricsImpl;
import org.apache.cassandra.sidecar.utils.FileStreamer;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.PARTIAL_CONTENT;
import static org.apache.cassandra.sidecar.utils.TestFileUtils.prepareTestFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    FileStreamHandler fileStreamHandler;
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

    @Test
    void failsWhenFileIsEmpty(VertxTestContext context) throws IOException
    {
        Path emptyFile = tempDir.resolve("empty-file");
        assertThat(Files.createFile(emptyFile)).exists();
        serverFuture(emptyFile.toString())
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

    @Test
    void testStreamEntireFile() throws IOException
    {
        VertxTestContext context = new VertxTestContext();
        int sizeInBytes = 1024;
        Path oneKbFile = prepareTestFile(tempDir, "one-kb-file", sizeInBytes);
        assertThat(oneKbFile).exists();
        serverFuture(oneKbFile.toString())
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

    @Test
    void testStreamRange() throws IOException
    {
        VertxTestContext context = new VertxTestContext();
        int sizeInBytes = 1024;
        Path oneKbFile = prepareTestFile(tempDir, "one-kb-file", sizeInBytes);
        assertThat(oneKbFile).exists();
        serverFuture(oneKbFile.toString())
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

    @Test
    void testStreamFileThatIsCreatedAfterFirstRequest(VertxTestContext context)
    {
        int sizeInBytes = 1024;
        Path oneKbFile = tempDir.resolve("one-kb-file");
        assertThat(oneKbFile).doesNotExist();
        serverFuture(oneKbFile.toString())
        .compose(server -> client.get(server.actualPort(), "127.0.0.1", "/stream")
                                 .expect(ResponsePredicate.SC_NOT_FOUND)
                                 .send())
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
        })
        .onSuccess(file -> client.get(server.actualPort(), "127.0.0.1", "/stream")
                                 .as(BodyCodec.buffer())
                                 .send(context.succeeding(response -> context.verify(() -> {
                                     assertThat(response.statusCode()).isEqualTo(OK.code());
                                     assertThat(response.body().length()).isEqualTo(sizeInBytes);
                                     context.completeNow();
                                 }))))
        .onFailure(context::failNow);
    }

    @Test
    void testAttemptToStreamDeletedFile(VertxTestContext context) throws IOException
    {
        int sizeInBytes = 1024;
        Path oneKbFile = prepareTestFile(tempDir, "one-kb-file", sizeInBytes);
        assertThat(oneKbFile).exists();
        serverFuture(oneKbFile.toString())
        .compose(server -> {
            // First let's stream a file that we just created
            return client.get(server.actualPort(), "127.0.0.1", "/stream")
                         .as(BodyCodec.buffer())
                         .send();
        })
        .compose(response -> {
            assertThat(response.statusCode()).isEqualTo(OK.code());
            assertThat(response.body().length()).isEqualTo(sizeInBytes);
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
            JsonObject responseJson = response.bodyAsJsonObject();
            assertThat(responseJson.getString("message"))
            .isEqualTo("The requested file does not exist");
            assertThat(responseJson.getInteger("code")).isEqualTo(404);
            assertThat(responseJson.getString("status")).isEqualTo("Not Found");
            context.completeNow();
        })
        .onFailure(context::failNow);
    }

    @Test
    void testStreamWithSizeQueryParam(VertxTestContext context) throws IOException
    {
        int sizeInBytes = 1024;
        Path oneKbFile = prepareTestFile(tempDir, "one-kb-file", sizeInBytes);
        assertThat(oneKbFile).exists();
        serverFuture(oneKbFile.toString())
        .onSuccess(server -> {
            client.get(server.actualPort(), "127.0.0.1", "/stream?size=" + sizeInBytes)
                  .as(BodyCodec.buffer())
                  .send(context.succeeding(response -> context.verify(() -> {
                      assertThat(response.statusCode()).isEqualTo(OK.code());
                      assertThat(response.body().length()).isEqualTo(sizeInBytes);
                      context.completeNow();
                  })));
        })
        .onFailure(context::failNow);
    }

    @Test
    void testStreamsPartialFileWhenSizeQueryParamDoesntMatchActualFileSize(VertxTestContext context) throws IOException
    {
        int sizeInBytes = 1024;
        Path oneKbFile = prepareTestFile(tempDir, "one-kb-file", sizeInBytes);
        assertThat(oneKbFile).exists();
        serverFuture(oneKbFile.toString())
        .onSuccess(server -> {
            client.get(server.actualPort(), "127.0.0.1", "/stream?size=5")
                  .as(BodyCodec.buffer())
                  .send(context.succeeding(response -> context.verify(() -> {
                      assertThat(response.statusCode()).isEqualTo(OK.code());
                      assertThat(response.body().length()).isEqualTo(5);
                      context.completeNow();
                  })));
        })
        .onFailure(context::failNow);
    }

    @Test
    void failsWithNegativeSizeQueryParam(VertxTestContext context) throws IOException
    {
        int sizeInBytes = 1024;
        Path oneKbFile = prepareTestFile(tempDir, "one-kb-file", sizeInBytes);
        assertThat(oneKbFile).exists();
        serverFuture(oneKbFile.toString())
        .onSuccess(server -> {
            client.get(server.actualPort(), "127.0.0.1", "/stream?size=-1")
                  .as(BodyCodec.buffer())
                  .send(context.succeeding(response -> context.verify(() -> {
                      assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                      assertThat(response.bodyAsJsonObject().getString("message")).isEqualTo("Range does not satisfy boundary requirements");
                      context.completeNow();
                  })));
        })
        .onFailure(context::failNow);
    }

    @Test
    void testSizeStreamedIsUpperBoundedByFileSize(VertxTestContext context) throws IOException
    {
        int sizeInBytes = 1024;
        Path oneKbFile = prepareTestFile(tempDir, "one-kb-file", sizeInBytes);
        assertThat(oneKbFile).exists();
        serverFuture(oneKbFile.toString())
        .onSuccess(server -> {
            client.get(server.actualPort(), "127.0.0.1", "/stream?size=200000")
                  .as(BodyCodec.buffer())
                  .send(context.succeeding(response -> context.verify(() -> {
                      assertThat(response.statusCode()).isEqualTo(OK.code());
                      assertThat(response.body().length()).isEqualTo(sizeInBytes);
                      context.completeNow();
                  })));
        })
        .onFailure(context::failNow);
    }

    @Test
    void failsWhenOffsetIsLargerThanActualFile(VertxTestContext context) throws IOException
    {
        int sizeInBytes = 1024;
        Path oneKbFile = prepareTestFile(tempDir, "one-kb-file", sizeInBytes);
        assertThat(oneKbFile).exists();
        serverFuture(oneKbFile.toString())
        .onSuccess(server -> {
            client.get(server.actualPort(), "127.0.0.1", "/stream?size=200000")
                  .putHeader("Range", "bytes=1025-2048")
                  .as(BodyCodec.buffer())
                  .send(context.succeeding(response -> context.verify(() -> {
                      assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                      assertThat(response.bodyAsJsonObject().getString("message"))
                      .isEqualTo("offset : 1025 is larger than the requested file length : 1024");
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

        fileStreamHandler = fileStreamHandler();
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

    FileStreamHandler fileStreamHandler()
    {
        InstanceMetrics instanceMetrics = new InstanceMetricsImpl(SharedMetricRegistries.getOrCreate("sidecar"));
        InstanceMetadata mockInstanceMetadata = mock(InstanceMetadata.class);
        when(mockInstanceMetadata.id()).thenReturn(1);
        when(mockInstanceMetadata.metrics()).thenReturn(instanceMetrics);

        InstanceMetadataFetcher mockFetcher = mock(InstanceMetadataFetcher.class);
        when(mockFetcher.instance(1)).thenReturn(mockInstanceMetadata);
        when(mockFetcher.instance("127.0.0.1")).thenReturn(mockInstanceMetadata);
        ServiceConfigurationImpl serviceConfiguration = new ServiceConfigurationImpl();
        ExecutorPools executorPools = new ExecutorPools(vertx, serviceConfiguration);
        FileStreamer fileStreamer = new FileStreamer(executorPools,
                                                     serviceConfiguration,
                                                     SidecarRateLimiter.create(0),
                                                     mockFetcher);
        return new FileStreamHandler(mockFetcher, fileStreamer, executorPools);
    }
}
