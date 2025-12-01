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

package org.apache.cassandra.sidecar.client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.cassandra.sidecar.client.request.RequestExecutorTest;

import static java.nio.file.Files.copy;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link VertxHttpClient}
 */
class VertxHttpClientTest
{
    private Vertx vertx;
    private MockWebServer mockServer;
    private HttpClientConfig config;
    private SidecarInstanceImpl sidecarInstance;

    @BeforeEach
    void setup() throws IOException
    {
        vertx = Vertx.vertx();
        mockServer = new MockWebServer();
        mockServer.start();

        config = new HttpClientConfig.Builder<>()
                 .ssl(false)
                 .timeoutMillis(30000)
                 .build();
        sidecarInstance = RequestExecutorTest.newSidecarInstance(mockServer);
    }

    @AfterEach
    void tearDown() throws IOException
    {
        if (mockServer != null)
        {
            mockServer.shutdown();
        }
        if (vertx != null)
        {
            vertx.close();
        }
    }

    @Test
    void testUploadSSTableClosesFile(@TempDir Path tempDirectory) throws Exception
    {
        runTestScenario(tempDirectory,
                        new MockResponse().setResponseCode(OK.code()),
                        new ExposeAsyncFileVertxHttpClient(vertx, config));
    }

    @Test
    void testUploadClosesFileOnUploadFailure(@TempDir Path tempDirectory) throws Exception
    {
        runTestScenario(tempDirectory,
                        new MockResponse().setResponseCode(INTERNAL_SERVER_ERROR.code()),
                        new ExposeAsyncFileVertxHttpClient(vertx, config));
    }

    @Test
    void testMultipleUploadClosesAllFiles(@TempDir Path tempDirectory) throws Exception
    {
        mockServer.enqueue(new MockResponse().setResponseCode(OK.code()));
        mockServer.enqueue(new MockResponse().setResponseCode(OK.code()));
        mockServer.enqueue(new MockResponse().setResponseCode(OK.code()));

        Path fileToUpload = prepareFile(tempDirectory);

        ExposeAsyncFileVertxHttpClient httpClient = new ExposeAsyncFileVertxHttpClient(vertx, config);

        // Upload the same file 3 times (simulating multiple file uploads)
        for (int i = 0; i < 3; i++)
        {
            HttpRequest<Buffer> vertxRequest = httpClient.webClient.put(mockServer.getPort(),
                                                                        mockServer.getHostName(),
                                                                        "/upload/test" + i);
            httpClient.executeUploadFileInternal(sidecarInstance, vertxRequest, fileToUpload.toString())
                      .get(30, TimeUnit.SECONDS);
        }

        assertThat(mockServer.getRequestCount()).isEqualTo(3);
        assertThat(httpClient.capturedFiles).hasSize(3);

        // Give async file close operations time to complete
        Thread.sleep(100);

        // Verify all the files are closed by attempting to call .end() which should throw IllegalStateException
        for (AsyncFile file : httpClient.capturedFiles)
        {
            assertThatThrownBy(file::end)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("File handle is closed");
        }
    }

    private void runTestScenario(Path tempDirectory,
                                 MockResponse mockResponse,
                                 ExposeAsyncFileVertxHttpClient httpClient) throws Exception
    {
        mockServer.enqueue(mockResponse);

        Path fileToUpload = prepareFile(tempDirectory);
        HttpRequest<Buffer> vertxRequest = httpClient.webClient.put(mockServer.getPort(),
                                                                    mockServer.getHostName(),
                                                                    "/upload/test");

        httpClient.executeUploadFileInternal(sidecarInstance, vertxRequest, fileToUpload.toString())
                  .get(30, TimeUnit.SECONDS);

        assertThat(mockServer.getRequestCount()).isEqualTo(1);

        // Give async file close operation time to complete
        Thread.sleep(100);

        // Verify file is closed by attempting to call .end() which should throw IllegalStateException
        assertThat(httpClient.capturedFiles).hasSize(1);
        assertThatThrownBy(() -> httpClient.capturedFiles.get(0).end())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("File handle is closed");
    }

    /**
     * Class that extends from {@link VertxHttpClient} for testing purposes and holds a reference to the
     * {@link AsyncFile} to ensure that the file has been closed.
     */
    static class ExposeAsyncFileVertxHttpClient extends VertxHttpClient
    {
        List<AsyncFile> capturedFiles = new ArrayList<>();

        ExposeAsyncFileVertxHttpClient(Vertx vertx, HttpClientConfig config)
        {
            super(vertx, config);
        }

        @Override
        protected Future<HttpResponse<Buffer>> sendFileStream(HttpRequest<Buffer> vertxRequest,
                                                              SimpleEntry<Long, AsyncFile> pair,
                                                              String filename)
        {
            capturedFiles.add(pair.getValue());
            return super.sendFileStream(vertxRequest, pair, filename);
        }
    }

    private Path prepareFile(Path tempDirectory) throws IOException
    {
        Path fileToUpload = tempDirectory.resolve("nb-1-big-TOC.txt");
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("sstables/nb-1-big-TOC.txt"))
        {
            assertThat(inputStream).isNotNull();
            copy(inputStream, fileToUpload, StandardCopyOption.REPLACE_EXISTING);
        }
        return fileToUpload;
    }
}
