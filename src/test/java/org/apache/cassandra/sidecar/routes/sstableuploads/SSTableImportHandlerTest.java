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

package org.apache.cassandra.sidecar.routes.sstableuploads;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static org.apache.cassandra.sidecar.utils.SSTableImporter.DEFAULT_COPY_DATA;
import static org.apache.cassandra.sidecar.utils.SSTableImporter.DEFAULT_INVALIDATE_CACHES;
import static org.apache.cassandra.sidecar.utils.SSTableImporter.DEFAULT_VERIFY_TOKENS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link SSTableImportHandler}
 */
@ExtendWith(VertxExtension.class)
public class SSTableImportHandlerTest extends BaseUploadsHandlerTest
{
    @Test
    void testInvalidUploadId(VertxTestContext context)
    {
        client.put(server.actualPort(), "localhost", "/api/v1/uploads/1234/keyspaces/ks/tables/tbl/import")
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .send(context.succeeding(response -> context.verify(() -> {
                  JsonObject error = response.bodyAsJsonObject();
                  assertThat(error.getInteger("code")).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
                  assertThat(error.getString("status")).isEqualTo("Bad Request");
                  assertThat(error.getString("message"))
                  .isEqualTo("Invalid upload id is supplied, uploadId=1234");
                  context.completeNow();
              })));
    }

    @Test
    void testInvalidKeyspace(VertxTestContext context)
    {
        UUID uploadId = UUID.randomUUID();
        client.put(server.actualPort(), "localhost", "/api/v1/uploads/"
                                                     + uploadId + "/keyspaces/_n$ks_/tables/tbl/import")
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .send(context.succeeding(response -> context.verify(() -> {
                  JsonObject error = response.bodyAsJsonObject();
                  assertThat(error.getInteger("code")).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
                  assertThat(error.getString("status")).isEqualTo("Bad Request");
                  assertThat(error.getString("message"))
                  .isEqualTo("Invalid characters in keyspace: _n$ks_");
                  context.completeNow();
              })));
    }

    @Test
    void testInvalidTable(VertxTestContext context)
    {
        UUID uploadId = UUID.randomUUID();
        client.put(server.actualPort(), "localhost",
                   "/api/v1/uploads/" + uploadId + "/keyspaces/ks/tables/_n$t_valid_/import")
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .send(context.succeeding(response -> context.verify(() -> {
                  JsonObject error = response.bodyAsJsonObject();
                  assertThat(error.getInteger("code")).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
                  assertThat(error.getString("status")).isEqualTo("Bad Request");
                  assertThat(error.getString("message"))
                  .isEqualTo("Invalid characters in table name: _n$t_valid_");
                  context.completeNow();
              })));
    }

    @Test
    void testNonExistentUploadDirectory(VertxTestContext context) throws InterruptedException
    {
        UUID uploadId = UUID.randomUUID();

        String requestURI = "/api/v1/uploads/" + uploadId + "/keyspaces/ks/tables/table/import";
        clientRequest(context, requestURI,
                      response -> assertThat(response.statusCode())
                                  .isEqualTo(HttpResponseStatus.NOT_FOUND.code()));
    }

    @Test
    void testFailsWhenCassandraIsUnavailable(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        createStagedUploadFiles(uploadId);
        testDelegate.setTableOperations(null);

        client.put(server.actualPort(), "localhost", "/api/v1/uploads/"
                                                     + uploadId + "/keyspaces/ks/tables/table/import")
              .expect(ResponsePredicate.SC_SERVICE_UNAVAILABLE)
              .send(context.succeedingThenComplete());
    }

    @Test
    void testFailsWhenImportReturnsNonEmptyListOfFailedDirectories(VertxTestContext context)
    throws IOException, InterruptedException
    {
        UUID uploadId = UUID.randomUUID();
        Path stagedUploadDirectory = createStagedUploadFiles(uploadId);
        String stageDirectoryAbsolutePath = stagedUploadDirectory.toString();
        when(mockCFOperations.importNewSSTables("ks", "table", stageDirectoryAbsolutePath,
                                                true, true, true,
                                                DEFAULT_VERIFY_TOKENS, DEFAULT_INVALIDATE_CACHES, true,
                                                DEFAULT_COPY_DATA))
        .thenReturn(Collections.singletonList(stageDirectoryAbsolutePath));

        String requestURI = "/api/v1/uploads/" + uploadId + "/keyspaces/ks/tables/table/import";
        clientRequest(context, requestURI,
                      response -> assertThat(response.statusCode())
                                  .isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));
    }

    @Test
    void testSucceeds(VertxTestContext context) throws IOException, InterruptedException
    {
        UUID uploadId = UUID.randomUUID();
        Path stagedUploadDirectory = createStagedUploadFiles(uploadId);
        String stageDirectoryAbsolutePath = stagedUploadDirectory.toString();
        when(mockCFOperations.importNewSSTables("ks", "table", stageDirectoryAbsolutePath,
                                                false, true, true,
                                                DEFAULT_VERIFY_TOKENS, DEFAULT_INVALIDATE_CACHES, true,
                                                DEFAULT_COPY_DATA))
        .thenReturn(Collections.emptyList());

        String requestURI = "/api/v1/uploads/" + uploadId + "/keyspaces/ks/tables/table/import";
        clientRequest(context, requestURI,
                      response -> assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code()));

        // should retrieve Future result from cache
        clientRequest(context, requestURI,
                      response -> assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code()));

        validateCleanup(context, stagedUploadDirectory);
    }

    @Test
    void testSucceedsWithResetLevel(VertxTestContext context) throws IOException, InterruptedException
    {
        UUID uploadId = UUID.randomUUID();
        Path stagedUploadDirectory = createStagedUploadFiles(uploadId);
        String stageDirectoryAbsolutePath = stagedUploadDirectory.toString();
        when(mockCFOperations.importNewSSTables("ks", "table", stageDirectoryAbsolutePath,
                                                false, true, true,
                                                DEFAULT_VERIFY_TOKENS, DEFAULT_INVALIDATE_CACHES, true,
                                                DEFAULT_COPY_DATA))
        .thenReturn(Collections.emptyList());

        String requestURI = "/api/v1/uploads/" + uploadId + "/keyspaces/ks/tables/table/import";
        sendRequest(context,
                    () -> client.put(server.actualPort(), "localhost", requestURI)
                                .addQueryParam("resetLevel", "false"),
                    context.succeeding(response -> context.verify(() -> {
                        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                        verify(mockCFOperations).importNewSSTables("ks",
                                                                   "table",
                                                                   stageDirectoryAbsolutePath,
                                                                   false,
                                                                   true,
                                                                   true,
                                                                   DEFAULT_VERIFY_TOKENS,
                                                                   DEFAULT_INVALIDATE_CACHES,
                                                                   true,
                                                                   DEFAULT_COPY_DATA);
                        context.completeNow();
                    })));
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void testSucceedsWithClearRepaired(VertxTestContext context) throws IOException, InterruptedException
    {
        UUID uploadId = UUID.randomUUID();
        Path stagedUploadDirectory = createStagedUploadFiles(uploadId);
        String stageDirectoryAbsolutePath = stagedUploadDirectory.toString();
        when(mockCFOperations.importNewSSTables("ks", "table", stageDirectoryAbsolutePath,
                                                true, false, true,
                                                DEFAULT_VERIFY_TOKENS, DEFAULT_INVALIDATE_CACHES, true,
                                                DEFAULT_COPY_DATA))
        .thenReturn(Collections.emptyList());

        String requestURI = "/api/v1/uploads/" + uploadId + "/keyspaces/ks/tables/table/import";
        sendRequest(context,
                    () -> client.put(server.actualPort(), "localhost", requestURI)
                                .addQueryParam("clearRepaired", "false"),
                    context.succeeding(response -> context.verify(() -> {
                        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                        verify(mockCFOperations).importNewSSTables("ks",
                                                                   "table",
                                                                   stageDirectoryAbsolutePath,
                                                                   true,
                                                                   false,
                                                                   true,
                                                                   DEFAULT_VERIFY_TOKENS,
                                                                   DEFAULT_INVALIDATE_CACHES,
                                                                   true,
                                                                   DEFAULT_COPY_DATA);
                        context.completeNow();
                    })));
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void testSucceedsWithVerifySSTables(VertxTestContext context) throws IOException, InterruptedException
    {
        UUID uploadId = UUID.randomUUID();
        Path stagedUploadDirectory = createStagedUploadFiles(uploadId);
        String stageDirectoryAbsolutePath = stagedUploadDirectory.toString();
        when(mockCFOperations.importNewSSTables("ks", "table", stageDirectoryAbsolutePath,
                                                true, true, false,
                                                DEFAULT_VERIFY_TOKENS, DEFAULT_INVALIDATE_CACHES, true,
                                                DEFAULT_COPY_DATA))
        .thenReturn(Collections.emptyList());

        String requestURI = "/api/v1/uploads/" + uploadId + "/keyspaces/ks/tables/table/import";
        sendRequest(context,
                    () -> client.put(server.actualPort(), "localhost", requestURI)
                                .addQueryParam("verifySSTables", "false"),
                    context.succeeding(response -> context.verify(() -> {
                        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                        verify(mockCFOperations).importNewSSTables("ks",
                                                                   "table",
                                                                   stageDirectoryAbsolutePath,
                                                                   true,
                                                                   true,
                                                                   false,
                                                                   DEFAULT_VERIFY_TOKENS,
                                                                   DEFAULT_INVALIDATE_CACHES,
                                                                   true,
                                                                   DEFAULT_COPY_DATA);
                        context.completeNow();
                    })));
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void testSucceedsWithExtendedVerify(VertxTestContext context) throws IOException, InterruptedException
    {
        UUID uploadId = UUID.randomUUID();
        Path stagedUploadDirectory = createStagedUploadFiles(uploadId);
        String stageDirectoryAbsolutePath = stagedUploadDirectory.toString();
        when(mockCFOperations.importNewSSTables("ks", "table", stageDirectoryAbsolutePath,
                                                true, true, true,
                                                DEFAULT_VERIFY_TOKENS, DEFAULT_INVALIDATE_CACHES, false,
                                                DEFAULT_COPY_DATA)).thenReturn(Collections.emptyList());

        String requestURI = "/api/v1/uploads/" + uploadId + "/keyspaces/ks/tables/table/import";
        sendRequest(context,
                    () -> client.put(server.actualPort(), "localhost", requestURI)
                                .addQueryParam("extendedVerify", "false"),
                    context.succeeding(response -> context.verify(() -> {
                        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                        verify(mockCFOperations).importNewSSTables("ks",
                                                                   "table",
                                                                   stageDirectoryAbsolutePath,
                                                                   true,
                                                                   true,
                                                                   true,
                                                                   DEFAULT_VERIFY_TOKENS,
                                                                   DEFAULT_INVALIDATE_CACHES,
                                                                   false,
                                                                   DEFAULT_COPY_DATA);
                        context.completeNow();
                    })));

        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    private void validateCleanup(VertxTestContext context, Path stagedUploadDirectory) throws InterruptedException
    {
        // give vertx some time to do the cleanup
        vertx.setTimer(500, id -> {
            assertThat(stagedUploadDirectory).doesNotExist(); // cleanup after success
            context.completeNow();
        });
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    private void clientRequest(VertxTestContext context, String requestURI, Consumer<HttpResponse<Buffer>> validator)
    throws InterruptedException
    {
        sendRequest(context,
                    () -> client.put(server.actualPort(), "localhost", requestURI),
                    context.succeeding(response -> context.verify(() -> {
                        validator.accept(response);
                        context.completeNow();
                    })));
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    private void sendRequest(VertxTestContext context, Supplier<HttpRequest<Buffer>> requestSupplier,
                             Handler<AsyncResult<HttpResponse<Buffer>>> handler)
    {
        requestSupplier.get()
                       .send(context.succeeding(r -> context.verify(() -> {
                           int statusCode = r.statusCode();
                           if (statusCode == HttpResponseStatus.ACCEPTED.code())
                           {
                               // retry the request in 100 ms when the request is accepted
                               vertx.setTimer(100, tid -> sendRequest(context, requestSupplier, handler));
                           }
                           else
                           {
                               handler.handle(Future.succeededFuture(r));
                           }
                       })));
    }
}
