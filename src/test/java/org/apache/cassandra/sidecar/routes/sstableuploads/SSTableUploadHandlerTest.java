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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.http.SidecarHttpResponseStatus;
import org.apache.cassandra.sidecar.snapshots.SnapshotUtils;

import static java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.GROUP_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_READ;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link SSTableUploadHandler}
 */
@ExtendWith(VertxExtension.class)
public class SSTableUploadHandlerTest extends BaseUploadsHandlerTest
{
    private static final String FILE_TO_BE_UPLOADED =
    "./src/test/resources/instance1/data/TestKeyspace/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshots"
    + "/TestSnapshot/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";

    @Test
    void testUploadWithoutMd5_expectSuccessfulUpload(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "without-md5.db", "",
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)), HttpResponseStatus.OK.code(), false);
    }

    @Test
    void testUploadWithCorrectMd5_expectSuccessfulUpload(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "with-correct-md5.db", "jXd/OF09/siBXSD3SWAm3A==",
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)), HttpResponseStatus.OK.code(), false);
    }

    @Test
    void testUploadWithIncorrectMd5_expectErrorCode(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "with-incorrect-md5.db", "incorrectMd5",
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)),
                                   SidecarHttpResponseStatus.CHECKSUM_MISMATCH.code(),
                                   false);
    }

    @Test
    void testInvalidFileName_expectErrorCode(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "ks$tbl-me-4-big-Data.db", "",
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)), HttpResponseStatus.BAD_REQUEST.code(),
                                   false);
    }

    @Test
    void testUploadWithoutContentLength_expectSuccessfulUpload(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "without-content-length.db",
                                   "jXd/OF09/siBXSD3SWAm3A==", 0, HttpResponseStatus.OK.code(), false);
    }

    @Test
    void testUploadTimeout_expectTimeoutError(VertxTestContext context) throws IOException
    {
        // if we send more than actual length, vertx goes hung, probably looking for more data than exists in the file,
        // we should see timeout error in this case
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "with-higher-content-length.db", "", 1000, -1, true);
    }

    @Test
    void testUploadWithLesserContentLength_expectSuccessfulUpload(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "with-lesser-content-length.db",
                                   "", Files.size(Paths.get(FILE_TO_BE_UPLOADED)) - 2, HttpResponseStatus.OK.code(),
                                   false);
    }

    @Test
    public void testInvalidUploadId(VertxTestContext context) throws IOException
    {
        sendUploadRequestAndVerify(null, context, "foo", "ks", "tbl", "with-lesser-content-length.db", "",
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)), HttpResponseStatus.BAD_REQUEST.code(),
                                   false, response -> {
            JsonObject error = response.bodyAsJsonObject();
            assertThat(error.getString("status")).isEqualTo("Bad Request");
            assertThat(error.getInteger("code")).isEqualTo(400);
            assertThat(error.getString("message")).isEqualTo("Invalid upload id is supplied, uploadId=foo");
        });
    }

    @Test
    public void testInvalidKeyspace(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "invalidKeyspace", "tbl", "with-lesser-content-length.db", "",
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)), HttpResponseStatus.BAD_REQUEST.code(),
                                   false);
    }

    @Test
    public void testInvalidTable(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "invalidTableName", "with-lesser-content-length.db", "",
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)), HttpResponseStatus.BAD_REQUEST.code(),
                                   false);
    }

    @Test
    public void testFreeSpacePercentCheckNotPassed(VertxTestContext context) throws IOException
    {
        when(mockSSTableUploadConfiguration.minimumSpacePercentageRequired()).thenReturn(100F);

        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "without-md5.db", "",
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)),
                                   HttpResponseStatus.INSUFFICIENT_STORAGE.code(), false);
    }

    @Test
    public void testConcurrentUploadLimitExceeded(VertxTestContext context) throws IOException
    {
        when(mockSSTableUploadConfiguration.concurrentUploadsLimit()).thenReturn(0);

        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "without-md5.db", "",
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)),
                                   HttpResponseStatus.TOO_MANY_REQUESTS.code(), false);
    }

    @Test
    public void testPermitCleanup(VertxTestContext context) throws IOException, InterruptedException
    {
        when(mockSSTableUploadConfiguration.concurrentUploadsLimit()).thenReturn(1);

        UUID uploadId = UUID.randomUUID();
        CountDownLatch latch = new CountDownLatch(1);
        sendUploadRequestAndVerify(latch, context, uploadId.toString(), "invalidKeyspace", "tbl",
                                   "without-md5.db", "", Files.size(Paths.get(FILE_TO_BE_UPLOADED)),
                                   HttpResponseStatus.BAD_REQUEST.code(), false);

        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();

        // checking if permits were released after bad requests
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "without-md5.db", "",
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)), HttpResponseStatus.OK.code(), false);
    }

    @Test
    public void testFilePermissionOnUpload(VertxTestContext context) throws IOException
    {
        String uploadId = UUID.randomUUID().toString();
        when(mockSSTableUploadConfiguration.filePermissions()).thenReturn("rwxr-xr-x");

        sendUploadRequestAndVerify(null, context, uploadId, "ks", "tbl", "without-md5.db", "",
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)), HttpResponseStatus.OK.code(),
                                   false, response -> {

            Path path = temporaryFolder.toPath()
                                       .resolve("staging")
                                       .resolve(uploadId)
                                       .resolve("ks")
                                       .resolve("tbl")
                                       .resolve("without-md5.db");

            try
            {
                Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
                assertThat(permissions).contains(OWNER_READ,
                                                 OWNER_WRITE,
                                                 OWNER_EXECUTE,
                                                 GROUP_READ,
                                                 GROUP_EXECUTE,
                                                 OTHERS_READ,
                                                 OTHERS_EXECUTE);
                assertThat(permissions).doesNotContain(GROUP_WRITE, OTHERS_WRITE);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    private void sendUploadRequestAndVerify(VertxTestContext context,
                                            UUID uploadId,
                                            String keyspace,
                                            String tableName,
                                            String targetFileName,
                                            String expectedMd5,
                                            long fileLength,
                                            int expectedRetCode,
                                            boolean expectTimeout)
    {
        sendUploadRequestAndVerify(null, context, uploadId.toString(), keyspace, tableName, targetFileName,
                                   expectedMd5, fileLength, expectedRetCode, expectTimeout);
    }

    private void sendUploadRequestAndVerify(CountDownLatch latch,
                                            VertxTestContext context,
                                            String uploadId,
                                            String keyspace,
                                            String tableName,
                                            String targetFileName,
                                            String expectedMd5,
                                            long fileLength,
                                            int expectedRetCode,
                                            boolean expectTimeout)
    {
        sendUploadRequestAndVerify(latch,
                                   context,
                                   uploadId,
                                   keyspace,
                                   tableName,
                                   targetFileName,
                                   expectedMd5,
                                   fileLength,
                                   expectedRetCode,
                                   expectTimeout,
                                   null);
    }

    private void sendUploadRequestAndVerify(CountDownLatch latch,
                                            VertxTestContext context,
                                            String uploadId,
                                            String keyspace,
                                            String tableName,
                                            String targetFileName,
                                            String expectedMd5,
                                            long fileLength,
                                            int expectedRetCode,
                                            boolean expectTimeout,
                                            Consumer<HttpResponse<Buffer>> responseValidator)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/uploads/" + uploadId + "/keyspaces/" + keyspace
                           + "/tables/" + tableName + "/components/" + targetFileName;
        HttpRequest<Buffer> req = client.put(server.actualPort(), "localhost", testRoute);
        if (!expectedMd5.isEmpty())
        {
            req.putHeader(HttpHeaderNames.CONTENT_MD5.toString(), expectedMd5);
        }
        if (fileLength != 0)
        {
            req.putHeader(HttpHeaderNames.CONTENT_LENGTH.toString(), Long.toString(fileLength));
        }

        AsyncFile fd = vertx.fileSystem().openBlocking(FILE_TO_BE_UPLOADED, new OpenOptions().setRead(true));
        req.sendStream(fd, response ->
        {
            if (expectTimeout)
            {
                assertThat(response.failed()).isTrue();
                context.completeNow();
                client.close();
                return;
            }

            HttpResponse<Buffer> httpResponse = response.result();
            assertThat(httpResponse.statusCode()).isEqualTo(expectedRetCode);

            if (responseValidator != null)
            {
                responseValidator.accept(httpResponse);
            }

            if (expectedRetCode == HttpResponseStatus.OK.code())
            {
                Path targetFilePath = Paths.get(SnapshotUtils.makeStagingDir(temporaryFolder.getAbsolutePath()),
                                                uploadId, keyspace, tableName, targetFileName);
                assertThat(Files.exists(targetFilePath)).isTrue();
            }

            if (latch != null)
            {
                latch.countDown();
            }
            else
            {
                context.completeNow();
            }
            client.close();
        });
    }
}
