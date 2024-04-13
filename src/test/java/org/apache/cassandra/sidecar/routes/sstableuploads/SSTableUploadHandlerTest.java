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
import org.apache.cassandra.sidecar.common.data.Digest;
import org.apache.cassandra.sidecar.common.data.MD5Digest;
import org.apache.cassandra.sidecar.common.data.XXHash32Digest;
import org.apache.cassandra.sidecar.common.http.SidecarHttpResponseStatus;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetricsImpl;
import org.apache.cassandra.sidecar.metrics.instance.UploadSSTableMetrics;
import org.apache.cassandra.sidecar.snapshots.SnapshotUtils;
import org.assertj.core.data.Percentage;

import static java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.GROUP_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_READ;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.apache.cassandra.sidecar.utils.TestFileUtils.prepareTestFile;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link SSTableUploadHandler}
 */
@ExtendWith(VertxExtension.class)
class SSTableUploadHandlerTest extends BaseUploadsHandlerTest
{
    private static final String FILE_TO_BE_UPLOADED =
    "./src/test/resources/instance1/data/TestKeyspace/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshots"
    + "/TestSnapshot/nb-1-big-Data.db";

    @Test
    void testUploadWithoutMd5_expectSuccessfulUpload(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "without-md5-Data.db", null,
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)), HttpResponseStatus.OK.code(), false);
    }

    @Test
    void testUploadWithCorrectMd5_expectSuccessfulUpload(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "with-correct-md5-Data.db",
                                   new MD5Digest("jXd/OF09/siBXSD3SWAm3A=="),
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)),
                                   HttpResponseStatus.OK.code(),
                                   false);
    }

    @Test
    void testUploadWithCorrectXXHash_expectSuccessfulUpload(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "with-correct-xxhash-Data.db",
                                   new XXHash32Digest("21228a35"),
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)),
                                   HttpResponseStatus.OK.code(),
                                   false);
    }

    @Test
    void testUploadWithCorrectXXHashAndCustomSeed_expectSuccessfulUpload(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "with-correct-xxhash-Data.db",
                                   new XXHash32Digest("b9510d6b", "55555555"),
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)),
                                   HttpResponseStatus.OK.code(),
                                   false);
    }

    @Test
    void testUploadWithIncorrectMd5_expectErrorCode(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "with-incorrect-md5-Data.db",
                                   new MD5Digest("incorrectMd5"),
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)),
                                   SidecarHttpResponseStatus.CHECKSUM_MISMATCH.code(),
                                   false);
    }

    @Test
    void testUploadWithIncorrectXXHash_expectErrorCode(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "with-incorrect-xxhash-Data.db",
                                   new XXHash32Digest("incorrectXXHash"),
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)),
                                   SidecarHttpResponseStatus.CHECKSUM_MISMATCH.code(),
                                   false);
    }

    @Test
    void testUploadWithIncorrectXXHashAndCustomSeed_expectErrorCode(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "with-incorrect-xxhash-Data.db",
                                   new XXHash32Digest("7a28edc0", "bad"),
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)),
                                   SidecarHttpResponseStatus.CHECKSUM_MISMATCH.code(),
                                   false);
    }

    @Test
    void testInvalidFileName_expectErrorCode(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "ks$tbl-me-4-big-Data.db", null,
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)), HttpResponseStatus.BAD_REQUEST.code(),
                                   false);
    }

    @Test
    void testUploadWithoutContentLength_expectSuccessfulUpload(VertxTestContext context)
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "without-content-length-Data.db",
                                   new MD5Digest("jXd/OF09/siBXSD3SWAm3A=="), 0, HttpResponseStatus.OK.code(), false);
    }

    @Test
    void testUploadTimeout_expectTimeoutError(VertxTestContext context)
    {
        // if we send more than actual length, vertx goes hung, probably looking for more data than exists in the file,
        // we should see timeout error in this case
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "with-higher-content-length-Data.db", null, 1000, -1,
                                   true);
    }

    @Test
    void testUploadWithLesserContentLength_expectSuccessfulUpload(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "with-lesser-content-length-Data.db",
                                   null, Files.size(Paths.get(FILE_TO_BE_UPLOADED)) - 2, HttpResponseStatus.OK.code(),
                                   false);
    }

    @Test
    void testInvalidUploadId(VertxTestContext context) throws IOException
    {
        sendUploadRequestAndVerify(null, context, "foo", "ks", "tbl", "with-lesser-content-length-Data.db", null,
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)), HttpResponseStatus.BAD_REQUEST.code(),
                                   false, response -> {
            JsonObject error = response.bodyAsJsonObject();
            assertThat(error.getString("status")).isEqualTo("Bad Request");
            assertThat(error.getInteger("code")).isEqualTo(400);
            assertThat(error.getString("message")).isEqualTo("Invalid upload id is supplied, uploadId=foo");
        }, FILE_TO_BE_UPLOADED);
    }

    @Test
    void testInvalidKeyspace(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "invalidKeyspace", "tbl", "with-lesser-content-length-Data.db", null,
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)), HttpResponseStatus.BAD_REQUEST.code(),
                                   false);
    }

    @Test
    void testInvalidTable(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "invalidTableName", "with-lesser-content-length-Data.db", null,
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)), HttpResponseStatus.BAD_REQUEST.code(),
                                   false);
    }

    @Test
    void testFreeSpacePercentCheckNotPassed(VertxTestContext context) throws IOException
    {
        when(mockSSTableUploadConfiguration.minimumSpacePercentageRequired()).thenReturn(100F);

        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "without-md5-Data.db", null,
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)),
                                   HttpResponseStatus.INSUFFICIENT_STORAGE.code(), false);
    }

    @Test
    void testConcurrentUploadLimitExceeded(VertxTestContext context) throws IOException
    {
        when(mockSSTableUploadConfiguration.concurrentUploadsLimit()).thenReturn(0);

        UUID uploadId = UUID.randomUUID();
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "without-md5-Dataa.db", null,
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)),
                                   HttpResponseStatus.TOO_MANY_REQUESTS.code(), false);
    }

    @Test
    void testPermitCleanup(VertxTestContext context) throws IOException, InterruptedException
    {
        when(mockSSTableUploadConfiguration.concurrentUploadsLimit()).thenReturn(1);

        UUID uploadId = UUID.randomUUID();
        CountDownLatch latch = new CountDownLatch(1);
        sendUploadRequestAndVerify(latch, context, uploadId.toString(), "invalidKeyspace", "tbl",
                                   "without-md5-Data.db", null, Files.size(Paths.get(FILE_TO_BE_UPLOADED)),
                                   HttpResponseStatus.BAD_REQUEST.code(), false);

        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();

        // checking if permits were released after bad requests
        sendUploadRequestAndVerify(context, uploadId, "ks", "tbl", "without-md5-Data.db", null,
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)), HttpResponseStatus.OK.code(), false);
    }

    @Test
    void testFilePermissionOnUpload(VertxTestContext context) throws IOException
    {
        String uploadId = UUID.randomUUID().toString();
        when(mockSSTableUploadConfiguration.filePermissions()).thenReturn("rwxr-xr-x");

        sendUploadRequestAndVerify(null, context, uploadId, "ks", "tbl", "without-md5-Data.db", null,
                                   Files.size(Paths.get(FILE_TO_BE_UPLOADED)), HttpResponseStatus.OK.code(),
                                   false, response -> {

            Path path = temporaryPath.resolve("staging")
                                     .resolve(uploadId)
                                     .resolve("ks")
                                     .resolve("tbl")
                                     .resolve("without-md5-Data.db");

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
        }, FILE_TO_BE_UPLOADED);
    }

    @Test
    void testRateLimitedByIngressFileRateLimiterUpload(VertxTestContext context) throws IOException
    {
        // upper-bound configured to 512 KBps in BaseUploadsHandlerTest#setup
        ingressFileRateLimiter.rate(256 * 1024L); // 256 KBps
        Path largeFilePath = prepareTestFile(temporaryPath, "1MB-File-Data.db", 1024 * 1024); // 1MB

        long startTime = System.nanoTime();
        String uploadId = UUID.randomUUID().toString();
        sendUploadRequestAndVerify(null, context, uploadId, "ks", "tbl", "1MB-File-Data.db", null,
                                   Files.size(largeFilePath), HttpResponseStatus.OK.code(),
                                   false, response -> {

            // SSTable upload should take around 4 seconds (256 KB/s for a 1MB file)
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
            assertThat(response).isNotNull();
            assertThat(elapsedMillis).isCloseTo(TimeUnit.SECONDS.toMillis(4),
                                                Percentage.withPercentage(5));
        }, largeFilePath.toString());
    }

    @Test
    void testRateLimitedByGlobalLimiterUpload(VertxTestContext context) throws IOException
    {
        // upper-bound configured to 512 KBps in BaseUploadsHandlerTest#setup
        // 1024 KBps Should not take effect, upper-bounded by global rate limiting
        ingressFileRateLimiter.rate(1024 * 1024L);
        Path largeFilePath = prepareTestFile(temporaryPath, "1MB-File-Data.db", 1024 * 1024); // 1MB

        long startTime = System.nanoTime();
        String uploadId = UUID.randomUUID().toString();
        sendUploadRequestAndVerify(null, context, uploadId, "ks", "tbl", "1MB-File-Data.db", null,
                                   Files.size(largeFilePath), HttpResponseStatus.OK.code(),
                                   false, response -> {

            // SSTable upload should take around 2 seconds (512 KB/s for a 1MB file)
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
            assertThat(response).isNotNull();
            assertThat(elapsedMillis).isCloseTo(TimeUnit.SECONDS.toMillis(2),
                                                Percentage.withPercentage(10));
        }, largeFilePath.toString());
    }

    private void sendUploadRequestAndVerify(VertxTestContext context,
                                            UUID uploadId,
                                            String keyspace,
                                            String tableName,
                                            String targetFileName,
                                            Digest expectedDigest,
                                            long fileLength,
                                            int expectedRetCode,
                                            boolean expectTimeout)
    {
        sendUploadRequestAndVerify(null, context, uploadId.toString(), keyspace, tableName, targetFileName,
                                   expectedDigest, fileLength, expectedRetCode, expectTimeout);
    }

    private void sendUploadRequestAndVerify(CountDownLatch latch,
                                            VertxTestContext context,
                                            String uploadId,
                                            String keyspace,
                                            String tableName,
                                            String targetFileName,
                                            Digest expectedDigest,
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
                                   expectedDigest,
                                   fileLength,
                                   expectedRetCode,
                                   expectTimeout,
                                   null,
                                   FILE_TO_BE_UPLOADED);
    }

    private void sendUploadRequestAndVerify(CountDownLatch latch,
                                            VertxTestContext context,
                                            String uploadId,
                                            String keyspace,
                                            String tableName,
                                            String targetFileName,
                                            Digest expectedDigest,
                                            long fileLength,
                                            int expectedRetCode,
                                            boolean expectTimeout,
                                            Consumer<HttpResponse<Buffer>> responseValidator,
                                            String fileToBeUploaded)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/uploads/" + uploadId + "/keyspaces/" + keyspace
                           + "/tables/" + tableName + "/components/" + targetFileName;
        HttpRequest<Buffer> req = client.put(server.actualPort(), "localhost", testRoute);
        if (expectedDigest != null)
        {
            req.headers().addAll(expectedDigest.headers());
        }
        if (fileLength != 0)
        {
            req.putHeader(HttpHeaderNames.CONTENT_LENGTH.toString(), Long.toString(fileLength));
        }

        AsyncFile fd = vertx.fileSystem().openBlocking(fileToBeUploaded, new OpenOptions().setRead(true));
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
            if (httpResponse.statusCode() != expectedRetCode)
            {
                context.failNow("Status code mismatched. Expected: " + expectedRetCode +
                                "; actual: " + httpResponse.statusCode());
                return;
            }
            UploadSSTableMetrics uploadMetrics = new InstanceMetricsImpl(registry(1)).uploadSSTable();
            UploadSSTableMetrics.UploadSSTableComponentMetrics componentMetrics = uploadMetrics.forComponent("Data.db");
            if (expectedRetCode == HttpResponseStatus.TOO_MANY_REQUESTS.code())
            {
                assertThat(uploadMetrics.rateLimitedCalls.metric.getValue()).isOne();
            }

            if (responseValidator != null)
            {
                responseValidator.accept(httpResponse);
            }

            if (expectedRetCode == HttpResponseStatus.OK.code())
            {
                Path targetFilePath = Paths.get(SnapshotUtils.makeStagingDir(canonicalTemporaryPath),
                                                uploadId, keyspace, tableName, targetFileName);
                assertThat(Files.exists(targetFilePath)).isTrue();
                try
                {
                    long expectedSize = Files.size(targetFilePath);
                    assertThat(componentMetrics.bytesUploadedRate.metric.getCount()).isEqualTo(expectedSize);
                    assertThat(uploadMetrics.totalBytesUploadedRate.metric.getCount()).isEqualTo(Files.size(targetFilePath));
                }
                catch (Exception e)
                {
                    if (latch != null)
                    {
                        latch.countDown();
                    }
                    else
                    {
                        context.failNow(e);
                    }
                    client.close();
                    return;
                }
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
