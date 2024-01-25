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

package org.apache.cassandra.sidecar.utils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.ext.web.handler.HttpException;
import org.assertj.core.api.InstanceOfAssertFactories;

import static org.apache.cassandra.sidecar.restore.RestoreJobUtil.checksum;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.from;

/**
 * Unit tests for {@link XXHash32ChecksumVerifier}
 */
class XXHash32ChecksumVerifierTest
{
    static Vertx vertx;
    static ExposeAsyncFileXXHash32ChecksumVerifier verifier;

    @TempDir
    static Path tempDir;
    static Path randomFilePath;

    @BeforeAll
    static void setup() throws IOException
    {
        vertx = Vertx.vertx();
        verifier = new ExposeAsyncFileXXHash32ChecksumVerifier(vertx.fileSystem());
        randomFilePath = TestFileUtils.prepareTestFile(tempDir, "random-file.txt", 1024);
    }

    @Test
    void testValidationSucceedsWhenChecksumIsNotNull() throws InterruptedException, IOException
    {
        runTestScenario(randomFilePath, null, null, false);
    }

    @Test
    void testFileDescriptorsClosedWithValidChecksum() throws IOException, InterruptedException
    {
        String expectedChecksum = checksum(randomFilePath.toFile());
        runTestScenario(randomFilePath, expectedChecksum, null, false);
    }

    @Test
    void failsWithNonDefaultSeedAndSeedIsNotPassedAsAnOption() throws IOException, InterruptedException
    {
        String expectedChecksum = checksum(randomFilePath.toFile(), 0x55555555);
        runTestScenario(randomFilePath, expectedChecksum, null, true);
    }

    @Test
    void testWithCustomSeed() throws IOException, InterruptedException
    {
        int seed = 0x55555555;
        String expectedChecksum = checksum(randomFilePath.toFile(), seed);
        runTestScenario(randomFilePath, expectedChecksum, Integer.toHexString(seed), false);
    }

    @Test
    void testFileDescriptorsClosedWithInvalidChecksum() throws IOException, InterruptedException
    {
        runTestScenario(randomFilePath, "invalid", null, true);
    }

    private void runTestScenario(Path filePath, String checksum, String seedHex,
                                 boolean errorExpectedDuringValidation) throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(1);
        MultiMap options = new HeadersMultiMap();
        if (checksum != null)
        {
            options.set("content-xxhash32", checksum);
        }
        if (seedHex != null)
        {
            options.set("content-xxhash32-seed", seedHex);
        }
        verifier.verify(options, filePath.toAbsolutePath().toString())
                .onComplete(complete -> {
                    if (errorExpectedDuringValidation)
                    {
                        assertThat(complete.failed()).isTrue();
                        assertThat(complete.cause())
                        .isInstanceOf(HttpException.class)
                        .extracting(from(t -> ((HttpException) t).getPayload()), as(InstanceOfAssertFactories.STRING))
                        .contains("Checksum mismatch. expected_checksum=" + checksum);
                    }
                    else
                    {
                        assertThat(complete.failed()).isFalse();
                        assertThat(complete.result()).endsWith("random-file.txt");
                    }
                    latch.countDown();
                });

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();

        assertThat(verifier.file).isNotNull();
        // we can't close the file if it's already closed, so we expect the exception here
        assertThatThrownBy(() -> verifier.file.end())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("File handle is closed");
    }

    /**
     * Class that extends from {@link XXHash32ChecksumVerifier} for testing purposes and holds a reference to the
     * {@link AsyncFile} to ensure that the file has been closed.
     */
    static class ExposeAsyncFileXXHash32ChecksumVerifier extends XXHash32ChecksumVerifier
    {
        AsyncFile file;

        public ExposeAsyncFileXXHash32ChecksumVerifier(FileSystem fs)
        {
            super(fs);
        }

        @Override
        protected Future<String> calculateHash(AsyncFile file, MultiMap options)
        {
            this.file = file;
            return super.calculateHash(file, options);
        }
    }
}
