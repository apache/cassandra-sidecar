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
import io.vertx.core.Vertx;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.common.data.XXHash32Digest;
import org.apache.cassandra.sidecar.restore.RestoreJobUtil;
import org.assertj.core.api.InstanceOfAssertFactories;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.from;

/**
 * Unit tests for {@link XXHash32DigestVerifier}
 */
class XXHash32DigestVerifierTest
{
    static Vertx vertx;

    @TempDir
    static Path tempDir;
    static Path randomFilePath;
    static RestoreJobUtil restoreJobUtil;

    @BeforeAll
    static void setup() throws IOException
    {
        vertx = Vertx.vertx();

        restoreJobUtil = new RestoreJobUtil(new Lz4XXHash32Provider());

        randomFilePath = TestFileUtils.prepareTestFile(tempDir, "random-file.txt", 1024);
    }

    @Test
    void failsWhenDigestIsNull()
    {
        assertThatNullPointerException().isThrownBy(() -> newVerifier(null)).withMessage("digest is required");
    }

    @Test
    void testFileDescriptorsClosedWithValidDigest() throws IOException, InterruptedException
    {
        XXHash32Digest digest = new XXHash32Digest(restoreJobUtil.checksum(randomFilePath.toFile()));
        runTestScenario(randomFilePath, digest, false);
    }

    @Test
    void failsWithNonDefaultSeedAndSeedIsNotPassedAsAnOption() throws IOException, InterruptedException
    {
        XXHash32Digest digest = new XXHash32Digest(restoreJobUtil.checksum(randomFilePath.toFile(), 0x55555555));
        runTestScenario(randomFilePath, digest, true);
    }

    @Test
    void testWithCustomSeed() throws IOException, InterruptedException
    {
        int seed = 0x55555555;
        XXHash32Digest digest = new XXHash32Digest(restoreJobUtil.checksum(randomFilePath.toFile(), seed), seed);
        runTestScenario(randomFilePath, digest, false);
    }

    @Test
    void testFileDescriptorsClosedWithInvalidDigest() throws InterruptedException
    {
        runTestScenario(randomFilePath, new XXHash32Digest("invalid"), true);
    }

    private void runTestScenario(Path filePath, XXHash32Digest digest,
                                 boolean errorExpectedDuringValidation) throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(1);
        ExposeAsyncFileXXHash32DigestVerifier verifier = newVerifier(digest);
        verifier.verify(filePath.toAbsolutePath().toString())
                .onComplete(complete -> {
                    if (errorExpectedDuringValidation)
                    {
                        assertThat(complete.failed()).isTrue();
                        assertThat(complete.cause())
                        .isInstanceOf(HttpException.class)
                        .extracting(from(t -> ((HttpException) t).getPayload()), as(InstanceOfAssertFactories.STRING))
                        .contains("Digest mismatch. expected_digest=" + digest.value());
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

    static ExposeAsyncFileXXHash32DigestVerifier newVerifier(XXHash32Digest digest)
    {
        return new ExposeAsyncFileXXHash32DigestVerifier(vertx.fileSystem(), digest);
    }

    /**
     * Class that extends from {@link XXHash32DigestVerifier} for testing purposes and holds a reference to the
     * {@link AsyncFile} to ensure that the file has been closed.
     */
    static class ExposeAsyncFileXXHash32DigestVerifier extends XXHash32DigestVerifier
    {
        AsyncFile file;

        public ExposeAsyncFileXXHash32DigestVerifier(FileSystem fs, XXHash32Digest digest)
        {
            super(fs, digest, new Lz4XXHash32Provider.Lz4XXHash32(maybeGetSeedOrDefault(digest)));
        }

        @Override
        protected Future<String> calculateDigest(AsyncFile file)
        {
            this.file = file;
            return super.calculateDigest(file);
        }
    }
}
