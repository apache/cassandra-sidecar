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
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link MD5ChecksumVerifier}
 */
class MD5ChecksumVerifierTest
{
    static Vertx vertx;
    static ExposeAsyncFileMD5ChecksumVerifier verifier;

    @TempDir
    Path tempDir;

    @BeforeAll
    static void setup()
    {
        vertx = Vertx.vertx();
        verifier = new ExposeAsyncFileMD5ChecksumVerifier(vertx.fileSystem());
    }

    @Test
    void testFileDescriptorsClosedWithValidChecksum() throws IOException, NoSuchAlgorithmException,
                                                             InterruptedException
    {
        Path randomFilePath = TestFileUtils.prepareTestFile(tempDir, "random-file.txt", 1024);
        byte[] randomBytes = Files.readAllBytes(randomFilePath);
        String expectedChecksum = Base64.getEncoder()
                                        .encodeToString(MessageDigest.getInstance("MD5")
                                                                     .digest(randomBytes));

        runTestScenario(randomFilePath, expectedChecksum);
    }

    @Test
    void testFileDescriptorsClosedWithInvalidChecksum() throws IOException, InterruptedException
    {
        Path randomFilePath = TestFileUtils.prepareTestFile(tempDir, "random-file.txt", 1024);
        runTestScenario(randomFilePath, "invalid");
    }

    private void runTestScenario(Path filePath, String checksum) throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(1);
        verifier.verify(new HeadersMultiMap().set("content-md5", checksum),
                        filePath.toAbsolutePath().toString())
                .onComplete(complete -> latch.countDown());

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();

        assertThat(verifier.file).isNotNull();
        // we can't close the file if it's already closed, so we expect the exception here
        assertThatThrownBy(() -> verifier.file.end())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("File handle is closed");
    }

    /**
     * Class that extends from {@link MD5ChecksumVerifier} for testing purposes and holds a reference to the
     * {@link AsyncFile} to ensure that the file has been closed.
     */
    static class ExposeAsyncFileMD5ChecksumVerifier extends MD5ChecksumVerifier
    {
        AsyncFile file;

        public ExposeAsyncFileMD5ChecksumVerifier(FileSystem fs)
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
