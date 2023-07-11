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
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;

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
        byte[] randomBytes = generateRandomBytes();
        Path randomFilePath = writeBytesToRandomFile(randomBytes);
        String expectedChecksum = Base64.getEncoder()
                                        .encodeToString(MessageDigest.getInstance("MD5")
                                                                     .digest(randomBytes));

        runTestScenario(randomFilePath, expectedChecksum);
    }

    @Test
    void testFileDescriptorsClosedWithInvalidChecksum() throws IOException, InterruptedException
    {
        Path randomFilePath = writeBytesToRandomFile(generateRandomBytes());
        runTestScenario(randomFilePath, "invalid");
    }

    private void runTestScenario(Path filePath, String checksum) throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(1);
        verifier.verify(checksum, filePath.toAbsolutePath().toString())
                .onComplete(complete -> latch.countDown());

        assertThat(latch.await(90, TimeUnit.SECONDS)).isTrue();

        assertThat(verifier.file).isNotNull();
        // we can't close the file if it's already closed, so we expect the exception here
        assertThatThrownBy(() -> verifier.file.end())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("File handle is closed");
    }

    private byte[] generateRandomBytes()
    {
        byte[] bytes = new byte[1024];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }

    private Path writeBytesToRandomFile(byte[] bytes) throws IOException
    {
        Path tempPath = tempDir.resolve("random-file.txt");
        try (RandomAccessFile writer = new RandomAccessFile(tempPath.toFile(), "rw"))
        {
            writer.write(bytes);
        }
        return tempPath;
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
        Future<String> calculateMD5(AsyncFile file)
        {
            this.file = file;
            return super.calculateMD5(file);
        }
    }
}
