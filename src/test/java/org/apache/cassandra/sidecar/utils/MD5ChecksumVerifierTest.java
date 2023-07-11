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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Vertx;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MD5ChecksumVerifier}
 */
class MD5ChecksumVerifierTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MD5ChecksumVerifierTest.class);

    static Vertx vertx;
    static MD5ChecksumVerifier verifier;

    @TempDir
    Path tempDir;

    @BeforeAll
    static void setup()
    {
        vertx = Vertx.vertx();
        verifier = new MD5ChecksumVerifier(vertx.fileSystem());
    }

    @Test
    void testFileDescriptorsClosed() throws IOException, NoSuchAlgorithmException,
                                            InterruptedException
    {
        int iterationCount = 100_000;
        byte[] randomBytes = generateRandomBytes();
        Path randomFilePath = writeBytesToRandomFile(randomBytes);
        String expectedChecksum = Base64.getEncoder()
                                        .encodeToString(MessageDigest.getInstance("MD5")
                                                                     .digest(randomBytes));

        CountDownLatch latch = new CountDownLatch(iterationCount);
        for (int i = 0; i < iterationCount; i++)
        {
            Future<String> result;
            // we should exceed the file descriptor limit in most systems, 4096 is the default in some systems
            if (i % 2 == 0)
            {
                result = verifier.verify(expectedChecksum, randomFilePath.toAbsolutePath().toString());
            }
            else
            {
                result = verifier.verify("invalid", randomFilePath.toAbsolutePath().toString());
            }

            result.onComplete(complete -> latch.countDown());
            if (i % 1000 == 0)
            {
                // Slow down to prevent OOMs
                Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
                LOGGER.info("Processed {} elements, latch={}", i, latch.getCount());
            }
        }

        assertThat(latch.await(90, TimeUnit.SECONDS)).isTrue();
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
}
