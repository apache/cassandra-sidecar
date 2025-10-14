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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link AsyncFileDigestCalculator}
 */
@ExtendWith(VertxExtension.class)
class AsyncFileDigestCalculatorTest
{

    @Test
    void testCalculateDigestForSmallFile(Vertx vertx, VertxTestContext context, @TempDir Path tempDir) throws IOException, NoSuchAlgorithmException
    {
        Path testFile = TestFileUtils.prepareTestFile(tempDir, "small-file.txt", 1024);
        String expectedDigest = md5Sum(testFile);
        DigestAlgorithm digestAlgorithm = new JdkMd5DigestProvider().get();

        AsyncFileDigestCalculator.calculateDigest(vertx, testFile.toString(), digestAlgorithm)
                                 .onComplete(context.succeeding(digest -> context.verify(() -> {
                                     assertThat(digest).isNotNull();
                                     assertThat(digest).isNotEmpty();
                                     assertThat(digest).isEqualTo(expectedDigest);
                                     context.completeNow();
                                 })));
    }

    @Test
    void testCalculateDigestForLargeFile(Vertx vertx, VertxTestContext context, @TempDir Path tempDir) throws IOException, NoSuchAlgorithmException
    {
        // File larger than DEFAULT_READ_BUFFER_SIZE (512KB)
        Path testFile = TestFileUtils.prepareTestFile(tempDir, "large-file.txt", 1024 * 1024);
        String expectedDigest = md5Sum(testFile);
        DigestAlgorithm digestAlgorithm = new JdkMd5DigestProvider().get();

        AsyncFileDigestCalculator.calculateDigest(vertx, testFile.toString(), digestAlgorithm)
                                 .onComplete(context.succeeding(digest -> context.verify(() -> {
                                     assertThat(digest).isNotNull();
                                     assertThat(digest).isNotEmpty();
                                     assertThat(digest).isEqualTo(expectedDigest);
                                     context.completeNow();
                                 })));
    }

    @Test
    void testCalculateDigestForEmptyFile(Vertx vertx, VertxTestContext context, @TempDir Path tempDir) throws IOException, NoSuchAlgorithmException
    {
        Path testFile = Files.createFile(tempDir.resolve("empty-file.txt"));
        String expectedDigest = md5Sum(testFile);
        DigestAlgorithm digestAlgorithm = new JdkMd5DigestProvider().get();

        AsyncFileDigestCalculator.calculateDigest(vertx, testFile.toString(), digestAlgorithm)
                                 .onComplete(context.succeeding(digest -> context.verify(() -> {
                                     assertThat(digest).isNotNull();
                                     assertThat(digest).isNotEmpty();
                                     assertThat(digest).isEqualTo(expectedDigest);
                                     context.completeNow();
                                 })));
    }

    @Test
    void testCalculateDigestWithXXHash32(Vertx vertx, VertxTestContext context, @TempDir Path tempDir) throws IOException
    {
        Path testFile = TestFileUtils.prepareTestFile(tempDir, "test-xxhash.txt", 2048);
        String expectedDigest = xxHash32Sum(testFile);
        DigestAlgorithm digestAlgorithm = new XXHash32Provider().get();

        AsyncFileDigestCalculator.calculateDigest(vertx, testFile.toString(), digestAlgorithm)
                                 .onComplete(context.succeeding(digest -> context.verify(() -> {
                                     assertThat(digest).isNotNull();
                                     assertThat(digest).isNotEmpty();
                                     assertThat(digest).isEqualTo(expectedDigest);
                                     context.completeNow();
                                 })));
    }

    @Test
    void testCalculateDigestWithXXHash32WithSeed(Vertx vertx,
                                                 VertxTestContext context,
                                                 @TempDir Path tempDir) throws IOException
    {
        Path testFile = TestFileUtils.prepareTestFile(tempDir, "test-xxhash-seed.txt", 2048);
        String expectedDigest = xxHash32Sum(testFile, 12345);
        DigestAlgorithm digestAlgorithm = new XXHash32Provider().get(12345);

        AsyncFileDigestCalculator.calculateDigest(vertx, testFile.toString(), digestAlgorithm)
                                 .onComplete(context.succeeding(digest -> context.verify(() -> {
                                     assertThat(digest).isNotNull();
                                     assertThat(digest).isNotEmpty();
                                     assertThat(digest).isEqualTo(expectedDigest);
                                     context.completeNow();
                                 })));
    }

    @Test
    void testCalculateDigestForNonExistentFile(Vertx vertx, VertxTestContext context, @TempDir Path tempDir)
    {
        Path nonExistentFile = tempDir.resolve("does-not-exist.txt");
        DigestAlgorithm digestAlgorithm = new JdkMd5DigestProvider().get();

        AsyncFileDigestCalculator.calculateDigest(vertx, nonExistentFile.toString(), digestAlgorithm)
                                 .onComplete(context.failing(cause -> context.verify(() -> {
                                     assertThat(cause).isNotNull();
                                     context.completeNow();
                                 })));
    }

    @Test
    void testCalculateDigestConsistency(Vertx vertx, VertxTestContext context, @TempDir Path tempDir) throws IOException, NoSuchAlgorithmException
    {
        Path testFile = TestFileUtils.prepareTestFile(tempDir, "consistent-file.txt", 4096);
        String expectedDigest = md5Sum(testFile);
        DigestAlgorithm digestAlgorithm1 = new JdkMd5DigestProvider().get();
        DigestAlgorithm digestAlgorithm2 = new JdkMd5DigestProvider().get();

        AsyncFileDigestCalculator.calculateDigest(vertx, testFile.toString(), digestAlgorithm1)
                                 .compose(digest1 ->
                                          AsyncFileDigestCalculator.calculateDigest(vertx, testFile.toString(), digestAlgorithm2)
                                                                   .map(digest2 -> {
                                                                       assertThat(digest1).isEqualTo(digest2);
                                                                       assertThat(digest1).isEqualTo(expectedDigest);
                                                                       assertThat(digest2).isEqualTo(expectedDigest);
                                                                       return digest2;
                                                                   })
                                 )
                                 .onComplete(context.succeeding(digest -> context.verify(() -> {
                                     assertThat(digest).isNotNull();
                                     context.completeNow();
                                 })));
    }

    @Test
    void testCalculateDigestWithDifferentContent(Vertx vertx,
                                                 VertxTestContext context,
                                                 @TempDir Path tempDir) throws IOException, NoSuchAlgorithmException
    {
        Path testFile1 = TestFileUtils.prepareTestFile(tempDir, "file1.txt", 1024);
        Path testFile2 = TestFileUtils.prepareTestFile(tempDir, "file2.txt", 1024);
        String expectedDigest1 = md5Sum(testFile1);
        String expectedDigest2 = md5Sum(testFile2);
        DigestAlgorithm digestAlgorithm1 = new JdkMd5DigestProvider().get();
        DigestAlgorithm digestAlgorithm2 = new JdkMd5DigestProvider().get();

        AsyncFileDigestCalculator.calculateDigest(vertx, testFile1.toString(), digestAlgorithm1)
                                 .compose(digest1 ->
                                          AsyncFileDigestCalculator.calculateDigest(vertx, testFile2.toString(), digestAlgorithm2)
                                                                   .map(digest2 -> {
                                                                       assertThat(digest1).isNotEqualTo(digest2);
                                                                       assertThat(digest1).isEqualTo(expectedDigest1);
                                                                       assertThat(digest2).isEqualTo(expectedDigest2);
                                                                       return digest2;
                                                                   })
                                 )
                                 .onComplete(context.succeeding(digest -> context.verify(() -> {
                                     assertThat(digest).isNotNull();
                                     context.completeNow();
                                 })));
    }

    @Test
    void testAsyncFileIsClosedOnSuccess(Vertx vertx, VertxTestContext context, @TempDir Path tempDir) throws IOException, NoSuchAlgorithmException
    {
        Path testFile = TestFileUtils.prepareTestFile(tempDir, "file-closed-success.txt", 1024);
        String expectedDigest = md5Sum(testFile);
        DigestAlgorithm digestAlgorithm = new JdkMd5DigestProvider().get();

        vertx.fileSystem()
             .open(testFile.toString(), new OpenOptions().setRead(true))
             .onSuccess(asyncFile -> {
                 AsyncFileDigestCalculator.calculateDigest(asyncFile, digestAlgorithm)
                                          .onComplete(context.succeeding(digest -> context.verify(() -> {
                                              assertThat(digest).isNotNull();
                                              assertThat(digest).isEqualTo(expectedDigest);
                                              // Closing a file which is already closed throws IllegalStateException
                                              // Closing it again to verify that it is closed
                                              assertThatThrownBy(asyncFile::end).isInstanceOf(IllegalStateException.class);
                                              context.completeNow();
                                          })));
             });
    }

    String md5Sum(Path path) throws NoSuchAlgorithmException, IOException
    {
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        byte[] bytes = Files.readAllBytes(path);
        return Base64.getEncoder()
                     .encodeToString(md5.digest(bytes));
    }

    String xxHash32Sum(Path path) throws IOException
    {
        return xxHash32Sum(path, 0);
    }

    String xxHash32Sum(Path path, int seed) throws IOException
    {
        DigestAlgorithm digestAlgorithm = new XXHash32Provider().get(seed);
        byte[] bytes = Files.readAllBytes(path);
        digestAlgorithm.update(bytes, 0, bytes.length);
        return digestAlgorithm.digest();
    }
}
