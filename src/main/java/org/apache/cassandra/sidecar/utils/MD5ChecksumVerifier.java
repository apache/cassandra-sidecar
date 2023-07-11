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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.web.handler.HttpException;

import static org.apache.cassandra.sidecar.common.http.SidecarHttpResponseStatus.CHECKSUM_MISMATCH;

/**
 * Implementation of {@link ChecksumVerifier}. Here we use MD5 implementation of {@link java.security.MessageDigest}
 * for calculating checksum. And match the calculated checksum with expected checksum obtained from request.
 */
public class MD5ChecksumVerifier implements ChecksumVerifier
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MD5ChecksumVerifier.class);
    public static final int DEFAULT_READ_BUFFER_SIZE = 64 * 1024; // 64KiB
    private final FileSystem fs;

    public MD5ChecksumVerifier(FileSystem fs)
    {
        this.fs = fs;
    }

    public Future<String> verify(String expectedChecksum, String filePath)
    {
        if (expectedChecksum == null)
        {
            return Future.succeededFuture(filePath);
        }

        LOGGER.debug("Validating MD5. expected_checksum={}", expectedChecksum);

        return fs.open(filePath, new OpenOptions())
                 .compose(this::calculateMD5)
                 .compose(computedChecksum -> {
                     if (!expectedChecksum.equals(computedChecksum))
                     {
                         LOGGER.error("Checksum mismatch. computed_checksum={}, expected_checksum={}, algorithm=MD5",
                                      computedChecksum, expectedChecksum);
                         return Future.failedFuture(new HttpException(CHECKSUM_MISMATCH.code(),
                                                                      String.format("Checksum mismatch. "
                                                                                    + "expected_checksum=%s, "
                                                                                    + "algorithm=MD5",
                                                                                    expectedChecksum)));
                     }
                     return Future.succeededFuture(filePath);
                 });
    }

    private Future<String> calculateMD5(AsyncFile file)
    {
        MessageDigest digest;
        try
        {
            digest = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
            return Future.failedFuture(e);
        }

        Promise<String> result = Promise.promise();
        file.pause()
            .setReadBufferSize(DEFAULT_READ_BUFFER_SIZE)
            .handler(buf -> digest.update(buf.getBytes()))
            .endHandler(_v -> {
                result.complete(Base64.getEncoder().encodeToString(digest.digest()));
                file.end();
            })
            .exceptionHandler(cause -> {
                LOGGER.error("Error while calculating MD5 checksum", cause);
                result.fail(cause);
                file.end();
            })
            .resume();
        return result.future();
    }
}
