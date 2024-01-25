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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.web.handler.HttpException;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.common.http.SidecarHttpResponseStatus.CHECKSUM_MISMATCH;

/**
 * Provides basic functionality to perform checksum validations using {@link AsyncFile}
 */
public abstract class AsyncFileChecksumVerifier implements ChecksumVerifier
{
    public static final int DEFAULT_READ_BUFFER_SIZE = 512 * 1024; // 512KiB
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected final FileSystem fs;

    protected AsyncFileChecksumVerifier(FileSystem fs)
    {
        this.fs = fs;
    }

    /**
     * @param options  a map with options required for validation
     * @param filePath path to SSTable component
     * @return a future String with the component path if verification is a success, otherwise a failed future
     */
    @Override
    public Future<String> verify(MultiMap options, String filePath)
    {
        String expectedChecksum = expectedChecksum(options);
        if (expectedChecksum == null)
        {
            return Future.succeededFuture(filePath);
        }

        logger.debug("Validating {}. expected_checksum={}", algorithm(), expectedChecksum);

        return fs.open(filePath, new OpenOptions())
                 .compose((AsyncFile asyncFile) -> calculateHash(asyncFile, options))
                 .compose(computedChecksum -> {
                     if (!expectedChecksum.equals(computedChecksum))
                     {
                         logger.error("Checksum mismatch. computed_checksum={}, expected_checksum={}, algorithm=MD5",
                                      computedChecksum, expectedChecksum);
                         return Future.failedFuture(new HttpException(CHECKSUM_MISMATCH.code(),
                                                                      String.format("Checksum mismatch. "
                                                                                    + "expected_checksum=%s, "
                                                                                    + "algorithm=%s",
                                                                                    expectedChecksum,
                                                                                    algorithm())));
                     }
                     return Future.succeededFuture(filePath);
                 });
    }

    /**
     * @param options a map with options required to determine if the validation can be performed or not
     * @return {@code true} if the expected checksum is contained in the options, {@code false} otherwise
     */
    @Override
    public boolean canVerify(MultiMap options)
    {
        return expectedChecksum(options) != null;
    }

    /**
     * @param options key / value options used for verification
     * @return the expected checksum from the options
     */
    @Nullable
    protected abstract String expectedChecksum(MultiMap options);

    /**
     * @return the name of the checksum algorithm
     */
    protected abstract String algorithm();

    /**
     * Returns a future with the calculated checksum for the provided {@link AsyncFile file}.
     *
     * @param asyncFile the async file to use for hash calculation
     * @param options   a map with options required for validation
     * @return a future with the computed checksum for the provided {@link AsyncFile file}
     */
    protected abstract Future<String> calculateHash(AsyncFile asyncFile, MultiMap options);

    protected void readFile(AsyncFile file, Promise<String> result, Handler<Buffer> onBufferAvailable,
                            Handler<Void> onReadComplete)
    {
        // Make sure to close the file when complete
        result.future().onComplete(ignored -> file.end());
        file.pause()
            .setReadBufferSize(DEFAULT_READ_BUFFER_SIZE)
            .handler(onBufferAvailable)
            .endHandler(onReadComplete)
            .exceptionHandler(cause -> {
                logger.error("Error while calculating the {} checksum", algorithm(), cause);
                result.fail(cause);
            })
            .resume();
    }
}
