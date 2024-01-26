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
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.common.data.Digest;

import static org.apache.cassandra.sidecar.common.http.SidecarHttpResponseStatus.CHECKSUM_MISMATCH;

/**
 * Provides basic functionality to perform digest validations using {@link AsyncFile}
 *
 * @param <D> the Digest type
 */
public abstract class AsyncFileDigestVerifier<D extends Digest> implements DigestVerifier
{
    public static final int DEFAULT_READ_BUFFER_SIZE = 512 * 1024; // 512KiB
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected final FileSystem fs;
    protected final D digest;

    protected AsyncFileDigestVerifier(FileSystem fs, D digest)
    {
        this.fs = fs;
        this.digest = digest;
    }

    /**
     * @param filePath path to SSTable component
     * @return a future String with the component path if verification is a success, otherwise a failed future
     */
    @Override
    public Future<String> verify(String filePath)
    {
        logger.debug("Validating {}. expected_checksum={}", digest.algorithm(), digest.value());

        return fs.open(filePath, new OpenOptions())
                 .compose(this::calculateHash)
                 .compose(computedChecksum -> {
                     if (!computedChecksum.equals(digest.value()))
                     {
                         logger.error("Checksum mismatch. computed_checksum={}, expected_checksum={}, algorithm=MD5",
                                      computedChecksum, digest.value());
                         return Future.failedFuture(new HttpException(CHECKSUM_MISMATCH.code(),
                                                                      String.format("Checksum mismatch. "
                                                                                    + "expected_checksum=%s, "
                                                                                    + "algorithm=%s",
                                                                                    digest.value(),
                                                                                    digest.algorithm())));
                     }
                     return Future.succeededFuture(filePath);
                 });
    }

    /**
     * Returns a future with the calculated checksum for the provided {@link AsyncFile file}.
     *
     * @param asyncFile the async file to use for hash calculation
     * @return a future with the computed checksum for the provided {@link AsyncFile file}
     */
    protected abstract Future<String> calculateHash(AsyncFile asyncFile);

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
                logger.error("Error while calculating the {} checksum", digest.algorithm(), cause);
                result.fail(cause);
            })
            .resume();
    }
}
