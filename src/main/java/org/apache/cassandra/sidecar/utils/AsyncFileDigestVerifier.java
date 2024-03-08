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
import java.util.Objects;

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
    private final DigestAlgorithm digestAlgorithm;

    protected AsyncFileDigestVerifier(FileSystem fs, D digest, DigestAlgorithm digestAlgorithm)
    {
        this.fs = fs;
        this.digest = Objects.requireNonNull(digest, "digest is required");
        this.digestAlgorithm = digestAlgorithm;
    }

    /**
     * @param filePath path to SSTable component
     * @return a future String with the component path if verification is a success, otherwise a failed future
     */
    @Override
    public Future<String> verify(String filePath)
    {
        logger.debug("Validating {}. expected_digest={}", digest.algorithm(), digest.value());

        return fs.open(filePath, new OpenOptions())
                 .compose(this::calculateDigest)
                 .compose(computedDigest -> {
                     if (!computedDigest.equals(digest.value()))
                     {
                         logger.error("Digest mismatch. computed_digest={}, expected_digest={}, algorithm=MD5",
                                      computedDigest, digest.value());
                         return Future.failedFuture(new HttpException(CHECKSUM_MISMATCH.code(),
                                                                      String.format("Digest mismatch. "
                                                                                    + "expected_digest=%s, "
                                                                                    + "algorithm=%s",
                                                                                    digest.value(),
                                                                                    digest.algorithm())));
                     }
                     return Future.succeededFuture(filePath);
                 });
    }

    /**
     * Returns a future with the calculated digest for the provided {@link AsyncFile file}.
     *
     * @param asyncFile the async file to use for digest calculation
     * @return a future with the computed digest for the provided {@link AsyncFile file}
     */
    protected Future<String> calculateDigest(AsyncFile asyncFile)
    {
        Promise<String> result = Promise.promise();

        readFile(asyncFile, result,
                 buf -> {
                     byte[] bytes = buf.getBytes();
                     digestAlgorithm.update(bytes, 0, bytes.length);
                 },
                 onReadComplete -> {
                     result.complete(digestAlgorithm.digest());
                     try
                     {
                         digestAlgorithm.close();
                     }
                     catch (IOException e)
                     {
                         logger.warn("Potential memory leak due to failed to close hasher {}",
                                     digestAlgorithm.getClass().getSimpleName());
                     }
                 });

        return result.future();
    }

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
                logger.error("Error while calculating the {} digest", digest.algorithm(), cause);
                result.fail(cause);
            })
            .resume();
    }
}
