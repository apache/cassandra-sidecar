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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;

/**
 * Utility class for asynchronously calculating file digests using Vert.x.
 * Reads files in chunks to avoid loading entire files into memory, making it suitable for large files.
 */
public class AsyncFileDigestCalculator
{
    public static final int DEFAULT_READ_BUFFER_SIZE = 512 * 1024; // 512KiB
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncFileDigestCalculator.class);

    /**
     * Returns a future with the calculated digest for the file at the provided path.
     *
     * @param vertx the Vertx instance
     * @param filePath the path to the file to use for digest calculation
     * @param digestAlgorithm the digest algorithm to use
     * @return a future with the computed digest for the file
     */
    public static Future<String> calculateDigest(Vertx vertx, String filePath, DigestAlgorithm digestAlgorithm)
    {
        return vertx.fileSystem()
                    .open(filePath, new OpenOptions().setRead(true).setCreate(false))
                    .compose(asyncFile -> calculateDigest(asyncFile, digestAlgorithm));
    }

    /**
     * Returns a future with the calculated digest for the provided {@link AsyncFile file}.
     *
     * @param asyncFile the async file to use for digest calculation
     * @return a future with the computed digest for the provided {@link AsyncFile file}
     */
    public static Future<String> calculateDigest(AsyncFile asyncFile, DigestAlgorithm digestAlgorithm)
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
                         LOGGER.warn("Potential memory leak due to failed to close hasher {}",
                                     digestAlgorithm.getClass().getSimpleName());
                     }
                 });

        return result.future();
    }

    private static void readFile(AsyncFile file,
                                 Promise<String> result,
                                 Handler<Buffer> onBufferAvailable,
                                 Handler<Void> onReadComplete)
    {
        // Make sure to close the file when complete
        result.future().onComplete(ignored -> file.end());
        file.pause()
            .setReadBufferSize(DEFAULT_READ_BUFFER_SIZE)
            .handler(onBufferAvailable)
            .endHandler(onReadComplete)
            .exceptionHandler(cause -> {
                LOGGER.error("Could not read file", cause);
                result.fail(cause);
            })
            .resume();
    }
}
