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

import java.io.File;
import java.nio.file.AtomicMoveNotSupportedException;

import com.google.common.util.concurrent.SidecarRateLimiter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.CopyOptions;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import org.apache.cassandra.sidecar.exceptions.ThrowableUtils;

/**
 * A class that handles SSTable Uploads
 */
@Singleton
public class SSTableUploader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableUploader.class);
    private static final String DEFAULT_TEMP_SUFFIX = ".tmp";

    private final FileSystem fs;
    private final SidecarRateLimiter rateLimiter;

    /**
     * Constructs an instance of {@link SSTableUploader} with provided params for uploading an SSTable component.
     *
     * @param vertx       Vertx reference
     * @param rateLimiter rate limiter for uploading SSTable components
     */
    @Inject
    public SSTableUploader(Vertx vertx,
                           @Named("IngressFileRateLimiter") SidecarRateLimiter rateLimiter)
    {
        this.fs = vertx.fileSystem();
        this.rateLimiter = rateLimiter;
    }

    /**
     * This method when called uploads the SSTable component in context and returns the component's path.
     *
     * @param readStream        server request from which file upload is acquired
     * @param uploadDirectory   the absolute path to the upload directory in the target {@code fs}
     * @param componentFileName the file name of the component
     * @param digestVerifier    the digest verifier instance
     * @param filePermissions   specifies the posix file permissions used to create the SSTable file
     * @return path of SSTable component to which data was uploaded
     */
    public Future<String> uploadComponent(ReadStream<Buffer> readStream,
                                          String uploadDirectory,
                                          String componentFileName,
                                          DigestVerifier digestVerifier,
                                          String filePermissions)
    {

        String targetPath = StringUtils.removeEnd(uploadDirectory, File.separator)
                            + File.separatorChar + componentFileName;

        return fs.mkdirs(uploadDirectory) // ensure the parent directory is created
                 .compose(v -> createTempFile(uploadDirectory, componentFileName, filePermissions))
                 .compose(tempFilePath -> streamAndVerify(readStream, tempFilePath, digestVerifier))
                 .compose(verifiedTempFilePath -> moveAtomicallyWithFallBack(verifiedTempFilePath, targetPath));
    }

    private Future<String> streamAndVerify(ReadStream<Buffer> readStream, String tempFilePath,
                                           DigestVerifier digestVerifier)
    {
        // pipe read stream to temp file
        return streamToFile(readStream, tempFilePath)
               .compose(v -> digestVerifier.verify(tempFilePath))
               .onFailure(throwable -> fs.delete(tempFilePath));
    }

    private Future<Void> streamToFile(ReadStream<Buffer> readStream, String tempFilename)
    {
        LOGGER.debug("Uploading data to={}", tempFilename);
        return fs.open(tempFilename, new OpenOptions()) // open the temp file
                 .map(file -> new RateLimitedWriteStream(rateLimiter, file))
                 .compose(file -> {
                     readStream.resume();
                     return readStream.pipeTo(file);
                 }); // stream to file
    }

    private Future<String> createTempFile(String uploadDirectory, String componentFileName, String permissions)
    {
        LOGGER.debug("Creating temp file in directory={} with name={}{}, permissions={}",
                     uploadDirectory, componentFileName, DEFAULT_TEMP_SUFFIX, permissions);

        return fs.createTempFile(uploadDirectory, componentFileName, DEFAULT_TEMP_SUFFIX, permissions);
    }

    private Future<String> moveAtomicallyWithFallBack(String source, String target)
    {
        LOGGER.debug("Moving from={} to={}", source, target);
        return fs.move(source, target, new CopyOptions().setAtomicMove(true))
                 .recover(cause -> {
                     Exception atomicMoveNotSupportedException =
                     ThrowableUtils.getCause(cause, AtomicMoveNotSupportedException.class);
                     if (atomicMoveNotSupportedException != null)
                     {
                         LOGGER.warn("Failed to perform atomic move from={} to={}", source, target, cause);
                         return fs.move(source, target, new CopyOptions().setAtomicMove(false));
                     }
                     return Future.failedFuture(cause);
                 })
                 .compose(v -> Future.succeededFuture(target));
    }

    /**
     * A {@link WriteStream} implementation that supports rate limiting.
     */
    public static class RateLimitedWriteStream implements WriteStream<Buffer>
    {
        private final SidecarRateLimiter limiter;
        private final WriteStream<Buffer> delegate;

        public RateLimitedWriteStream(SidecarRateLimiter limiter, WriteStream<Buffer> delegate)
        {
            this.limiter = limiter;
            this.delegate = delegate;
        }

        @Override
        public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler)
        {
            return delegate.exceptionHandler(handler);
        }

        @Override
        public Future<Void> write(Buffer data)
        {
            limiter.acquire(data.length()); // apply backpressure on the received bytes
            return delegate.write(data);
        }

        @Override
        public void write(Buffer data, Handler<AsyncResult<Void>> handler)
        {
            limiter.acquire(data.length()); // apply backpressure on the received bytes
            delegate.write(data, handler);
        }

        @Override
        public Future<Void> end()
        {
            return delegate.end();
        }

        @Override
        public void end(Handler<AsyncResult<Void>> handler)
        {
            delegate.end(handler);
        }

        @Override
        public Future<Void> end(Buffer data)
        {
            return delegate.end(data);
        }

        @Override
        public void end(Buffer data, Handler<AsyncResult<Void>> handler)
        {
            delegate.end(data, handler);
        }

        @Override
        public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize)
        {
            return delegate.setWriteQueueMaxSize(maxSize);
        }

        @Override
        public boolean writeQueueFull()
        {
            return delegate.writeQueueFull();
        }

        @Override
        public WriteStream<Buffer> drainHandler(Handler<Void> handler)
        {
            return delegate.drainHandler(handler);
        }
    }
}
