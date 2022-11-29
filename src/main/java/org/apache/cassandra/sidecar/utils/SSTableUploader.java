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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.CopyOptions;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.streams.ReadStream;

/**
 * A class that handles SSTable Uploads
 */
@Singleton
public class SSTableUploader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableUploader.class);
    private static final String DEFAULT_TEMP_SUFFIX = ".tmp";

    private final FileSystem fs;
    private final ChecksumVerifier checksumVerifier;

    /**
     * Constructs an instance of {@link SSTableUploader} with provided params for uploading an SSTable component.
     *
     * @param vertx            Vertx reference
     * @param checksumVerifier verifier for checking integrity of upload
     */
    @Inject
    public SSTableUploader(Vertx vertx, ChecksumVerifier checksumVerifier)
    {
        this.fs = vertx.fileSystem();
        this.checksumVerifier = checksumVerifier;
    }

    /**
     * This method when called uploads the SSTable component in context and returns the component's path.
     *
     * @param readStream        server request from which file upload is acquired
     * @param uploadDirectory   the absolute path to the upload directory in the target {@code fs}
     * @param componentFileName the file name of the component
     * @param expectedChecksum  for verifying upload integrity, passed in through request
     * @return path of SSTable component to which data was uploaded
     */
    public Future<String> uploadComponent(ReadStream<Buffer> readStream,
                                          String uploadDirectory,
                                          String componentFileName,
                                          String expectedChecksum)
    {

        String targetPath = StringUtils.removeEnd(uploadDirectory, File.separator)
                            + File.separatorChar + componentFileName;

        return fs.mkdirs(uploadDirectory) // ensure the parent directory is created
                 .compose(v -> createTempFile(uploadDirectory, componentFileName)) // create a temporary file
                 .compose(tempFilePath -> streamAndVerify(readStream, tempFilePath, expectedChecksum))
                 .compose(verifiedTempFilePath -> moveAtomicallyWithFallBack(verifiedTempFilePath, targetPath));
    }

    private Future<String> streamAndVerify(ReadStream<Buffer> readStream, String tempFilePath, String expectedChecksum)
    {
        // pipe read stream to temp file
        return streamToFile(readStream, tempFilePath)
               .compose(v -> checksumVerifier.verify(expectedChecksum, tempFilePath));
    }

    private Future<Void> streamToFile(ReadStream<Buffer> readStream, String tempFilename)
    {
        LOGGER.debug("Uploading data to={}", tempFilename);
        return fs.open(tempFilename, new OpenOptions()) // open the temp file
                 .compose(file -> {
                     readStream.resume();
                     return readStream.pipeTo(file);
                 }); // stream to file
    }

    private Future<String> createTempFile(String uploadDirectory, String componentFileName)
    {
        LOGGER.debug("Creating temp file in directory={} with name={}{}",
                     uploadDirectory, componentFileName, DEFAULT_TEMP_SUFFIX);
        return fs.createTempFile(uploadDirectory, componentFileName, DEFAULT_TEMP_SUFFIX, /* perms */ (String) null);
    }

    private Future<String> moveAtomicallyWithFallBack(String source, String target)
    {
        LOGGER.debug("Moving from={} to={}", source, target);
        return fs.move(source, target, new CopyOptions().setAtomicMove(true))
                 .recover(cause -> {
                     if (hasCause(cause, AtomicMoveNotSupportedException.class, 10))
                     {
                         LOGGER.warn("Failed to perform atomic move from={} to={}", source, target, cause);
                         return fs.move(source, target, new CopyOptions().setAtomicMove(false));
                     }
                     return Future.failedFuture(cause);
                 })
                 .compose(v -> Future.succeededFuture(target));
    }

    /**
     * Returns true if a cause of type {@code type} is found in the stack trace before exceeding the {@code depth}
     *
     * @param cause the original cause
     * @param type  the exception type to test
     * @param depth the maximum depth to check in the stack trace
     * @return true if the exception of type {@code type} exists in the stacktrace, false otherwise
     */
    private static boolean hasCause(Throwable cause, Class<? extends Throwable> type, int depth)
    {
        int i = 0;
        while (i < depth)
        {
            if (cause == null)
                return false;

            if (type.isInstance(cause))
                return true;

            cause = cause.getCause();

            i++;
        }
        return false;
    }
}
