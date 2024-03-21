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

package org.apache.cassandra.sidecar.restore;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.data.SSTableImportOptions;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.db.RestoreSliceDatabaseAccessor;
import org.apache.cassandra.sidecar.exceptions.RestoreJobException;
import org.apache.cassandra.sidecar.exceptions.RestoreJobExceptions;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.exceptions.ThrowableUtils;
import org.apache.cassandra.sidecar.stats.RestoreJobStats;
import org.apache.cassandra.sidecar.stats.Timer;
import org.apache.cassandra.sidecar.utils.SSTableImporter;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static io.vertx.core.Future.fromCompletionStage;
import static org.apache.cassandra.sidecar.utils.AsyncFileSystemUtils.ensureSufficientStorage;

/**
 * An async executable of {@link RestoreSlice} downloads object from remote, validates,
 * and imports SSTables into Cassandra.
 * It the execution ever fails, the cause should only be
 * {@link org.apache.cassandra.sidecar.exceptions.RestoreJobException}
 * <p>
 * Note that the class is package private, and it is not intended to be referenced by other packages.
 */
public class RestoreSliceTask implements RestoreSliceHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreSliceTask.class);

    private final RestoreSlice slice;
    private final StorageClient s3Client;
    private final ExecutorPools.TaskExecutorPool executorPool;
    private final SSTableImporter importer;
    private final double requiredUsableSpacePercentage;
    private final RestoreSliceDatabaseAccessor sliceDatabaseAccessor;
    private final RestoreJobStats stats;
    private final RestoreJobUtil restoreJobUtil;
    private long taskStartTimeNanos = -1;

    public RestoreSliceTask(RestoreSlice slice,
                            StorageClient s3Client,
                            ExecutorPools.TaskExecutorPool executorPool,
                            SSTableImporter importer,
                            double requiredUsableSpacePercentage,
                            RestoreSliceDatabaseAccessor sliceDatabaseAccessor,
                            RestoreJobStats stats,
                            RestoreJobUtil restoreJobUtil)
    {
        Preconditions.checkArgument(!slice.job().isManagedBySidecar()
                                    || sliceDatabaseAccessor != null,
                                    "sliceDatabaseAccessor cannot be null");
        this.slice = slice;
        this.s3Client = s3Client;
        this.executorPool = executorPool;
        this.importer = importer;
        this.requiredUsableSpacePercentage = requiredUsableSpacePercentage;
        this.sliceDatabaseAccessor = sliceDatabaseAccessor;
        this.stats = stats;
        this.restoreJobUtil = restoreJobUtil;
    }

    public static RestoreSliceHandler failed(RestoreJobException cause, RestoreSlice slice)
    {
        return new Failed(cause, slice);
    }

    @Override
    public void handle(Promise<RestoreSlice> event)
    {
        this.taskStartTimeNanos = restoreJobUtil.currentTimeNanos();
        if (failOnCancelled(event))
            return;

        // The slice, when being process, requires a total of slice size (download) + uncompressed (unzip) to use.
        // The protection below guards the slice being process, if the usable disk space falls below the threshold
        // after considering the slice
        ensureSufficientStorage(slice.stageDirectory().toString(),
                                slice.compressedSize() + slice.uncompressedSize(),
                                requiredUsableSpacePercentage,
                                executorPool)
        .compose(v -> {
            RestoreJob job = slice.job();
            if (job.isManagedBySidecar())
            {
                if (job.status == RestoreJobStatus.CREATED)
                {
                    if (Files.exists(slice.stagedObjectPath()))
                    {
                        LOGGER.debug("The slice has been staged already. sliceKey={} stagedFilePath={}",
                                     slice.key(), slice.stagedObjectPath());
                        slice.completeStagePhase(); // update the flag if missed
                        sliceDatabaseAccessor.updateStatus(slice);
                        event.tryComplete(slice);
                        return Future.succeededFuture();
                    }

                    // 1. check object existence and validate eTag / checksum
                    return checkObjectExistence(event)
                           .compose(headObject -> downloadSlice(event))
                           .<Void>compose(file -> {
                               slice.completeStagePhase();
                               sliceDatabaseAccessor.updateStatus(slice);
                               return Future.succeededFuture();
                           })
                           // completed staging. A new task is produced when it comes to import
                           .onSuccess(_v -> event.tryComplete(slice))
                           // handle unexpected errors thrown during download slice call, that do not close event
                           .onFailure(cause -> event.tryFail(RestoreJobExceptions.ofSlice(cause.getMessage(), slice, cause)));
                }
                else if (job.status == RestoreJobStatus.STAGED)
                {
                    return unzipAndImport(event, slice.stagedObjectPath().toFile(),
                                          // persist status
                                          () -> sliceDatabaseAccessor.updateStatus(slice));
                }
                else
                {
                    String msg = "Unexpected restore job status. Expected only CREATED or STAGED when " +
                                 "processing active slices. Found status: " + job.status;
                    Exception unexpectedState = new IllegalStateException(msg);
                    return Future.failedFuture(RestoreJobExceptions.ofFatalSlice("Unexpected restore job status",
                                                                                 slice, unexpectedState));
                }
            }
            else
            {
                return downloadSliceAndImport(event);
            }
        })
        .onSuccess(v -> event.tryComplete(slice))
        .onFailure(cause -> event.tryFail(RestoreJobExceptions.ofSlice(cause.getMessage(), slice, cause)));
    }

    private Future<Void> downloadSliceAndImport(Promise<RestoreSlice> event)
    {
        // 1. check object existence and validate eTag / checksum
        return checkObjectExistence(event)
               // 2. download slice/object when the remote object exists
               .compose(headObject -> downloadSlice(event))
               // 3. unzip the file and import/commit
               .compose(file -> unzipAndImport(event, file));
    }

    private Future<?> checkObjectExistence(Promise<RestoreSlice> event)
    {
        // skip query s3 if the object existence is already confirmed
        if (slice.existsOnS3())
        {
            LOGGER.debug("The slice already exists on S3. jobId={} sliceKey={}", slice.jobId(), slice.key());
            return Future.succeededFuture();
        }

        // even if the file already exists on disk, we should still check the object existence
        return
        fromCompletionStage(s3Client.objectExists(slice))
        .onSuccess(exists -> {
            long durationNanos = currentTimeInNanos() - slice.creationTimeNanos();
            stats.captureSliceReplicationTime(durationNanos);
            slice.setExistsOnS3();
            LOGGER.debug("Slice is now available on S3. jobId={} sliceKey={} replicationTimeNanos={}",
                         slice.jobId(), slice.key(), durationNanos);
        })
        .onFailure(cause -> {
            S3Exception s3Exception = ThrowableUtils.getCause(cause, S3Exception.class);
            if (s3Exception == null) // has non-null cause, but not S3Exception
            {
                event.tryFail(RestoreJobExceptions.ofFatalSlice("Unexpected error when checking object existence",
                                                                slice, cause));
            }
            else if (s3Exception instanceof NoSuchKeyException)
            {
                event.tryFail(RestoreJobExceptions.ofSlice("Object not found", slice, null));
            }
            else if (s3Exception.statusCode() == 412)
            {
                // When checksum/eTag does not match, it should be an unrecoverable error and fail immediately.
                // For such scenario, we expect "S3Exception: (Status Code: 412)". Also see,
                // https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_RequestSyntax
                event.tryFail(RestoreJobExceptions.ofFatalSlice("Object checksum mismatched",
                                                                slice, s3Exception));
                stats.captureSliceChecksumMismatch(slice.owner().id());
            }
            else if (s3Exception.statusCode() == 403)
            {
                // Fail immediately if 403 forbidden is returned.
                // There might be permission issue on accessing the object.
                event.tryFail(RestoreJobExceptions.ofFatalSlice("Object access is forbidden",
                                                                slice, s3Exception));
                stats.captureTokenUnauthorized();
            }
            else if (s3Exception.statusCode() == 400 &&
                     s3Exception.getMessage().contains("token has expired"))
            {
                // Fail the job if 400, token has expired.
                // https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
                event.tryFail(RestoreJobExceptions.ofFatalSlice("Token has expired", slice, s3Exception));
                stats.captureTokenExpired();
            }
            else
            {
                // Retry the other S3Exceptions
                event.tryFail(RestoreJobExceptions.ofSlice("Unable to check object existence",
                                                           slice, s3Exception));
            }
        });
    }

    private long currentTimeInNanos()
    {
        return restoreJobUtil.currentTimeNanos();
    }

    private Future<File> downloadSlice(Promise<RestoreSlice> event)
    {
        if (slice.isCancelled())
        {
            RestoreJobFatalException ex = RestoreJobExceptions.ofFatalSlice("Restore slice is cancelled",
                                                                            slice, null);
            event.tryFail(ex);
            return Future.failedFuture(ex);
        }

        if (slice.downloadAttempt() > 0)
        {
            LOGGER.debug("Retrying downloading slice. sliceKey={}", slice.key());
            stats.captureSliceDownloadRetry(slice.owner().id());
        }

        LOGGER.info("Begin downloading restore slice. sliceKey={}", slice.key());
        Future<File> future =
        fromCompletionStage(s3Client.downloadObjectIfAbsent(slice))
        .onFailure(cause -> {
            slice.incrementDownloadAttempt();
            if (ThrowableUtils.getCause(cause, ApiCallTimeoutException.class) != null)
            {
                LOGGER.warn("Downloading restore slice times out. sliceKey={}", slice.key());
                stats.captureSliceDownloadTimeout(slice.owner().id());
            }
            event.tryFail(RestoreJobExceptions.ofFatalSlice("Unrecoverable error when downloading object",
                                                            slice, cause));
        });

        return Timer.measureTimeTaken(future, duration -> {
            LOGGER.info("Finish downloading restore slice. sliceKey={}", slice.key());
            stats.captureSliceDownloaded(slice.owner().id(),
                                         slice.compressedSize(),
                                         slice.uncompressedSize(),
                                         duration);
        });
    }

    @VisibleForTesting
    Future<Void> unzipAndImport(Promise<RestoreSlice> event, File file)
    {
        return unzipAndImport(event, file, null);
    }

    Future<Void> unzipAndImport(Promise<RestoreSlice> event, File file, Runnable onSuccessCommit)
    {
        if (file == null) // the condition should never happen. Having it here for logic completeness
        {
            return Future.failedFuture(RestoreJobExceptions.ofFatalSlice("Object not found from disk",
                                                                         slice, null));
        }

        // run the rest in the executor pool, instead of S3 client threadpool
        return unzip(file)
               .compose(this::validateFiles)
               .compose(this::commit)
               .compose(x -> {
                   if (onSuccessCommit == null)
                   {
                       return Future.succeededFuture();
                   }

                   return executorPool.<Void>executeBlocking(promise -> {
                       onSuccessCommit.run();
                       promise.tryComplete();
                   });
               })
               .onSuccess(x -> {
                   slice.completeImportPhase();
                   event.tryComplete(slice);
               })
               .onFailure(failure -> {
                   logWarnIfHasHttpExceptionCauseOnCommit(failure, slice);
                   event.tryFail(RestoreJobExceptions.propagate("Fail to commit slice. "
                                                                + slice.shortDescription(), failure));
               });
    }

    private Future<File> unzip(File zipFile)
    {
        Future<File> future = executorPool.executeBlocking(promise -> {
            if (failOnCancelled(promise))
                return;

            // targetPathInStaging points to the directory named after uploadId
            // SSTableImporter expects the file system structure to be uploadId/keyspace/table/sstables
            File targetDir = slice.stageDirectory()
                                  .resolve(slice.keyspace())
                                  .resolve(slice.table())
                                  .toFile();

            boolean targetDirExist = targetDir.isDirectory();

            if (!zipFile.exists())
            {
                if (targetDirExist)
                {
                    LOGGER.debug("The files in slice are already extracted. Maybe it is a retried task? " +
                                 "jobId={} sliceKey={}", slice.jobId(), slice.key());
                    promise.complete(targetDir);
                }
                else
                {
                    promise.tryFail(new RestoreJobException("Object not found from disk. File: " + zipFile));
                }
                // return early
                return;
            }

            try
            {
                Files.createDirectories(targetDir.toPath());
                // Remove all existing files under the target directory
                // The validation step later expects only the files registered in the manifest.
                RestoreJobUtil.cleanDirectory(targetDir.toPath());
                RestoreJobUtil.unzip(zipFile, targetDir);
                // Notify the next step that unzip is complete
                promise.complete(targetDir);
                // Then, delete the downloaded zip file
                if (!zipFile.delete())
                {
                    LOGGER.warn("File deletion attempt failed. jobId={} sliceKey={} file={}",
                                slice.jobId(), slice.key(), zipFile.getAbsolutePath());
                }
            }
            catch (Exception cause)
            {
                promise.tryFail(RestoreJobExceptions.propagate("Failed to unzip. File: " + zipFile, cause));
            }
        }, false); // unordered

        return Timer.measureTimeTaken(future, d -> stats.captureSliceUnzipTime(slice.owner().id(), d));
    }

    // Validate integrity of the files from the zip. The failures from any step is fatal and not retryable.
    private Future<File> validateFiles(File directory)
    {
        Future<File> future = executorPool.executeBlocking(promise -> {
            if (failOnCancelled(promise))
                return;

            Map<String, String> checksums;
            try
            {
                File manifestFile = new File(directory, RestoreSliceManifest.MANIFEST_FILE_NAME);
                RestoreSliceManifest manifest = RestoreSliceManifest.read(manifestFile);
                checksums = manifest.mergeAllChecksums();
            }
            catch (RestoreJobFatalException e)
            {
                promise.tryFail(e);
                return;
            }

            if (checksums.isEmpty())
            {
                promise.tryFail(new RestoreJobFatalException("The downloaded slice has no data. " +
                                                             "Directory: " + directory));
                return;
            }

            // exclude the manifest file
            File[] files = directory.listFiles((dir, name) -> !name.equals(RestoreSliceManifest.MANIFEST_FILE_NAME));
            if (files == null || files.length != checksums.size())
            {
                String msg = "Number of files does not match. Expected: " + checksums.size() +
                             "; Actual: " + (files == null ? 0 : files.length) +
                             "; Directory: " + directory;
                promise.tryFail(new RestoreJobFatalException(msg));
                return;
            }

            compareChecksums(checksums, files, promise);

            // capture the data component size of sstables
            for (File file : files)
            {
                if (file.getName().endsWith("-Data.db"))
                {
                    stats.captureSSTableDataComponentSize(slice.owner().id(), file.length());
                }
            }

            // all files match with the provided checksums
            promise.tryComplete(directory);
        }, false); // unordered

        return Timer.measureTimeTaken(future, d -> stats.captureSliceValidationTime(slice.owner().id(), d));
    }

    private void compareChecksums(Map<String, String> expectedChecksums, File[] files, Promise<?> promise)
    {
        for (File file : files)
        {
            String name = file.getName();
            String expectedChecksum = expectedChecksums.get(name);
            if (expectedChecksum == null)
            {
                promise.tryFail(new RestoreJobFatalException("File not found in manifest. File: " + name));
                return;
            }

            try
            {
                String actualChecksum = restoreJobUtil.checksum(file);
                if (!actualChecksum.equals(expectedChecksum))
                {
                    String msg = "Checksum does not match. Expected: " + expectedChecksum +
                                 "; actual: " + actualChecksum + "; file: " + file;
                    promise.tryFail(new RestoreJobFatalException(msg));
                    return;
                }
            }
            catch (Exception cause)
            {
                promise.tryFail(new RestoreJobFatalException("Failed to calculate checksum. File: " + file));
                return;
            }
        }
    }

    private Future<Void> commit(File directory)
    {
        if (slice.isCancelled())
            return Future.failedFuture(RestoreJobExceptions.ofFatalSlice("Restore slice is cancelled",
                                                                         slice, null));

        LOGGER.info("Begin committing SSTables. jobId={} sliceKey={}", slice.jobId(), slice.key());

        SSTableImportOptions options = slice.job().importOptions;
        SSTableImporter.ImportOptions importOptions = new SSTableImporter.ImportOptions.Builder()
                                                      .host(slice.owner().host())
                                                      .keyspace(slice.keyspace())
                                                      .tableName(slice.table())
                                                      .directory(directory.toString())
                                                      .resetLevel(options.resetLevel())
                                                      .clearRepaired(options.clearRepaired())
                                                      .verifySSTables(options.verifySSTables())
                                                      .verifyTokens(options.verifyTokens())
                                                      .invalidateCaches(options.invalidateCaches())
                                                      .extendedVerify(options.extendedVerify())
                                                      .copyData(options.copyData())
                                                      .uploadId(slice.uploadId())
                                                      .build();
        Future<Void> future = importer.scheduleImport(importOptions)
                                      .onSuccess(ignored -> LOGGER.info("Finish committing SSTables. jobId={} sliceKey={}",
                                                                        slice.jobId(), slice.key()));
        return Timer.measureTimeTaken(future, d -> stats.captureSliceImportTime(slice.owner().id(), d));
    }

    private boolean failOnCancelled(Promise<?> promise)
    {
        if (slice.isCancelled())
            promise.tryFail(RestoreJobExceptions.ofFatalSlice("Restore slice is cancelled", slice, null));

        return slice.isCancelled();
    }

    // SSTableImporter could fail an import with HttpException,
    // which does not implement toString to log the details, i.e. status code and payload
    // The method is to log the details if it finds the cause contains HttpException
    private void logWarnIfHasHttpExceptionCauseOnCommit(Throwable throwable, RestoreSlice slice)
    {
        HttpException httpException = ThrowableUtils.getCause(throwable, HttpException.class);
        if (httpException == null)
        {
            return;
        }

        LOGGER.warn("Committing slice failed with HttpException. jobId={} sliceKey={} statusCode={} " +
                    "exceptionPayload={}", slice.jobId(), slice.key(), httpException.getStatusCode(),
                    httpException.getPayload(), httpException);
    }

    @Override
    public long elapsedInNanos()
    {
        return taskStartTimeNanos == -1 ? -1 :
               currentTimeInNanos() - taskStartTimeNanos;
    }

    @Override
    public RestoreSlice slice()
    {
        return slice;
    }

    /**
     * A RestoreSliceHandler that immediately fails the slice/promise.
     * Used when the processor already knows that a slice should not be processed for some reason
     * as indicated in cause field.
     */
    public static class Failed implements RestoreSliceHandler
    {
        private final RestoreJobException cause;
        private final RestoreSlice slice;

        public Failed(RestoreJobException cause, RestoreSlice slice)
        {
            this.cause = cause;
            this.slice = slice;
        }

        @Override
        public void handle(Promise<RestoreSlice> promise)
        {
            promise.tryFail(cause);
        }

        @Override
        public long elapsedInNanos()
        {
            // it fails immediately
            return 0;
        }

        @Override
        public RestoreSlice slice()
        {
            return slice;
        }
    }
}
