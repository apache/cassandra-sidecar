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
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.cluster.locator.LocalTokenRangesProvider;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.data.SSTableImportOptions;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.utils.ThrowableUtils;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreRangeDatabaseAccessor;
import org.apache.cassandra.sidecar.exceptions.RestoreJobException;
import org.apache.cassandra.sidecar.exceptions.RestoreJobExceptions;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.metrics.RestoreMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.StopWatch;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics;
import org.apache.cassandra.sidecar.utils.SSTableImporter;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static io.vertx.core.Future.fromCompletionStage;
import static org.apache.cassandra.sidecar.utils.AsyncFileSystemUtils.ensureSufficientStorage;

/**
 * An async executable of {@link RestoreRange} downloads object from remote, validates,
 * and imports SSTables into Cassandra.
 * It the execution ever fails, the cause should only be
 * {@link org.apache.cassandra.sidecar.exceptions.RestoreJobException}
 * <p>
 * Note that the class is package private, and it is not intended to be referenced by other packages.
 */
public class RestoreRangeTask implements RestoreRangeHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreRangeTask.class);

    private final RestoreRange range;
    private final StorageClient s3Client;
    private final TaskExecutorPool executorPool;
    private final SSTableImporter importer;
    private final double requiredUsableSpacePercentage;
    private final RestoreRangeDatabaseAccessor rangeDatabaseAccessor;
    private final RestoreJobUtil restoreJobUtil;
    private final LocalTokenRangesProvider localTokenRangesProvider;
    private final RestoreMetrics metrics;
    private final InstanceMetrics instanceMetrics;
    private long taskStartTimeNanos = -1;

    public RestoreRangeTask(RestoreRange range,
                            StorageClient s3Client,
                            TaskExecutorPool executorPool,
                            SSTableImporter importer,
                            double requiredUsableSpacePercentage,
                            RestoreRangeDatabaseAccessor rangeDatabaseAccessor,
                            RestoreJobUtil restoreJobUtil,
                            LocalTokenRangesProvider localTokenRangesProvider,
                            SidecarMetrics metrics)
    {
        Preconditions.checkArgument(!range.job().isManagedBySidecar()
                                    || rangeDatabaseAccessor != null,
                                    "rangeDatabaseAccessor cannot be null");
        this.range = range;
        this.s3Client = s3Client;
        this.executorPool = executorPool;
        this.importer = importer;
        this.requiredUsableSpacePercentage = requiredUsableSpacePercentage;
        this.rangeDatabaseAccessor = rangeDatabaseAccessor;
        this.restoreJobUtil = restoreJobUtil;
        this.localTokenRangesProvider = localTokenRangesProvider;
        this.metrics = metrics.server().restore();
        this.instanceMetrics = metrics.instance(range.owner().id());
    }

    public static RestoreRangeHandler failed(RestoreJobException cause, RestoreRange range)
    {
        return new Failed(cause, range);
    }

    @Override
    public long elapsedInNanos()
    {
        return taskStartTimeNanos == -1 ? -1 :
               currentTimeInNanos() - taskStartTimeNanos;
    }

    @Override
    public RestoreRange range()
    {
        return range;
    }

    @Override
    public void handle(Promise<RestoreRange> event)
    {
        this.taskStartTimeNanos = restoreJobUtil.currentTimeNanos();

        // exit early if the range has been cancelled already; the same check is performed at many following steps to avoid wasting computation
        failOnCancelled(range, null)
        // The range, when being process, requires a total of range size (download) + uncompressed (unzip) to use.
        // The protection below guards the range being process, if the usable disk space falls below the threshold
        // after considering the range
        .compose(v -> ensureSufficientStorage(range.stageDirectory().toString(),
                                              range.estimatedSpaceRequiredInBytes(),
                                              requiredUsableSpacePercentage,
                                              executorPool))
        .compose(v -> failOnCancelled(range, v))
        .compose(v -> {
            RestoreJob job = range.job();
            if (job.isManagedBySidecar())
            {
                if (job.status == RestoreJobStatus.STAGE_READY)
                {
                    if (Files.exists(range.stagedObjectPath()))
                    {
                        LOGGER.info("The slice has been staged already. sliceKey={} stagedFilePath={}",
                                     range.sliceKey(), range.stagedObjectPath());
                        range.completeStagePhase(); // update the flag if missed
                        rangeDatabaseAccessor.updateStatus(range);
                        return Future.succeededFuture();
                    }

                    // 1. check object existence and validate eTag / checksum
                    return failOnCancelled(range, null)
                           .compose(value -> checkObjectExistence())
                           .compose(value -> failOnCancelled(range, value))
                           .compose(headObject -> downloadSlice())
                           .compose(value -> failOnCancelled(range, value))
                           .compose(file -> {
                               // completed staging. A new task is produced when it comes to import
                               range.completeStagePhase();
                               rangeDatabaseAccessor.updateStatus(range);
                               return Future.succeededFuture();
                           });
                }
                else if (job.status == RestoreJobStatus.IMPORT_READY)
                {
                    return unzipAndImport(range.stagedObjectPath().toFile(),
                                          // persist status
                                          () -> rangeDatabaseAccessor.updateStatus(range));
                }
                else
                {
                    String msg = "Unexpected restore job status. Expected only STAGE_READY or IMPORT_READY when " +
                                 "processing active slices. Found status: " + job.statusWithOptionalDescription();
                    Exception unexpectedState = new IllegalStateException(msg);
                    return Future.failedFuture(RestoreJobExceptions.ofFatal("Unexpected restore job status",
                                                                            range, unexpectedState));
                }
            }
            else
            {
                return downloadSliceAndImport();
            }
        })
        .onSuccess(v -> event.tryComplete(range))
        .onFailure(cause -> event.tryFail(RestoreJobExceptions.propagate(cause)));
    }

    private Future<Void> downloadSliceAndImport()
    {
        return failOnCancelled(range, null)
               // 1. check object existence and validate eTag / checksum
               .compose(value -> checkObjectExistence())
               .compose(value -> failOnCancelled(range, value))
               // 2. download slice/object (with partNumber) when the remote object exists
               .compose(value -> downloadSlice())
               .compose(value -> failOnCancelled(range, value))
               // 3. unzip the file and import/commit
               .compose(this::unzipAndImport);
    }

    private Future<?> checkObjectExistence()
    {
        // skip query s3 if the object existence is already confirmed
        if (range.existsOnS3())
        {
            LOGGER.debug("The slice already exists on S3. jobId={} sliceKey={}", range.jobId(), range.sliceKey());
            return Future.succeededFuture();
        }

        // even if the file already exists on disk, we should still check the object existence
        return
        fromCompletionStage(s3Client.objectExists(range))
        .compose(headObjectResponse -> { // on success
            long durationNanos = currentTimeInNanos() - range.sliceCreationTimeNanos();
            metrics.sliceReplicationTime.metric.update(durationNanos, TimeUnit.NANOSECONDS);
            range.setExistsOnS3(headObjectResponse.contentLength());
            LOGGER.debug("Slice is now available on S3. jobId={} sliceKey={} replicationTimeNanos={}",
                         range.jobId(), range.sliceKey(), durationNanos);
            return Future.succeededFuture();
        }, cause -> { // failure mapper: converts throwable to restore job specific exceptions
            S3Exception s3Exception = ThrowableUtils.getCause(cause, S3Exception.class);
            RestoreJobException jobException;
            if (s3Exception == null) // has non-null cause, but not S3Exception
            {
                jobException = RestoreJobExceptions.ofFatal("Unexpected error when checking object existence", range, cause);
            }
            else if (s3Exception instanceof NoSuchKeyException)
            {
                jobException = RestoreJobExceptions.of("Object not found", range, null);
            }
            else if (s3Exception.statusCode() == 412)
            {
                // When checksum/eTag does not match, it should be an unrecoverable error and fail immediately.
                // For such scenario, we expect "S3Exception: (Status Code: 412)". Also see,
                // https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_RequestSyntax
                jobException = RestoreJobExceptions.ofFatal("Object checksum mismatched", range, s3Exception);
                instanceMetrics.restore().sliceChecksumMismatches.metric.update(1);
            }
            else if (s3Exception.statusCode() == 403)
            {
                // Fail immediately if 403 forbidden is returned.
                // There might be permission issue on accessing the object.
                jobException = RestoreJobExceptions.ofFatal("Object access is forbidden", range, s3Exception);
                metrics.tokenUnauthorized.metric.update(1);
            }
            else if (s3Exception.statusCode() == 400 &&
                     s3Exception.getMessage().contains("token has expired"))
            {
                // Fail the job if 400, token has expired.
                // https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
                jobException = RestoreJobExceptions.ofFatal("Token has expired", range, s3Exception);
                metrics.tokenExpired.metric.update(1);
            }
            else
            {
                // Retry the other S3Exceptions
                jobException = RestoreJobExceptions.of("Unable to check object existence", range, s3Exception);
            }

            return Future.failedFuture(jobException);
        });
    }

    private long currentTimeInNanos()
    {
        return restoreJobUtil.currentTimeNanos();
    }

    // download the slice that covers the restore range
    private Future<File> downloadSlice()
    {
        if (range.downloadAttempt() > 0)
        {
            LOGGER.debug("Retrying downloading slice. sliceKey={}", range.sliceKey());
            instanceMetrics.restore().sliceDownloadRetries.metric.update(1);
        }

        LOGGER.info("Begin downloading restore slice. sliceKey={}", range.sliceKey());
        Future<File> future =
        s3Client.downloadObjectIfAbsent(range, executorPool)
        .recover(cause -> { // converts to restore job exception
            LOGGER.warn("Failed to download restore slice. sliceKey={}", range.sliceKey(), cause);

            range.incrementDownloadAttempt();
            if (ThrowableUtils.getCause(cause, ApiCallTimeoutException.class) != null)
            {
                LOGGER.warn("Downloading restore slice times out. sliceKey={}", range.sliceKey());
                instanceMetrics.restore().sliceDownloadTimeouts.metric.update(1);
                return Future.failedFuture(RestoreJobExceptions.of("Download object times out. Retry later", range, cause));
            }
            return Future.failedFuture(RestoreJobExceptions.ofFatal("Unrecoverable error when downloading object", range, cause));
        });

        return StopWatch.measureTimeTaken(future, duration -> {
            LOGGER.info("Finish downloading restore slice. sliceKey={}", range.sliceKey());
            instanceMetrics.restore().sliceDownloadTime.metric.update(duration, TimeUnit.NANOSECONDS);
            instanceMetrics.restore().sliceCompressedSizeInBytes.metric.update(range.sliceCompressedSize());
            instanceMetrics.restore().sliceUncompressedSizeInBytes.metric.update(range.sliceUncompressedSize());
        });
    }

    @VisibleForTesting
    Future<Void> unzipAndImport(File file)
    {
        return unzipAndImport(file, null);
    }

    Future<Void> unzipAndImport(File file, Runnable onSuccessCommit)
    {
        if (file == null) // the condition should never happen. Having it here for logic completeness
        {
            return Future.failedFuture(RestoreJobExceptions.ofFatal("Slice not found from disk", range, null));
        }

        // run the rest in the executor pool, instead of S3 client threadpool
        return failOnCancelled(range, file)
               .compose(this::unzip)
               .compose(value -> failOnCancelled(range, value))
               .compose(this::validateFiles)
               .compose(value -> failOnCancelled(range, value))
               .compose(this::commit)
               .compose(
               success -> { // successMapper
                   if (onSuccessCommit == null)
                   {
                       return Future.succeededFuture();
                   }

                   return executorPool.runBlocking(() -> {
                       range.completeImportPhase();
                       onSuccessCommit.run();
                   });
               },
               failure -> { // failureMapper
                   logWarnIfHasHttpExceptionCauseOnCommit(failure, range);
                   return Future.failedFuture(RestoreJobExceptions.propagate("Fail to commit range. "
                                                                             + range.shortDescription(), failure));
               });
    }

    private Future<File> unzip(File zipFile)
    {
        Future<File> future = executorPool.executeBlocking(() -> unzipAction(zipFile), false); // unordered
        return StopWatch.measureTimeTaken(future, d -> instanceMetrics.restore().sliceUnzipTime.metric.update(d, TimeUnit.NANOSECONDS));
    }

    File unzipAction(File zipFile) throws RestoreJobException
    {
        // targetPathInStaging points to the directory named after uploadId
        // SSTableImporter expects the file system structure to be uploadId/keyspace/table/sstables
        File targetDir = range.stageDirectory()
                              .resolve(range.keyspace())
                              .resolve(range.table())
                              .toFile();

        boolean targetDirExist = targetDir.isDirectory();

        if (!zipFile.exists())
        {
            if (targetDirExist)
            {
                LOGGER.debug("The files in slice are already extracted. Maybe it is a retried task? " +
                             "jobId={} sliceKey={}", range.jobId(), range.sliceKey());
                // return early
                return targetDir;
            }
            else
            {
                throw new RestoreJobException("Object not found from disk. File: " + zipFile);
            }
        }

        try
        {
            Files.createDirectories(targetDir.toPath());
            // Remove all existing files under the target directory
            // The validation step later expects only the files registered in the manifest.
            RestoreJobUtil.cleanDirectory(targetDir.toPath());
            RestoreJobUtil.unzip(zipFile, targetDir);
            // Then, delete the downloaded zip file
            if (!zipFile.delete())
            {
                LOGGER.warn("File deletion attempt failed. jobId={} sliceKey={} file={}",
                            range.jobId(), range.sliceKey(), zipFile.getAbsolutePath());
            }
            // Notify the next step that unzip is complete
            return targetDir;
        }
        catch (Exception cause)
        {
            throw RestoreJobExceptions.propagate("Failed to unzip. File: " + zipFile, cause);
        }
    }

    // Validate integrity of the files from the zip. If the SSTables that are fully out of the owning range of the node is removed
    private Future<File> validateFiles(File directory)
    {
        Future<File> future = executorPool.executeBlocking(() -> validateFilesAction(directory), false); // unordered
        return StopWatch.measureTimeTaken(future, d -> instanceMetrics.restore().sliceValidationTime.metric.update(d, TimeUnit.NANOSECONDS));
    }

    File validateFilesAction(File directory) throws RestoreJobException, IOException
    {
        File manifestFile = new File(directory, RestoreSliceManifest.MANIFEST_FILE_NAME);
        RestoreSliceManifest manifest = RestoreSliceManifest.read(manifestFile);

        if (manifest.isEmpty())
        {
            throw new RestoreJobFatalException("The downloaded slice has no data. " +
                                               "Directory: " + directory);
        }

        // validate the SSTable ranges with the owning range of the node and remove the out-of-range sstables
        if (range.job().isManagedBySidecar())
        {
            removeOutOfRangeSSTables(directory, manifest);
        }

        Map<String, String> checksums = manifest.mergeAllChecksums();

        // exclude the manifest file
        File[] files = directory.listFiles((dir, name) -> !name.equals(RestoreSliceManifest.MANIFEST_FILE_NAME));
        if (files == null || files.length != checksums.size())
        {
            String msg = "Number of files does not match. Expected: " + checksums.size() +
                         "; Actual: " + (files == null ? 0 : files.length) +
                         "; Directory: " + directory;
            throw new RestoreJobFatalException(msg);
        }

        compareChecksums(checksums, files);

        // capture the data component size of sstables
        for (File file : files)
        {
            if (file.getName().endsWith("-Data.db"))
            {
                instanceMetrics.restore().dataSSTableComponentSize.metric.update(file.length());
            }
        }

        // all files match with the provided checksums
        return directory;
    }

    // Remove all the SSTables that does not belong this node
    // The method modifies the input manifest and delete files under directory, if out of range sstables are found
    private void removeOutOfRangeSSTables(File directory, RestoreSliceManifest manifest) throws RestoreJobException, IOException
    {
        Set<TokenRange> ranges = localTokenRangesProvider.localTokenRanges(range.keyspace()).get(range.owner().id());
        if (ranges == null || ranges.isEmpty())
        {
            // Note: retry is allowed for the failure
            throw new RestoreJobException("Unable to fetch local range, retry later");
        }

        // 1. remove the sstables that are fully out of range
        // 2. detect if there is any range that partially overlaps. In that case, signal that this node is required to run nodetool cleanup on job completion
        Iterator<Map.Entry<String, RestoreSliceManifest.ManifestEntry>> it = manifest.entrySet().iterator();
        while (it.hasNext())
        {
            RestoreSliceManifest.ManifestEntry entry = it.next().getValue();
            // TokenRange is open-closed, hence subtracting one from the rangeStart read from manifest
            TokenRange sstableRange = new TokenRange(entry.startToken().subtract(BigInteger.ONE),
                                                     entry.endToken());

            boolean hasOverlap = false;
            boolean fullyEnclosed = false;
            for (TokenRange owningRange : ranges)
            {
                if (hasOverlap)
                {
                    break;
                }

                hasOverlap = owningRange.overlaps(sstableRange);

                if (hasOverlap)
                {
                    fullyEnclosed = owningRange.encloses(sstableRange);
                }
            }

            // fully out of range
            if (!hasOverlap)
            {
                // remove the entry from manifest
                it.remove();
                // delete the files
                for (String fileName : entry.componentsChecksum().keySet())
                {
                    Path path = directory.toPath().resolve(fileName);
                    Files.deleteIfExists(path);
                }
            }
            // overlaps, but is not fully enclosed; we need to run cleanup on this node
            else if (!fullyEnclosed)
            {
                range.requestOutOfRangeDataCleanup();
            }
        }
    }

    private void compareChecksums(Map<String, String> expectedChecksums, File[] files) throws RestoreJobFatalException
    {
        for (File file : files)
        {
            String name = file.getName();
            String expectedChecksum = expectedChecksums.get(name);
            if (expectedChecksum == null)
            {
                throw new RestoreJobFatalException("File not found in manifest. File: " + name);
            }

            try
            {
                String actualChecksum = restoreJobUtil.checksum(file);
                if (!actualChecksum.equals(expectedChecksum))
                {
                    String msg = "Checksum does not match. Expected: " + expectedChecksum +
                                 "; actual: " + actualChecksum + "; file: " + file;
                    throw new RestoreJobFatalException(msg);
                }
            }
            catch (IOException cause)
            {
                throw new RestoreJobFatalException("Failed to calculate checksum. File: " + file, cause);
            }
        }
    }

    Future<Void> commit(File directory)
    {
        LOGGER.info("Begin committing SSTables. jobId={} sliceKey={}", range.jobId(), range.sliceKey());

        SSTableImportOptions options = range.job().importOptions;
        SSTableImporter.ImportOptions importOptions = new SSTableImporter.ImportOptions.Builder()
                                                      .host(range.owner().host())
                                                      .keyspace(range.keyspace())
                                                      .tableName(range.table())
                                                      .directory(directory.toString())
                                                      .resetLevel(options.resetLevel())
                                                      .clearRepaired(options.clearRepaired())
                                                      .verifySSTables(options.verifySSTables())
                                                      .verifyTokens(options.verifyTokens())
                                                      .invalidateCaches(options.invalidateCaches())
                                                      .extendedVerify(options.extendedVerify())
                                                      .copyData(options.copyData())
                                                      .uploadId(range.uploadId())
                                                      .build();
        Future<Void> future = importer.scheduleImport(importOptions)
                                      .onSuccess(ignored -> LOGGER.info("Finish committing SSTables. jobId={} sliceKey={}",
                                                                        range.jobId(), range.sliceKey()));
        return StopWatch.measureTimeTaken(future, d -> instanceMetrics.restore().sliceImportTime.metric.update(d, TimeUnit.NANOSECONDS));
    }

    static <T> Future<T> failOnCancelled(RestoreRange range, T value)
    {
        if (range.isCancelled())
        {
            return Future.failedFuture(RestoreJobExceptions.ofFatal("Restore range is cancelled",
                                                                    range, null));
        }

        return Future.succeededFuture(value);
    }

    // SSTableImporter could fail an import with HttpException,
    // which does not implement toString to log the details, i.e. status code and payload
    // The method is to log the details if it finds the cause contains HttpException
    private void logWarnIfHasHttpExceptionCauseOnCommit(Throwable throwable, RestoreRange range)
    {
        HttpException httpException = ThrowableUtils.getCause(throwable, HttpException.class);
        if (httpException == null)
        {
            return;
        }

        LOGGER.warn("Committing range failed with HttpException. " +
                    "jobId={} startToken={} endToken={} sliceKey={} statusCode={} exceptionPayload={}",
                    range.jobId(), range.startToken(), range.endToken(), range.sliceKey(),
                    httpException.getStatusCode(), httpException.getPayload(), httpException);
    }

    // For testing only. Unsafe to call in production code.
    @VisibleForTesting
    void removeOutOfRangeSSTablesUnsafe(File directory, RestoreSliceManifest manifest) throws RestoreJobException, IOException
    {
        removeOutOfRangeSSTables(directory, manifest);
    }

    // For testing only. Unsafe to call in production code.
    @VisibleForTesting
    void compareChecksumsUnsafe(Map<String, String> expectedChecksums, File[] files) throws RestoreJobFatalException
    {
        compareChecksums(expectedChecksums, files);
    }

    /**
     * A RestoreSliceHandler that immediately fails the slice/promise.
     * Used when the processor already knows that a slice should not be processed for some reason
     * as indicated in cause field.
     */
    public static class Failed implements RestoreRangeHandler
    {
        private final RestoreJobException cause;
        private final RestoreRange range;

        public Failed(RestoreJobException cause, RestoreRange range)
        {
            this.cause = cause;
            this.range = range;
        }

        @Override
        public void handle(Promise<RestoreRange> promise)
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
        public RestoreRange range()
        {
            return range;
        }
    }
}
