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

package org.apache.cassandra.sidecar.livemigration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.request.LiveMigrationFilesVerificationRequest;
import org.apache.cassandra.sidecar.common.request.data.Digest;
import org.apache.cassandra.sidecar.common.response.DigestResponse;
import org.apache.cassandra.sidecar.common.response.InstanceFileInfo;
import org.apache.cassandra.sidecar.common.response.InstanceFilesListResponse;
import org.apache.cassandra.sidecar.common.response.LiveMigrationFilesVerificationResponse;
import org.apache.cassandra.sidecar.concurrent.AsyncConcurrentTaskExecutor;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.DigestMismatchException;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.FileVerificationFailureException;
import org.apache.cassandra.sidecar.utils.DigestVerifierFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

import static org.apache.cassandra.sidecar.common.response.InstanceFileInfo.FileType.DIRECTORY;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.localPath;

/**
 * Verifies file integrity between source and destination instances during live migration
 * by comparing file lists and validating file digests using configurable digest algorithms.
 *
 * <p>The verification process consists of three stages:
 * <ol>
 *   <li>Fetch file lists from both source and destination instances concurrently</li>
 *   <li>Compare file metadata (size, type, modification time) - fails when there are mismatches</li>
 *   <li>Verify cryptographic digests (MD5 or XXHash32) with configurable concurrency</li>
 * </ol>
 *
 * <p>The task supports cancellation, and tracks detailed metrics including
 * metadata matches/mismatches, digest verification results, and failure counts.
 */
public class LiveMigrationFilesVerificationTask implements LiveMigrationTask<LiveMigrationFilesVerificationResponse>
{
    public static final String FILES_VERIFICATION_TASK_TYPE = "files-verification-task";
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationFilesVerificationTask.class);
    private final Vertx vertx;
    private final String id;
    private final String source;
    private final int port;
    // Instance metadata of current instance
    private final InstanceMetadata instanceMetadata;
    private final ExecutorPools executorPools;
    private final LiveMigrationFilesVerificationRequest request;
    private final SidecarClient sidecarClient;
    private final LiveMigrationConfiguration liveMigrationConfiguration;
    private final DigestVerifierFactory digestVerifierFactory;
    private final AtomicReference<State> state;
    private final Promise<Void> completionPromise = Promise.promise();
    private final AtomicInteger filesNotFoundAtSource = new AtomicInteger(0);
    private final AtomicInteger filesNotFoundAtDestination = new AtomicInteger(0);
    private final AtomicInteger metadataMatched = new AtomicInteger(0);
    private final AtomicInteger metadataMismatches = new AtomicInteger(0);
    private final AtomicInteger digestMismatches = new AtomicInteger(0);
    private final AtomicInteger digestVerificationFailures = new AtomicInteger(0);
    private final AtomicInteger filesMatched = new AtomicInteger(0);
    private final String logPrefix;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private volatile AsyncConcurrentTaskExecutor<String> asyncConcurrentTaskExecutor;

    @VisibleForTesting
    protected LiveMigrationFilesVerificationTask(Builder builder)
    {
        this.vertx = builder.vertx;
        this.id = builder.id;
        this.source = builder.source;
        this.port = builder.port;
        this.instanceMetadata = builder.instanceMetadata;
        this.executorPools = builder.executorPools;
        this.request = builder.request;
        this.sidecarClient = builder.sidecarClient;
        this.liveMigrationConfiguration = builder.liveMigrationConfiguration;
        this.digestVerifierFactory = builder.digestVerifierFactory;
        this.logPrefix = "fileVerifyTaskId=" + id;
        this.state = new AtomicReference<>(State.NOT_STARTED);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public Future<Void> validate()
    {
        return abortIfCancelled(new Object())
               // Get files info from both source and destination
               .compose(v -> Future.join(listFilesInLocal(), fetchSourceFileList()))

               // Compare the files information before calculating digests
               .compose(this::abortIfCancelled)
               .compose(cf -> compareFilesMetadata(cf.resultAt(0), cf.resultAt(1)))

               // Create digest comparison tasks
               .compose(this::abortIfCancelled)
               .compose(this::getDigestComparisonTasks)

               // Compare digests
               .compose(this::abortIfCancelled)
               .compose(this::compareDigests)

               .andThen(ar -> {
                   if (ar.failed())
                   {
                       LOGGER.error("{} Files verification task failed during validation", logPrefix, ar.cause());
                       // Fail the task if it is in IN_PROGRESS state.
                       state.compareAndSet(State.IN_PROGRESS, State.FAILED);
                   }
                   else
                   {
                       State currentState = state.compareAndExchange(State.IN_PROGRESS, State.COMPLETED);
                       if (currentState == State.CANCELLED)
                       {
                           LOGGER.debug("{} Task was cancelled before completion", logPrefix);
                       }
                       else if (currentState != State.IN_PROGRESS)
                       {
                           LOGGER.warn("{} Unexpected state transition failure. State was {}", logPrefix, currentState);
                       }
                       else
                       {
                           LOGGER.info("{} Files verification task completed successfully", logPrefix);
                       }
                   }
               });
    }

    private Future<List<InstanceFileInfo>> listFilesInLocal()
    {
        return executorPools.internal()
                            .executeBlocking(() -> new CassandraInstanceFilesImpl(instanceMetadata,
                                                                                  liveMigrationConfiguration)
                                                   .files());
    }

    private Future<InstanceFilesListResponse> fetchSourceFileList()
    {
        return Future.fromCompletionStage(
                     sidecarClient.liveMigrationListInstanceFilesAsync(new SidecarInstanceImpl(source, port)))
                     .onFailure(cause -> LOGGER.error("{} Failed to obtain list of files from source {}:{}",
                                                      logPrefix, source, port, cause));
    }

    private @NotNull Future<List<InstanceFileInfo>> compareFilesMetadata(List<InstanceFileInfo> localFiles,
                                                                         InstanceFilesListResponse sourceFiles)
    {
        Map<String, InstanceFileInfo> filesAtLocal =
        localFiles.stream()
                  .collect(Collectors.toMap(fileInfo -> fileInfo.fileUrl, fileInfo -> fileInfo));

        Map<String, InstanceFileInfo> filesAtSource =
        sourceFiles.getFiles().stream()
                   .collect(Collectors.toMap(fileInfo -> fileInfo.fileUrl, fileInfo -> fileInfo));

        for (Map.Entry<String, InstanceFileInfo> localFileEntry : filesAtLocal.entrySet())
        {
            String fileUrl = localFileEntry.getKey();
            InstanceFileInfo localFile = localFileEntry.getValue();
            if (filesAtSource.containsKey(fileUrl))
            {
                InstanceFileInfo sourceFile = filesAtSource.get(fileUrl);
                // compare files metadata
                if (localFile.equals(sourceFile) ||
                    (localFile.fileType == DIRECTORY && sourceFile.fileType == DIRECTORY))
                {
                    // File info matches if either of them are directories or
                    // their size, last modified time etc... matches
                    LOGGER.debug("{} {} file metadata matched with source", logPrefix, fileUrl);
                    metadataMatched.incrementAndGet();
                }
                else
                {
                    // Files metadata did not match
                    metadataMismatches.incrementAndGet();
                    LOGGER.error("{} {} did not match with source. Local file info={}, source file info={}",
                                 logPrefix, fileUrl, localFile, sourceFile);
                }

                // done with processing current local file, remove it
                filesAtSource.remove(fileUrl);
            }
            else
            {
                // File is not available at source, report it
                LOGGER.error("{} File {} exists at destination but not found at source", logPrefix, fileUrl);
                filesNotFoundAtSource.incrementAndGet();
            }
        }

        for (Map.Entry<String, InstanceFileInfo> sourceFileEntry : filesAtSource.entrySet())
        {
            // Remaining files are not present at destination, report error for them
            LOGGER.error("{} File {} exists at source but not found at destination", logPrefix, sourceFileEntry.getKey());
            filesNotFoundAtDestination.incrementAndGet();
        }

        if (filesNotFoundAtDestination.get() > 0 ||
            filesNotFoundAtSource.get() > 0 ||
            metadataMismatches.get() > 0)
        {
            FileVerificationFailureException exception =
            new FileVerificationFailureException("Files list did not match between source and destination");

            return Future.failedFuture(exception);
        }

        return Future.succeededFuture(localFiles);
    }

    private @NotNull Future<List<Future<String>>> getDigestComparisonTasks(List<InstanceFileInfo> localFiles)
    {
        // now verify digests of each local file with source file
        List<Callable<Future<String>>> tasks = new ArrayList<>(localFiles.size());
        localFiles.stream()
                  .filter(fileInfo -> fileInfo.fileType != DIRECTORY)
                  .forEach(fileInfo -> tasks.add(() -> verifyDigest(fileInfo)));
        asyncConcurrentTaskExecutor = new AsyncConcurrentTaskExecutor<>(vertx, tasks, request.maxConcurrency());
        List<Future<String>> verifyTaskResults = asyncConcurrentTaskExecutor.start();
        return Future.join(verifyTaskResults)
                     .transform(ar -> Future.succeededFuture(verifyTaskResults));
    }

    private Future<Void> compareDigests(List<Future<String>> futures)
    {
        boolean verificationFailed = false;
        for (Future<String> future : futures)
        {
            if (future.failed())
            {
                verificationFailed = true;
                Throwable cause = future.cause();
                if (cause instanceof DigestMismatchException)
                {
                    DigestMismatchException ex = (DigestMismatchException) cause;
                    LOGGER.error("{} File digests did not match for {}.", logPrefix, ex.fileUrl(), cause);
                    digestMismatches.incrementAndGet();
                }
                else
                {
                    LOGGER.error("{} Failed to verify digest for file. Check previous logs for file details",
                                 logPrefix, cause);
                    digestVerificationFailures.incrementAndGet();
                }
                continue;
            }

            // Digest verification succeeded
            filesMatched.incrementAndGet();
        }

        if (verificationFailed)
        {
            return Future.failedFuture(
            new FileVerificationFailureException("File digests did not match between source and destination"));
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    @VisibleForTesting
    Future<String> verifyDigest(InstanceFileInfo fileInfo)
    {
        return getSourceFileDigest(fileInfo)
               .compose(digest -> {
                   String path = localPath(fileInfo.fileUrl, instanceMetadata).toAbsolutePath().toString();
                   return digestVerifierFactory.verifier(MultiMap.caseInsensitiveMultiMap().addAll(digest.headers()))
                                               .verify(path)
                                               .compose(verified -> Future.succeededFuture(path))
                                               .recover(cause -> Future.failedFuture(
                                               new DigestMismatchException(path, fileInfo.fileUrl, cause)));
               })
               .onSuccess(filePath -> LOGGER.debug("{} Verified file {}", logPrefix, fileInfo.fileUrl))
               .onFailure(cause -> LOGGER.error("{} Failed to verify file {}", logPrefix, fileInfo.fileUrl, cause));
    }

    private Future<Digest> getSourceFileDigest(InstanceFileInfo fileInfo)
    {
        return Future.fromCompletionStage(sidecarClient.liveMigrationFileDigestAsync(new SidecarInstanceImpl(source, port),
                                                                                     fileInfo.fileUrl,
                                                                                     request.digestAlgorithm()))
                     .compose(this::toDigest);
    }

    Future<Digest> toDigest(DigestResponse digestResponse)
    {
        try
        {
            return Future.succeededFuture(digestResponse.toDigest());
        }
        catch (Exception e)
        {
            return Future.failedFuture(e);
        }
    }

    @Override
    public String id()
    {
        return id;
    }

    @Override
    public String type()
    {
        return FILES_VERIFICATION_TASK_TYPE;
    }

    @Override
    public void start()
    {
        if (state.compareAndSet(State.NOT_STARTED, State.IN_PROGRESS))
        {
            validate().onComplete(ar -> {
                if (ar.failed())
                {
                    completionPromise.tryFail(ar.cause());
                }
                else
                {
                    completionPromise.tryComplete(ar.result());
                }
            });
        }
        else
        {
            LOGGER.warn("{} Cannot start task. Current state: {}", logPrefix, state.get());
        }
    }

    @VisibleForTesting
    Future<Void> future()
    {
        return completionPromise.future();
    }

    public boolean hasStarted()
    {
        return state.get() != State.NOT_STARTED;
    }

    /**
     * Checks if the task has been cancelled and aborts the validation pipeline if so.
     * Updates task state to {@link State#CANCELLED} if currently
     * {@link State#NOT_STARTED} or {@link State#IN_PROGRESS}.
     *
     * @param t   the value to pass through
     * @param <T> the type of the value
     * @return succeeded future with the input if not cancelled, failed future otherwise
     */
    @VisibleForTesting
    <T> Future<T> abortIfCancelled(T t)
    {
        if (cancelled.get())
        {
            State latestState = state.updateAndGet(currentState -> {
                if (currentState == State.NOT_STARTED || currentState == State.IN_PROGRESS)
                {
                    return State.CANCELLED;
                }
                return currentState;
            });

            if (latestState == State.CANCELLED)
            {
                LOGGER.info("{} Files verification task successfully cancelled.", logPrefix);
            }
            else
            {
                LOGGER.warn("{} could not update files verification task status to cancelled. Current state={}",
                            logPrefix, state.get());
            }
            return Future.failedFuture("Task got cancelled");
        }

        return Future.succeededFuture(t);
    }

    @Override
    public LiveMigrationFilesVerificationResponse getResponse()
    {
        return new LiveMigrationFilesVerificationResponse(id,
                                                          request.digestAlgorithm(),
                                                          this.state.get().name(),
                                                          source,
                                                          port,
                                                          filesNotFoundAtSource.get(),
                                                          filesNotFoundAtDestination.get(),
                                                          metadataMatched.get(),
                                                          metadataMismatches.get(),
                                                          digestMismatches.get(),
                                                          digestVerificationFailures.get(),
                                                          filesMatched.get());
    }

    @Override
    public void cancel()
    {
        if (cancelled.compareAndSet(false, true))
        {
            LOGGER.info("{} Cancelling files verification task", logPrefix);
            abortIfCancelled(null);
            AsyncConcurrentTaskExecutor<String> executor = this.asyncConcurrentTaskExecutor;
            if (executor != null)
            {
                executor.cancelTasks();
            }

            if (!completionPromise.future().isComplete())
            {
                completionPromise.tryFail("Task was cancelled.");
            }
        }
        else
        {
            LOGGER.info("{} Files verification task already cancelled", logPrefix);
        }
    }

    public boolean isCancelled()
    {
        return cancelled.get();
    }

    @Override
    public boolean isCompleted()
    {
        return completionPromise.future().isComplete();
    }

    /**
     * Represents the current state of the file verification task
     */
    public enum State
    {
        NOT_STARTED, IN_PROGRESS, COMPLETED, FAILED, CANCELLED
    }

    /**
     * {@code LiveMigrationFilesVerificationTask} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, LiveMigrationFilesVerificationTask>
    {
        private Vertx vertx;
        private String id;
        private String source;
        private int port;
        private InstanceMetadata instanceMetadata;
        private ExecutorPools executorPools;
        private LiveMigrationFilesVerificationRequest request;
        private SidecarClient sidecarClient;
        private LiveMigrationConfiguration liveMigrationConfiguration;
        private DigestVerifierFactory digestVerifierFactory;

        protected Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code vertx} and returns a reference to this Builder enabling method chaining.
         *
         * @param vertx the {@code vertx} to set
         * @return a reference to this Builder
         */
        public Builder vertx(Vertx vertx)
        {
            return update(b -> b.vertx = vertx);
        }

        /**
         * Sets the {@code id} and returns a reference to this Builder enabling method chaining.
         *
         * @param id the {@code id} to set
         * @return a reference to this Builder
         */
        public Builder id(String id)
        {
            return update(b -> b.id = id);
        }

        /**
         * Sets the {@code source} and returns a reference to this Builder enabling method chaining.
         *
         * @param source the {@code source} to set
         * @return a reference to this Builder
         */
        public Builder source(String source)
        {
            return update(b -> b.source = source);
        }

        /**
         * Sets the {@code port} and returns a reference to this Builder enabling method chaining.
         *
         * @param port the {@code port} to set
         * @return a reference to this Builder
         */
        public Builder port(int port)
        {
            return update(b -> b.port = port);
        }

        /**
         * Sets the {@code instanceMetadata} and returns a reference to this Builder enabling method chaining.
         *
         * @param instanceMetadata the {@code instanceMetadata} to set
         * @return a reference to this Builder
         */
        public Builder instanceMetadata(InstanceMetadata instanceMetadata)
        {
            return update(b -> b.instanceMetadata = instanceMetadata);
        }

        /**
         * Sets the {@code executorPools} and returns a reference to this Builder enabling method chaining.
         *
         * @param executorPools the {@code executorPools} to set
         * @return a reference to this Builder
         */
        public Builder executorPools(ExecutorPools executorPools)
        {
            return update(b -> b.executorPools = executorPools);
        }

        /**
         * Sets the {@code request} and returns a reference to this Builder enabling method chaining.
         *
         * @param request the {@code request} to set
         * @return a reference to this Builder
         */
        public Builder request(LiveMigrationFilesVerificationRequest request)
        {
            return update(b -> b.request = request);
        }

        /**
         * Sets the {@code sidecarClient} and returns a reference to this Builder enabling method chaining.
         *
         * @param sidecarClient the {@code sidecarClient} to set
         * @return a reference to this Builder
         */
        public Builder sidecarClient(SidecarClient sidecarClient)
        {
            return update(b -> b.sidecarClient = sidecarClient);
        }

        /**
         * Sets the {@code liveMigrationConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param liveMigrationConfiguration the {@code liveMigrationConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder liveMigrationConfiguration(LiveMigrationConfiguration liveMigrationConfiguration)
        {
            return update(b -> b.liveMigrationConfiguration = liveMigrationConfiguration);
        }

        /**
         * Sets the {@code digestVerifierFactory} and returns a reference to this Builder enabling method chaining.
         *
         * @param digestVerifierFactory the {@code digestVerifierFactory} to set
         * @return a reference to this Builder
         */
        public Builder digestVerifierFactory(DigestVerifierFactory digestVerifierFactory)
        {
            return update(b -> b.digestVerifierFactory = digestVerifierFactory);
        }

        /**
         * Returns a {@code LiveMigrationFilesVerificationTask} built from the parameters previously set.
         *
         * @return a {@code LiveMigrationFilesVerificationTask} built with parameters of this
         * {@code LiveMigrationFilesVerificationTask.Builder}
         */
        @Override
        public LiveMigrationFilesVerificationTask build()
        {
            Objects.requireNonNull(vertx, "vertx is required");
            Objects.requireNonNull(id, "id is required");
            Objects.requireNonNull(source, "source is required");
            Objects.requireNonNull(instanceMetadata, "instanceMetadata is required");
            Objects.requireNonNull(executorPools, "executorPools is required");
            Objects.requireNonNull(request, "request is required");
            Objects.requireNonNull(sidecarClient, "sidecarClient is required");
            Objects.requireNonNull(liveMigrationConfiguration, "liveMigrationConfiguration is required");
            Objects.requireNonNull(digestVerifierFactory, "digestVerifierFactory is required");

            return new LiveMigrationFilesVerificationTask(this);
        }
    }
}
