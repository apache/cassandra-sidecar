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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.exceptions.ThrowableUtils;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Manages the restore job per instance
 * {@link #trySubmit(RestoreSlice, RestoreJob)} is the main entrypoint to submit new slices,
 * typically from the create slices endpoint.
 */
public class RestoreJobManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreJobManager.class);

    private final Map<UUID, RestoreSliceTracker> jobs = new ConcurrentHashMap<>();
    private final RestoreProcessor processor;
    private final ExecutorPools executorPools;
    private final InstanceMetadata instanceMetadata;
    private final RestoreJobConfiguration restoreJobConfig;

    public RestoreJobManager(RestoreJobConfiguration restoreJobConfig,
                             InstanceMetadata instanceMetadata,
                             ExecutorPools executorPools,
                             RestoreProcessor restoreProcessor)
    {
        this(restoreJobConfig, instanceMetadata, executorPools, restoreProcessor, true);
    }

    @VisibleForTesting
    public RestoreJobManager(RestoreJobConfiguration restoreJobConfig,
                             InstanceMetadata instanceMetadata,
                             ExecutorPools executorPools,
                             RestoreProcessor restoreProcessor,
                             boolean deleteOnStart)
    {
        this.restoreJobConfig = restoreJobConfig;
        this.instanceMetadata = instanceMetadata;
        this.executorPools = executorPools;
        this.processor = restoreProcessor;
        // delete obsolete on start up. Once instance is started, the jobDiscoverer will find the jobs to cleanup
        if (deleteOnStart)
        {
            deleteObsoleteDataAsync();
        }
    }

    /**
     * Submit a slice to be processed in the background
     *
     * @param slice slice of restore data to be processed
     * @param restoreJob restore job
     * @return status of the submitted slice
     * @throws RestoreJobFatalException the job has failed
     */
    public RestoreSliceTracker.Status trySubmit(RestoreSlice slice, RestoreJob restoreJob)
    throws RestoreJobFatalException
    {
        RestoreSliceTracker tracker = jobs.computeIfAbsent(slice.jobId(),
                                                           id -> new RestoreSliceTracker(restoreJob, processor));
        return tracker.trySubmit(slice);
    }

    /**
     * Update the restore job reference in tracker, in order for pending restore slices to read the latest
     * restore job, especially the credentials to download from cloud storage.
     *
     * @param job restore job to update
     */
    void updateRestoreJob(RestoreJob job)
    {
        RestoreSliceTracker tracker = jobs.computeIfAbsent(job.jobId,
                                                           id -> new RestoreSliceTracker(job, processor));
        tracker.updateRestoreJob(job);
    }

    /**
     * Remove the tracker of the job when it is completed and delete its data on disk. The method internal.
     * It should only be called by the background task, when it discovers the job is
     * in the final {@link org.apache.cassandra.sidecar.common.data.RestoreJobStatus}, i.e. SUCCEEDED or FAILED.
     *
     * @param jobId job id
     */
    void removeJobInternal(UUID jobId)
    {
        RestoreSliceTracker tracker = jobs.remove(jobId);
        if (tracker != null)
        {
            tracker.cleanupInternal();
        }
        // There might be no tracker, but the job has data on disk.
        deleteDataOfJobAsync(jobId);
    }

    /**
     * Find obsolete job data on disk and delete them
     * The obsoleteness is determined by {@link RestoreJobConfiguration#jobDiscoveryRecencyDays}
     */
    Future<Void> deleteObsoleteDataAsync()
    {
        return findObsoleteJobDataDirs()
               .compose(pathStream -> {
                   try (Stream<Path> stream = pathStream)
                   {
                       // use 'join' to complete the other deletes, when there is an error
                       List<Future> deletes = stream.map(this::deleteDataAsync)
                                                    .collect(Collectors.toList());
                       return CompositeFuture.join(deletes)
                              .compose(compositeFuture -> {
                                  // None of them should fail. Having the branch here for logic completeness
                                  if (compositeFuture.failed())
                                  {
                                      LOGGER.warn("Unexpected error while deleting files.",
                                                  compositeFuture.cause());
                                  }
                                  return Future.<Void>succeededFuture();
                              });
                   }
               })
               .recover(any -> Future.succeededFuture());
    }

    /**
     * Find the restore job directories that are older than {@link RestoreJobConfiguration#jobDiscoveryRecencyDays}
     * @return a future of stream of path that should be closed on success. When failed to list, no stream is created.
     */
    Future<Stream<Path>> findObsoleteJobDataDirs()
    {
        Path rootDir = Paths.get(instanceMetadata.stagingDir());
        if (!Files.exists(rootDir))
            return Future.succeededFuture(Stream.empty());

        return executorPools.internal().executeBlocking(promise -> {
            try
            {
                Stream<Path> obsoleteDirs = Files.walk(rootDir, 1)
                                                 .filter(this::isObsoleteRestoreJobDir);
                promise.complete(obsoleteDirs);
            }
            catch (IOException ioe)
            {
                LOGGER.warn("Error on listing restore job data directories.", ioe);
            }
            finally
            {
                // Ensure the promise is complete. It is a no-op, if the promise is already completed.
                promise.tryComplete(Stream.empty());
            }
        });
    }

    // Deletes quietly w/o returning failed futures
    private Future<Void> deleteDataOfJobAsync(UUID jobId)
    {
        Path stagingDir = Paths.get(instanceMetadata.stagingDir());
        if (!Files.exists(stagingDir))
            return Future.succeededFuture();

        String prefixedJobId = RestoreJobUtil.prefixedJobId(jobId);
        return executorPools.internal().runBlocking(() -> {
            try (Stream<Path> rootDirs = Files.walk(stagingDir, 1))
            {
                rootDirs
                .filter(path -> Files.isDirectory(path) && path.startsWith(prefixedJobId))
                .forEach(this::deleteDataAsync);
            }
            catch (IOException ioe) // thrown from Files.walk.
            {
                LOGGER.warn("Error on listing staged restore job directories. Path={}", stagingDir, ioe);
            }
        });
    }

    // Deletes quietly w/o returning failed futures
    private Future<Void> deleteDataAsync(Path root)
    {
        return executorPools.internal().runBlocking(() -> {
            try (Stream<Path> pathStream = Files.walk(root))
            {
                pathStream
                .sorted(Comparator.reverseOrder())
                .forEach(path -> ThrowableUtils.propogate(() -> Files.delete(path)));
            }
            catch (Exception ioe)
            {
                LOGGER.warn("Error on deleting data. Path={}", root, ioe);
            }
        });
    }

    // returns true only when all conditions are met
    // 1. the path is a directory,
    // 2. it is older than jobDiscoveryRecencyDays
    // 3. its file name indicates it is a restore job directory
    boolean isObsoleteRestoreJobDir(Path path)
    {
        File file = path.toFile();
        if (!file.isDirectory())
            return false;

        long originTs = RestoreJobUtil.timestampFromRestoreJobDir(file.getName());
        if (originTs == -1)
            return false;

        long delta = System.currentTimeMillis() - originTs;
        long gapInMillis = TimeUnit.DAYS.toMillis(restoreJobConfig.jobDiscoveryRecencyDays());
        return delta > gapInMillis;
    }
}
