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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
    private static final Object PRESENT = new Object();

    private final Map<UUID, RestoreSliceTracker> jobs = new ConcurrentHashMap<>();
    private final Cache<UUID, Object> deletedJobs;
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
        this.deletedJobs = Caffeine.newBuilder().expireAfterAccess(1, TimeUnit.DAYS).build();
        // delete obsolete on start up. Once instance is started, the jobDiscoverer will find the jobs to clean up
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
                                                           id -> new RestoreSliceTracker(restoreJob, processor, instanceMetadata));
        return tracker.trySubmit(slice);
    }

    /**
     * Update the restore job reference in tracker, in order for pending restore slices to read the latest
     * restore job, especially the credentials to download from cloud storage.
     *
     * @param restoreJob restore job to update
     */
    void updateRestoreJob(RestoreJob restoreJob)
    {
        RestoreSliceTracker tracker = jobs.computeIfAbsent(restoreJob.jobId,
                                                           id -> new RestoreSliceTracker(restoreJob, processor, instanceMetadata));
        tracker.updateRestoreJob(restoreJob);
    }

    /**
     * Remove the tracker of the job when it is completed and delete its data on disk. The method runs async and it for internal use only.
     * It should only be called by the background task, when it discovers the job is
     * in the final {@link org.apache.cassandra.sidecar.common.data.RestoreJobStatus}, i.e. SUCCEEDED or FAILED.
     *
     * @param jobId job id
     */
    void removeJobInternal(UUID jobId)
    {
        if (deletedJobs.getIfPresent(jobId) == PRESENT)
        {
            LOGGER.debug("The job is already removed. Skipping. jobId={}", jobId);
            return;
        }

        executorPools
        .internal()
        .runBlocking(() -> {
            RestoreSliceTracker tracker = jobs.remove(jobId);
            if (tracker != null)
            {
                tracker.cleanupInternal();
            }
        })
        .recover(cause -> {
            // There might be no tracker, but the job has data on disk.
            LOGGER.warn("Failed to clean up restore job. Recover and proceed to delete the on-disk files. jobId={}", jobId, cause);
            return Future.succeededFuture();
        })
        .compose(v -> deleteDataOfJobAsync(jobId))
        .onSuccess(v -> deletedJobs.put(jobId, PRESENT));
    }

    /**
     * Find obsolete job data on disk and delete them
     * The obsoleteness is determined by {@link RestoreJobConfiguration#jobDiscoveryRecencyDays}
     */
    void deleteObsoleteDataAsync()
    {
        findObsoleteJobDataDirs()
        .compose(pathStream -> executorPools
                               .internal()
                               .runBlocking(() -> {
                                   try (Stream<Path> stream = pathStream)
                                   {
                                       stream.forEach(this::deleteRecursively);
                                   }
                               }))
        .onFailure(cause -> LOGGER.warn("Unexpected error while deleting files.", cause));
    }

    /**
     * Find the restore job directories that are older than {@link RestoreJobConfiguration#jobDiscoveryRecencyDays}
     * Note that the returned Stream should be closed by the caller.
     * @return a future of stream of path. When failed to list, return a failed failure.
     */
    Future<Stream<Path>> findObsoleteJobDataDirs()
    {
        Path rootDir = Paths.get(instanceMetadata.stagingDir());
        if (!Files.exists(rootDir))
            return Future.succeededFuture(Stream.empty());

        return executorPools.internal()
                            .executeBlocking(() -> Files.walk(rootDir, 1)
                                                        .filter(this::isObsoleteRestoreJobDir));
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
                .forEach(this::deleteRecursively);
            }
            catch (IOException ioe) // thrown from Files.walk.
            {
                LOGGER.warn("Error on listing staged restore job directories. Path={}", stagingDir, ioe);
            }
        });
    }

    // Delete files from the root recursively and quietly w/o throwing any exception
    private void deleteRecursively(Path root)
    {
        try (Stream<Path> pathStream = Files.walk(root))
        {
            pathStream
            .sorted(Comparator.reverseOrder())
            .forEach(path -> ThrowableUtils.propagate(() -> Files.delete(path)));
        }
        catch (Exception exception)
        {
            LOGGER.warn("Error on deleting data. Path={}", root, exception);
        }
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
