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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.jetbrains.annotations.NotNull;

/**
 * In-memory only tracker that tracks the progress of the slices in a restore job.
 */
public class RestoreJobProgressTracker
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreJobProgressTracker.class);

    private volatile RestoreJob restoreJob;
    private volatile boolean cleanupOutOfRangeRequested = false;
    private final InstanceMetadata instanceMetadata;
    private final Map<RestoreRange, Status> ranges = new ConcurrentHashMap<>();
    private final RestoreProcessor processor;
    private final AtomicReference<RestoreJobFatalException> failureRef = new AtomicReference<>();

    public RestoreJobProgressTracker(RestoreJob restoreJob, RestoreProcessor restoreProcessor, InstanceMetadata instanceMetadata)
    {
        this.restoreJob = restoreJob;
        this.processor = restoreProcessor;
        this.instanceMetadata = instanceMetadata;
    }

    /**
     * Submit a restore range to be processed in the background
     * @param range range of restore data to be processed
     * @return status of the submitted slice
     * @throws RestoreJobFatalException any of the ranges encounters a fatal failure
     */
    Status trySubmit(RestoreRange range) throws RestoreJobFatalException
    {
        // The job fails early, prevents further submissions
        if (failureRef.get() != null)
            throw failureRef.get();

        Status status = ranges.putIfAbsent(range, Status.PENDING);

        if (status == null)
        {
            RestoreRange rangeWithTracker = range.unbuild()
                                                 .restoreJobProgressTracker(this)
                                                 .build();
            processor.submit(rangeWithTracker);
            return Status.CREATED;
        }

        return status;
    }

    void removeRange(RestoreRange range)
    {
        Status status = ranges.remove(range);

        if (status != null)
        {
            processor.remove(range);
        }
    }

    void updateRestoreJob(@NotNull RestoreJob restoreJob)
    {
        Objects.requireNonNull(restoreJob, "Cannot nullify restore job");
        this.restoreJob = restoreJob;
    }

    @NotNull
    public RestoreJob restoreJob()
    {
        return restoreJob;
    }

    public void completeRange(RestoreRange range)
    {
        ranges.put(range, Status.COMPLETED);
    }

    public void fail(RestoreJobFatalException exception)
    {
        boolean applied = failureRef.compareAndSet(null, exception);
        if (!applied)
        {
            LOGGER.debug("The restore job is already failed. Ignoring the exception. jobId={}",
                         restoreJob.jobId, exception);
            return;
        }
        cleanupInternal();
    }

    public boolean isFailed()
    {
        return failureRef.get() != null;
    }

    public void requestOutOfRangeDataCleanup()
    {
        cleanupOutOfRangeRequested = true;
    }

    /**
     * Internal method to clean up the {@link RestoreSlice}.
     * It validates the slices and log warnings if they are not in a final state,
     * i.e. {@link Status#PENDING} and no {@link #failureRef}
     */
    void cleanupInternal()
    {
        ranges.forEach((range, status) -> {
            if (!isFailed() && status != Status.COMPLETED)
            {
                LOGGER.warn("Clean up pending restore slice when the job has not failed. " +
                            "jobId={}, sliceId={}, startToken={}, endToken={}",
                            restoreJob.jobId, range.sliceId(), range.startToken(), range.endToken());
            }
            range.cancel();
        });
        ranges.clear();

        runOnCompletion();
    }

    /**
     * Run operations on restore job completion, including success and failure cases
     */
    private void runOnCompletion()
    {
        if (cleanupOutOfRangeRequested)
        {
            CassandraAdapterDelegate delegate = instanceMetadata.delegate();
            StorageOperations operations = delegate == null ? null : delegate.storageOperations();
            if (operations == null)
            {
                LOGGER.warn("Out of range data cleanup for the restore job is requested. It failed to start the operation. jobId={}", restoreJob.jobId);
                return;
            }

            try
            {
                operations.outOfRangeDataCleanup(restoreJob.keyspaceName, restoreJob.tableName);
            }
            catch (Throwable cause)
            {
                LOGGER.warn("Clean up out of range data has failed", cause);
            }
        }
    }

    /**
     * Enum holds possible statues of {@link RestoreSlice}
     */
    public enum Status
    {
        CREATED,
        PENDING,
        COMPLETED,

        // only used for sidecar-managed jobs
        FAILED,
    }
}
