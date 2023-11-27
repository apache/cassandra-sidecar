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

import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.jetbrains.annotations.NotNull;

/**
 * In-memory only tracker that tracks the progress of the slices in a restore job.
 */
public class RestoreSliceTracker
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreSliceTracker.class);

    private volatile RestoreJob restoreJob;
    private final Map<RestoreSlice, Status> slices = new ConcurrentHashMap<>();
    private final RestoreProcessor processor;
    private final AtomicReference<RestoreJobFatalException> failureRef = new AtomicReference<>();

    public RestoreSliceTracker(RestoreJob restoreJob, RestoreProcessor restoreProcessor)
    {
        this.restoreJob = restoreJob;
        this.processor = restoreProcessor;
    }

    /**
     * Submit a slice to be processed in the background
     * @param slice slice of restore data to be processed
     * @return status of the submitted slice
     * @throws RestoreJobFatalException any of the slices encounters a fatal failure
     */
    Status trySubmit(RestoreSlice slice) throws RestoreJobFatalException
    {
        // The job fails early, prevents further submissions
        if (failureRef.get() != null)
            throw failureRef.get();

        Status status = slices.putIfAbsent(slice, Status.PENDING);

        if (status == null)
        {
            slice.registerTracker(this);
            processor.submit(slice);
            return Status.CREATED;
        }

        return status;
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

    public void completeSlice(RestoreSlice slice)
    {
        slices.put(slice, Status.COMPLETED);
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

    /**
     * Internal method to clean up the {@link RestoreSlice}.
     * It validates the slices and log warnings if they are not in a final state,
     * i.e. {@link Status#PENDING} and no {@link #failureRef}
     */
    void cleanupInternal()
    {
        slices.forEach((slice, status) -> {
            if (failureRef.get() == null && status != Status.COMPLETED)
            {
                LOGGER.warn("Clean up pending restore slice when the job has not failed. jobId={}, sliceId={}",
                            restoreJob.jobId, slice.sliceId());
            }
            slice.cancel();
        });
        slices.clear();
    }

    /**
     * Enum holds possible statues of {@link RestoreSlice}
     */
    public enum Status
    {
        CREATED,
        PENDING,
        COMPLETED,
    }
}
