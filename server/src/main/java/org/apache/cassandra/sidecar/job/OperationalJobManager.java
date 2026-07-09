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

package org.apache.cassandra.sidecar.job;

import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.exceptions.OperationalJobConflictException;
import org.jetbrains.annotations.Nullable;

/**
 * An abstraction of the management and tracking of long-running jobs running on the sidecar.
 */
@Singleton
public class OperationalJobManager
{
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final OperationalJobTracker jobTracker;
    private final OperationalJobCoordinator coordinator;
    private final TaskExecutorPool internalExecutorPool;

    /**
     * Creates a manager instance with a coordinator for cluster-wide operation mutual exclusion. Instances that
     * do not support coordination bind a {@link DisabledOperationalJobCoordinator}, which fails coordination
     * requests.
     *
     * @param jobTracker  the tracker for the operational jobs
     * @param coordinator the coordinator for cluster-wide operations
     */
    @Inject
    public OperationalJobManager(OperationalJobTracker jobTracker,
                                 OperationalJobCoordinator coordinator,
                                 ExecutorPools executorPools)
    {
        this.jobTracker = jobTracker;
        this.coordinator = coordinator;
        this.internalExecutorPool = executorPools.internal();
    }

    /**
     * Fetches the inflight jobs being tracked on the sidecar
     *
     * @return instances of the jobs that are in pending or running states
     */
    public List<OperationalJobInfo> allInflightJobs()
    {
        return jobTracker.jobsView().values()
                         .stream()
                         .filter(j -> !j.status().isCompleted())
                         .collect(Collectors.toList());
    }

    /**
     * Fetch the job using its UUID
     *
     * @param jobId identifier of the job
     * @return instance of the job info or null
     */
    public OperationalJobInfo getJobIfExists(UUID jobId)
    {
        return jobTracker.get(jobId);
    }

    /**
     * Try to submit the job to execute asynchronously, if it is not currently being
     * tracked and not running. The job is triggered on a separate internal thread-pool.
     * The job execution failure behavior is tracked within the {@link OperationalJob}.
     * This method provides a callback mechanism and configurable wait time for job completion.
     *
     * @param job                 OperationalJob instance to submit
     * @param onComplete          callback to invoke when the job completes (successfully or with exception)
     * @param serviceExecutorPool the executor pool to use for waiting on job completion
     * @param waitTime            the maximum time to wait for job completion before returning
     */
    public void trySubmitJob(OperationalJob job,
                             BiConsumer<OperationalJob, OperationalJobConflictException> onComplete,
                             TaskExecutorPool serviceExecutorPool,
                             DurationSpec waitTime)
    {
        try
        {
            checkConflict(job);
        }
        catch (OperationalJobConflictException oje)
        {
            onComplete.accept(job, oje);
            return;
        }

        if (!job.requiresCoordination())
        {
            trackAndExecute(job, onComplete, serviceExecutorPool, waitTime);
            return;
        }

        // Acquiring the active operation lock might perform blocking storage I/O so it must
        // run off the event loop
        acquireActiveOperationLock(job)
        .onComplete(ar ->
        {
            if (ar.succeeded() && Boolean.TRUE.equals(ar.result()))
            {
                // Distributed jobs that finish on other nodes opt out of auto-release; the orchestration
                // layer clears the lock once all nodes reach a terminal state.
                if (job.releasesOnCompletion())
                {
                    job.asyncResult().onComplete(result -> releaseActiveOperationLock(job));
                }
                trackAndExecute(job, onComplete, serviceExecutorPool, waitTime);
            }
            else
            {
                OperationalJobConflictException conflict = coordinationConflict(job, ar.cause());
                job.failToStart(conflict);
                jobTracker.computeIfAbsent(job.jobId(), jobId -> job);
                onComplete.accept(job, conflict);
            }
        });
    }

    /**
     * Tracks the job and submits it for asynchronous execution on the internal executor pool, then arranges for
     * {@code onComplete} to be invoked with the result (or after {@code waitTime} elapses).
     *
     * @param job                 the job to track and execute
     * @param onComplete          callback to invoke when the job completes
     * @param serviceExecutorPool the executor pool to use for waiting on job completion
     * @param waitTime            the maximum time to wait for job completion before returning
     */
    private void trackAndExecute(OperationalJob job,
                                 BiConsumer<OperationalJob, OperationalJobConflictException> onComplete,
                                 TaskExecutorPool serviceExecutorPool,
                                 DurationSpec waitTime)
    {
        // New job is submitted for all cases when we do not have a corresponding downstream job
        OperationalJob tracked = jobTracker.computeIfAbsent(job.jobId(), jobId -> job);
        if (tracked == job)
        {
            internalExecutorPool.executeBlocking(job::execute);
        }

        // Get the result, waiting for the specified wait time for result
        job.asyncResult(serviceExecutorPool, waitTime)
           .onComplete(v -> onComplete.accept(job, null));
    }

    /**
     * Checks the job tracker for existing inflight jobs with the same operation before checking downstream for
     * corresponding running job on the Cassandra node as a conflict of the job being submitted.
     * @param job instance of the job to check conflicts for
     * @throws OperationalJobConflictException when a conflicting inflight job is found
     */
    private void checkConflict(OperationalJob job) throws OperationalJobConflictException
    {
        List<OperationalJob> sameOperationJobs = jobTracker.inflightJobsByOperation(job.name());
        if (job.hasConflict(sameOperationJobs))
        {
            throw new OperationalJobConflictException("The same operational job is already running on Cassandra. operationName='" + job.name() + '\'');
        }
    }

    /**
     * For jobs that require cluster-wide coordination, attempts to acquire the active operation lock via the
     * coordinator. The acquisition runs on the internal executor pool because it might performs blocking storage I/O
     *
     * @param job the job requiring coordination
     * @return a future resolving to {@code true} if the lock was acquired, {@code false} if another operation
     *         already holds it, or a failed future if coordination could not be attempted (e.g. coordination is
     *         disabled on this instance or the storage call failed)
     */
    private Future<Boolean> acquireActiveOperationLock(OperationalJob job)
    {
        return internalExecutorPool.executeBlocking(() -> coordinator.trySetActive(job.operationType(), job.jobId()), false);
    }

    /**
     * Releases the active operation lock previously acquired for the given job. The release performs
     * blocking storage I/O, so it runs on the internal executor pool off the event loop. A failure to clear is logged
     * rather than surfaced, since the job has already completed by this point.
     *
     * @param job the job whose active operation lock should be released
     */
    private void releaseActiveOperationLock(OperationalJob job)
    {
        internalExecutorPool.executeBlocking(() -> coordinator.clearActive(job.operationType(), job.jobId()), false)
                            .onFailure(e -> logger.error("Failed to clear active operation lock. jobId={} operationType={}",
                                                         job.jobId(), job.operationType(), e));
    }

    /**
     * Builds the conflict exception describing why the active operation lock could not be acquired.
     *
     * @param job   the job that could not be coordinated
     * @param cause the failure cause when coordination could not be attempted, or {@code null} when the lock is
     *              simply held by another active operation
     * @return the conflict exception to report to the caller
     */
    private OperationalJobConflictException coordinationConflict(OperationalJob job, @Nullable Throwable cause)
    {
        if (cause != null)
        {
            return new OperationalJobConflictException("Unable to coordinate operation. operationType='"
                                                       + job.operationType() + "', reason='" + cause.getMessage() + '\'');
        }
        return new OperationalJobConflictException("An active operation already exists. operationType='"
                                                   + job.operationType() + '\'');
    }
}
