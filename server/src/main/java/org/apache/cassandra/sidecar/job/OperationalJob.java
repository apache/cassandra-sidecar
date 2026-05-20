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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.tasks.Task;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An abstract class representing operational jobs that run on Cassandra
 */
public abstract class OperationalJob implements Task<Void>, OperationalJobInfo
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OperationalJob.class);

    // use v1 time-based uuid
    private final UUID jobId;
    @Nullable
    private final UUID nodeId;

    private final Promise<Void> executionPromise;
    private volatile boolean isExecuting = false;
    private volatile Instant startTime;
    private volatile Instant lastUpdate;
    private volatile List<UUID> nodesPending;
    private volatile List<UUID> nodesExecuting;
    private volatile List<UUID> nodesSucceeded;
    private volatile List<UUID> nodesFailed;

    /**
     * Constructs a job with a unique UUID, in Pending state
     *
     * @param jobId UUID representing the Job to be created
     */
    protected OperationalJob(UUID jobId)
    {
        this(jobId, null);
    }

    /**
     * Constructs a job with a unique UUID and an associated node identifier, in Pending state
     *
     * @param jobId  UUID representing the Job to be created
     * @param nodeId UUID of the node this job is running on, or {@code null} if not applicable
     */
    protected OperationalJob(UUID jobId, @Nullable UUID nodeId)
    {
        Preconditions.checkArgument(jobId.version() == 1, "OperationalJob accepts only time-based UUID");
        this.jobId = jobId;
        this.nodeId = nodeId;
        this.executionPromise = Promise.promise();
        this.nodesPending = nodeId != null ? new ArrayList<>(Collections.singletonList(nodeId)) : Collections.emptyList();
        this.nodesExecuting = Collections.emptyList();
        this.nodesSucceeded = Collections.emptyList();
        this.nodesFailed = Collections.emptyList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID jobId()
    {
        return jobId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public @Nullable UUID nodeId()
    {
        return nodeId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public @Nullable Instant startTime()
    {
        return startTime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public @Nullable Instant lastUpdate()
    {
        return lastUpdate;
    }

    /**
     * Updates the last update time for this job.
     *
     * @param lastUpdate the new last update time
     */
    protected void lastUpdate(Instant lastUpdate)
    {
        this.lastUpdate = lastUpdate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NotNull
    public List<UUID> nodesPending()
    {
        return nodesPending;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NotNull
    public List<UUID> nodesExecuting()
    {
        return nodesExecuting;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NotNull
    public List<UUID> nodesSucceeded()
    {
        return nodesSucceeded;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NotNull
    public List<UUID> nodesFailed()
    {
        return nodesFailed;
    }

    /**
     * @return whether this job requires coordination across multiple nodes
     */
    public boolean requiresCoordination()
    {
        return false;
    }

    @Override
    public final Void result()
    {
        // Update when operational jobs can contain result rather than status
        throw new UnsupportedOperationException("No result is expected from an OperationalJob");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long creationTime()
    {
        return UUIDs.unixTimestamp(jobId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isExecuting()
    {
        return isExecuting;
    }

    /**
     * Determine whether the operational job is stale by considering both the referenceTimestampInMillis and the ttlInMillis
     *
     * @return true if the job's life duration has exceeded ttlInMillis; otherwise, false
     */
    public boolean isStale(long referenceTimestampInMillis, long ttlInMillis)
    {
        long createdAt = creationTime();
        Preconditions.checkArgument(referenceTimestampInMillis >= createdAt, "Invalid referenceTimestampInMillis");
        Preconditions.checkArgument(ttlInMillis >= 0, "Invalid ttlInMillis");
        return referenceTimestampInMillis - createdAt > ttlInMillis;
    }

    /**
     * Validates if the job has conflict either leveraging either other same operation jobs on the same node and/or a
     * custom job-specific implementation for conflict determination.
     *
     * @param sameOperationJobs list of jobs being tracked by the tracker that have the same operation
     * @return true if the job is has a conflict.
     */
    public abstract boolean hasConflict(@NotNull List<OperationalJob> sameOperationJobs);

    /**
     * {@inheritDoc}
     */
    @Override
    public String name()
    {
        String simpleName = this.getClass().getSimpleName();
        return simpleName.isEmpty() ? this.getClass().getName() : simpleName;
    }

    /**
     * Determines the status of the job. OperationalJob subclasses could choose to override the method.
     * <p>
     * For long-lived jobs, the implementations should return the {@link OperationalJobStatus#RUNNING} status intelligently.
     * If the operationMode is LEAVING, the corresponding OperationalJob is {@link OperationalJobStatus#RUNNING}.
     * In this case, even if the OperationalJobStatus determined from this method is {@link OperationalJobStatus#CREATED},
     * the concrete implementation can override and return {@link OperationalJobStatus#RUNNING}.
     * <p>
     * For short-lived jobs, i.e. the result is known right away, the implementations do not return the {@link OperationalJobStatus#RUNNING} status.
     * They return either {@link OperationalJobStatus#SUCCEEDED} or {@link OperationalJobStatus#FAILED}
     *
     * @return status of the OperationalJob execution
     */
    @Override
    public OperationalJobStatus status()
    {
        Future<Void> fut = asyncResult();
        // Jobs that are created and yet to be picked up by the executor thread
        if (!isExecuting && !fut.isComplete())
        {
            return OperationalJobStatus.CREATED;
        }
        if (!fut.isComplete())
        {
            return OperationalJobStatus.RUNNING;
        }
        else if (fut.failed())
        {
            return OperationalJobStatus.FAILED;
        }
        else
        {
            return OperationalJobStatus.SUCCEEDED;
        }
    }

    public Future<Void> asyncResult()
    {
        return executionPromise.future();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nullable
    public String failureReason()
    {
        Future<Void> fut = asyncResult();
        if (fut.isComplete() && fut.failed() && fut.cause() != null)
        {
            String message = fut.cause().getMessage();
            return message != null ? message : String.format("Encountered %s during job execution", fut.cause().getClass().getName());
        }
        return null;
    }

    /**
     * Get the async result with waiting for at most the specified wait time
     * <p>
     * Note: This call does not block the calling thread.
     *
     * @param executorPool executor pool to run the timer
     * @param waitTime     maximum time to wait before returning
     * @return a future that is either the result of the configured timeout based on {@code waitTime} or the async
     * result. A succeeded future here, represents either a timeout or the result of the job and a failure is
     * represented by an exception thrown by the job execution, within the configured timeout.
     */
    public Future<Void> asyncResult(TaskExecutorPool executorPool, DurationSpec waitTime)
    {
        Future<Void> resultFut = asyncResult();
        if (resultFut.isComplete())
        {
            return resultFut;
        }

        // complete the max wait time promise either when exceeding the wait time, or the result is available
        Promise<Boolean> maxWaitTimePromise = Promise.promise();
        executorPool.setTimer(waitTime.toMillis(), d -> maxWaitTimePromise.tryComplete(true)); // complete with true, meaning timeout
        resultFut.onComplete(res -> maxWaitTimePromise.tryComplete(false)); // complete with false, meaning not timeout
        Future<Boolean> maxWaitTimeFut = maxWaitTimePromise.future();
        // Completes as soon as any future succeeds, or when all futures fail. Note that maxWaitTimePromise is
        // closed as soon as resultFut completes
        return Future.any(maxWaitTimeFut, resultFut)
                     // If this lambda below is evaluated, either one of the futures have completed;
                     // In either case, the future corresponding to the job execution is returned
                     .compose(f -> {
                         boolean isTimeout = maxWaitTimeFut.result();
                         if (isTimeout)
                         {
                             return Future.succeededFuture();
                         }
                         // otherwise, the result of the job is available
                         return resultFut;
                     });
    }

    /**
     * OperationalJob body. The implementation returns a Future representing the job execution.
     */
    protected abstract Future<Void> executeInternal() throws Exception;

    /**
     * Execute the job behavior as specified in the internal execution {@link #executeInternal()},
     * while tracking the status of the job's lifecycle.
     */
    @Override
    public void execute(Promise<Void> promise)
    {
        isExecuting = true;
        startTime = Instant.now();
        lastUpdate = startTime;
        if (nodeId != null)
        {
            nodesPending = Collections.emptyList();
            nodesExecuting = Collections.singletonList(nodeId);
        }
        LOGGER.info("Executing job. jobId={}", jobId);
        promise.future().onComplete(executionPromise);
        try
        {
            Future<Void> internalFuture = executeInternal();
            internalFuture.onComplete(ar -> {
                if (ar.succeeded())
                {
                    if (nodeId != null)
                    {
                        nodesExecuting = Collections.emptyList();
                        nodesSucceeded = Collections.singletonList(nodeId);
                    }
                    lastUpdate = Instant.now();
                    promise.tryComplete();
                    LOGGER.info("Complete job execution. jobId={} status={}", jobId, status());
                }
                else
                {
                    if (nodeId != null)
                    {
                        nodesExecuting = Collections.emptyList();
                        nodesFailed = Collections.singletonList(nodeId);
                    }
                    lastUpdate = Instant.now();
                    promise.tryFail(ar.cause());
                    LOGGER.error("Job execution failed. jobId={} reason={}", jobId, ar.cause().getMessage());
                }
            });
        }
        catch (Throwable e)
        {
            OperationalJobException oje = OperationalJobException.wraps(e);
            if (nodeId != null)
            {
                nodesExecuting = Collections.emptyList();
                nodesFailed = Collections.singletonList(nodeId);
            }
            lastUpdate = Instant.now();
            LOGGER.error("Job execution failed. jobId={} reason={}", jobId, oje.getMessage(), oje);
            promise.tryFail(oje);
        }
    }
}
