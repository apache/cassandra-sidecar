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

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.tasks.Task;

/**
 * An abstract class representing operational jobs that run on Cassandra
 */
public abstract class OperationalJob implements Task<Void>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OperationalJob.class);

    // use v1 time-based uuid
    public final UUID jobId;

    private final Promise<Void> executionPromise;
    private volatile boolean isExecuting = false;

    /**
     * Constructs a job with a unique UUID, in Pending state
     *
     * @param jobId UUID representing the Job to be created
     */
    protected OperationalJob(UUID jobId)
    {
        Preconditions.checkArgument(jobId.version() == 1, "OperationalJob accepts only time-based UUID");
        this.jobId = jobId;
        this.executionPromise = Promise.promise();
    }

    @Override
    public final Void result()
    {
        // Update when operational jobs can contain result rather than status
        throw new UnsupportedOperationException("No result is expected from an OperationalJob");
    }

    /**
     * @return unix timestamp of the job creation time in milliseconds
     */
    public long creationTime()
    {
        return UUIDs.unixTimestamp(jobId);
    }

    /**
     * @return whether the operational job is executing or not.
     */
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
     * Determines the status of the job. OperationalJob subclasses could choose to override the method.
     * <p>
     * For long-lived jobs, the implementations should return the {@link OperationalJobStatus#RUNNING} status intelligently.
     * The condition of {@link OperationalJobStatus#RUNNING} is implementation-specific.
     * For example, node decommission is tracked by the operationMode exposed from Cassandra.
     * If the operationMode is LEAVING, the corresponding OperationalJob is {@link OperationalJobStatus#RUNNING}.
     * In this case, even if the OperationalJobStatus determined from this method is {@link OperationalJobStatus#CREATED},
     * the concrete implementation can override and return {@link OperationalJobStatus#RUNNING}.
     * <p>
     * For short-lived jobs, i.e. the result is known right away, the implementations do not return the {@link OperationalJobStatus#RUNNING} status.
     * They return either {@link OperationalJobStatus#SUCCEEDED} or {@link OperationalJobStatus#FAILED}
     *
     * @return status of the OperationalJob execution
     */
    public OperationalJobStatus status()
    {
        Future<Void> fut = asyncResult();
        if (!isExecuting)
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
     * Get the async result with waiting for at most the specified wait time
     * <p>
     * Note: This call does not block the calling thread.
     * The call-site should handle the possible failed future with {@link TimeoutException} from this method.
     *
     * @param executorPool executor pool to run the timer
     * @param waitTime     maximum time to wait before returning
     * @return the async result or a failed future of {@link OperationalJobException} with the cause
     * {@link TimeoutException} after exceeding the wait time
     */
    public Future<Void> asyncResult(TaskExecutorPool executorPool, Duration waitTime)
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
                     // We want to return the result when applicable, of course.
                     // If this lambda below is evaluated, both futures are completed;
                     // Depending on whether timeout flag is set, it either throws or complete with result
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
     * OperationalJob body. The implementation is executed in a blocking manner.
     *
     * @throws OperationalJobException OperationalJobException that wraps job failure
     */
    protected abstract void executeInternal() throws OperationalJobException;

    /**
     * Execute the job behavior as specified in the internal execution {@link #executeInternal()},
     * while tracking the status of the job's lifecycle.
     */
    @Override
    public void execute(Promise<Void> promise)
    {
        isExecuting = true;
        LOGGER.info("Executing job. jobId={}", jobId);
        promise.future().onComplete(executionPromise);
        try
        {
            // Blocking call to perform concrete job-specific execution, returning the status
            executeInternal();
            promise.tryComplete();
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Complete job execution. jobId={} status={}", jobId, status());
            }
        }
        catch (Throwable e)
        {
            OperationalJobException oje = OperationalJobException.wraps(e);
            LOGGER.error("Job execution failed. jobId={} reason='{}'", jobId, oje.getMessage(), oje);
            promise.tryFail(oje);
        }
    }
}
