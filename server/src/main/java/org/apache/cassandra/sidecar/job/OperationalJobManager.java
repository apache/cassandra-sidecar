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
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.exceptions.OperationalJobConflictException;

/**
 * An abstraction of the management and tracking of long-running jobs running on the sidecar.
 */
@Singleton
public class OperationalJobManager
{
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final OperationalJobTracker jobTracker;

    private final TaskExecutorPool internalExecutorPool;

    /**
     * Creates a manager instance with a default sized job-tracker.
     *
     * @param jobTracker the tracker for the operational jobs
     */
    @Inject
    public OperationalJobManager(OperationalJobTracker jobTracker, ExecutorPools executorPools)
    {
        this.jobTracker = jobTracker;
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

            // New job is submitted for all cases when we do not have a corresponding downstream job
            jobTracker.computeIfAbsent(job.jobId(), jobId -> {
                internalExecutorPool.executeBlocking(job::execute);
                return job;
            });
        }
        catch (OperationalJobConflictException oje)
        {
            onComplete.accept(job, oje);
            return;
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
}
