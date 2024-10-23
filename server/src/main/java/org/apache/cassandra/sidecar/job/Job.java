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

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.common.utils.JobResult;
import org.apache.cassandra.sidecar.common.utils.JobResult.JobStatus;

/**
 * An abstract class representing a Job managed by the sidecar.
 *
 */
public abstract class Job
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

    protected UUID jobId;
    protected JobStatus status;
    protected String failureReason;
    final CountDownLatch latch = new CountDownLatch(1);

    protected Job()
    {

    }
    /**
     * Constructs a job with a unique UUID, in Pending state
     * @param jobId UUID representing the Job to be created
     */
    protected Job(UUID jobId)
    {
        this.jobId = jobId;
        this.status = JobStatus.Pending;
        this.failureReason = "";
    }

    @VisibleForTesting
    protected Job(UUID jobId, JobStatus status)
    {
        this.jobId = jobId;
        this.status = status;
        this.failureReason = "";
    }

    public void setStatus(JobStatus status)
    {
        this.status = status;
    }
    public JobStatus status()
    {
        return status;
    }
    public String failureReason()
    {
        return failureReason;
    }

    public UUID jobId()
    {
        return jobId;
    }

    /**
     * Supplier specifying the functionality of the job to be triggered when the job is executed. Subclasses to
     * provide operation-specific implementations
     * @return a function with the operation implementation that returns a {@code JobResult}
     * @throws Exception
     */
    public abstract Supplier<JobResult> jobOperationSupplier() throws Exception;

    /**
     * Provide a meaningful name of the operation executed by the concrete subclass.
     * @return the name of the operation. eg. nodetool command name
     */
    public abstract String operation();

    /**
     * Specifies the downstream operation to be performed to check if the job is running on the Cassandra node/cluster.
     * This functionality is provided by the Job when it is asynchronously triggering the job via the {@code JobManager}
     * For synchronous jobs this should always return false.
     * @return true if the job is running downstream
     */
    public abstract boolean jobInProgress();
    public void execute()
    {
        try
        {
            LOGGER.info("Executing job with ID: {}", jobId);
            JobResult result = jobOperationSupplier().get();
            status = result.status();
            failureReason = result.reason();
            LOGGER.debug("Job with ID: {} returned with status: {}", jobId, status);
        }
        catch (Exception e)
        {
            LOGGER.error("Failed to execute job {} with reason: {}", jobId, e.getMessage());
            status = JobStatus.Failed;
            failureReason = e.getMessage();
        }
        latch.countDown();
    }

    /**
     * Returns if the job execution completes within the provided wait time (in seconds)
     * @param waitSeconds no. of seconds to wait for the job execution to complete
     * @return true if the job completed within the specified time
     */
    public boolean isResultAvailable(long waitSeconds)
    {
        try
        {
            return (!latch.await(waitSeconds, TimeUnit.SECONDS)) ? false : true;
        }
        catch (final InterruptedException e)
        {
            return false;
        }
    }
}
