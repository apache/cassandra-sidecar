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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.utils.JobResult.JobStatus;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;

/**
 * An abstraction of the management and tracking of long-running jobs running on the sidecar.
 */
@Singleton
public class JobManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JobManager.class);

    private final JobTracker jobTracker;
    private final ExecutorPools executorPools;

    /**
     * Creates a manager instance with a default sized job-tracker.
     * @param executorPools
     */
    @Inject
    public JobManager(ExecutorPools executorPools)
    {
        this.executorPools = executorPools;
        jobTracker = new JobTracker(64);
    }

    /**
     * Fetches the inflight jobs being tracked on the sidecar
     * @return instances of the jobs that are in pending or running states
     */
    public List<Job> allInflightJobs()
    {
        return jobTracker.getJobsView().values()
                         .stream()
                         .filter(j -> j.status() == JobStatus.Pending || j.status() == JobStatus.Running)
                         .collect(Collectors.toList());
    }

    /**
     * Fetch the job using its UUID
     * @param jobId identifier of the job
     * @return instance of the job or null
     */
    public Job getJobIfExists(String jobId)
    {
        return jobTracker.get(jobId);
    }

    /**
     * Asynchronously submit (and lazily create, via the supplier) the job, if it is not currently being
     * tracked and is not running downstream. The job is triggered on a separate internal thread-pool.
     * The job execution failure behavior is tracked within the job.
     * @param jobId job identifier
     * @param jobSupplier supplier used to create an instance of the job
     * @return the instance of the job that is either being tracked or was just submitted
     */
    public Job trySubmitJob(UUID jobId, Supplier<Job> jobSupplier)
    {
        Job job;
        if (!jobTracker.containsKey(jobId))
        {
            job = jobSupplier.get();
            LOGGER.info("Created job with ID: {}, operation: {}", job.jobId(), job.operation());
            jobTracker.put(jobId, job);
            if (!job.jobInProgress())
            {
                LOGGER.info("Triggering downstream job with ID: {}, operation: {}", job.jobId(), job.operation());
                executorPools.service().executeBlocking(() -> {
                    triggerJob(job);
                    return job;
                });
            }
        }
        else
        {
            job = jobTracker.get(jobId);
        }
        return job;
    }

    private void triggerJob(Job job)
    {
        executorPools.internal().runBlocking(() -> job.execute());
        job.setStatus(JobStatus.Running);
    }
}
