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
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationsJobException;
import org.apache.cassandra.sidecar.common.utils.OperationsJobResult.OperationsJobStatus;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;

import static org.apache.cassandra.sidecar.common.utils.OperationsJobResult.OperationsJobStatus.RUNNING;

/**
 * An abstraction of the management and tracking of long-running jobs running on the sidecar.
 */
@Singleton
public class OperationsJobManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OperationsJobManager.class);

    private final OperationsJobTracker jobTracker;
    private final ExecutorPools executorPools;

    /**
     * Creates a manager instance with a default sized job-tracker.
     * @param executorPools
     */
    @Inject
    public OperationsJobManager(ExecutorPools executorPools, OperationsJobTracker jobTracker)
    {
        this.executorPools = executorPools;
        this.jobTracker = jobTracker;
    }

    /**
     * Fetches the inflight jobs being tracked on the sidecar
     * @return instances of the jobs that are in pending or running states
     */
    public List<OperationsJob> allInflightJobs()
    {
        return jobTracker.getJobsView().values()
                         .stream()
                         .filter(j -> j.status() == OperationsJobStatus.PENDING || j.status() == RUNNING)
                         .collect(Collectors.toList());
    }

    /**
     * Fetch the job using its UUID
     * @param jobId identifier of the job
     * @return instance of the job or null
     */
    public OperationsJob getJobIfExists(UUID jobId)
    {
        return jobTracker.get(jobId);
    }

    /**
     * Asynchronously submit (and lazily create) the job, if it is not currently being
     * tracked and is not running downstream. The job is triggered on a separate internal thread-pool.
     * The job execution failure behavior is tracked within the {@link OperationsJob}.
     * @param headerJobId job identifier
     * @param jobCreator function used to create an instance of the job based on the UUID parameter
     * @return the instance of the job that is either being tracked or was just submitted
     */
    public OperationsJob trySubmitJob(UUID headerJobId, Function<UUID, OperationsJob> jobCreator)
    {
        UUID computedJobId = (headerJobId == null) ? UUID.randomUUID() : headerJobId;
        OperationsJob job = jobCreator.apply(computedJobId);
        maybeProcessDownstreamRunningJob(headerJobId, job);

        if (headerJobId != null && jobTracker.containsKey(headerJobId))
        {
            OperationsJob cachedJob = jobTracker.get(headerJobId);
            LOGGER.warn("Stale cache entry as there is no downstream job with jobId: {}, operation:{}",
                         cachedJob.jobId(), cachedJob.operation());
        }

        // New job is submitted for all cases when we do not have a corresponding downstream job
        return jobTracker.computeIfAbsent(job.jobId(), j -> submitJob(job));
    }

    private void maybeProcessDownstreamRunningJob(UUID headerJobId, OperationsJob job)
    {
        if (job.isRunningDownstream())
        {
            UUID responseJobId = null;
            OperationsJob cachedJob = (headerJobId != null) ? jobTracker.get(headerJobId) : null;

            if (cachedJob != null && cachedJob.status() == RUNNING)
            {
                responseJobId = headerJobId;
            }
            throw new OperationsJobException("Conflicting job running downstream", responseJobId);
        }
    }

    private OperationsJob submitJob(OperationsJob job)
    {
        LOGGER.info("Triggering downstream job with ID: {}, operation: {}", job.jobId(), job.operation());
        executorPools.internal().runBlocking(() -> job.execute());
        return job;
    }
}
