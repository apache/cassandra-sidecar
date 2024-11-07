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
import org.apache.cassandra.sidecar.common.utils.OperationsJobResult.OperationsJobStatus;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;

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
                         .filter(j -> j.status() == OperationsJobStatus.Pending || j.status() == OperationsJobStatus.Running)
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
     * Asynchronously submit (and lazily create, via the supplier) the job, if it is not currently being
     * tracked and is not running downstream. The job is triggered on a separate internal thread-pool.
     * The job execution failure behavior is tracked within the {@link OperationsJob}.
     * @param jobId job identifier
     * @param jobSupplier supplier used to create an instance of the job
     * @return the instance of the job that is either being tracked or was just submitted
     */
    public OperationsJob trySubmitJob(UUID jobId, Supplier<OperationsJob> jobSupplier)
    {

        return jobTracker.computeIfAbsent(jobId, id -> {
            OperationsJob job = jobSupplier.get();
            LOGGER.info("Created job with ID: {}, operation: {}", job.jobId(), job.operation());
            if (!job.checkInflightJob())
            {
                LOGGER.info("Triggering downstream job with ID: {}, operation: {}", job.jobId(), job.operation());
                executorPools.internal().runBlocking(() -> job.execute());
            }
            return job;
        });
    }
}
