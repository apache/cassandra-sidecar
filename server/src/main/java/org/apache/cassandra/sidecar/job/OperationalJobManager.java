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
import java.util.stream.Collectors;
import javax.inject.Inject;

import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.exceptions.OperationalJobConflictException;

/**
 * An abstraction of the management and tracking of long-running jobs running on the sidecar.
 */
@Singleton
public class OperationalJobManager
{
    private final OperationalJobTracker jobTracker;

    /**
     * Creates a manager instance with a default sized job-tracker.
     *
     * @param jobTracker the tracker for the operational jobs
     */
    @Inject
    public OperationalJobManager(OperationalJobTracker jobTracker)
    {
        this.jobTracker = jobTracker;
    }

    /**
     * Fetches the inflight jobs being tracked on the sidecar
     *
     * @return instances of the jobs that are in pending or running states
     */
    public List<OperationalJob> allInflightJobs()
    {
        return jobTracker.getJobsView().values()
                         .stream()
                         .filter(j -> !j.asyncResult().isComplete())
                         .collect(Collectors.toList());
    }

    /**
     * Fetch the job using its UUID
     *
     * @param jobId identifier of the job
     * @return instance of the job or null
     */
    public OperationalJob getJobIfExists(UUID jobId)
    {
        return jobTracker.get(jobId);
    }

    /**
     * Try to submit the job to execute asynchronously, if it is not currently being
     * tracked and not running. The job is triggered on a separate internal thread-pool.
     * The job execution failure behavior is tracked within the {@link OperationalJob}.
     *
     * @param job OperationalJob instance to submit
     * @return OperationalJob instance that is submitted
     * @throws OperationalJobConflictException when the same operational job is already running on Cassandra
     */
    public OperationalJob trySubmitJob(OperationalJob job) throws OperationalJobConflictException
    {
        checkConflict(job);

        // New job is submitted for all cases when we do not have a corresponding downstream job
        return jobTracker.computeIfAbsent(job.jobId, jobId -> job);
    }

    private void checkConflict(OperationalJob job) throws OperationalJobConflictException
    {
        // The job is not yet submitted (and running), but its status indicates that there is an identical job running on Cassandra already
        // In this case, this job submission is rejected.
        if (job.status() == OperationalJobStatus.RUNNING)
        {
            throw new OperationalJobConflictException("The same operational job is already running on Cassandra. operationName='" + job.name() + '\'');
        }
    }
}
