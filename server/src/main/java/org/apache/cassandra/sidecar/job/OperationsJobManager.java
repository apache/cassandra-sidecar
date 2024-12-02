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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationsJobException;
import org.apache.cassandra.sidecar.common.utils.OperationsJobResult.OperationsJobStatus;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

/**
 * An abstraction of the management and tracking of long-running jobs running on the sidecar.
 */
@Singleton
public class OperationsJobManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OperationsJobManager.class);

    private final OperationsJobTracker jobTracker;
    private final ExecutorPools executorPools;
    private final Vertx vertx;
    private final ServiceConfiguration config;

    /**
     * Creates a manager instance with a default sized job-tracker.
     * @param executorPools
     */
    @Inject
    public OperationsJobManager(Vertx vertx, ExecutorPools executorPools, SidecarConfiguration config, OperationsJobTracker jobTracker)
    {
        this.vertx = vertx;
        this.executorPools = executorPools;
        this.jobTracker = jobTracker;
        this.config = config.serviceConfiguration();
    }

    /**
     * Fetches the inflight jobs being tracked on the sidecar
     * @return instances of the jobs that are in pending or running states
     */
    public List<OperationsJob> allInflightJobs()
    {
        return jobTracker.getJobsView().values()
                         .stream()
                         .filter(j -> !j.status().isComplete())
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
     * @param job function used to create an instance of the job based on the UUID parameter
     */
    public void trySubmitJob(UUID headerJobId, OperationsJob job)
    {
        maybeProcessDownstreamRunningJob(headerJobId, job);

        if (headerJobId != null)
        {
            OperationsJob cachedJob = jobTracker.get(headerJobId);
            if (cachedJob != null)
            {
                LOGGER.warn("Stale cache entry as there is no downstream job with jobId: {}, operation:{}",
                            cachedJob.jobId(), cachedJob.operation());
            }
        }

        // New job is submitted for all cases when we do not have a corresponding downstream job
        jobTracker.computeIfAbsent(job.jobId(), j -> {
            Future<OperationsJobStatus> status = submitJob(job);
            LOGGER.info("Future isComplete:" + status.isComplete());
//            LOGGER.error("Future isComplete promise:" +status.hashCode());
            return job;
        });
    }

    private void maybeProcessDownstreamRunningJob(UUID headerJobId, OperationsJob job)
    {
        if (job.isRunningDownstream())
        {
            UUID responseJobId = null;
            OperationsJob cachedJob = (headerJobId != null) ? jobTracker.get(headerJobId) : null;

            // TODO: Future holds no value until complete => we check for completeness instead of RUNNING
            // cachedJob.status().result() == RUNNING
            // When we have a cached job with the header jobId
            if (cachedJob != null && !cachedJob.status().isComplete())
            {
                responseJobId = headerJobId;
            }
            throw new OperationsJobException("Conflicting job running downstream", responseJobId);
        }
    }

    // Result of submit() is cached => needs to be future of status, so we can poll from main thread
    private Future<OperationsJobStatus> submitJob(OperationsJob job)
    {
        long timeoutMillis = config.operationsJobSyncResponseTimeout();
        Future<Void> timerFuture = Future.future(promise ->
                                                 vertx.setTimer(timeoutMillis, id -> promise.complete())
        );

        LOGGER.info("Triggering downstream job with ID: {}, operation: {}", job.jobId(), job.operation());
        Future<OperationsJobStatus> actualFuture = executorPools.internal().executeBlocking(p -> job.execute(p), false);

        job.setStatus(actualFuture);
//        LOGGER.info("Failed?: "+actualFuture.failed());
//        LOGGER.error("Failed? promise:" +actualFuture.hashCode());

        return actualFuture.failed() ? actualFuture : Future.any(timerFuture.map(v -> false), actualFuture)
                     .compose(cf -> {
                         // Determine which future completed first
                         // TODO: Can be merged
                         if (cf.succeeded() && cf.resultAt(0) != null)
                         {
                             LOGGER.info("Timed out waiting for response");
                             return actualFuture;
                         }
                         else if (cf.succeeded() && cf.resultAt(1) != null)
                         {
                             LOGGER.info("Execution succeeded within time limit");
                             return actualFuture;
                         }
                         else
                         {
                             LOGGER.error("Unexpected failure executing the job: {}", job.jobId, cf.cause());
                             return Future.failedFuture("Unexpected failure");
                         }
                     });
    }
}
