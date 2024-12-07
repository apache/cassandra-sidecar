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
import org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException;
import org.apache.cassandra.sidecar.common.utils.OperationalJobResult.OperationalJobStatus;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

/**
 * An abstraction of the management and tracking of long-running jobs running on the sidecar.
 */
@Singleton
public class OperationalJobManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OperationalJobManager.class);

    private final OperationalJobTracker jobTracker;
    private final ExecutorPools executorPools;
    private final Vertx vertx;
    private final ServiceConfiguration config;

    /**
     * Creates a manager instance with a default sized job-tracker.
     * @param executorPools
     */
    @Inject
    public OperationalJobManager(Vertx vertx, ExecutorPools executorPools, SidecarConfiguration config, OperationalJobTracker jobTracker)
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
    public List<OperationalJob> allInflightJobs()
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
    public OperationalJob getJobIfExists(UUID jobId)
    {
        return jobTracker.get(jobId);
    }

    /**
     * Asynchronously submit (and lazily create) the job, if it is not currently being
     * tracked and is not running downstream. The job is triggered on a separate internal thread-pool.
     * The job execution failure behavior is tracked within the {@link OperationalJob}.
     * @param headerJobId job identifier
     * @param job function used to create an instance of the job based on the UUID parameter
     */
    public void trySubmitJob(UUID headerJobId, OperationalJob job)
    {
        maybeProcessDownstreamRunningJob(headerJobId, job);

        if (headerJobId != null)
        {
            OperationalJob cachedJob = jobTracker.get(headerJobId);
            if (cachedJob != null)
            {
                LOGGER.warn("Stale cache entry as there is no downstream job with jobId: {}, operation:{}",
                            cachedJob.jobId(), cachedJob.operation());
            }
        }

        // New job is submitted for all cases when we do not have a corresponding downstream job
        jobTracker.computeIfAbsent(job.jobId(), j -> {
            Future<OperationalJobStatus> status = submitJob(job);
            return job;
        });
    }

    private void maybeProcessDownstreamRunningJob(UUID headerJobId, OperationalJob job)
    {
        if (job.isRunningDownstream())
        {
            UUID responseJobId = null;
            OperationalJob cachedJob = (headerJobId != null) ? jobTracker.get(headerJobId) : null;
            if (cachedJob != null && !cachedJob.status().isComplete())
            {
                responseJobId = headerJobId;
            }
            throw new OperationalJobException("Conflicting job running downstream", responseJobId);
        }
    }

    private Future<OperationalJobStatus> submitJob(OperationalJob job)
    {
        long timeoutMillis = config.operationalJobSyncResponseTimeoutMillis();
        Future<Void> timerFuture = Future.future(promise ->
                                                 vertx.setTimer(timeoutMillis, id -> promise.complete())
        );

        LOGGER.info("Triggering downstream job with ID: {}, operation: {}", job.jobId(), job.operation());
        Future<OperationalJobStatus> actualFuture = executorPools.internal().executeBlocking(p -> job.execute(p), false);

        job.setStatus(actualFuture);

        return actualFuture.failed() ? actualFuture : Future.any(timerFuture.map(v -> false), actualFuture)
                     .compose(cf -> {
                         if (cf.succeeded() && (cf.resultAt(0) != null || cf.resultAt(1) != null))
                         {
                             LOGGER.info("Timed out waiting for response for job {}", job.jobId);
                             return actualFuture;
                         }
                         else
                         {
                             LOGGER.error("Unexpected failure executing the job: {}", job.jobId, cf.cause());
                             return Future.failedFuture(cf.cause());
                         }
                     });
    }
}
