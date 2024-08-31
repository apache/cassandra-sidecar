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

package org.apache.cassandra.sidecar.restore;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.LocalDate;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.locator.LocalTokenRangesProvider;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobDatabaseAccessor;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreRangeDatabaseAccessor;
import org.apache.cassandra.sidecar.db.RestoreSliceDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.metrics.RestoreMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * {@link RestoreJobDiscoverer} handles background restore job discovery and handling it according to job status
 */
@Singleton
public class RestoreJobDiscoverer implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreJobDiscoverer.class);

    private final RestoreJobConfiguration restoreJobConfig;
    private final SidecarSchema sidecarSchema;
    private final RestoreJobDatabaseAccessor restoreJobDatabaseAccessor;
    private final RestoreSliceDatabaseAccessor restoreSliceDatabaseAccessor;
    private final RestoreRangeDatabaseAccessor restoreRangeDatabaseAccessor;
    private final Provider<RestoreJobManagerGroup> restoreJobManagerGroupSingleton;
    private final LocalTokenRangesProvider localTokenRangesProvider;
    private final InstanceMetadataFetcher instanceMetadataFetcher;
    private final RestoreMetrics metrics;
    private final JobIdsByDay jobIdsByDay;
    private final RingTopologyRefresher ringTopologyRefresher;
    private int inflightJobsCount = 0;
    private int jobDiscoveryRecencyDays;
    private PeriodicTaskExecutor periodicTaskExecutor;

    @Inject
    public RestoreJobDiscoverer(SidecarConfiguration config,
                                SidecarSchema sidecarSchema,
                                RestoreJobDatabaseAccessor restoreJobDatabaseAccessor,
                                RestoreSliceDatabaseAccessor restoreSliceDatabaseAccessor,
                                RestoreRangeDatabaseAccessor restoreRangeDatabaseAccessor,
                                Provider<RestoreJobManagerGroup> restoreJobManagerGroupProvider,
                                LocalTokenRangesProvider cachedLocalTokenRanges,
                                InstanceMetadataFetcher instanceMetadataFetcher,
                                RingTopologyRefresher ringTopologyRefresher,
                                SidecarMetrics metrics)
    {
        this(config.restoreJobConfiguration(),
             sidecarSchema,
             restoreJobDatabaseAccessor,
             restoreSliceDatabaseAccessor,
             restoreRangeDatabaseAccessor,
             restoreJobManagerGroupProvider,
             cachedLocalTokenRanges,
             instanceMetadataFetcher,
             ringTopologyRefresher,
             metrics);
    }

    @VisibleForTesting
    RestoreJobDiscoverer(RestoreJobConfiguration restoreJobConfig,
                         SidecarSchema sidecarSchema,
                         RestoreJobDatabaseAccessor restoreJobDatabaseAccessor,
                         RestoreSliceDatabaseAccessor restoreSliceDatabaseAccessor,
                         RestoreRangeDatabaseAccessor restoreRangeDatabaseAccessor,
                         Provider<RestoreJobManagerGroup> restoreJobManagerGroupProvider,
                         LocalTokenRangesProvider cachedLocalTokenRanges,
                         InstanceMetadataFetcher instanceMetadataFetcher,
                         RingTopologyRefresher ringTopologyRefresher,
                         SidecarMetrics metrics)
    {
        this.restoreJobConfig = restoreJobConfig;
        this.sidecarSchema = sidecarSchema;
        this.restoreJobDatabaseAccessor = restoreJobDatabaseAccessor;
        this.restoreSliceDatabaseAccessor = restoreSliceDatabaseAccessor;
        this.restoreRangeDatabaseAccessor = restoreRangeDatabaseAccessor;
        this.jobDiscoveryRecencyDays = restoreJobConfig.jobDiscoveryRecencyDays();
        this.restoreJobManagerGroupSingleton = restoreJobManagerGroupProvider;
        this.localTokenRangesProvider = cachedLocalTokenRanges;
        this.instanceMetadataFetcher = instanceMetadataFetcher;
        this.ringTopologyRefresher = ringTopologyRefresher;
        this.metrics = metrics.server().restore();
        this.jobIdsByDay = new JobIdsByDay();
    }

    @Override
    public boolean shouldSkip()
    {
        boolean shouldSkip = !sidecarSchema.isInitialized();
        if (shouldSkip)
        {
            LOGGER.trace("Skipping restore job discovering");
        }
        // skip the task when sidecar schema is not initialized
        return shouldSkip;
    }

    @Override
    public long delay()
    {
        // delay value is re-evaluated when rescheduling
        return hasInflightJobs()
               ? restoreJobConfig.jobDiscoveryActiveLoopDelayMillis()
               : restoreJobConfig.jobDiscoveryIdleLoopDelayMillis();
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        executeInternal();
        promise.tryComplete();
    }

    public void executeInternal()
    {
        Preconditions.checkState(periodicTaskExecutor != null, "Loop executor is not registered");

        // Log one message every few minutes should be acceptable
        LOGGER.info("Discovering restore jobs. " +
                    "inflightJobsCount={} delayMs={} jobDiscoveryRecencyDays={}",
                    inflightJobsCount, delay(), jobDiscoveryRecencyDays);

        boolean hasInflightJobBefore = hasInflightJobs();
        // reset in-flight jobs
        inflightJobsCount = 0;
        List<RestoreJob> restoreJobs = restoreJobDatabaseAccessor.findAllRecent(jobDiscoveryRecencyDays);
        long nowMillis = System.currentTimeMillis();
        LocalDate today = LocalDate.fromMillisSinceEpoch(nowMillis);
        RestoreJobManagerGroup restoreJobManagers = restoreJobManagerGroupSingleton.get();
        int days = 0;
        int abortedJobs = 0;
        int expiredJobs = 0;
        for (RestoreJob job : restoreJobs)
        {
            if (jobIdsByDay.shouldLogJob(job))
            {
                LOGGER.info("Found job. jobId={} job={}", job.jobId, job);
            }

            try
            {
                switch (job.status)
                {
                    case CREATED:
                    case STAGED:
                        if (job.expireAt == null // abort all old jobs that has no expireAt value
                            || job.expireAt.getTime() < nowMillis)
                        {
                            expiredJobs += 1;
                            boolean aborted = abortJob(job);
                            abortedJobs += (aborted ? 1 : 0);
                            break; // do not proceed further if the job has expired
                        }
                        // find the oldest non-completed job
                        days = Math.max(days, delta(today, job.createdAt));
                        restoreJobManagers.updateRestoreJob(job);
                        if (job.isManagedBySidecar())
                        {
                            ringTopologyRefresher.register(job);
                            // todo: potential exceedingly number of queries
                            findSlicesAndSubmit(job);
                        }
                        inflightJobsCount += 1;
                        break;
                    case FAILED:
                    case ABORTED:
                    case SUCCEEDED:
                        restoreJobManagers.removeJobInternal(job);
                        if (job.isManagedBySidecar())
                        {
                            ringTopologyRefresher.unregister(job);
                        }
                        break;
                    default:
                        LOGGER.warn("Encountered unknown job status. jobId={} status={}", job.jobId, job.status);
                }
            }
            catch (Exception exception) // do not fail on the job. Continue to drain the entire list
            {
                LOGGER.warn("Exception on processing job. jobId: {}", job.jobId, exception);
            }
        }
        jobIdsByDay.cleanupMaybe();
        // shrink recency to lookup less data next time
        jobDiscoveryRecencyDays = Math.min(days, jobDiscoveryRecencyDays);
        LOGGER.info("Exit job discovery. " +
                    "inflightJobsCount={} " +
                    "jobDiscoveryRecencyDays={} " +
                    "expiredJobs={} " +
                    "abortedJobs={}",
                    inflightJobsCount, jobDiscoveryRecencyDays, expiredJobs, abortedJobs);
        metrics.activeJobs.metric.setValue(inflightJobsCount);

        boolean hasInflightJobsNow = hasInflightJobs();
        // need to update delay time; reschedule self
        if (hasInflightJobBefore != hasInflightJobsNow)
        {
            periodicTaskExecutor.reschedule(this);
        }
    }

    @Override
    public void registerPeriodicTaskExecutor(PeriodicTaskExecutor executor)
    {
        this.periodicTaskExecutor = executor;
    }

    // find all slices of the job that should be downloaded to the local instances,
    // according to the cluster token ownership
    private void findSlicesAndSubmit(RestoreJob restoreJob)
    {
        localTokenRangesProvider.localTokenRanges(restoreJob.keyspaceName)
                                .forEach((key, ranges) -> {
                                    int instanceId = key;
                                    InstanceMetadata instance = instanceMetadataFetcher.instance(instanceId);
                                    ranges.forEach(range -> findSlicesOfRangeAndSubmit(instance, restoreJob, range));
                                });
    }

    // try to submit the slice.
    // If it is already exist, it is a no-op.
    // If the submission fails, the slice status of the instance is updated.
    private void findSlicesOfRangeAndSubmit(InstanceMetadata instance, RestoreJob restoreJob, TokenRange range)
    {
        short bucketId = 0; // TODO: update the implementation to pick proper bucketId
        restoreSliceDatabaseAccessor
        .selectByJobByBucketByTokenRange(restoreJob, bucketId, range)
        .forEach(slice -> {
            // set the owner instance, which is not read from database
            TokenRangeReplicasResponse topology = ringTopologyRefresher.cachedReplicaByTokenRange(restoreJob);
            List<RestoreRange> splits = slice.splitMaybe(topology)
                                             .stream()
                                             .map(s -> {
                                                 String uploadId = RestoreJobUtil.generateUniqueUploadId(s.jobId(), s.sliceId());
                                                 return RestoreRange.builderFromSlice(s)
                                                                    .ownerInstance(instance)
                                                                    .stageDirectory(Paths.get(instance.stagingDir()), uploadId)
                                                                    .build();
                                             })
                                             .collect(Collectors.toList());
            RestoreJobManagerGroup managerGroup = restoreJobManagerGroupSingleton.get();
            for (RestoreRange split : splits)
            {
                RestoreJobProgressTracker.Status status = submit(instance, managerGroup, restoreJob, split);
                if (status == RestoreJobProgressTracker.Status.CREATED)
                {
                    restoreRangeDatabaseAccessor.create(split);
                }
            }
        });
    }

    private RestoreJobProgressTracker.Status submit(InstanceMetadata instance, RestoreJobManagerGroup managerGroup,
                                                    RestoreJob job, RestoreRange range)
    {
        try
        {
            return managerGroup.trySubmit(instance, range, job);
        }
        catch (RestoreJobFatalException e)
        {
            LOGGER.error("Restore range failed. startToken={} endToken={} instance={}",
                         range.startToken(), range.endToken(), range.owner().host());
            range.fail(e);
            restoreRangeDatabaseAccessor.updateStatus(range);
            return RestoreJobProgressTracker.Status.FAILED;
        }
    }

    private boolean abortJob(RestoreJob job)
    {
        LOGGER.info("Abort expired job. jobId={} job={}", job.jobId, job);
        try
        {
            restoreJobDatabaseAccessor.abort(job.jobId, "Expired");
            return true;
        }
        catch (Exception exception) // do not fail on the job. Continue to drain the entire list
        {
            LOGGER.warn("Exception on aborting job. jobId: " + job.jobId, exception);
        }
        return false;
    }

    // get the number of days delta between 2 dates. Always return non-negative values
    private int delta(LocalDate date1, LocalDate date2)
    {
        return Math.abs(date1.getDaysSinceEpoch() - date2.getDaysSinceEpoch());
    }

    static class JobIdsByDay
    {
        private final Map<Integer, Map<UUID, RestoreJobStatus>> jobsByDay = new HashMap<>();
        private final Set<Integer> discoveredDays = new HashSet<>(); // contains the days of the jobs seen from the current round of discovery

        /**
         * Log the jobs when any of the condition is met:
         * - newly discovered
         * - in CREATED status
         * - status changed
         *
         * @return true to log the job
         */
        boolean shouldLogJob(RestoreJob job)
        {
            int day = job.createdAt.getDaysSinceEpoch();
            discoveredDays.add(day);
            Map<UUID, RestoreJobStatus> jobs = jobsByDay.computeIfAbsent(day, key -> new HashMap<>());
            RestoreJobStatus oldStatus = jobs.put(job.jobId, job.status);
            return oldStatus == null || job.status == RestoreJobStatus.CREATED || oldStatus != job.status;
        }

        void cleanupMaybe()
        {
            // remove all the jobIds of the days that are not discovered
            jobsByDay.keySet().removeIf(day -> !discoveredDays.contains(day));
            discoveredDays.clear();
        }

        @VisibleForTesting
        Map<Integer, Map<UUID, RestoreJobStatus>> jobsByDay()
        {
            return jobsByDay;
        }
    }

    @VisibleForTesting
    boolean hasInflightJobs()
    {
        return inflightJobsCount != 0;
    }

    @VisibleForTesting
    int jobDiscoveryRecencyDays()
    {
        return jobDiscoveryRecencyDays;
    }
}
