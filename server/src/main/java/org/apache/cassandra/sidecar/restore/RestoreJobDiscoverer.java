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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
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
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobDatabaseAccessor;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreRangeDatabaseAccessor;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.db.RestoreSliceDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.metrics.RestoreMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * {@link RestoreJobDiscoverer} handles background restore job discovery and handling it according to job status
 */
@Singleton
public class RestoreJobDiscoverer implements PeriodicTask, RingTopologyChangeListener
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
    private final AtomicBoolean isExecuting = new AtomicBoolean(false);
    private final TaskExecutorPool executorPool;
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
                                InstanceMetadataFetcher instanceMetadataFetcher,
                                RingTopologyRefresher ringTopologyRefresher,
                                ExecutorPools executorPools,
                                SidecarMetrics metrics)
    {
        this(config.restoreJobConfiguration(),
             sidecarSchema,
             restoreJobDatabaseAccessor,
             restoreSliceDatabaseAccessor,
             restoreRangeDatabaseAccessor,
             restoreJobManagerGroupProvider,
             instanceMetadataFetcher,
             ringTopologyRefresher,
             executorPools,
             metrics);
    }

    @VisibleForTesting
    RestoreJobDiscoverer(RestoreJobConfiguration restoreJobConfig,
                         SidecarSchema sidecarSchema,
                         RestoreJobDatabaseAccessor restoreJobDatabaseAccessor,
                         RestoreSliceDatabaseAccessor restoreSliceDatabaseAccessor,
                         RestoreRangeDatabaseAccessor restoreRangeDatabaseAccessor,
                         Provider<RestoreJobManagerGroup> restoreJobManagerGroupProvider,
                         InstanceMetadataFetcher instanceMetadataFetcher,
                         RingTopologyRefresher ringTopologyRefresher,
                         ExecutorPools executorPools,
                         SidecarMetrics metrics)
    {
        this.restoreJobConfig = restoreJobConfig;
        this.sidecarSchema = sidecarSchema;
        this.restoreJobDatabaseAccessor = restoreJobDatabaseAccessor;
        this.restoreSliceDatabaseAccessor = restoreSliceDatabaseAccessor;
        this.restoreRangeDatabaseAccessor = restoreRangeDatabaseAccessor;
        this.jobDiscoveryRecencyDays = restoreJobConfig.jobDiscoveryMinimumRecencyDays();
        this.restoreJobManagerGroupSingleton = restoreJobManagerGroupProvider;
        this.instanceMetadataFetcher = instanceMetadataFetcher;
        this.ringTopologyRefresher = ringTopologyRefresher;
        this.localTokenRangesProvider = ringTopologyRefresher;
        this.metrics = metrics.server().restore();
        this.jobIdsByDay = new JobIdsByDay();
        this.executorPool = executorPools.internal();
    }

    @Override
    public ScheduleDecision scheduleDecision()
    {
        return shouldSkip() ? ScheduleDecision.SKIP : ScheduleDecision.EXECUTE;
    }

    @Override
    public DurationSpec delay()
    {
        // The delay value is evaluated on scheduling the next run
        // see, org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor.executeAndScheduleNext
        return hasInflightJobs()
               ? restoreJobConfig.jobDiscoveryActiveLoopDelay()
               : restoreJobConfig.jobDiscoveryIdleLoopDelay();
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        tryExecuteDiscovery();
        promise.tryComplete();
    }

    /**
     * Try to execute the job discovery task in the blocking way. It returns immediately, if another thread is executing already.
     * The method can be invoked by external call-sites.
     */
    public void tryExecuteDiscovery()
    {
        if (!isExecuting.compareAndSet(false, true))
        {
            LOGGER.debug("Another thread is executing the restore job discovery already. Skipping...");
            return;
        }

        try
        {
            executeInternal();
        }
        finally
        {
            isExecuting.set(false);
        }
    }

    private boolean shouldSkip()
    {
        boolean shouldSkip = !sidecarSchema.isInitialized();
        if (shouldSkip)
        {
            LOGGER.trace("Skipping restore job discovering due to sidecarSchema not initialized");
        }
        boolean executing = isExecuting.get();
        shouldSkip = shouldSkip || executing;
        if (executing)
        {
            LOGGER.trace("Skipping restore job discovering due to overlapping execution of this task");
        }

        // skip the task
        return shouldSkip;
    }

    private void executeInternal()
    {
        Preconditions.checkState(periodicTaskExecutor != null, "Loop executor is not registered");

        // Log one message every few minutes should be acceptable
        LOGGER.info("Discovering restore jobs. " +
                    "inflightJobsCount={} delayMs={} jobDiscoveryRecencyDays={}",
                    inflightJobsCount, delay(), jobDiscoveryRecencyDays);

        // reset in-flight jobs
        inflightJobsCount = 0;
        RunContext context = new RunContext();
        List<RestoreJob> restoreJobs = restoreJobDatabaseAccessor.findAllRecent(context.nowMillis, jobDiscoveryRecencyDays);
        RestoreJobManagerGroup restoreJobManagers = restoreJobManagerGroupSingleton.get();
        for (RestoreJob job : restoreJobs)
        {
            try
            {
                processOneJob(job, restoreJobManagers, context);
            }
            catch (Exception exception) // do not fail on the job. Continue to drain the entire list
            {
                LOGGER.warn("Exception on processing job. jobId: {}", job.jobId, exception);
            }
        }
        jobIdsByDay.cleanupMaybe();
        // resize to the earliestInDays with the minimum days as defined in jobDiscoveryMinimumRecencyDays
        jobDiscoveryRecencyDays = Math.max(context.earliestInDays, restoreJobConfig.jobDiscoveryMinimumRecencyDays());
        LOGGER.info("Exit job discovery. " +
                    "inflightJobsCount={} " +
                    "jobDiscoveryRecencyDays={} " +
                    "expiredJobs={} " +
                    "abortedJobs={}",
                    inflightJobsCount, jobDiscoveryRecencyDays, context.expiredJobs, context.abortedJobs);
        metrics.activeJobs.metric.setValue(inflightJobsCount);
    }

    @Override
    public void registerPeriodicTaskExecutor(PeriodicTaskExecutor executor)
    {
        this.periodicTaskExecutor = executor;
    }

    @Override
    public void onRingTopologyChanged(String keyspace, TokenRangeReplicasResponse oldTopology, TokenRangeReplicasResponse newTopology)
    {
        if (oldTopology == null)
        {
            LOGGER.debug("Received RingTopologyChanged notification for new topology discovered. " +
                         "It is already handled inline at findSlicesAndSubmit. Exiting early. " +
                         "keyspace={}", keyspace);
            return;
        }

        Map<Integer, Set<TokenRange>> localRangesFromOld = RingTopologyRefresher.calculateLocalTokenRanges(instanceMetadataFetcher, oldTopology);
        Map<Integer, Set<TokenRange>> localRangesFromNew = RingTopologyRefresher.calculateLocalTokenRanges(instanceMetadataFetcher, newTopology);
        if (Objects.equals(localRangesFromOld, localRangesFromNew))
        {
            LOGGER.debug("Local token ranges derived from both topology are the same. No need to update restore ranges.");
            return;
        }

        // Populate the lostRanges and the gainedRanges
        // For each lost range, we want to cancel the RestoreRange that covers it
        // For each gained range, we want to create the RestoreRange
        Map<Integer, Set<TokenRange>> lostRanges = new HashMap<>(localRangesFromNew.size());
        Map<Integer, Set<TokenRange>> gainedRanges = new HashMap<>(localRangesFromNew.size());
        for (Integer instanceId : Sets.union(localRangesFromOld.keySet(), localRangesFromNew.keySet()))
        {
            Set<TokenRange> rangesFromOld = localRangesFromOld.get(instanceId);
            Set<TokenRange> rangesFromNew = localRangesFromNew.get(instanceId);
            Preconditions.checkState(rangesFromNew != null || rangesFromOld != null,
                                     "Token ranges of instance: " + instanceId + " do not exist in both old and new");
            if (rangesFromOld == null) // new node
            {
                gainedRanges.put(instanceId, rangesFromNew);
            }
            else if (rangesFromNew == null) // removed node
            {
                lostRanges.put(instanceId, rangesFromOld);
            }
            else // both new and old ranges exist and they differs
            {
                TokenRange.SymmetricDiffResult symmetricDiffResult = TokenRange.symmetricDiff(rangesFromOld, rangesFromNew);
                // ranges that are no longer in the new topology are lost
                lostRanges.put(instanceId, symmetricDiffResult.onlyInLeft);
                // ranges that are new in the new topology are gained
                gainedRanges.put(instanceId, symmetricDiffResult.onlyInRight);
            }
        }

        Set<UUID> jobIds = ringTopologyRefresher.allRestoreJobsOfKeyspace(keyspace);
        for (UUID jobId : jobIds)
        {
            RestoreJob restoreJob = restoreJobDatabaseAccessor.find(jobId);
            if (restoreJob == null)
            {
                continue;
            }

            try
            {
                // First, discard all the restore ranges that cover the lost ranges
                lostRanges.forEach((instanceId, ranges) -> discardLostRanges(restoreJob, instanceId, ranges));
                // Next, submit the RestoreRanges from the newly gained ranges
                gainedRanges.forEach((instanceId, ranges) -> findSlicesOfCassandraNodeAndSubmit(restoreJob, instanceId, ranges));
            }
            catch (Exception e)
            {
                // log the warning and continue to process other jobs
                LOGGER.warn("Unexpected exception when adjusting restore job ranges. jobId={}", jobId, e);
            }
        }
    }

    private void processOneJob(RestoreJob job, RestoreJobManagerGroup restoreJobManagers, RunContext context)
    {
        if (jobIdsByDay.shouldLogJob(job))
        {
            LOGGER.info("Found job. jobId={} job={}", job.jobId, job);
        }

        switch (job.status)
        {
            case STAGED:
                // unset the flag, so that it can re-discover the slices when the job status changes to a ready status
                jobIdsByDay.unsetSlicesDiscovered(job); // no break by design
            case CREATED:
            case STAGE_READY:
            case IMPORT_READY:
                if (job.hasExpired(context.nowMillis))
                {
                    abortExpiredJob(job, restoreJobManagers, context);
                    break; // do not proceed further if the job has expired
                }
                // find the oldest non-completed job
                context.earliestInDays = Math.max(context.earliestInDays, delta(context.today, job.createdAt));
                restoreJobManagers.updateRestoreJob(job);
                processSidecarManagedJobMaybe(job);
                inflightJobsCount += 1;
                break;
            case FAILED:
            case ABORTED:
            case SUCCEEDED:
                finalizeJob(restoreJobManagers, job);
                break;
            default:
                LOGGER.warn("Encountered unknown job status. jobId={} status={}", job.jobId, job.status);
        }
    }

    private void abortExpiredJob(RestoreJob job, RestoreJobManagerGroup restoreJobManagers, RunContext context)
    {
        context.expiredJobs += 1;
        boolean aborted = abortJob(job);
        if (aborted)
        {
            // finalize the job once aborted; otherwise retry in the next periodic task run
            context.abortedJobs += 1;
            finalizeJob(restoreJobManagers, job);
        }
    }

    private void processSidecarManagedJobMaybe(RestoreJob job)
    {
        if (!job.isManagedBySidecar())
        {
            return;
        }

        // Only force refresh topology for the first time in each stage
        // RestoreJobDiscoverer is registered as a RingTopologyListener to receive future topology changed notifications, if any
        ringTopologyRefresher.register(job, this);
        if (shouldFindSlicesAndSubmit(job))
        {
            findSlicesAndSubmit(job);
            // Mark the flag. It prevents finding slices (which is expensive) until the flag is unset.
            jobIdsByDay.markSlicesDiscovered(job);
        }
    }

    private boolean shouldFindSlicesAndSubmit(RestoreJob job)
    {
        return (job.status == RestoreJobStatus.STAGE_READY || job.status == RestoreJobStatus.IMPORT_READY)
               && !jobIdsByDay.isSliceDiscovered(job);
    }

    private void finalizeJob(RestoreJobManagerGroup restoreJobManagers, RestoreJob job)
    {
        restoreJobManagers.removeJobInternal(job);
        if (job.isManagedBySidecar())
        {
            ringTopologyRefresher.unregister(job, this);
        }
    }

    // find all slices of the job that should be downloaded to the local instances,
    // according to the cluster token ownership
    private void findSlicesAndSubmit(RestoreJob restoreJob)
    {
        localTokenRangesProvider.localTokenRanges(restoreJob.keyspaceName, true)
                                .forEach((instanceId, ranges) -> findSlicesOfCassandraNodeAndSubmit(restoreJob, instanceId, ranges));
    }

    // find all slices according to the Cassandra node denoted by the instanceId and the token ranges
    private void findSlicesOfCassandraNodeAndSubmit(RestoreJob restoreJob, int instanceId, Set<TokenRange> ranges)
    {
        InstanceMetadata instance = instanceMetadataFetcher.instance(instanceId);
        ranges.forEach(range -> findSlicesOfRangeAndSubmit(instance, restoreJob, range));
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
            // Check if the slice needs to be trimmed/split
            RestoreSlice trimmed = slice.trimMaybe(range);
            String uploadId = RestoreJobUtil.generateUniqueUploadId(trimmed.jobId(), trimmed.sliceId());
            RestoreRange restoreRange = RestoreRange.builderFromSlice(trimmed)
                                                    // set the owner instance, which is not read from database
                                                    .ownerInstance(instance)
                                                    .stageDirectory(Paths.get(instance.stagingDir()), uploadId)
                                                    .build();
            RestoreJobProgressTracker.Status status = submit(instance, restoreJob, restoreRange);
            if (status == RestoreJobProgressTracker.Status.CREATED)
            {
                restoreRangeDatabaseAccessor.create(restoreRange);
            }
        });
    }

    private void discardLostRanges(RestoreJob restoreJob, int instanceId, Set<TokenRange> otherRanges)
    {
        InstanceMetadata instance = instanceMetadataFetcher.instance(instanceId);
        RestoreJobManagerGroup managerGroup = restoreJobManagerGroupSingleton.get();
        Set<RestoreRange> overlappingRanges = managerGroup.discardOverlappingRanges(instance, restoreJob, otherRanges);
        calculateRemainingRangesAndResubmit(restoreJob, instanceId, otherRanges, overlappingRanges);
    }

    // There could be still ranges remaining after subtracting the overlapping parts.
    // The method calculates the ranges that still remain to the Cassandra node (identified by instanceId), and
    // re-submit the RestoreRange for the remaining ranges.
    private void calculateRemainingRangesAndResubmit(RestoreJob restoreJob,
                                                     int instanceId,
                                                     Set<TokenRange> otherRanges,
                                                     Set<RestoreRange> overlappingRanges)
    {
        Set<TokenRange> existingRanges = overlappingRanges.stream()
                                                          .map(RestoreRange::tokenRange)
                                                          .collect(Collectors.toSet());
        TokenRange.SymmetricDiffResult symmetricDiffResult = TokenRange.symmetricDiff(existingRanges, otherRanges);
        Set<TokenRange> remainedRanges = symmetricDiffResult.onlyInLeft;
        findSlicesOfCassandraNodeAndSubmit(restoreJob, instanceId, remainedRanges);
    }

    private RestoreJobProgressTracker.Status submit(InstanceMetadata instance, RestoreJob job, RestoreRange range)
    {
        RestoreJobManagerGroup managerGroup = restoreJobManagerGroupSingleton.get();

        try
        {
            return managerGroup.trySubmit(instance, range, job);
        }
        catch (RestoreJobFatalException e)
        {
            LOGGER.error("Restore range failed. startToken={} endToken={} instance={}",
                         range.startToken(), range.endToken(), range.owner().host(), e);
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
        // tracks the jobIds that have their slices already discovered
        private final Map<Integer, Set<UUID>> sliceDiscoveredJobsByDay = new HashMap<>();
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
            int day = populateDiscoveredDay(job);
            Map<UUID, RestoreJobStatus> jobs = jobsByDay.computeIfAbsent(day, key -> new HashMap<>());
            RestoreJobStatus oldStatus = jobs.put(job.jobId, job.status);
            return oldStatus == null || job.status == RestoreJobStatus.CREATED || oldStatus != job.status;
        }

        void markSlicesDiscovered(RestoreJob job)
        {
            int day = populateDiscoveredDay(job);
            sliceDiscoveredJobsByDay.compute(day, (key, value) -> {
                if (value == null)
                {
                    value = new HashSet<>();
                }
                value.add(job.jobId);
                return value;
            });
        }

        boolean isSliceDiscovered(RestoreJob job)
        {
            int day = populateDiscoveredDay(job);
            return sliceDiscoveredJobsByDay.getOrDefault(day, Collections.emptySet())
                                           .contains(job.jobId);
        }

        void unsetSlicesDiscovered(RestoreJob job)
        {
            int day = populateDiscoveredDay(job);
            sliceDiscoveredJobsByDay.compute(day, (key, value) -> {
               if (value == null)
               {
                   return null;
               }

               value.remove(job.jobId);
               return value;
            });
        }

        void cleanupMaybe()
        {
            // remove all the jobIds of the days that are not discovered
            jobsByDay.keySet().removeIf(day -> !discoveredDays.contains(day));
            discoveredDays.clear();
        }

        private int populateDiscoveredDay(RestoreJob job)
        {
            int day = job.createdAt.getDaysSinceEpoch();
            discoveredDays.add(day);
            return day;
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

    static class RunContext
    {
        long nowMillis = System.currentTimeMillis();
        LocalDate today = LocalDate.fromMillisSinceEpoch(nowMillis);
        int earliestInDays = 0;
        int abortedJobs = 0;
        int expiredJobs = 0;
    }
}
