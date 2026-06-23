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
import java.util.concurrent.locks.ReentrantLock;
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
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.locator.LocalTokenRangesProvider;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobDatabaseAccessor;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreRangeDatabaseAccessor;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.db.RestoreSliceDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.server.RestoreMetrics;
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
    /**
     * Single mutual-exclusion gate for restore-job discovery. The slow loop ({@link #tryExecuteDiscovery})
     * acquires it via {@link ReentrantLock#lock()} — it blocks briefly if a fast-loop tick or
     * {@link #processJobNow} is in progress, but it never skips. The fast {@link StatusCheckTask}
     * and {@link #processJobNow} acquire it via {@link ReentrantLock#tryLock()} — they skip if the
     * slow loop is in. This encodes the priority asymmetry (slow = correctness, fast = best-effort)
     * directly at each call site and keeps {@link JobIdsByDay} accessed by exactly one thread at a
     * time without any internal locking.
     */
    private final ReentrantLock executionLock = new ReentrantLock();
    private final StatusCheckTask statusCheckTask = new StatusCheckTask();
    // Volatile because it is read by the periodic-task scheduler thread (via scheduleDecision /
    // hasInflightJobs / delay) without holding executionLock. Written under the lock at the end of
    // every slow- and fast-loop pass to the freshly recounted in-flight job count. Reads outside
    // the lock can be slightly stale (one pass behind) but never torn or drifted.
    private volatile int inflightJobsCount = 0;
    // Volatile because initLocalDatacenterMaybe writes it without holding the lock (the JMX/CQL
    // call may block, so we keep it off the critical section). Idempotent — concurrent writers
    // would assign the same value.
    private volatile String localDatacenter = null;
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
             null,
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
                         // Only set when testing; Do not add PeriodicTaskExecutor to constructor, as it creates circular dependency
                         // this.periodicTaskExecutor is set when deploying the task to PeriodicTaskExecutor
                         PeriodicTaskExecutor executor,
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
        this.periodicTaskExecutor = executor;
        this.jobIdsByDay = new JobIdsByDay();
    }

    @Override
    public void deploy(Vertx vertx, PeriodicTaskExecutor executor)
    {
        this.periodicTaskExecutor = executor;
        PeriodicTask.super.deploy(vertx, executor);
        // Co-deploy the fast status-check loop. It reads only job.status for already-known
        // in-flight jobs and reacts to phase transitions without waiting for the slow loop.
        executor.schedule(statusCheckTask);
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
        initLocalDatacenterMaybe();
        tryExecuteDiscovery();
        promise.tryComplete();
    }

    /**
     * Run the slow discovery pass synchronously, blocking until {@link #executionLock} is free.
     * Unlike a fast-loop tick or a {@link #processJobNow} wake-up, the slow pass never skips —
     * it is the correctness guarantee for restore-job discovery.
     */
    public void tryExecuteDiscovery()
    {
        executionLock.lock();
        try
        {
            executeInternal();
        }
        finally
        {
            executionLock.unlock();
        }
    }

    private boolean shouldSkip()
    {
        if (!sidecarSchema.isInitialized())
        {
            LOGGER.trace("Skipping restore job discovering due to sidecarSchema not initialized");
            return true;
        }
        return false;
    }

    private void executeInternal()
    {
        Preconditions.checkState(periodicTaskExecutor != null, "Loop executor is not registered");

        LOGGER.debug("Discovering restore jobs. " +
                     "inflightJobsCount={} jobDiscoveryRecencyDays={}",
                     inflightJobsCount, jobDiscoveryRecencyDays);

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
        // Recount and publish under the lock so unsynchronized readers (scheduleDecision /
        // hasInflightJobs) observe a consistent post-pass value.
        inflightJobsCount = jobIdsByDay.inflightJobIds().size();
        LOGGER.info("Exit job discovery. " +
                    "inflightJobsCount={} " +
                    "delay={} " +
                    "jobDiscoveryRecencyDays={} " +
                    "expiredJobs={} " +
                    "abortedJobs={}",
                    inflightJobsCount, delay(), jobDiscoveryRecencyDays, context.expiredJobs, context.abortedJobs);
        metrics.activeJobs.metric.setValue(inflightJobsCount);
    }

    /**
     * Snapshot of restore job IDs currently considered in-flight (non-terminal). Acquires
     * {@link #executionLock} so external callers (tests, the fast loop) can use it without
     * holding the gate themselves; reentrant for callers already inside the lock.
     */
    Set<UUID> inflightJobIds()
    {
        executionLock.lock();
        try
        {
            return jobIdsByDay.inflightJobIds();
        }
        finally
        {
            executionLock.unlock();
        }
    }

    /**
     * Applies the same transition handling as a full discovery pass, but only for a single freshly-read job.
     * Called by the fast status-check loop when it detects a status change on a known in-flight job.
     * A no-op when the job's status matches the last-known status cached during discovery.
     */
    void handleStatusTransition(RestoreJob currentJob)
    {
        executionLock.lock();
        try
        {
            int day = currentJob.createdAt.getDaysSinceEpoch();
            RestoreJobStatus previousStatus = jobIdsByDay.getKnownStatus(currentJob.jobId, day);
            if (previousStatus == null)
            {
                // Job isn't tracked yet; let the next full discovery pass pick it up so that
                // the in-flight set and derived state stay consistent.
                return;
            }
            if (previousStatus == currentJob.status)
            {
                return;
            }

            RunContext context = new RunContext();
            RestoreJobManagerGroup managers = restoreJobManagerGroupSingleton.get();
            try
            {
                processOneJob(currentJob, managers, context);
            }
            catch (Exception e)
            {
                LOGGER.warn("Exception on processing status transition. jobId={} previousStatus={} newStatus={}",
                            currentJob.jobId, previousStatus, currentJob.status, e);
            }
            // Recount and publish under the lock so the volatile counter and the gauge reflect
            // the fast-loop transition immediately, without waiting for the next slow-loop pass.
            inflightJobsCount = jobIdsByDay.inflightJobIds().size();
            metrics.activeJobs.metric.setValue(inflightJobsCount);
        }
        finally
        {
            executionLock.unlock();
        }
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

        // stop proceeding further if the local datacenter is excluded from the restore job
        if (isLocalDatacenterExcluded(job))
        {
            LOGGER.info("Restore job is configured to skip running on the local datacenter. " +
                        "jobId={} localDatacenter={} targetDatacenter={}",
                        job.jobId, localDatacenter, job.localDatacenter);
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

    private void initLocalDatacenterMaybe()
    {
        if (localDatacenter != null)
        {
            return;
        }

        try
        {
            NodeSettings nodeSettings = instanceMetadataFetcher.callOnFirstAvailableInstance(i -> i.delegate().nodeSettings());
            localDatacenter = nodeSettings.datacenter();
        }
        catch (CassandraUnavailableException cue)
        {
            LOGGER.debug("localDatacenter is not initialized", cue);
        }
    }

    private boolean isLocalDatacenterExcluded(RestoreJob job)
    {
        if (!job.shouldRestoreToLocalDatacenterOnly)
        {
            return false;
        }

        if (localDatacenter == null)
        {
            LOGGER.debug("The restore job should restore only to the local datacenter, but the local datacenter is undetermined yet; skip this run");
            return true;
        }

        // when job should restore to local datacenter only, but the target datacenter is not the local one
        return !Objects.equals(localDatacenter, job.localDatacenter);
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
            LOGGER.error("The restore job has already failed. jobId={} startToken={} endToken={} instance={}",
                         job.jobId, range.startToken(), range.endToken(), range.owner().host(), e);
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

    /**
     * Per-day cache of the latest known {@link RestoreJobStatus} for each job, plus a per-day
     * set of jobs whose slices have already been discovered.
     *
     * <p>Intentionally <b>not</b> internally synchronized. All callers must hold
     * {@link RestoreJobDiscoverer#executionLock}; that single outer gate is the sole source of
     * thread safety for this state. The asymmetric {@code lock()}/{@code tryLock()} pattern at
     * the call sites guarantees exactly one thread is inside this class at a time.
     */
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

        RestoreJobStatus getKnownStatus(UUID jobId, int day)
        {
            Map<UUID, RestoreJobStatus> jobs = jobsByDay.get(day);
            return jobs == null ? null : jobs.get(jobId);
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

        /**
         * Snapshot of jobIds whose latest known status is non-terminal. Caller must hold
         * {@link RestoreJobDiscoverer#executionLock}.
         */
        Set<UUID> inflightJobIds()
        {
            Set<UUID> result = new HashSet<>();
            for (Map<UUID, RestoreJobStatus> jobs : jobsByDay.values())
            {
                for (Map.Entry<UUID, RestoreJobStatus> entry : jobs.entrySet())
                {
                    if (!entry.getValue().isFinal())
                    {
                        result.add(entry.getKey());
                    }
                }
            }
            return result;
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

    /**
     * Immediately processes a restore job without waiting for the next discovery loop iteration.
     * Called by UpdateRestoreJobHandler after a phase signal (STAGE_READY) is written to DB.
     * This is safe to call concurrently with the discovery loop — the DB write is the durable
     * source of truth, and duplicate processing is deduplicated by existing idempotency checks.
     *
     * <p>Acquires {@link #executionLock} via {@link ReentrantLock#tryLock()}: if the slow loop or
     * the fast loop is already in, the wake-up is skipped — the slow loop reads the DB freshly
     * on its next pass and will pick up the same transition, so dropping the wake-up is benign.
     *
     * @param restoreJob the restore job to process immediately
     */
    public void processJobNow(RestoreJob restoreJob)
    {
        initLocalDatacenterMaybe();
        if (!executionLock.tryLock())
        {
            LOGGER.debug("Discovery is already running. Skipping wake-up. jobId={}", restoreJob.jobId);
            return;
        }
        try
        {
            RestoreJobManagerGroup restoreJobManagers = restoreJobManagerGroupSingleton.get();
            restoreJobManagers.updateRestoreJob(restoreJob);
            processSidecarManagedJobMaybe(restoreJob);
        }
        finally
        {
            executionLock.unlock();
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

    @VisibleForTesting
    StatusCheckTask statusCheckTask()
    {
        return statusCheckTask;
    }

    static class RunContext
    {
        long nowMillis = System.currentTimeMillis();
        LocalDate today = LocalDate.fromMillisSinceEpoch(nowMillis);
        int earliestInDays = 0;
        int abortedJobs = 0;
        int expiredJobs = 0;
    }

    /**
     * Fast status-check loop that complements the slow {@link RestoreJobDiscoverer} discovery loop.
     *
     * <p>Runs on {@link RestoreJobConfiguration#jobDiscoveryStatusCheckInterval()} (default ~1s) and
     * performs cheap point-reads of {@code job.status} for the in-flight jobs the discoverer already knows
     * about. When a status transition is detected the task delegates to
     * {@link RestoreJobDiscoverer#handleStatusTransition(RestoreJob)} so peer Sidecar instances react to
     * phase signals without waiting for the slow full-scan loop. The slow loop remains the correctness
     * and recovery guarantee (new-job discovery, expired-job aborts, missed signals).
     *
     * <p>Implemented as a non-static inner class so it has direct access to the discoverer's state and
     * shares its lifecycle — it isn't an independent component.
     */
    class StatusCheckTask implements PeriodicTask
    {
        @Override
        public DurationSpec delay()
        {
            return restoreJobConfig.jobDiscoveryStatusCheckInterval();
        }

        @Override
        public ScheduleDecision scheduleDecision()
        {
            if (!sidecarSchema.isInitialized())
            {
                return ScheduleDecision.SKIP;
            }
            if (inflightJobsCount == 0)
            {
                return ScheduleDecision.SKIP;
            }
            return ScheduleDecision.EXECUTE;
        }

        @Override
        public void execute(Promise<Void> promise)
        {
            // Resolve local DC once per tick before acquiring the lock — the JMX/CQL call may
            // block, and resolving it inside handleStatusTransition would re-attempt the call
            // for every in-flight job whenever Cassandra is unavailable.
            initLocalDatacenterMaybe();
            // tryLock — skip this tick if the slow loop or processJobNow is in. Best-effort by
            // design; the next tick (or the slow loop's next pass) will pick up any transition we
            // miss.
            if (!executionLock.tryLock())
            {
                promise.tryComplete();
                return;
            }

            try
            {
                for (UUID jobId : jobIdsByDay.inflightJobIds())
                {
                    try
                    {
                        RestoreJob current = restoreJobDatabaseAccessor.find(jobId);
                        if (current == null)
                        {
                            continue;
                        }
                        handleStatusTransition(current);
                    }
                    catch (Exception e)
                    {
                        // Do not fail the whole pass on one job; the slow loop will retry it.
                        LOGGER.warn("Exception on status check for jobId={}", jobId, e);
                    }
                }
            }
            finally
            {
                executionLock.unlock();
                promise.tryComplete();
            }
        }
    }
}
