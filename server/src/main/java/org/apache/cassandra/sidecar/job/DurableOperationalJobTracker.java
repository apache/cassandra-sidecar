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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;
import org.apache.cassandra.sidecar.job.storage.StorageProvider;
import org.apache.cassandra.sidecar.utils.InvocationTrackingFunction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A durable implementation of {@link OperationalJobTracker} that persists job state
 * via a {@link StorageProvider}. A local {@link ConcurrentHashMap} caches live
 * {@link OperationalJob} references for the current process, since executing jobs
 * with Vert.x promises cannot be reconstituted from storage. Once a job completes,
 * it is removed from the local map, and subsequent lookups are served from storage.
 *
 * <p>Storage currently receives only two writes per job: the initial
 * {@link OperationalJobStatus#CREATED CREATED} record on submission and the terminal status on
 * completion. As a result, if the sidecar restarts mid-operation, or if the
 * terminal-status update exhausts its retries (see {@link #updateTerminalStatus}), the persisted
 * record can remain stuck at {@code CREATED} even though the operation has since progressed or
 * finished. There is currently no marker distinguishing a record that is genuinely still
 * {@code CREATED} from one whose true state was simply never recorded; adding such a marker together
 * with a reconciliation sweep (leveraging {@link StorageProvider#findAllJobs(int)}) is tracked as
 * follow-up work in
 * <a href="https://issues.apache.org/jira/browse/CASSSIDECAR-482">CASSSIDECAR-482</a>.
 */
@Singleton
public class DurableOperationalJobTracker implements OperationalJobTracker
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DurableOperationalJobTracker.class);
    private static final int MAX_STORAGE_WRITE_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 100;
    private static final long RETRY_JITTER_MS = 100;

    private final ConcurrentHashMap<UUID, OperationalJob> liveJobs;
    private final StorageProvider storageProvider;
    private final TaskExecutorPool executor;

    @Inject
    public DurableOperationalJobTracker(ServiceConfiguration serviceConfiguration,
                                        StorageProvider storageProvider,
                                        TaskExecutorPool executor)
    {
        this.liveJobs = new ConcurrentHashMap<>(serviceConfiguration.operationalJobTrackerSize());
        this.storageProvider = storageProvider;
        this.executor = executor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationalJob computeIfAbsent(UUID jobId, Function<UUID, OperationalJob> mappingFunction)
    {
        if (!storageProvider.isAvailable())
        {
            throw new IllegalStateException("Storage provider is not available");
        }

        InvocationTrackingFunction<UUID, OperationalJob> mappingFunctionTracker =
        new InvocationTrackingFunction<>(mappingFunction);
        OperationalJob job = liveJobs.computeIfAbsent(jobId, mappingFunctionTracker);

        if (mappingFunctionTracker.wasInvoked())
        {
            persistInitialRecord(job, 1);
        }

        return job;
    }

    private void persistInitialRecord(OperationalJob job, int attempt)
    {
        executor.runBlocking(() -> storageProvider.persistJob(OperationalJobRecord.fromOperationalJob(job)))
                .onSuccess(v -> job.asyncResult().onComplete(ar -> {
                    updateTerminalStatus(job);
                    liveJobs.remove(job.jobId());
                }))
                .onFailure(e -> {
                    LOGGER.warn("Failed to persist job {} to storage (attempt {}/{}). error={}",
                                job.jobId(), attempt, MAX_STORAGE_WRITE_ATTEMPTS, e.getMessage());
                    if (attempt < MAX_STORAGE_WRITE_ATTEMPTS)
                    {
                        // Add jitter so that concurrent sidecar processes retrying against the same transient
                        // Cassandra blip do not all retry in lockstep
                        long delay = RETRY_DELAY_MS * attempt + ThreadLocalRandom.current().nextLong(RETRY_JITTER_MS);
                        executor.setTimer(delay, id -> persistInitialRecord(job, attempt + 1));
                    }
                    else
                    {
                        // Persist exhausted its retries, but the job is already executing on a separate executor.
                        // Keep it in liveJobs so in-process status queries and conflict detection still see it.
                        LOGGER.error("Failed to persist job {} to storage after {} attempts. "
                                     + "Job will be tracked in-memory only.", job.jobId(), MAX_STORAGE_WRITE_ATTEMPTS, e);
                        job.asyncResult().onComplete(ar -> liveJobs.remove(job.jobId()));
                    }
                });
    }

    @Nullable
    @Override
    public OperationalJobInfo get(UUID jobId)
    {
        OperationalJob liveJob = liveJobs.get(jobId);
        if (liveJob != null)
        {
            return liveJob;
        }

        OperationalJobRecord record = storageProvider.findJob(jobId);
        if (record == null)
        {
            return null;
        }
        return enrichWithNodeStatuses(record);
    }

    @NotNull
    @Override
    public Map<UUID, OperationalJob> jobsView()
    {
        return Collections.unmodifiableMap(liveJobs);
    }

    @NotNull
    @Override
    public List<OperationalJob> inflightJobsByOperation(String operation)
    {
        return liveJobs.values()
                       .stream()
                       .filter(j -> j.name().equals(operation) &&
                                    (j.status() == OperationalJobStatus.RUNNING ||
                                     j.status() == OperationalJobStatus.CREATED))
                       .collect(Collectors.toList());
    }

    /**
     * Enriches an {@link OperationalJobRecord} with per-node status data from storage.
     * If the record already has non-empty node lists (e.g. populated by a storage provider
     * that joins the data in a single query), the record is returned as is.
     */
    private OperationalJobRecord enrichWithNodeStatuses(OperationalJobRecord record)
    {
        if (!record.nodesPending().isEmpty()
            || !record.nodesExecuting().isEmpty()
            || !record.nodesSucceeded().isEmpty()
            || !record.nodesFailed().isEmpty())
        {
            return record;
        }

        Map<UUID, OperationalJobStatus> nodeStatuses =
        storageProvider.getNodeStatusesForOperation(record.jobId());
        if (nodeStatuses.isEmpty())
        {
            return record;
        }

        List<UUID> pending = new ArrayList<>();
        List<UUID> executing = new ArrayList<>();
        List<UUID> succeeded = new ArrayList<>();
        List<UUID> failed = new ArrayList<>();

        for (Map.Entry<UUID, OperationalJobStatus> entry : nodeStatuses.entrySet())
        {
            switch (entry.getValue())
            {
                case CREATED:
                    pending.add(entry.getKey());
                    break;
                case RUNNING:
                    executing.add(entry.getKey());
                    break;
                case SUCCEEDED:
                    succeeded.add(entry.getKey());
                    break;
                case FAILED:
                    failed.add(entry.getKey());
                    break;
                default:
                    throw new IllegalStateException("Invalid state = " + entry.getValue());
            }
        }

        return OperationalJobRecord.builder()
                                   .jobId(record.jobId())
                                   .operationType(record.operationType())
                                   .status(record.status())
                                   .startTime(record.startTime())
                                   .lastUpdate(record.lastUpdate())
                                   .failureReason(record.failureReason())
                                   .nodeExecutionOrder(record.nodeExecutionOrder())
                                   .operationMetadata(record.operationMetadata())
                                   .nodesPending(Collections.unmodifiableList(pending))
                                   .nodesExecuting(Collections.unmodifiableList(executing))
                                   .nodesSucceeded(Collections.unmodifiableList(succeeded))
                                   .nodesFailed(Collections.unmodifiableList(failed))
                                   .build();
    }

    /**
     * Attempts to update the terminal status in storage with retry.
     * If all attempts fail, logs a warning and continues.
     */
    private void updateTerminalStatus(OperationalJob job)
    {
        updateTerminalStatus(job, 1);
    }

    private void updateTerminalStatus(OperationalJob job, int attempt)
    {
        try
        {
            storageProvider.updateJobStatus(job.jobId(), job.operationType(), job.status(), job.failureReason());
        }
        catch (RuntimeException e)
        {
            LOGGER.warn("Failed to update terminal status for job {} (attempt {}/{}). error={}",
                        job.jobId(), attempt, MAX_STORAGE_WRITE_ATTEMPTS, e.getMessage());
            if (attempt < MAX_STORAGE_WRITE_ATTEMPTS)
            {
                // Add jitter so that concurrent sidecar processes retrying against the same transient
                // Cassandra blip do not all retry in lockstep
                long delay = RETRY_DELAY_MS * attempt + ThreadLocalRandom.current().nextLong(RETRY_JITTER_MS);
                executor.setTimer(delay, id -> updateTerminalStatus(job, attempt + 1));
            }
            else
            {
                LOGGER.error("Exhausted retries when updating terminal status for job {}. " +
                             "Manual intervention may be required to correct job metadata.", job.jobId(), e);
            }
        }
    }
}
