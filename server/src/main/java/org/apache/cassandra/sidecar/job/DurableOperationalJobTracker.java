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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;
import org.apache.cassandra.sidecar.job.storage.StorageProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A durable implementation of {@link OperationalJobTracker} that persists job state
 * via a {@link StorageProvider}. A local {@link ConcurrentHashMap} caches live
 * {@link OperationalJob} references for the current process, since executing jobs
 * with Vert.x promises cannot be reconstituted from storage. Once a job completes,
 * it is removed from the local map, and subsequent lookups are served from storage.
 */
@Singleton
public class DurableOperationalJobTracker implements OperationalJobTracker
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DurableOperationalJobTracker.class);
    private static final int MAX_STATUS_UPDATE_ATTEMPTS = 2;
    private static final long RETRY_DELAY_MS = 100;

    private final ConcurrentHashMap<UUID, OperationalJob> liveJobs;
    private final StorageProvider storageProvider;

    @Inject
    public DurableOperationalJobTracker(ServiceConfiguration serviceConfiguration,
                                        StorageProvider storageProvider)
    {
        this.liveJobs = new ConcurrentHashMap<>(serviceConfiguration.operationalJobTrackerSize());
        this.storageProvider = storageProvider;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationalJob computeIfAbsent(UUID jobId, Function<UUID, OperationalJob> mappingFunction)
    {
        return liveJobs.computeIfAbsent(jobId, id -> {
            OperationalJob job = mappingFunction.apply(id);

            storageProvider.persistJob(OperationalJobRecord.fromOperationalJob(job));

            job.asyncResult().onComplete(ar -> {
                liveJobs.remove(job.jobId());
                updateTerminalStatus(job);
            });

            return job;
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

        return storageProvider.findJob(jobId);
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
     * Attempts to update the terminal status in storage with retry.
     * If all attempts fail, logs a warning and continues.
     */
    private void updateTerminalStatus(OperationalJob job)
    {
        OperationalJobStatus terminalStatus = job.status();
        String failureReason = job.failureReason();
        for (int attempt = 1; attempt <= MAX_STATUS_UPDATE_ATTEMPTS; attempt++)
        {
            try
            {
                storageProvider.updateJobStatus(job.jobId(), job.operationType(), terminalStatus, failureReason);
                return;
            }
            catch (RuntimeException e)
            {
                LOGGER.warn("Failed to update terminal status for job {} (attempt {}/{}). error={}",
                            job.jobId(), attempt, MAX_STATUS_UPDATE_ATTEMPTS, e.getMessage());
                if (attempt < MAX_STATUS_UPDATE_ATTEMPTS)
                {
                    try
                    {
                        Thread.sleep(RETRY_DELAY_MS * attempt);
                    }
                    catch (InterruptedException ie)
                    {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }
}
