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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Tracks and stores the results of long-running jobs running on the sidecar
 */
@Singleton
public class OperationalJobTracker
{
    public static final long ONE_DAY_TTL = TimeUnit.DAYS.toMillis(1); // todo: consider making it configurable

    private static final Logger LOGGER = LoggerFactory.getLogger(OperationalJobTracker.class);
    private final Map<UUID, OperationalJob> map;

    @Inject
    public OperationalJobTracker(ServiceConfiguration serviceConfiguration)
    {
        this(serviceConfiguration.operationalJobTrackerSize());
    }

    public OperationalJobTracker(int initialCapacity)
    {
        map = Collections.synchronizedMap(new LinkedHashMap<UUID, OperationalJob>(initialCapacity)
        {
            /**
             * {@inheritDoc}
             */
            @Override
            protected boolean removeEldestEntry(Map.Entry<UUID, OperationalJob> eldest)
            {
                // We have reached capacity and the oldest entry is either ready for cleanup or stale
                if (map.size() > initialCapacity)
                {
                    OperationalJob job = eldest.getValue();
                    OperationalJobStatus status = job.status();
                    if (status.isCompleted() && job.isStale(System.currentTimeMillis(), ONE_DAY_TTL))
                    {
                        LOGGER.debug("Expiring completed and stale job due to job tracker has reached max size. jobId={} status={} createdAt={}",
                                     job.jobId(), status, job.creationTime());
                        return true;
                    }
                    else
                    {
                        LOGGER.warn("Job tracker reached max size, but the eldest job is not completed yet. " +
                                    "Not evicting. jobId={} status={}", job.jobId(), status);
                        // TODO: Optionally trigger cleanup to fetch next oldest to evict
                    }
                }

                return false;
            }
        });
    }

    public OperationalJob computeIfAbsent(UUID key, Function<UUID, OperationalJob> mappingFunction)
    {
        return map.computeIfAbsent(key, mappingFunction);
    }

    public OperationalJob get(UUID key)
    {
        return map.get(key);
    }

    /**
     * Returns an immutable copy of the underlying map, to provide a consistent view of the map, minimizing contention
     *
     * @return an immutable copy of the underlying mapping
     */
    @NotNull
    Map<UUID, OperationalJob> jobsView()
    {
        return Collections.unmodifiableMap(map);
    }

    /**
     * Filters the inflight (created or running) jobs matching the job name from the jobsView
     * @return list of inflight jobs being tracked
     */
    @NotNull
    List<OperationalJob> inflightJobsByOperation(String operation)
    {
        return jobsView().values()
                         .stream()
                         .filter(j -> (j.name().equals(operation)) &&
                                      (j.status() == OperationalJobStatus.RUNNING ||
                                       j.status() == OperationalJobStatus.CREATED))
                         .collect(Collectors.toList());
    }

    @VisibleForTesting
    OperationalJob put(OperationalJob job)
    {
        return map.put(job.jobId(), job);
    }

    @VisibleForTesting
    int size()
    {
        return map.size();
    }
}
