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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jetbrains.annotations.NotNull;

/**
 * Tracks and stores the results of long-running jobs running on the sidecar
 */
public class OperationalJobTracker
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OperationalJobTracker.class);
    private final Map<UUID, OperationalJob> map;

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
                    if (eldest.getValue().status.isComplete() && System.nanoTime() - eldest.getValue().creationTime() > TimeUnit.DAYS.toNanos(1))
                    {
                        LOGGER.warn("Job tracker reached max size. Expiring job wth uuid={}, state={}, created={}",
                                    eldest.getKey(), eldest.getValue().status());
                        return true;
                    }
                    else
                    {
                        LOGGER.warn("Job tracker reached max size. Not evicting oldest job uuid={} status={}", eldest.getKey(), eldest.getValue().status());
                        // TODO: Optionally trigger cleanup to fetch next oldest to evict
                    }
                }

                return false;
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    public OperationalJob computeIfAbsent(UUID key, Function<UUID, OperationalJob> mappingFunction)
    {
        return map.computeIfAbsent(key, mappingFunction);
    }

    /**
     * {@inheritDoc}
     */
    public OperationalJob put(UUID key, OperationalJob job)
    {
        return map.put(key, job);
    }

    /**
     * {@inheritDoc}
     */
    public int size()
    {
        return map.size();
    }


    /**
     * {@inheritDoc}
     */
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
    Map<UUID, OperationalJob> getJobsView()
    {
        return Collections.unmodifiableMap(map);
    }
}
