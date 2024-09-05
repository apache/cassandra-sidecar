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

package org.apache.cassandra.sidecar.metrics;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

import static org.apache.cassandra.sidecar.metrics.SidecarMetrics.APP_PREFIX;

/**
 * A {@link StatsCounter} implementation that accumulates statistics during the operation
 * of a {@link Cache}
 */
public class CacheStatsCounter implements StatsCounter
{
    public static final String CACHE_PREFIX = APP_PREFIX + ".Cache";
    protected final MetricRegistry metricRegistry;

    protected final NamedMetric<DeltaGauge> hits;
    protected final NamedMetric<DeltaGauge> misses;
    protected final NamedMetric<Timer> loadSuccess;
    protected final NamedMetric<Timer> loadFailure;
    protected final NamedMetric<DeltaGauge> evictions;
    protected final LongAdder totalLoadTimeNanos = new LongAdder();

    public CacheStatsCounter(MetricRegistry metricRegistry, String cacheName)
    {
        this.metricRegistry = Objects.requireNonNull(metricRegistry, "Metric registry can not be null");

        String domain = CACHE_PREFIX + "." + cacheName;
        hits = NamedMetric.builder(name -> metricRegistry.gauge(name, DeltaGauge::new))
                          .withDomain(domain)
                          .withName("Hits")
                          .build();
        misses = NamedMetric.builder(name -> metricRegistry.gauge(name, DeltaGauge::new))
                            .withDomain(domain)
                            .withName("Misses")
                            .build();
        loadSuccess = NamedMetric.builder(metricRegistry::timer)
                                 .withDomain(domain)
                                 .withName("LoadSuccess")
                                 .build();
        loadFailure = NamedMetric.builder(metricRegistry::timer)
                                 .withDomain(domain)
                                 .withName("LoadFailure")
                                 .build();
        evictions = NamedMetric.builder(name -> metricRegistry.gauge(name, DeltaGauge::new))
                               .withDomain(domain)
                               .withName("Evictions")
                               .build();
    }

    @Override
    public void recordHits(@NonNegative int count)
    {
        hits.metric.update(count);
    }

    @Override
    public void recordMisses(@NonNegative int count)
    {
        misses.metric.update(count);
    }

    @Override
    public void recordLoadSuccess(@NonNegative long loadTime)
    {
        loadSuccess.metric.update(loadTime, TimeUnit.NANOSECONDS);
        totalLoadTimeNanos.add(loadTime);
    }

    @Override
    public void recordLoadFailure(@NonNegative long loadTime)
    {
        loadFailure.metric.update(loadTime, TimeUnit.NANOSECONDS);
        totalLoadTimeNanos.add(loadTime);
    }

    @Override
    public void recordEviction()
    {
        recordEviction(1);
    }

    /**
     * @deprecated
     */
    @Deprecated
    public void recordEviction(int weight)
    {
        evictions.metric.update(weight);
    }

    @Override
    public @NonNull CacheStats snapshot()
    {
        return CacheStats.of(hits.metric.getValue(), misses.metric.getValue(),
                             loadSuccess.metric.getCount(), loadFailure.metric.getCount(),
                             totalLoadTimeNanos.sum(), evictions.metric.getValue(), 0);
    }

    @Override
    public String toString()
    {
        return snapshot().toString();
    }
}
