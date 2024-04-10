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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.NoopMetricRegistry;
import com.codahale.metrics.Timer;

/**
 * Allows filtering of metrics based on configured allow list. Metrics are filtered out before registering them.
 */
public class FilteringMetricRegistry extends MetricRegistry
{
    private static final NoopMetricRegistry NO_OP_METRIC_REGISTRY = new NoopMetricRegistry(); // supplies no-op metrics
    private final Predicate<String> isAllowedPredicate;
    private final Map<String, Metric> excludedMetrics = new ConcurrentHashMap<>();

    public FilteringMetricRegistry(Predicate<String> isAllowedPredicate)
    {
        this.isAllowedPredicate = new CachedPredicate(isAllowedPredicate);
    }

    @Override
    public Counter counter(String name)
    {
        if (isAllowedPredicate.test(name))
        {
            return super.counter(name);
        }
        return typeChecked(excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::counter), Counter.class);
    }

    @Override
    public Counter counter(String name, MetricSupplier<Counter> supplier)
    {
        if (isAllowedPredicate.test(name))
        {
            return super.counter(name, supplier);
        }
        return typeChecked(excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::counter), Counter.class);
    }

    @Override
    public Histogram histogram(String name)
    {
        if (isAllowedPredicate.test(name))
        {
            return super.histogram(name);
        }
        return typeChecked(excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::histogram), Histogram.class);
    }

    @Override
    public Histogram histogram(String name, MetricSupplier<Histogram> supplier)
    {
        if (isAllowedPredicate.test(name))
        {
            return super.histogram(name, supplier);
        }
        return typeChecked(excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::histogram), Histogram.class);
    }

    @Override
    public Meter meter(String name)
    {
        if (isAllowedPredicate.test(name))
        {
            return super.meter(name);
        }
        return typeChecked(excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::meter), Meter.class);
    }

    @Override
    public Meter meter(String name, MetricSupplier<Meter> supplier)
    {
        if (isAllowedPredicate.test(name))
        {
            return super.meter(name, supplier);
        }
        return typeChecked(excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::meter), Meter.class);
    }

    @Override
    public Timer timer(String name)
    {
        if (isAllowedPredicate.test(name))
        {
            return super.timer(name);
        }
        return typeChecked(excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::timer), Timer.class);
    }

    @Override
    public Timer timer(String name, MetricSupplier<Timer> supplier)
    {
        if (isAllowedPredicate.test(name))
        {
            return super.timer(name, supplier);
        }
        return typeChecked(excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::timer), Timer.class);
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends Gauge> T gauge(String name)
    {
        if (isAllowedPredicate.test(name))
        {
            return super.gauge(name);
        }
        return (T) typeChecked(excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::gauge), Gauge.class);
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends Gauge> T gauge(String name, MetricSupplier<T> supplier)
    {
        if (isAllowedPredicate.test(name))
        {
            return super.gauge(name, supplier);
        }
        return (T) typeChecked(excludedMetrics.computeIfAbsent(name, k -> supplier.newMetric() /* unregistered metric */), Gauge.class);
    }

    /**
     * @return all the metrics including the allowed and disallowed metrics. This is to prevent re-registering of
     * excluded metrics
     */
    @Override
    public Map<String, Metric> getMetrics()
    {
        Map<String, Metric> allMetrics = new HashMap<>();
        allMetrics.putAll(super.getMetrics());
        allMetrics.putAll(excludedMetrics);
        return Collections.unmodifiableMap(allMetrics);
    }

    /**
     * @return metrics registered with {@code super.register()}. This will be useful for testing purposes to check
     * what metrics are actually captured
     */
    public Map<String, Metric> getIncludedMetrics()
    {
        return super.getMetrics();
    }

    /**
     * Metric specific retrieve methods such as {@code counter(name)} retrieve a noop instance if metric is filtered.
     * Prefer calling those over register method, register method returns an unregistered metric if the metric is
     * filtered. In some cases Noop metric instance has a performance advantage.
     */
    public <T extends Metric> T register(String name, T metric) throws IllegalArgumentException
    {
        if (metric == null)
        {
            throw new IllegalArgumentException("Metric can not be null");
        }

        // The metric is registered by calling the register() directly
        // We need to test whether it is allowed first
        if (isAllowedPredicate.test(name))
        {
            return super.register(name, metric);
        }

        return (T) typeChecked(excludedMetrics.computeIfAbsent(name, key -> metric), metric.getClass());
    }

    private <T extends Metric> T typeChecked(Metric metric, Class<T> type)
    {
        if (type.isInstance(metric))
        {
            return (T) metric;
        }
        throw new IllegalArgumentException("Metric already present with type " + metric.getClass());
    }

    /**
     * {@link CachedPredicate} remembers results of the {@link Predicate} it maintains. This is to avoid
     * redundant calls to delegate predicate.
     */
    static class CachedPredicate implements Predicate<String>
    {
        private final Predicate<String> delegate;
        private final Map<String, Boolean> results = new ConcurrentHashMap<>();

        CachedPredicate(Predicate<String> delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public boolean test(String s)
        {
            return results.computeIfAbsent(s, t -> delegate.test(s));
        }
    }
}
