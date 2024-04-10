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
import java.util.Set;
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
import io.vertx.core.impl.ConcurrentHashSet;

/**
 * Allows filtering of metrics based on configured allow list. Metrics are filtered out before registering them.
 */
public class FilteringMetricRegistry extends MetricRegistry
{
    private static final NoopMetricRegistry NO_OP_METRIC_REGISTRY = new NoopMetricRegistry(); // supplies no-op metrics
    private final Predicate<String> isAllowed;
    private final Set<String> included = new ConcurrentHashSet<>();
    private final Map<Class, Metric> cachedMetricPerType = new ConcurrentHashMap<>();
    private final Map<String, Metric> excludedMetrics = new ConcurrentHashMap<>();

    public FilteringMetricRegistry(Predicate<String> isAllowed)
    {
        this.isAllowed = isAllowed;
    }

    @Override
    public Counter counter(String name)
    {
        if (isAllowed.test(name))
        {
            included.add(name);
            return super.counter(name);
        }
        return (Counter) excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::counter);
    }

    @Override
    public Counter counter(String name, MetricSupplier<Counter> supplier)
    {
        if (isAllowed.test(name))
        {
            included.add(name);
            return super.counter(name, supplier);
        }
        return (Counter) excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::counter);
    }

    @Override
    public Histogram histogram(String name)
    {
        if (isAllowed.test(name))
        {
            included.add(name);
            return super.histogram(name);
        }
        return (Histogram) excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::histogram);
    }

    @Override
    public Histogram histogram(String name, MetricSupplier<Histogram> supplier)
    {
        if (isAllowed.test(name))
        {
            included.add(name);
            return super.histogram(name, supplier);
        }
        return (Histogram) excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::histogram);
    }

    @Override
    public Meter meter(String name)
    {
        if (isAllowed.test(name))
        {
            included.add(name);
            return super.meter(name);
        }
        return (Meter) excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::meter);
    }

    @Override
    public Meter meter(String name, MetricSupplier<Meter> supplier)
    {
        if (isAllowed.test(name))
        {
            included.add(name);
            return super.meter(name, supplier);
        }
        return (Meter) excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::meter);
    }

    @Override
    public Timer timer(String name)
    {
        if (isAllowed.test(name))
        {
            included.add(name);
            return super.timer(name);
        }
        return (Timer) excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::timer);
    }

    @Override
    public Timer timer(String name, MetricSupplier<Timer> supplier)
    {
        if (isAllowed.test(name))
        {
            included.add(name);
            return super.timer(name, supplier);
        }
        return (Timer) excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::timer);
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends Gauge> T gauge(String name)
    {
        if (isAllowed.test(name))
        {
            included.add(name);
            return super.gauge(name);
        }
        return (T) excludedMetrics.computeIfAbsent(name, NO_OP_METRIC_REGISTRY::gauge);
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends Gauge> T gauge(String name, MetricSupplier<T> supplier)
    {
        if (isAllowed.test(name))
        {
            included.add(name);
            return super.gauge(name, supplier);
        }
        return (T) excludedMetrics.computeIfAbsent(name, k -> supplier.newMetric() /* unregistered metric */);
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

        if (included.contains(name))
        {
            // Call super register to retrieve the included metric
            return super.register(name, metric);
        }

        // The metric is registered by calling the register() directly
        // We need to test whether it is allowed first
        if (isAllowed.test(name))
        {
            included.add(name);
            return super.register(name, metric);
        }

        // The metric is disallowed, but it is a guage, which can have type variants.
        // Simply add the metric to the excluded map and return the same metric instance back.
        if (metric instanceof Gauge)
        {
            return (T) excludedMetrics.computeIfAbsent(name, k -> metric);
        }

        // For other metrics (including custom metrics), cache the instance for the metric type
        // and return the cached instance
        T cached = (T) cachedMetricPerType.computeIfAbsent(metric.getClass(), k -> metric);
        return (T) excludedMetrics.computeIfAbsent(name, key -> cached);
    }
}
