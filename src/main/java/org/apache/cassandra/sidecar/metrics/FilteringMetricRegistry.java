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

import java.util.ArrayList;
import java.util.List;

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
    private final List<MetricFilter> filters = new ArrayList<>();

    public FilteringMetricRegistry(List<MetricFilter> filters)
    {
        this.filters.addAll(filters);
    }

    public synchronized void configureFilters(List<MetricFilter> newFilters)
    {
        filters.clear();
        filters.addAll(newFilters);
    }

    /**
     * Check if the metric is allowed to register. Yes if any of the metric filters match the metric.
     * @param metricName name
     * @return true if allowed; false otherwise
     */
    public boolean isAllowed(String metricName)
    {
        return filters.stream().allMatch(filter -> filter.isAllowed(metricName));
    }

    @Override
    public Counter counter(String name)
    {
        if (isAllowed(name))
        {
            return super.counter(name);
        }
        return NO_OP_METRIC_REGISTRY.counter(name);
    }

    @Override
    public Counter counter(String name, MetricSupplier<Counter> supplier)
    {
        if (isAllowed(name))
        {
            return super.counter(name, supplier);
        }
        return NO_OP_METRIC_REGISTRY.counter(name);
    }

    @Override
    public Histogram histogram(String name)
    {
        if (isAllowed(name))
        {
            return super.histogram(name);
        }
        return NO_OP_METRIC_REGISTRY.histogram(name);
    }

    @Override
    public Histogram histogram(String name, MetricSupplier<Histogram> supplier)
    {
        if (isAllowed(name))
        {
            return super.histogram(name, supplier);
        }
        return NO_OP_METRIC_REGISTRY.histogram(name);
    }

    @Override
    public Meter meter(String name)
    {
        if (isAllowed(name))
        {
            return super.meter(name);
        }
        return NO_OP_METRIC_REGISTRY.meter(name);
    }

    @Override
    public Meter meter(String name, MetricSupplier<Meter> supplier)
    {
        if (isAllowed(name))
        {
            return super.meter(name, supplier);
        }
        return NO_OP_METRIC_REGISTRY.meter(name);
    }

    @Override
    public Timer timer(String name)
    {
        if (isAllowed(name))
        {
            return super.timer(name);
        }
        return NO_OP_METRIC_REGISTRY.timer(name);
    }

    @Override
    public Timer timer(String name, MetricSupplier<Timer> supplier)
    {
        if (isAllowed(name))
        {
            return super.timer(name, supplier);
        }
        return NO_OP_METRIC_REGISTRY.timer(name);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T extends Gauge> T gauge(String name)
    {
        if (isAllowed(name))
        {
            return super.gauge(name);
        }
        return NO_OP_METRIC_REGISTRY.gauge(name);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T extends Gauge> T gauge(String name, MetricSupplier<T> supplier)
    {
        if (isAllowed(name))
        {
            return super.gauge(name, supplier);
        }
        return supplier.newMetric(); // unregistered metric
    }

    /**
     * Metric specific retrieve methods such as {@code counter(name)} retrieve a noop instance if metric is filtered.
     * Prefer calling those over register method, register method returns an unregistered metric if the metric is
     * filtered. In some cases Noop metric instance has a performance advantage.
     */
    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T extends Metric> T register(String name, T metric) throws IllegalArgumentException
    {
        if (isAllowed(name))
        {
            return super.register(name, metric);
        }

        if (metric == null)
        {
            throw new IllegalArgumentException("Metric can not be null");
        }
        return metric;
    }
}
