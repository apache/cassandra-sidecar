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
import java.util.function.Function;
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
    private final Predicate<String> isAllowed;
    private final Map<String, Metric> excludedMetrics = new ConcurrentHashMap<>();
    private volatile Map<String, Metric> allMetrics;

    public FilteringMetricRegistry(Predicate<String> isAllowedPredicate)
    {
        this.isAllowed = new CachedPredicate(isAllowedPredicate);
    }

    @Override
    public Counter counter(String name)
    {
        if (isAllowed.test(name))
        {
            return super.counter(name);
        }
        return typeChecked(addExcludedMetricIfNotExists(name, NO_OP_METRIC_REGISTRY::counter), Counter.class);
    }

    @Override
    public Counter counter(String name, MetricSupplier<Counter> supplier)
    {
        if (isAllowed.test(name))
        {
            return super.counter(name, supplier);
        }
        return typeChecked(addExcludedMetricIfNotExists(name, NO_OP_METRIC_REGISTRY::counter), Counter.class);
    }

    @Override
    public Histogram histogram(String name)
    {
        if (isAllowed.test(name))
        {
            return super.histogram(name);
        }
        return typeChecked(addExcludedMetricIfNotExists(name, NO_OP_METRIC_REGISTRY::histogram), Histogram.class);
    }

    @Override
    public Histogram histogram(String name, MetricSupplier<Histogram> supplier)
    {
        if (isAllowed.test(name))
        {
            return super.histogram(name, supplier);
        }
        return typeChecked(addExcludedMetricIfNotExists(name, NO_OP_METRIC_REGISTRY::histogram), Histogram.class);
    }

    @Override
    public Meter meter(String name)
    {
        if (isAllowed.test(name))
        {
            return super.meter(name);
        }
        return typeChecked(addExcludedMetricIfNotExists(name, NO_OP_METRIC_REGISTRY::meter), Meter.class);
    }

    @Override
    public Meter meter(String name, MetricSupplier<Meter> supplier)
    {
        if (isAllowed.test(name))
        {
            return super.meter(name, supplier);
        }
        return typeChecked(addExcludedMetricIfNotExists(name, NO_OP_METRIC_REGISTRY::meter), Meter.class);
    }

    @Override
    public Timer timer(String name)
    {
        if (isAllowed.test(name))
        {
            return super.timer(name);
        }
        return typeChecked(addExcludedMetricIfNotExists(name, NO_OP_METRIC_REGISTRY::timer), Timer.class);
    }

    @Override
    public Timer timer(String name, MetricSupplier<Timer> supplier)
    {
        if (isAllowed.test(name))
        {
            return super.timer(name, supplier);
        }
        return typeChecked(addExcludedMetricIfNotExists(name, NO_OP_METRIC_REGISTRY::timer), Timer.class);
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends Gauge> T gauge(String name)
    {
        if (isAllowed.test(name))
        {
            return super.gauge(name);
        }
        return (T) typeChecked(addExcludedMetricIfNotExists(name, NO_OP_METRIC_REGISTRY::gauge), Gauge.class);
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends Gauge> T gauge(String name, MetricSupplier<T> supplier)
    {
        if (isAllowed.test(name))
        {
            return super.gauge(name, supplier);
        }
        return (T) typeChecked(addExcludedMetricIfNotExists(name, k -> supplier.newMetric() /* unregistered metric */), Gauge.class);
    }

    /**
     * The performance characteristics of this method depend on the frequency of changes to the
     * metrics. If modifications to the registry are infrequent, this implementation will perform
     * best with low garbage being created. If modifications to the registry are frequent, more garbage
     * will be created.
     *
     * @return all the metrics including the allowed and disallowed metrics. This is to prevent re-registering of
     * excluded metrics
     */
    @Override
    public Map<String, Metric> getMetrics()
    {
        Map<String, Metric> existingAllMetrics = allMetrics;
        if (existingAllMetrics != null)
        {
            return existingAllMetrics;
        }

        synchronized (this)
        {
            if (allMetrics == null)
            {
                existingAllMetrics = new HashMap<>();
                existingAllMetrics.putAll(super.getMetrics());
                existingAllMetrics.putAll(excludedMetrics);
                allMetrics = existingAllMetrics = Collections.unmodifiableMap(existingAllMetrics);
            }
            else
            {
                existingAllMetrics = allMetrics;
            }
        }
        return existingAllMetrics;
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
     *
     * @param name   the name of the metric
     * @param metric the metric
     * @param <T>    the type of the metric
     * @return {@code metric}
     * @throws IllegalArgumentException if the name is already registered or metric variable is null
     */
    @Override
    public <T extends Metric> T register(String name, T metric) throws IllegalArgumentException
    {
        T registeredMetric = registerInternal(name, metric);

        // allMetrics needs to be recomputed every time a metric is registered
        synchronized (this)
        {
            allMetrics = null;
        }

        return registeredMetric;
    }

    /**
     * Removes the metric with the given name from the set of excluded metrics, and if the metric
     * does not exists it removes it from the underlying metrics.
     *
     * @param name the name of the metric
     * @return whether or not the metric was removed
     */
    @Override
    public boolean remove(String name)
    {
        boolean removeResult = true;
        Metric removedMetric = excludedMetrics.remove(name);
        if (removedMetric == null)
        {
            removeResult = super.remove(name);
        }

        // force allMetrics to be recomputed every time a metric is removed
        synchronized (this)
        {
            allMetrics = null;
        }

        return removeResult;
    }

    private <T extends Metric> T registerInternal(String name, T metric)
    {
        if (metric == null)
        {
            throw new IllegalArgumentException("Metric can not be null");
        }
        // The metric is registered by calling the register() directly
        // We need to test whether it is allowed first
        if (isAllowed.test(name))
        {
            return super.register(name, metric);
        }

        return (T) typeChecked(addExcludedMetricIfNotExists(name, key -> metric), metric.getClass());
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
     * Adds excluded metric if it does not exist in the excluded list yet. This method ensures
     * that when adding a metric to the list of excluded metrics, a recomputation is forced
     * for the metrics returned by the {@link #getMetrics()} method.
     *
     * @param name            the name of the metric
     * @param mappingFunction the mapping function to compute the value of the metric
     * @return the current value of the excluded metric if it exists, or the new computed
     * value if it doesn't
     */
    private Metric addExcludedMetricIfNotExists(String name, Function<String, ? extends Metric> mappingFunction)
    {
        return excludedMetrics.computeIfAbsent(name, k -> {
            // allMetrics needs to be recomputed when an excluded metric is registered
            synchronized (this)
            {
                allMetrics = null;
            }

            return mappingFunction.apply(k);
        });
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
