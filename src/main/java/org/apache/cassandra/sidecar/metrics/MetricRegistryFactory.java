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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

/**
 * Provider for getting {@link FilteringMetricRegistry} based on provided metrics configuration
 */
@Singleton
public class MetricRegistryFactory
{
    private final String globalRegistryName;
    private final ReadWriteLock readWriteLock;
    private final List<MetricFilter> inclusions;
    private final List<MetricFilter> exclusions;

    @Inject
    public MetricRegistryFactory(SidecarConfiguration sidecarConfiguration)
    {
        this(sidecarConfiguration.metricsConfiguration().registryName(),
             MetricFilter.parse(sidecarConfiguration.metricsConfiguration().includeConfigurations()),
             MetricFilter.parse(sidecarConfiguration.metricsConfiguration().excludeConfigurations()));
    }

    @VisibleForTesting
    public MetricRegistryFactory(String globalRegistryName, List<MetricFilter> inclusions, List<MetricFilter> exclusions)
    {
        this.globalRegistryName = globalRegistryName;
        this.readWriteLock = new ReentrantReadWriteLock();
        this.inclusions = new ArrayList<>(inclusions);
        this.exclusions = new ArrayList<>(exclusions);
    }

    public MetricRegistry getOrCreate()
    {
        return getOrCreate(globalRegistryName);
    }

    public MetricRegistry getOrCreate(int cassInstanceId)
    {
        String instanceRegistryName = globalRegistryName + "_" + cassInstanceId;
        return getOrCreate(instanceRegistryName);
    }

    /**
     * Provides a {@link FilteringMetricRegistry} with given name and provided filters in configuration.
     * @param name registry name
     * @return a {@link MetricRegistry} that can filter out metrics
     */
    public MetricRegistry getOrCreate(String name)
    {
        // the metric registry already exists
        if (SharedMetricRegistries.names().contains(name))
        {
            return SharedMetricRegistries.getOrCreate(name);
        }

        FilteringMetricRegistry metricRegistry;
        readWriteLock.readLock().lock();
        try
        {
            metricRegistry = new FilteringMetricRegistry(this::isAllowed);
        }
        finally
        {
            readWriteLock.readLock().unlock();
        }
        SharedMetricRegistries.add(name, metricRegistry);
        return SharedMetricRegistries.getOrCreate(name);
    }

    /**
     * Check if the metric is allowed to register
     * The evaluation order is inclusions first, then exclusions. In other words,
     * a metric name is allowed if it is in the inclusions, but not in the exclusions.
     * <p>
     * Note that an empty inclusions means including all
     *
     * @param name  metric name
     * @return true if allowed; false otherwise
     */
    private boolean isAllowed(String name)
    {
        boolean included = inclusions.isEmpty() || inclusions.stream().anyMatch(filter -> filter.matches(name));
        boolean excluded = exclusions.stream().anyMatch(filter -> filter.matches(name));
        return included && !excluded;
    }
}
