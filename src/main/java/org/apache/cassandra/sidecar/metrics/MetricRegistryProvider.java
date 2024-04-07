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
public class MetricRegistryProvider
{
    private final String globalRegistryName;
    private final List<MetricFilter> include = new ArrayList<>();
    private final List<MetricFilter> exclude = new ArrayList<>();

    @Inject
    public MetricRegistryProvider(SidecarConfiguration sidecarConfiguration)
    {
        this(sidecarConfiguration.metricsConfiguration().registryName(),
             MetricFilter.parse(sidecarConfiguration.metricsConfiguration().includeConfigurations()),
             MetricFilter.parse(sidecarConfiguration.metricsConfiguration().excludeConfigurations()));
    }

    @VisibleForTesting
    public MetricRegistryProvider(String globalRegistryName, List<MetricFilter> include, List<MetricFilter> exclude)
    {
        this.globalRegistryName = globalRegistryName;
        this.include.addAll(include);
        this.exclude.addAll(exclude);
    }

    public synchronized void configureFilters(List<MetricFilter> include, List<MetricFilter> exclude)
    {
        this.include.clear();
        this.exclude.clear();
        this.include.addAll(include);
        this.exclude.addAll(exclude);
    }

    public MetricRegistry registry()
    {
        return registry(globalRegistryName);
    }

    public MetricRegistry registry(int casInstanceId)
    {
        String instanceRegistryName = globalRegistryName + "_" + casInstanceId;
        return registry(instanceRegistryName);
    }

    public MetricRegistry registry(String name)
    {
        FilteringMetricRegistry metricRegistry = new FilteringMetricRegistry(include, exclude);
        SharedMetricRegistries.add(name, metricRegistry);
        return SharedMetricRegistries.getOrCreate(name);
    }
}
