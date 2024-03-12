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

package org.apache.cassandra.sidecar.metrics.instance;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;

/**
 * Provider for getting instance specific metrics given Cassandra instance id or host name.
 */
public class InstanceMetricProviderImpl implements InstanceMetricProvider
{
    protected final Map<Integer, InstanceMetrics> idToMetrics = new ConcurrentHashMap<>();
    protected final Map<String, InstanceMetrics> hostToMetrics = new ConcurrentHashMap<>();
    protected final InstancesConfig instancesConfig;
    protected final InstanceMetricRegistry.Factory instanceMetricRegistryFactory;

    public InstanceMetricProviderImpl(InstancesConfig instancesConfig,
                                      InstanceMetricRegistry.Factory instanceMetricRegistryFactory)
    {
        this.instancesConfig = instancesConfig;
        this.instanceMetricRegistryFactory = instanceMetricRegistryFactory;
    }

    @Override
    public InstanceMetrics metrics(int instanceId)
    {
        InstanceMetadata instanceMetadata = instancesConfig.instanceFromId(instanceId);
        if (instanceMetadata == null)
        {
            throw new IllegalArgumentException("Instance specific metrics requested for non existent instance");
        }

        return idToMetrics
               .computeIfAbsent(instanceId, id -> {
                   InstanceMetricRegistry instanceMetricRegistry = instanceMetricRegistryFactory.registry(id);
                   return new InstanceMetricsImpl(instanceMetricRegistry);
               });
    }

    @Override
    public InstanceMetrics metrics(String host)
    {
        InstanceMetadata instanceMetadata = instancesConfig.instanceFromHost(host);
        if (instanceMetadata == null)
        {
            throw new IllegalArgumentException("Instance specific metrics requested for non existent instance");
        }

        return hostToMetrics
               .computeIfAbsent(host, h -> {
                   InstanceMetricRegistry instanceMetricRegistry
                   = instanceMetricRegistryFactory.registry(instanceMetadata.id());
                   return new InstanceMetricsImpl(instanceMetricRegistry);
               });
    }
}
