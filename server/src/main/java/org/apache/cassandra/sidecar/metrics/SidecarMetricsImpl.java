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

import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Tracks both server metrics and Cassandra instance specific metrics that Sidecar maintains.
 */
public class SidecarMetricsImpl implements SidecarMetrics
{
    protected final MetricRegistryFactory registryFactory;
    protected final InstanceMetadataFetcher instanceMetadataFetcher;
    protected final ServerMetrics serverMetrics;

    public SidecarMetricsImpl(MetricRegistryFactory registryFactory, InstanceMetadataFetcher instanceMetadataFetcher)
    {
        this.registryFactory = registryFactory;
        this.instanceMetadataFetcher = instanceMetadataFetcher;

        MetricRegistry globalMetricRegistry = registryFactory.getOrCreate();
        this.serverMetrics = new ServerMetricsImpl(globalMetricRegistry);
    }

    @Override
    public ServerMetrics server()
    {
        return serverMetrics;
    }

    @Override
    public InstanceMetrics instance(int instanceId)
    {
        InstanceMetadata instanceMetadata = instanceMetadataFetcher.instance(instanceId);
        if (instanceMetadata == null)
        {
            throw new IllegalArgumentException("Instance metrics requested for non existent instance id " + instanceId);
        }
        return instanceMetadata.metrics();
    }

    @Override
    public InstanceMetrics instance(String host)
    {
        InstanceMetadata instanceMetadata = instanceMetadataFetcher.instance(host);
        if (instanceMetadata == null)
        {
            throw new IllegalArgumentException("Instance metrics requested for non existent host " + host);
        }
        return instanceMetadata.metrics();
    }
}
