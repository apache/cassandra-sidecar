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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;

/**
 * A dropwizard {@link MetricRegistry} associated with a specific Cassandra instance.
 */
public class InstanceMetricRegistry extends MetricRegistry
{
    private final int instanceId;

    public InstanceMetricRegistry(int instanceId)
    {
        this.instanceId = instanceId;
    }

    public int instanceId()
    {
        return instanceId;
    }

    /**
     * A factory for creating instance specific {@link MetricRegistry} provided an instance id.
     */
    @Singleton
    public static class RegistryFactory
    {
        private final MetricRegistryNameFactory nameFactory;
        private final InstancesConfig instancesConfig;

        @Inject
        public RegistryFactory(MetricRegistryNameFactory nameFactory, InstancesConfig instancesConfig)
        {
            this.nameFactory = nameFactory;
            this.instancesConfig = instancesConfig;
        }

        public InstanceMetricRegistry registry(int instanceId)
        {
            InstanceMetadata instanceMetadata = instancesConfig.instanceFromId(instanceId);
            if (instanceMetadata == null)
            {
                throw new IllegalArgumentException("Instance specific registry requested for non existent instance");
            }

            String registryName = nameFactory.forInstance(instanceId);
            InstanceMetricRegistry instanceMetricRegistry = new InstanceMetricRegistry(instanceId);

            SharedMetricRegistries.add(registryName, instanceMetricRegistry);
            return (InstanceMetricRegistry) SharedMetricRegistries.getOrCreate(registryName);
        }
    }
}
