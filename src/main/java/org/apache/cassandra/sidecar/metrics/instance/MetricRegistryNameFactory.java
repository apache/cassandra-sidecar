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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

/**
 * Provides registry name for instance specific registry {@link InstanceMetricRegistry}.
 */
@Singleton
public class MetricRegistryNameFactory
{
    private final String globalRegistryName;

    @Inject
    public MetricRegistryNameFactory(SidecarConfiguration sidecarConfiguration)
    {
        this(sidecarConfiguration.metricsConfiguration().registryName());
    }

    public MetricRegistryNameFactory(String globalRegistryName)
    {
        this.globalRegistryName = globalRegistryName;
    }

    /**
     * @return registry name for creating {@link org.apache.cassandra.sidecar.metrics.instance.InstanceMetricRegistry}
     */
    public String forInstance(int instanceId)
    {
        return globalRegistryName + "_" + instanceId;
    }
}
