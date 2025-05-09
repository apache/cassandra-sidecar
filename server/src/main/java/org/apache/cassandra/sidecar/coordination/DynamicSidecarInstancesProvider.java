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

package org.apache.cassandra.sidecar.coordination;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.client.SidecarInstancesProvider;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;

/**
 * A {@link SidecarInstancesProvider} implementation that returns Sidecar instances based on the configured
 * {@link InstancesMetadata} for the local Sidecar
 */
public class DynamicSidecarInstancesProvider implements SidecarInstancesProvider
{
    private final InstancesMetadata instancesMetadata;
    private final ServiceConfiguration serviceConfiguration;

    public DynamicSidecarInstancesProvider(InstancesMetadata instancesMetadata, ServiceConfiguration serviceConfiguration)
    {
        this.instancesMetadata = instancesMetadata;
        this.serviceConfiguration = serviceConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<SidecarInstance> instances()
    {
        return instancesMetadata.instances()
                                .stream()
                                .map(instanceMetadata -> new SidecarInstanceImpl(instanceMetadata.host(), serviceConfiguration.port()))
                                .collect(Collectors.toList());
    }
}
