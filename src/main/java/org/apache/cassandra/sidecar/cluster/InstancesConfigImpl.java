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

package org.apache.cassandra.sidecar.cluster;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;

/**
 * Local implementation of InstancesConfig.
 */
public class InstancesConfigImpl implements InstancesConfig
{
    private final Map<Integer, InstanceMetadata> idToInstanceMetas;
    private final Map<String, InstanceMetadata> hostToInstanceMetas;
    private final List<InstanceMetadata> instanceMetas;

    public InstancesConfigImpl(List<InstanceMetadata> instanceMetas)
    {
        this.idToInstanceMetas = instanceMetas.stream()
                                              .collect(Collectors.toMap(InstanceMetadata::id,
                                                                        instanceMeta -> instanceMeta));
        this.hostToInstanceMetas = instanceMetas.stream()
                                                .collect(Collectors.toMap(InstanceMetadata::host,
                                                                          instanceMeta -> instanceMeta));
        this.instanceMetas = instanceMetas;
    }

    public List<InstanceMetadata> instances()
    {
        return instanceMetas;
    }

    public InstanceMetadata instanceFromId(int id)
    {
        InstanceMetadata instanceMetadata = idToInstanceMetas.get(id);
        if (instanceMetadata == null)
        {
            throw new IllegalArgumentException("Instance id " + id + " not found");
        }
        return instanceMetadata;
    }

    public InstanceMetadata instanceFromHost(String host)
    {
        InstanceMetadata instanceMetadata = hostToInstanceMetas.get(host);
        if (instanceMetadata == null)
        {
            throw new IllegalArgumentException("Instance with host address " + host + " not found");
        }
        return instanceMetadata;
    }
}
