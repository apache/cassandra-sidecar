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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.exceptions.NoSuchCassandraInstanceException;
import org.jetbrains.annotations.NotNull;

/**
 * Local implementation of InstancesMetadata.
 */
public class InstancesMetadataImpl implements InstancesMetadata
{
    private final Map<Integer, InstanceMetadata> idToInstanceMetadata;
    private final Map<String, InstanceMetadata> ipToInstanceMetadata;
    private final Map<String, InstanceMetadata> hostNameToInstanceMetadata;
    private final List<InstanceMetadata> instanceMetadataList;

    public InstancesMetadataImpl(InstanceMetadata instanceMetadata)
    {
        this(Collections.singletonList(instanceMetadata));
    }

    public InstancesMetadataImpl(List<InstanceMetadata> instanceMetadataList)
    {
        this.instanceMetadataList = instanceMetadataList;
        this.idToInstanceMetadata = new HashMap<>(instanceMetadataList.size());
        this.ipToInstanceMetadata = new HashMap<>(instanceMetadataList.size());
        this.hostNameToInstanceMetadata = new HashMap<>(instanceMetadataList.size());
        for (InstanceMetadata instanceMetadata : instanceMetadataList)
        {
            this.idToInstanceMetadata.put(instanceMetadata.id(), instanceMetadata);
            this.ipToInstanceMetadata.put(instanceMetadata.ipAddress(), instanceMetadata);
            // 'host' could be IP already, in such case, hostNameToInstanceMetadata map is identical to ipToInstanceMetadata
            this.hostNameToInstanceMetadata.put(instanceMetadata.host(), instanceMetadata);
        }
    }

    @Override
    public @NotNull List<InstanceMetadata> instances()
    {
        return instanceMetadataList;
    }

    @Override
    public InstanceMetadata instanceFromId(int id) throws NoSuchCassandraInstanceException
    {
        InstanceMetadata instanceMetadata = idToInstanceMetadata.get(id);
        if (instanceMetadata == null)
        {
            throw new NoSuchCassandraInstanceException("Instance id '" + id + "' not found");
        }
        return instanceMetadata;
    }

    @Override
    public InstanceMetadata instanceFromHost(String hostOrIpAddress) throws NoSuchCassandraInstanceException
    {
        // if the 'host' string is IP address string
        InstanceMetadata instanceMetadata = ipToInstanceMetadata.get(hostOrIpAddress);
        if (instanceMetadata == null)
        {
            // if the host string is hostname string, resolve the ip address and loop up again
            instanceMetadata = hostNameToInstanceMetadata.get(hostOrIpAddress);
        }

        if (instanceMetadata == null)
        {
            throw new NoSuchCassandraInstanceException("Instance with host address '" + hostOrIpAddress + "' not found");
        }
        return instanceMetadata;
    }
}
