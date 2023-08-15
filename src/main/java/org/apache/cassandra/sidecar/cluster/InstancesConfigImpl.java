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

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.apache.cassandra.sidecar.exceptions.NoSuchSidecarInstanceException;
import org.jetbrains.annotations.NotNull;

/**
 * Local implementation of InstancesConfig.
 */
public class InstancesConfigImpl implements InstancesConfig
{
    private final Map<Integer, InstanceMetadata> idToInstanceMetadata;
    private final Map<String, InstanceMetadata> hostToInstanceMetadata;
    private final List<InstanceMetadata> instanceMetadataList;
    private final DnsResolver dnsResolver;

    public InstancesConfigImpl(InstanceMetadata instanceMetadata, DnsResolver dnsResolver)
    {
        this(Collections.singletonList(instanceMetadata), dnsResolver);
    }

    public InstancesConfigImpl(List<InstanceMetadata> instanceMetadataList, DnsResolver dnsResolver)
    {
        this.idToInstanceMetadata = instanceMetadataList.stream()
                                                        .collect(Collectors.toMap(InstanceMetadata::id,
                                                                                  Function.identity()));
        this.hostToInstanceMetadata = instanceMetadataList.stream()
                                                          .collect(Collectors.toMap(InstanceMetadata::host,
                                                                                    Function.identity()));
        this.instanceMetadataList = instanceMetadataList;
        this.dnsResolver = dnsResolver;
    }

    @Override
    public @NotNull List<InstanceMetadata> instances()
    {
        return instanceMetadataList;
    }

    @Override
    public InstanceMetadata instanceFromId(int id) throws NoSuchSidecarInstanceException
    {
        InstanceMetadata instanceMetadata = idToInstanceMetadata.get(id);
        if (instanceMetadata == null)
        {
            throw new NoSuchSidecarInstanceException("Instance id '" + id + "' not found");
        }
        return instanceMetadata;
    }

    @Override
    public InstanceMetadata instanceFromHost(String host) throws NoSuchSidecarInstanceException
    {
        InstanceMetadata instanceMetadata = hostToInstanceMetadata.get(host);
        if (instanceMetadata == null)
        {
            try
            {
                instanceMetadata = hostToInstanceMetadata.get(dnsResolver.resolve(host));
            }
            catch (UnknownHostException cause)
            {
                throw new NoSuchSidecarInstanceException("Instance with host address '" + host + "' not found, "
                                                         + "and an error occurred when attempting to resolve its "
                                                         + "IP address.", cause);
            }
            if (instanceMetadata == null)
            {
                throw new NoSuchSidecarInstanceException("Instance with host address '" + host + "' not found");
            }
        }
        return instanceMetadata;
    }
}
