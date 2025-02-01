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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.exceptions.NoSuchCassandraInstanceException;
import org.jetbrains.annotations.NotNull;

/**
 * Local implementation of InstancesMetadata.
 */
public class InstancesMetadataImpl implements InstancesMetadata
{
    private static final Logger LOGGER = LoggerFactory.getLogger(InstancesMetadataImpl.class);
    private static final long ONE_SECOND = TimeUnit.SECONDS.toMillis(1);

    private final AtomicLong lastUpdateTimestamp;
    private final DnsResolver dnsResolver;
    private final List<InstanceMetadata> instanceMetadataList;
    private final Map<Integer, InstanceMetadata> idToInstanceMetadata;
    private final Map<String, InstanceMetadata> hostNameToInstanceMetadata;
    private volatile Map<String, InstanceMetadata> ipToInstanceMetadata;

    public InstancesMetadataImpl(InstanceMetadata instanceMetadata, DnsResolver dnsResolver)
    {
        this(Collections.singletonList(instanceMetadata), dnsResolver);
    }

    public InstancesMetadataImpl(List<InstanceMetadata> instanceMetadataList, DnsResolver dnsResolver)
    {
        this.dnsResolver = dnsResolver;
        this.instanceMetadataList = instanceMetadataList;
        this.idToInstanceMetadata = new HashMap<>(instanceMetadataList.size());
        this.ipToInstanceMetadata = new HashMap<>(instanceMetadataList.size());
        this.hostNameToInstanceMetadata = new HashMap<>(instanceMetadataList.size());
        for (InstanceMetadata instanceMetadata : instanceMetadataList)
        {
            this.idToInstanceMetadata.put(instanceMetadata.id(), instanceMetadata);
            if (instanceMetadata.ipAddress() != null)
            {
                this.ipToInstanceMetadata.put(instanceMetadata.ipAddress(), instanceMetadata);
            }
            // 'host' could be IP already, in such case, hostNameToInstanceMetadata map is identical to ipToInstanceMetadata
            this.hostNameToInstanceMetadata.put(instanceMetadata.host(), instanceMetadata);
        }
        this.lastUpdateTimestamp = new AtomicLong(0); // The field stores the timestamp value, but let's not query system clock for initialization
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
        // if the input string is hostname string, resolve the ip address and loop up again
        InstanceMetadata instanceMetadata = hostNameToInstanceMetadata.get(hostOrIpAddress);
        if (instanceMetadata != null)
        {
            return instanceMetadata;
        }

        // the input string is probably IP address string
        instanceMetadata = ipToInstanceMetadata.get(hostOrIpAddress);
        if (instanceMetadata != null)
        {
            return instanceMetadata;
        }

        // maybe the input string is host, try to resolve it
        String ipAddress = hostOrIpAddress;
        try
        {
            ipAddress = dnsResolver.resolve(hostOrIpAddress);
        }
        catch (UnknownHostException e)
        {
            // Do not exit; try to look up again after updating the IpToInstanceMetadata map
            LOGGER.debug("Failed to resolve IP address for {}", hostOrIpAddress);
        }

        // maybe the IP of the hostname has been updated in the DNS, and client is using the updated IP
        // update the ipToInstanceMetadata and look up with the IP address again
        maybeUpdateIpToInstanceMetadata();
        instanceMetadata = ipToInstanceMetadata.get(ipAddress);
        if (instanceMetadata != null)
        {
            return instanceMetadata;
        }

        throw new NoSuchCassandraInstanceException("Instance with host address '" + hostOrIpAddress + "' not found");
    }

    // Update the IpToInstanceMetadata if it has been enough time since the last update
    //
    // Memory visibility effects:
    // The method does not guarantee the caller see the latest value in the IpToInstanceMetadata when the method exits.
    // However, once the reference of IpToInstanceMetadata is updated, the subsequent procedures should see the updated value
    private void maybeUpdateIpToInstanceMetadata()
    {
        long now = System.currentTimeMillis();
        long lastValue = lastUpdateTimestamp.get();
        if (now - lastValue <= ONE_SECOND // update at most once a second
            || !lastUpdateTimestamp.compareAndSet(lastValue, now)) // another thread has updated the timestamp. let it update the map
        {
            return;
        }

        Map<String, InstanceMetadata> updated = new HashMap<>(instanceMetadataList.size());
        for (InstanceMetadata instanceMetadata : instanceMetadataList)
        {
            String ipAddress = instanceMetadata.ipAddress(); // existing ip
            try
            {
                ipAddress = instanceMetadata.refreshIpAddress(); // updated ip
            }
            catch (UnknownHostException uhe)
            {
                // log a warning and continue; Do not update the ipAddress for this instance
                LOGGER.warn("Failed to resolve IP address from host. Going to use the existing IP address. host={}",
                            instanceMetadata.host(), uhe);
            }
            if (ipAddress != null)
            {
                updated.put(ipAddress, instanceMetadata);
            }
        }
        this.ipToInstanceMetadata = updated;
    }
}
