/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.coordination;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Host;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.client.SidecarInstance;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Partitioner;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Partitioners;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract class defining common logic for token ring providers
 */
public abstract class TokenRingProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TokenRingProvider.class);
    protected final InstancesMetadata instancesMetadata;
    protected final InstanceMetadataFetcher fetcher;
    protected final DnsResolver dnsResolver;

    public TokenRingProvider(InstancesMetadata instancesMetadata, InstanceMetadataFetcher fetcher, DnsResolver dnsResolver)
    {
        this.instancesMetadata = instancesMetadata;
        this.fetcher = fetcher;
        this.dnsResolver = dnsResolver;
    }

    /**
     * Gets primary token ranges for all the instances in the cluster grouped by Ip address of the instance.
     * Ranges should be open-closed bound.
     *
     * @param partitioner partitioner
     * @param dc          data center
     * @return map of token ranges per Cassandra instance IP
     */
    protected abstract Map<String, List<Range<BigInteger>>> getAllTokenRanges(Partitioner partitioner, @Nullable String dc);

    /**
     * Gets primary token ranges of the given sidecar instance.
     *
     * @param instance Sidecar instance
     * @param dc       data center
     * @return primary token ranges for the SidecarInstance
     */
    public abstract Map<String, List<Range<BigInteger>>> getPrimaryRanges(SidecarInstance instance, String dc);

    /**
     * Gets primary token ranges for local cassandra instances.
     *
     * @param dc optionally filter by DC.
     * @return token ranges per local Cassandra instance
     */
    public Map<String, List<Range<BigInteger>>> getPrimaryTokenRanges(@Nullable String dc)
    {
        Set<String> instanceIpsManagedBySidecar = instanceIpsManagedBySidecar();
        if (instanceIpsManagedBySidecar.isEmpty())
        {
            return Collections.emptyMap();
        }

        Partitioner partitioner = partitioner();
        return getAllTokenRanges(partitioner, dc)
               .entrySet()
               .stream()
               .filter(entry -> instanceIpsManagedBySidecar.contains(entry.getKey()))
               .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Gets list of data centers in the cluster
     *
     * @return set of data centers
     */
    public abstract Set<String> dcs();

    @Nullable
    public String localDc()
    {
        NodeSettings nodeSettings = fetcher.callOnFirstAvailableInstance(instance-> instance.delegate().nodeSettings());
        return nodeSettings.datacenter();
    }

    /**
     * Returns the partitioner
     * Ex: RandomPartitioner, Murmur3Partitioner
     *
     * @return partitioner
     */
    public Partitioner partitioner()
    {
        String[] tokens = fetcher.callOnFirstAvailableInstance(instance -> instance.delegate().nodeSettings().partitioner().split("\\."));
        return Partitioners.from(tokens[tokens.length - 1]);
    }

    // Helpers

    protected String getIpFromHost(Host host)
    {
        return getIpFromHost(dnsResolver, host);
    }

    protected static String getIpFromHost(DnsResolver dnsResolver, Host host)
    {
        // if the IP address is already resolved for the host (it generally should be), use it.
        // this also avoids the case where the driver connects to the local node with an IPv6 or IPv4 address and is
        // able to resolve its host name, we want to avoid attempting to resolve by host name here in the event
        // that the configured DNS resolver resolves the wrong IP class for the configured node.
        @SuppressWarnings("deprecation") InetAddress address = host.getAddress();
        String hostAddress = address.getHostAddress();
        if (hostAddress != null)
        {
            return hostAddress;
        }
        else
        {
            // otherwise resolve the IP from the host name.
            return getIp(dnsResolver, address.getHostName());
        }
    }

    protected String getIp(String nodeName)
    {
        return getIp(dnsResolver, nodeName);
    }

    protected static String getIp(DnsResolver dnsResolver, String nodeName)
    {
        try
        {
            return dnsResolver.resolve(nodeName);
        }
        catch (UnknownHostException e)
        {
            LOGGER.error("Could not resolve hostname for host nodeName={}", nodeName);
            throw new RuntimeException(e);
        }
    }

    private Set<String> instanceIpsManagedBySidecar()
    {
        return instancesMetadata.instances().stream().map(InstanceMetadata::host).map(this::getIp).collect(Collectors.toSet());
    }
}
