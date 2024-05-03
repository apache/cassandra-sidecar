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

package org.apache.cassandra.sidecar.locator;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.jetbrains.annotations.NotNull;

/**
 * Get token ranges owned and replicated to the local Cassandra instance(s) by keyspace
 * The results are cached and gets invalidated when local instances or cluster topology changed
 */
@Singleton
public class CachedLocalTokenRanges implements LocalTokenRangesProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CachedLocalTokenRanges.class);
    private final InstancesConfig instancesConfig;
    private final DnsResolver dnsResolver;

    @GuardedBy("this")
    private Set<Integer> localInstanceIdsCache;
    @GuardedBy("this")
    private Set<Host> allInstancesCache;
    @GuardedBy("this")
    private Set<Host> localInstancesCache;
    @GuardedBy("this")
    private ImmutableMap<String, Map<Integer, Set<TokenRange>>> localTokenRangesCache;

    @Inject
    public CachedLocalTokenRanges(InstancesConfig instancesConfig, DnsResolver dnsResolver)
    {
        this.instancesConfig = instancesConfig;
        this.dnsResolver = dnsResolver;
        this.localTokenRangesCache = null;
        this.localInstanceIdsCache = null;
        this.allInstancesCache = null;
        this.localInstancesCache = null;
    }

    @Override
    @Nullable
    public Map<Integer, Set<TokenRange>> localTokenRanges(String keyspace)
    {
        List<InstanceMetadata> localInstances = instancesConfig.instances();

        if (localInstances.isEmpty())
        {
            LOGGER.warn("No local instances found");
            return Collections.emptyMap();
        }

        CassandraAdapterDelegate delegate = localInstances.get(0).delegate();
        Metadata metadata = delegate == null ? null : delegate.metadata();
        if (metadata == null)
        {
            LOGGER.debug("Not yet connect to Cassandra cluster");
            return Collections.emptyMap();
        }

        if (metadata.getKeyspace(keyspace) == null)
        {
            throw new NoSuchElementException("Keyspace does not exist. keyspace: " + keyspace);
        }

        Set<Integer> localInstanceIds = localInstances.stream()
                                                      .map(InstanceMetadata::id)
                                                      .collect(Collectors.toSet());
        Set<Host> allInstances = metadata.getAllHosts();
        return getCacheOrReload(metadata, keyspace, localInstanceIds, localInstances, allInstances);
    }

    /**
     * Return the token ranges owned and replicated to the host according to the replication strategy of the keyspace
     * The result set is unmodifiable.
     */
    @Nullable
    private Pair<Host, Set<TokenRange>> tokenRangesOfHost(Metadata metadata,
                                                          String keyspace,
                                                          InstanceMetadata instance,
                                                          Map<IpAddressAndPort, Host> allHosts)
    {
        Host host;
        try
        {
            final IpAddressAndPort ip = IpAddressAndPort.of(dnsResolver.resolve(instance.host()), instance.port());
            host = allHosts.get(ip);
            if (host == null)
            {
                LOGGER.warn("Could not map InstanceMetadata to Host host={} port={} ip={}",
                            instance.host(), instance.port(), ip.ipAddress);
                return null;
            }
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException("Failed to resolve hostname to ip. hostname: " + instance.host(), e);
        }
        return Pair.of(host, tokenRangesOfHost(metadata, keyspace, host));
    }

    public Set<TokenRange> tokenRangesOfHost(Metadata metadata, String keyspace, Host host)
    {
        return metadata.getTokenRanges(keyspace, host)
                       .stream()
                       .flatMap(range -> TokenRange.from(range).stream())
                       .collect(Collectors.toSet());
    }

    /**
     * Reload the locally cached token ranges when needed
     */
    @Nullable
    private synchronized Map<Integer, Set<TokenRange>> getCacheOrReload(Metadata metadata,
                                                                        String keyspace,
                                                                        Set<Integer> localInstanceIds,
                                                                        List<InstanceMetadata> localInstances,
                                                                        Set<Host> allInstances)
    {
        // exit early if no change is found
        boolean isClusterTheSame = allInstances.equals(allInstancesCache)
                                   && localInstanceIds.equals(localInstanceIdsCache);
        if (localTokenRangesCache != null
            && localTokenRangesCache.containsKey(keyspace)
            && isClusterTheSame)
        {
            return localTokenRangesCache.get(keyspace);
        }

        // otherwise, reload the token ranges
        localInstanceIdsCache = localInstanceIds;
        allInstancesCache = allInstances;
        if (allInstances.isEmpty())
        {
            LOGGER.warn("No instances found in client session");
        }
        Map<IpAddressAndPort, Host> allHosts = new HashMap<>(allInstancesCache.size());
        BiConsumer<InetSocketAddress, Host> putNullSafe = (endpoint, host) -> {
            if (endpoint != null)
            {
                allHosts.put(IpAddressAndPort.of(endpoint), host);
            }
        };
        for (Host host : allInstancesCache)
        {
            putNullSafe.accept(host.getSocketAddress(), host);
            putNullSafe.accept(host.getListenSocketAddress(), host);
            putNullSafe.accept(host.getBroadcastSocketAddress(), host);
        }

        ImmutableMap.Builder<String, Map<Integer, Set<TokenRange>>> perKeyspaceBuilder = ImmutableMap.builder();
        ImmutableSet.Builder<Host> hostBuilder = ImmutableSet.builder();
        if (isClusterTheSame && localInstancesCache != null)
        {
            hostBuilder.addAll(localInstancesCache);
        }

        for (KeyspaceMetadata ks : metadata.getKeyspaces())
        {
            if (isClusterTheSame && localTokenRangesCache != null && localTokenRangesCache.containsKey(ks.getName()))
            {
                // we don't need to rebuild if already cached
                perKeyspaceBuilder.put(ks.getName(), localTokenRangesCache.get(ks.getName()));
            }
            else
            {
                ImmutableMap.Builder<Integer, Set<TokenRange>> resultBuilder = ImmutableMap.builder();
                for (InstanceMetadata instance : localInstances)
                {
                    Pair<Host, Set<TokenRange>> pair = tokenRangesOfHost(metadata, keyspace, instance, allHosts);
                    if (pair != null)
                    {
                        hostBuilder.add(pair.getKey());
                        resultBuilder.put(instance.id(), Collections.unmodifiableSet(pair.getValue()));
                    }
                }
                perKeyspaceBuilder.put(ks.getName(), resultBuilder.build());
            }
        }
        localTokenRangesCache = perKeyspaceBuilder.build();
        localInstancesCache = hostBuilder.build();
        if (localInstancesCache.isEmpty())
        {
            LOGGER.warn("Unable to determine local instances from client meta-data!");
        }
        return localTokenRangesCache.get(keyspace);
    }

    private static class IpAddressAndPort
    {
        final String ipAddress;
        final int port;

        static IpAddressAndPort of(@NotNull InetSocketAddress endpoint)
        {
            return IpAddressAndPort.of(endpoint.getAddress().getHostAddress(),
                                       endpoint.getPort());
        }

        static IpAddressAndPort of(String ipAddress, int port)
        {
            return new IpAddressAndPort(ipAddress, port);
        }

        IpAddressAndPort(String ipAddress, int port)
        {
            this.ipAddress = ipAddress;
            this.port = port;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            IpAddressAndPort that = (IpAddressAndPort) o;
            return port == that.port && Objects.equals(ipAddress, that.ipAddress);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(ipAddress, port);
        }
    }
}
