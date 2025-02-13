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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Token;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.locator.LocalTokenRangesProvider;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Partitioner;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Partitioners;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;


/**
 * Class for getting token range related information using cassandra client's session.
 * Token ranges are cached to avoid making dns calls when cluster topology has not changed.
 */
@Singleton
public class CassandraClientTokenRingProvider extends TokenRingProvider implements LocalTokenRangesProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClientTokenRingProvider.class);
    @GuardedBy("this")
    private volatile Map<String, Map<String, List<Range<BigInteger>>>> assignedRangesOfAllInstancesByDcCache = null;
    @GuardedBy("this")
    private volatile Map<Host, Integer> localHostsCache = null;
    @GuardedBy("this")
    private volatile Set<Host> allInstancesCache = null;


    @Inject
    public CassandraClientTokenRingProvider(InstancesMetadata instancesMetadata, InstanceMetadataFetcher instanceMetadataFetcher, DnsResolver dnsResolver)
    {
        super(instancesMetadata, instanceMetadataFetcher, dnsResolver);
    }

    @Override
    @Nullable
    public Map<Integer, Set<TokenRange>> localTokenRanges(String keyspace)
    {
        checkAndReloadReloadCaches();
        Metadata metadata = instancesMetadata.instances().get(0).delegate().metadata();
        if (keyspace == null || metadata.getKeyspace(keyspace) == null)
        {
            throw new NoSuchElementException("Keyspace does not exist. keyspace: " + keyspace);
        }
        return perKeySpaceTokenRangesOfAllInstances(metadata).get(keyspace)
                                                             .entrySet()
                                                             .stream()
                                                             .filter(entry -> localHostsCache.containsKey(entry.getKey()))
                                                             .collect(Collectors.toMap(entry -> localHostsCache.get(entry.getKey()), Map.Entry::getValue));
    }

    public Set<Host> localInstances()
    {
        checkAndReloadReloadCaches();
        return localHostsCache.keySet();
    }

    public Set<Host> allInstances()
    {
        checkAndReloadReloadCaches();
        return allInstancesCache;
    }

    @Override
    protected Map<String, List<Range<BigInteger>>> getAllTokenRanges(Partitioner partitioner,
                                                                     String dc)
    {
        checkAndReloadReloadCaches();
        return assignedRangesOfAllInstancesByDcCache.entrySet()
                                                    .stream()
                                                    .filter(entry -> dc == null || dc.equalsIgnoreCase(entry.getKey()))
                                                    .flatMap(entry -> entry.getValue().entrySet().stream())
                                                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Map<String, List<Range<BigInteger>>> getPrimaryRanges(SidecarInstance instance,
                                                                 String dc)
    {
        checkAndReloadReloadCaches();
        return assignedRangesOfAllInstancesByDcCache.entrySet()
                                                    .stream()
                                                    .filter(entry -> dc == null || dc.equalsIgnoreCase(entry.getKey()))
                                                    .flatMap(entry -> entry.getValue().entrySet().stream())
                                                    .filter(entry -> matchesSidecar(entry.getKey(), instance))
                                                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Set<String> dcs()
    {
        List<InstanceMetadata> localInstances = instancesMetadata.instances();
        Metadata metadata = validatedMetadata(localInstances);
        return metadata.getAllHosts().stream().map(Host::getDatacenter).collect(Collectors.toSet());
    }

    @Override
    public Partitioner partitioner()
    {
        return extractPartitioner(instancesMetadata.instances().get(0).delegate().metadata());
    }

    public static Partitioner extractPartitioner(Metadata metadata)
    {
        String[] tokens = metadata.getPartitioner().split("\\.");
        return Partitioners.from(tokens[tokens.length - 1]);
    }

    private void checkAndReloadReloadCaches()
    {
        List<InstanceMetadata> localInstances = instancesMetadata.instances();
        Metadata metadata = validatedMetadata(localInstances);

        Set<Integer> localInstanceIds = localInstances.stream().map(InstanceMetadata::id).collect(Collectors.toSet());
        if (localHostsCache == null
            || !new HashSet<>(localHostsCache.values()).equals(localInstanceIds)
            || allInstancesCache == null
            || !allInstancesCache.equals(metadata.getAllHosts()))
        {
            synchronized (this)
            {
                // If cluster configuration changes.
                localHostsCache = localInstanceIds();
                allInstancesCache = metadata.getAllHosts();
                assignedRangesOfAllInstancesByDcCache = assignedRangesOfAllInstancesByDc(metadata);
            }
        }
    }

    private boolean matchesSidecar(String hostIp, SidecarInstance sidecarInstance)
    {
        return getIp(sidecarInstance.hostname()).equals(hostIp);
    }

    public Map<String, Map<String, List<Range<BigInteger>>>> assignedRangesOfAllInstancesByDc(Metadata metadata)
    {
        return assignedRangesOfAllInstancesByDc(dnsResolver, metadata);
    }

    @VisibleForTesting
    public static Map<String, Map<String, List<Range<BigInteger>>>> assignedRangesOfAllInstancesByDc(DnsResolver dnsResolver, Metadata metadata)
    {
        Partitioner partitioner = extractPartitioner(metadata);
        Map<String, List<CassandraInstance>> perDcHosts = new HashMap<>(4);
        for (Host host : metadata.getAllHosts())
        {
            Token minToken = host.getTokens().stream().min(Comparable::compareTo).orElseThrow(() -> new RuntimeException("No token found for host: " + host));
            perDcHosts.computeIfAbsent(host.getDatacenter(), (dc) -> new ArrayList<>())
                      .add(new CassandraInstance(tokenToString(minToken), getIpFromHost(dnsResolver, host)));
        }
        perDcHosts.forEach((dc, hosts) -> hosts.sort(Comparator.comparing(o -> new BigInteger(o.token))));

        return perDcHosts.entrySet().stream()
                         .collect(Collectors.toMap(Map.Entry::getKey, e -> calculateTokenRanges(partitioner, e.getValue())));
    }

    protected static String tokenToString(Token token)
    {
        if (token.getType() == DataType.bigint())
        {
            return Long.toString((long) token.getValue());
        }
        else if (token.getType() == DataType.varint())
        {
            return token.getValue().toString();
        }
        throw new UnsupportedOperationException("Unsupported token type: " + token.getType());
    }

    protected static Map<String, List<Range<BigInteger>>> calculateTokenRanges(Partitioner partitioner, List<CassandraInstance> sortedPerDcHosts)
    {
        // RingTopologyRefresher.calculate...
        return calculateTokenRanges(sortedPerDcHosts, 1, partitioner)
               .entries().stream()
               .collect(Collectors.groupingBy(e -> e.getKey().node, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
    }

    private Map<Host, Integer> localInstanceIds()
    {
        List<InstanceMetadata> localInstances = instancesMetadata.instances();
        Set<Host> hosts = localInstances.get(0).delegate().metadata().getAllHosts();
        Map<String, Integer> localIps = localInstances.stream()
                                                      .collect(Collectors.toMap(instanceMetadata -> getIp(instanceMetadata.host()), InstanceMetadata::id));
        Map<String, Host> ipToHost = hosts.stream()
                                          .collect(Collectors.toMap(this::getIpFromHost, Function.identity()));

        return localIps.entrySet()
                       .stream()
                       .collect(Collectors.toMap(entry -> ipToHost.get(entry.getKey()), entry -> localIps.get(entry.getKey())));
    }

    private Metadata validatedMetadata(List<InstanceMetadata> localInstances)
    {
        if (localInstances.isEmpty())
        {
            LOGGER.warn("No local instances found");
            throw new RuntimeException("No local instances found");
        }
        return fetcher.callOnFirstAvailableInstance(instanceMetadata -> instanceMetadata.delegate().metadata());
    }

    private static Map<String, Map<Host, Set<TokenRange>>> perKeySpaceTokenRangesOfAllInstances(final Metadata metadata)
    {
        Map<String, Map<Host, Set<TokenRange>>> perKeyspaceTokenRanges = new HashMap<>();
        for (KeyspaceMetadata ks : metadata.getKeyspaces())
        {
            Map<Host, Set<TokenRange>> perHostTokenRanges = new HashMap<>();
            for (Host host : metadata.getAllHosts())
            {
                Set<TokenRange> tokenRanges = metadata.getTokenRanges(ks.getName(), host)
                                                      .stream()
                                                      .flatMap(range -> TokenRange.from(range).stream())
                                                      .collect(Collectors.toSet());
                perHostTokenRanges.put(host, tokenRanges);
            }
            perKeyspaceTokenRanges.put(ks.getName(), perHostTokenRanges);
        }
        return perKeyspaceTokenRanges;
    }

    public static Multimap<CassandraInstance, Range<BigInteger>> calculateTokenRanges(List<CassandraInstance> instances,
                                                                                      int replicationFactor,
                                                                                      Partitioner partitioner)
    {
        Preconditions.checkArgument(replicationFactor != 0, "Calculation token ranges wouldn't work with RF 0");
        Preconditions.checkArgument(instances.isEmpty() || replicationFactor <= instances.size(),
                                    "Calculation token ranges wouldn't work when RF ("
                                    + replicationFactor + ") is greater than number of Cassandra instances "
                                    + instances.size());
        Multimap<CassandraInstance, Range<BigInteger>> tokenRanges = ArrayListMultimap.create();

        for (int index = 0; index < instances.size(); ++index)
        {
            CassandraInstance instance = instances.get(index);
            int disjointReplica = (instances.size() + index - replicationFactor) % instances.size();
            BigInteger rangeStart = new BigInteger((instances.get(disjointReplica)).token);
            BigInteger rangeEnd = new BigInteger(instance.token);
            if (rangeStart.compareTo(rangeEnd) >= 0)
            {
                tokenRanges.put(instance, Range.openClosed(rangeStart, partitioner.maximumToken().toBigInteger()));
                if (!rangeEnd.equals(partitioner.minimumToken().toBigInteger()))
                    tokenRanges.put(instance, Range.openClosed(partitioner.minimumToken().toBigInteger(), rangeEnd));
            }
            else
            {
                tokenRanges.put(instance, Range.openClosed(rangeStart, rangeEnd));
            }
        }
        return tokenRanges;
    }

    /**
     * Class to encapsule Cassandra instance data
     */
    private static class CassandraInstance
    {
        private final String token;
        private final String node;

        public CassandraInstance(String token, String node)
        {
            this.token = token;
            this.node = node;
        }
    }
}
