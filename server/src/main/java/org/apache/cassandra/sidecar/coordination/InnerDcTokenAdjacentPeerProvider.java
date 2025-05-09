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

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Token;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.utils.DriverUtils;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.VisibleForTesting;

import static org.apache.cassandra.sidecar.config.yaml.CassandraInputValidationConfigurationImpl.DEFAULT_FORBIDDEN_KEYSPACES;


/**
 * Return Sidecar(s) adjacent to current Sidecar in the token ring within the same datacenter.
 */
@Singleton
public class InnerDcTokenAdjacentPeerProvider implements SidecarPeerProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(InnerDcTokenAdjacentPeerProvider.class);

    protected final InstanceMetadataFetcher instanceFetcher;
    private final CassandraClientTokenRingProvider cassandraClientTokenRingProvider;
    private final ServiceConfiguration serviceConfiguration;
    private final DnsResolver dnsResolver;
    private final DriverUtils driverUtils;

    @Inject
    public InnerDcTokenAdjacentPeerProvider(InstanceMetadataFetcher instanceFetcher,
                                            CassandraClientTokenRingProvider cassandraClientTokenRingProvider,
                                            ServiceConfiguration serviceConfiguration,
                                            DnsResolver dnsResolver,
                                            DriverUtils driverUtils)
    {
        this.instanceFetcher = instanceFetcher;
        this.cassandraClientTokenRingProvider = cassandraClientTokenRingProvider;
        this.serviceConfiguration = serviceConfiguration;
        this.dnsResolver = dnsResolver;
        this.driverUtils = driverUtils;
    }

    @Override
    public Set<SidecarInstance> get()
    {
        Metadata metadata;
        try
        {
            metadata = instanceFetcher.callOnFirstAvailableInstance(instance -> instance.delegate().metadata());
        }
        catch (Throwable cause)
        {
            LOGGER.debug("Unable to retrieve metadata", cause);
            return Set.of();
        }

        List<KeyspaceMetadata> keyspaces = metadata.getKeyspaces()
                                                   .stream()
                                                   // TODO: this should be from configured
                                                   .filter(ks -> !DEFAULT_FORBIDDEN_KEYSPACES.contains(ks.getName()))
                                                   .collect(Collectors.toList());
        if (keyspaces.isEmpty())
        {
            LOGGER.warn("No user keyspaces found");
            return Set.of();
        }

        Set<Host> localHosts = cassandraClientTokenRingProvider.localInstances();
        String localDc = Objects.requireNonNull(localHosts, "CachedLocalTokenRanges not initialized")
                                .stream()
                                .map(Host::getDatacenter)
                                .filter(Objects::nonNull)
                                .findAny()
                                .orElseThrow(() -> new RuntimeException("No local instances found."));
        Optional<KeyspaceMetadata> maxRfKeyspace = keyspaces.stream()
                                                            .filter(ks -> ks.getReplication().containsKey(localDc))
                                                            .max(Comparator.comparingInt(a -> Integer.parseInt(a.getReplication().get(localDc))));
        if (maxRfKeyspace.isEmpty())
        {
            LOGGER.info("No keyspace found replicated in DC dc={}", localDc);
            return Set.of();
        }

        int rf = Integer.parseInt(maxRfKeyspace.get().getReplication().get(localDc));
        int quorum = rf / 2;
        List<Pair<Host, BigInteger>> sortedLocalDcHosts = Objects.requireNonNull(cassandraClientTokenRingProvider.allInstances(),
                                                                                 "CachedLocalTokenRanges not initialized").stream()
                                                                 .filter(host -> host.getDatacenter().equals(localDc))
                                                                 .map(host -> Pair.of(host, minToken(host)))
                                                                 .sorted(Comparator.comparing(Pair::getRight))
                                                                 .collect(Collectors.toList());

        BigInteger localMinToken = minToken(localHosts);
        return adjacentHosts(driverUtils, localHosts::contains, localMinToken, sortedLocalDcHosts, quorum)
               .stream()
               .map(host -> driverUtils.getSocketAddress(host).getAddress().getHostAddress())
               .map(sidecarIpAddress -> {
                   String sidecarHostname = sidecarIpAddress;
                   try
                   {
                       sidecarHostname = dnsResolver.reverseResolve(sidecarIpAddress);
                   }
                   catch (UnknownHostException unknownHostException)
                   {
                       LOGGER.warn("Unable to reverse resolve hostname for {}", sidecarHostname, unknownHostException);
                   }
                   return new SidecarInstanceImpl(sidecarHostname, sidecarServicePort(sidecarHostname));
               })
               .collect(Collectors.toSet());
    }

    @VisibleForTesting
    protected int sidecarServicePort(String sidecarHostname)
    {
        return serviceConfiguration.port();
    }

    public static BigInteger minToken(Collection<Host> hosts)
    {
        return hosts.stream()
                    .map(InnerDcTokenAdjacentPeerProvider::minToken)
                    .min(BigInteger::compareTo)
                    .orElseThrow(() -> new RuntimeException("No min token found on hosts"));
    }

    public static BigInteger minToken(Host host)
    {
        return host.getTokens()
                   .stream()
                   .map(InnerDcTokenAdjacentPeerProvider::tokenToBigInteger)
                   .min(BigInteger::compareTo)
                   .orElseThrow(() -> new RuntimeException("No min token found on host: " + host.getHostId()));
    }

    public static BigInteger tokenToBigInteger(com.datastax.driver.core.Token token)
    {
        if (token.getType() == DataType.varint()) // BigInteger - RandomPartitioner
        {
            return (BigInteger) token.getValue();
        }
        else if (token.getType() == DataType.bigint()) // Long - Murmur3Partitioner
        {
            return BigInteger.valueOf((Long) token.getValue());
        }
        throw new IllegalArgumentException("Unsupported token type: " + token.getType() +
                                           ". Only tokens of Murmur3Partitioner and RandomPartitioner are supported.");
    }

    protected static BigInteger minToken(Stream<TokenRange> tokenRanges)
    {
        return tokenRanges
               .map(TokenRange::start)
               .min(Token::compareTo)
               .map(Token::toBigInteger)
               .orElseThrow(() -> new IllegalStateException("No tokens for host"));
    }

    /**
     * Using the minToken per host find the next adjacent host(s) in the token ring
     *
     * @param isLocal            predicate that returns true if host is local to the Sidecar, used to validate output.
     * @param localMinToken      min token owned by this Sidecar
     * @param sortedLocalDcHosts list of dc-local Cassandra hosts sorted by minToken
     * @param quorum             minimum availability required to meet maximum replication factor in DC
     * @return set of hosts that are adjacent to current Sidecar
     */
    protected static Set<Host> adjacentHosts(DriverUtils driverUtils,
                                             Predicate<Host> isLocal,
                                             BigInteger localMinToken,
                                             List<Pair<Host, BigInteger>> sortedLocalDcHosts,
                                             int quorum)
    {
        Set<Host> adjacentHosts = new HashSet<>(quorum);

        // all hosts in token order
        int idx = Collections.binarySearch(sortedLocalDcHosts, null, (o1, o2) -> {
            BigInteger token1 = (o1 == null) ? localMinToken : o1.getValue();
            BigInteger token2 = (o2 == null) ? localMinToken : o2.getValue();
            return token1.compareTo(token2);
        });

        if (idx < 0)
        {
            throw new IllegalStateException("Could not find local instance");
        }

        for (int i = 1; i <= quorum; i++)
        {
            // if max RF is greater than the number of other available hosts then it will wrap around
            int nextIdx = (idx + i) % (sortedLocalDcHosts.size());
            Host nextHost = sortedLocalDcHosts.get(nextIdx).getKey();
            if (isLocal.test(nextHost))
            {
                LOGGER.warn("Insufficient other hosts to satisfy quorum quorum={} numHosts={}", quorum, i);
                quorum = i - 1;
                break;
            }
        }

        for (int i = 1; i <= quorum; i++)
        {
            int nextIdx = (idx + i) % (sortedLocalDcHosts.size());
            adjacentHosts.add(sortedLocalDcHosts.get(nextIdx).getKey());
        }

        Preconditions.checkArgument(adjacentHosts.size() == quorum, String.format("Failed to find %d adjacent node(s) in the ring", quorum));
        for (Host host : adjacentHosts)
        {
            if (isLocal.test(host))
            {
                InetAddress address = driverUtils.getSocketAddress(host).getAddress();
                LOGGER.warn("Local instance selected as adjacent host localMinToken={} hostId={} address={} hostname={} canonicalHostname={}",
                            localMinToken,
                            host.getHostId(),
                            address.getHostAddress(),
                            address.getHostName(),
                            address.getCanonicalHostName());
                throw new IllegalArgumentException(String.format("Local instance selected as adjacent host: %s", host.getHostId()));
            }
        }

        return adjacentHosts;
    }
}
