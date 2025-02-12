package org.apache.cassandra.sidecar.coordination;

import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Token;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;


import static org.apache.cassandra.sidecar.config.yaml.CassandraInputValidationConfigurationImpl.DEFAULT_FORBIDDEN_KEYSPACES;


/**
 * Return Sidecar(s) adjacent to current Sidecar in the token ring within the same datacenter.
 */
@Singleton
public class InnerDcTokenAdjacentPeerProvider implements SidecarPeerProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(InnerDcTokenAdjacentPeerProvider.class);

    protected final InstancesMetadata instancesMetadata;
    private final CassandraClientTokenRingProvider cassandraClientTokenRingProvider;
    private final ServiceConfiguration serviceConfiguration;
    private final DnsResolver dnsResolver;

    @Inject
    public InnerDcTokenAdjacentPeerProvider(InstancesMetadata instancesMetadata,
                                            CassandraClientTokenRingProvider cassandraClientTokenRingProvider,
                                            ServiceConfiguration serviceConfiguration,
                                            DnsResolver dnsResolver)
    {
        this.instancesMetadata = instancesMetadata;
        this.cassandraClientTokenRingProvider = cassandraClientTokenRingProvider;
        this.serviceConfiguration = serviceConfiguration;
        this.dnsResolver = dnsResolver;
    }

    public Set<PeerInstance> get()
    {
        Map<Integer, InstanceMetadata> localInstances = instancesMetadata
                                                        .instances()
                                                        .stream()
                                                        .collect(Collectors.toMap(InstanceMetadata::id, Function.identity()));

        if (localInstances.isEmpty())
        {
            LOGGER.debug("No local instances found");
            return new HashSet<>();
        }

        Metadata metadata = localInstances.values().stream().findFirst()
                                          .map(in -> in.delegate().metadata())
                                          .orElse(null);
        if (metadata == null)
        {
            LOGGER.debug("Not yet connect to Cassandra cluster");
            return new HashSet<>();
        }

        final List<KeyspaceMetadata> keyspaces = metadata.getKeyspaces()
                                                         .stream()
                                                         .filter(ks -> !DEFAULT_FORBIDDEN_KEYSPACES.contains(ks.getName()))
                                                         .collect(Collectors.toList());
        if (keyspaces.isEmpty())
        {
            LOGGER.warn("No user keyspaces found");
            return new HashSet<>();
        }

        Set<Host> localHosts = cassandraClientTokenRingProvider.localInstances();
        final String localDc = Objects.requireNonNull(localHosts, "CachedLocalTokenRanges not initialized")
                                      .stream()
                                      .map(Host::getDatacenter)
                                      .filter(Objects::nonNull)
                                      .findAny()
                                      .orElseThrow(() -> new RuntimeException("No local instances found."));
        final Optional<KeyspaceMetadata> maxRfKeyspace = keyspaces.stream()
                                                                  .filter(ks -> ks.getReplication().containsKey(localDc))
                                                                  .max(Comparator.comparingInt(a -> Integer.parseInt(a.getReplication().get(localDc))));
        if (!maxRfKeyspace.isPresent())
        {
            LOGGER.info("No keyspace found replicated in DC dc={}", localDc);
            return new HashSet<>();
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
        return adjacentHosts(localHosts::contains, localMinToken, sortedLocalDcHosts, quorum)
               .stream()
               .map(host -> Pair.of(host, host.getAddress().getHostAddress()))
               .map(pair -> {
                   try
                   {
                       return Pair.of(pair.getKey(), dnsResolver.reverseResolve(pair.getValue()));
                   }
                   catch (UnknownHostException e)
                   {
                       return pair;
                   }
               })
               .map(pair -> new PeerInstanceImpl(pair.getValue(), sidecarServicePort(pair.getKey())))
               .collect(Collectors.toSet());
    }

    protected int sidecarServicePort(Host host)
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
    protected static Set<Host> adjacentHosts(Predicate<Host> isLocal,
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
                LOGGER.warn("Local instance selected as adjacent host localMinToken={} hostId={} address={} hostname={} canonicalHostname={}",
                            localMinToken,
                            host.getHostId(),
                            host.getAddress().getHostAddress(),
                            host.getAddress().getHostName(),
                            host.getAddress().getCanonicalHostName());
                throw new IllegalArgumentException(String.format("Local instance selected as adjacent host: %s", host.getHostId()));
            }
        }

        return adjacentHosts;
    }
}
