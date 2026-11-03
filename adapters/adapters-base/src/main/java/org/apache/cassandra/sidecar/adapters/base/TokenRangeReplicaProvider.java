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

package org.apache.cassandra.sidecar.adapters.base;

import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.management.InstanceNotFoundException;
import javax.management.ReflectionException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.adapters.base.NodeInfo.NodeState;
import org.apache.cassandra.sidecar.adapters.base.NodeInfo.NodeStatus;
import org.apache.cassandra.sidecar.adapters.base.data.ReplicaGroups;
import org.apache.cassandra.sidecar.adapters.base.jmx.ClusterMembershipJmxOperations;
import org.apache.cassandra.sidecar.adapters.base.jmx.EndpointSnitchJmxOperations;
import org.apache.cassandra.sidecar.adapters.base.jmx.GossipDependentStorageJmxOperations;
import org.apache.cassandra.sidecar.adapters.base.jmx.ReplicaGroupsInfoJmxOperations;
import org.apache.cassandra.sidecar.adapters.base.jmx.StorageJmxOperations;
import org.apache.cassandra.sidecar.common.response.GossipInfoResponse;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse.ReplicaInfo;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse.ReplicaMetadata;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Partitioner;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Token;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRangeReplicas;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.utils.GossipInfoParser;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.sidecar.adapters.base.jmx.ClusterMembershipJmxOperations.FAILURE_DETECTOR_OBJ_NAME;
import static org.apache.cassandra.sidecar.adapters.base.jmx.EndpointSnitchJmxOperations.ENDPOINT_SNITCH_INFO_OBJ_NAME;
import static org.apache.cassandra.sidecar.adapters.base.jmx.ReplicaGroupsInfoJmxOperations.REPLICA_GROUPS_INFO_OBJ_NAME;
import static org.apache.cassandra.sidecar.adapters.base.jmx.StorageJmxOperations.STORAGE_SERVICE_OBJ_NAME;
import static org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRangeReplicas.generateTokenRangeReplicas;

/**
 * Aggregates the replica-set by token range
 */
public class TokenRangeReplicaProvider
{

    private interface KeyspaceToRangeMappingFunc extends Function<String, Map<List<String>, List<String>>>
    {
    }

    protected final JmxClient jmxClient;
    private final DnsResolver dnsResolver;

    private static final Logger LOGGER = LoggerFactory.getLogger(TokenRangeReplicaProvider.class);
    private static final String UNKNOWN_STATUS = "Unknown";

    public TokenRangeReplicaProvider(JmxClient jmxClient, DnsResolver dnsResolver)
    {

        this.jmxClient = jmxClient;
        this.dnsResolver = dnsResolver;
    }

    public TokenRangeReplicasResponse tokenRangeReplicas(Name keyspace, Partitioner partitioner)
    {
        Objects.requireNonNull(keyspace, "keyspace must be non-null");

        // Cassandra versions that support witness replicas report the whole placement, witnesses included, in a
        // single call, so the response is built from one consistent view of the ring
        TokenRangeReplicasResponse fromReplicaGroups = tokenRangeReplicasFromReplicaGroups(keyspace.name(), partitioner);
        if (fromReplicaGroups != null)
        {
            return fromReplicaGroups;
        }

        StorageJmxOperations storage = initializeStorageOps();

        List<TokenRangeReplicas> naturalTokenRangeReplicas =
        getTokenRangeReplicas("Natural", keyspace.name(), partitioner, storage::getRangeToEndpointWithPortMap);
        // Pending ranges include bootstrap tokens and leaving endpoints as represented in the Cassandra TokenMetadata
        List<TokenRangeReplicas> pendingTokenRangeReplicas =
        getTokenRangeReplicas("Pending", keyspace.name(), partitioner, storage::getPendingRangeToEndpointWithPortMap);

        // Merge natural and pending range replicas to generate candidates for write-replicas
        List<TokenRangeReplicas> allTokenRangeReplicas = new ArrayList<>(naturalTokenRangeReplicas);
        allTokenRangeReplicas.addAll(pendingTokenRangeReplicas);

        Map<String, String> hostToDatacenter = buildHostToDatacenterMapping(allTokenRangeReplicas);

        // Retrieve map of all token ranges (pending & primary) to endpoints
        List<ReplicaInfo> writeReplicas = writeReplicasFromPendingRanges(allTokenRangeReplicas, hostToDatacenter);

        List<ReplicaInfo> readReplicas = readReplicasFromReplicaMapping(naturalTokenRangeReplicas, hostToDatacenter);
        Map<String, ReplicaMetadata> replicaMetadata = replicaMetadata(allTokenRangeReplicas,
                                                                       storage,
                                                                       hostToDatacenter);

        return new TokenRangeReplicasResponse(writeReplicas,
                                              readReplicas,
                                              replicaMetadata);
    }

    /**
     * @return the token range replicas built from Cassandra's ReplicaGroupsInfo MBean, witnesses included, or
     * {@code null} when they cannot be, in which case they are built from the StorageService MBean without witnesses
     */
    @Nullable
    private TokenRangeReplicasResponse tokenRangeReplicasFromReplicaGroups(String keyspace, Partitioner partitioner)
    {
        try
        {
            ReplicaGroups replicaGroups = new ReplicaGroups(initializeReplicaGroupsInfoProxy()
                                                            .replicaGroupsForKeyspaceByDatacenter(keyspace));
            return new TokenRangeReplicasResponse(replicaInfos(replicaGroups.writePlacement, replicaGroups.peers, partitioner),
                                                  replicaInfos(replicaGroups.readPlacement, replicaGroups.peers, partitioner),
                                                  replicaMetadata(replicaGroups.peers));
        }
        catch (RuntimeException exception)
        {
            // Witnesses are left unreported rather than reported as none, so clients that need them fail instead
            // of reading from a witness
            if (isMissingOperation(exception))
            {
                // Cassandra versions without witness replicas do not register the MBean
                LOGGER.debug("Cassandra does not report replica groups, keyspace={}", keyspace);
            }
            else
            {
                LOGGER.warn("Failed to retrieve replica groups, reporting token range replicas without witnesses "
                            + "for keyspace={}", keyspace, exception);
            }
            return null;
        }
    }

    /**
     * @return {@code true} if the JMX call failed because Cassandra has no such MBean or operation
     */
    @VisibleForTesting
    static boolean isMissingOperation(Throwable throwable)
    {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause())
        {
            if (cause instanceof InstanceNotFoundException
                || cause instanceof ReflectionException
                || cause instanceof NoSuchMethodException)
            {
                return true;
            }
        }
        return false;
    }

    private static List<ReplicaInfo> replicaInfos(List<ReplicaGroups.RangePlacement> placement,
                                                  Map<Integer, ReplicaGroups.Peer> peers,
                                                  Partitioner partitioner)
    {
        List<ReplicaInfo> replicaInfos = new ArrayList<>();
        for (ReplicaGroups.RangePlacement range : placement)
        {
            Map<String, List<String>> replicasByDc = new HashMap<>();
            Map<String, List<String>> witnessesByDc = new HashMap<>();
            for (Map.Entry<String, List<ReplicaGroups.Replica>> byDatacenter : range.replicasByDatacenter.entrySet())
            {
                for (ReplicaGroups.Replica replica : byDatacenter.getValue())
                {
                    String endpoint = endpoint(peers, replica.nodeId);
                    replicasByDc.computeIfAbsent(byDatacenter.getKey(), dc -> new ArrayList<>()).add(endpoint);
                    if (replica.isTransient)
                    {
                        witnessesByDc.computeIfAbsent(byDatacenter.getKey(), dc -> new ArrayList<>()).add(endpoint);
                    }
                }
            }
            // The range that wraps ends at the minimum token; it is reported ending at the maximum token instead, as
            // the StorageService path reports it
            for (TokenRangeReplicas bounds : generateTokenRangeReplicas(Token.from(range.start),
                                                                        Token.from(range.end),
                                                                        partitioner,
                                                                        Collections.emptySet()))
            {
                replicaInfos.add(new ReplicaInfo(bounds.start().toBigInteger().toString(),
                                                 bounds.end().toBigInteger().toString(),
                                                 replicasByDc,
                                                 witnessesByDc));
            }
        }
        replicaInfos.sort(Comparator.comparing((ReplicaInfo info) -> new BigInteger(info.start()))
                                    .thenComparing(info -> new BigInteger(info.end())));
        return replicaInfos;
    }

    private static String endpoint(Map<Integer, ReplicaGroups.Peer> peers, int nodeId)
    {
        ReplicaGroups.Peer peer = peers.get(nodeId);
        if (peer == null)
        {
            throw new IllegalStateException("Replica group references node " + nodeId + ", which is not in peer_metadata");
        }
        return peer.broadcastAddressAndPort;
    }

    private Map<String, ReplicaMetadata> replicaMetadata(Map<Integer, ReplicaGroups.Peer> peers)
    {
        Map<String, ReplicaMetadata> replicaMetadata = new HashMap<>(peers.size());
        for (ReplicaGroups.Peer peer : peers.values())
        {
            HostAndPort hap = HostAndPort.fromString(peer.broadcastAddressAndPort);
            String fqdn;
            try
            {
                fqdn = dnsResolver.reverseResolve(hap.getHost());
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(String.format("Failed to resolve fqdn for replica %s ", peer.broadcastAddressAndPort), e);
            }
            replicaMetadata.put(peer.broadcastAddressAndPort, new ReplicaMetadata(state(peer.nodeState),
                                                                                 status(peer.nodeStatus),
                                                                                 fqdn,
                                                                                 hap.getHost(),
                                                                                 hap.getPort(),
                                                                                 peer.datacenter));
        }
        return replicaMetadata;
    }

    /**
     * @return the state reported for a node, from its cluster metadata state
     */
    @VisibleForTesting
    static String state(String nodeState)
    {
        switch (nodeState)
        {
            case "REGISTERED":
            case "BOOTSTRAPPING":
                return NodeState.JOINING.displayName();
            case "BOOT_REPLACING":
                return NodeState.REPLACING.displayName();
            case "LEAVING":
            case "LEFT":
                return NodeState.LEAVING.displayName();
            case "MOVING":
                return NodeState.MOVING.displayName();
            default:
                return NodeState.NORMAL.displayName();
        }
    }

    /**
     * @return the status reported for a node, from its failure detector status
     */
    @VisibleForTesting
    static String status(String nodeStatus)
    {
        switch (nodeStatus)
        {
            case "UP":
                return NodeStatus.UP.displayName();
            case "DOWN":
                return NodeStatus.DOWN.displayName();
            default:
                return UNKNOWN_STATUS;
        }
    }

    private List<TokenRangeReplicas> getTokenRangeReplicas(String rangeType, String keyspace, Partitioner partitioner,
                                                           KeyspaceToRangeMappingFunc rangeMappingSupplier)
    {
        Map<List<String>, List<String>> rangeMappings = rangeMappingSupplier.apply(keyspace);
        LOGGER.debug("{} token range mappings for keyspace={}, rangeMappings={}", rangeType, keyspace, rangeMappings);
        return transformRangeMappings(rangeMappings, partitioner);
    }

    private List<TokenRangeReplicas> transformRangeMappings(Map<List<String>, List<String>> replicaMappings,
                                                            Partitioner partitioner)
    {
        return replicaMappings.entrySet()
                              .stream()
                              .map(entry -> generateTokenRangeReplicas(Token.from(entry.getKey().get(0)),
                                                                       Token.from(entry.getKey().get(1)),
                                                                       partitioner,
                                                                       new HashSet<>(entry.getValue())))
                              .flatMap(Collection::stream)
                              .collect(toList());
    }

    private Map<String, ReplicaMetadata> replicaMetadata(List<TokenRangeReplicas> replicaSet,
                                                         StorageJmxOperations storage,
                                                         Map<String, String> hostToDatacenter)
    {
        List<String> joiningNodes = storage.getJoiningNodesWithPort();
        List<String> leavingNodes = storage.getLeavingNodesWithPort();
        List<String> movingNodes = storage.getMovingNodesWithPort();

        List<String> liveNodes = storage.getLiveNodesWithPort();
        List<String> deadNodes = storage.getUnreachableNodesWithPort();


        String rawGossipInfo = getRawGossipInfo();
        GossipInfoResponse gossipInfo = GossipInfoParser.parse(rawGossipInfo);

        StateWithReplacement state = new StateWithReplacement(joiningNodes, leavingNodes, movingNodes, gossipInfo);
        RingProvider.Status status = new RingProvider.Status(liveNodes, deadNodes);

        return replicaSet.stream()
                         .map(TokenRangeReplicas::replicaSet)
                         .flatMap(Collection::stream)
                         .distinct()
                         .map(replica -> {
                             try
                             {
                                 HostAndPort hap = HostAndPort.fromString(replica);
                                 String fqdn = dnsResolver.reverseResolve(hap.getHost());
                                 String datacenter = hostToDatacenter.get(replica);
                                 return new AbstractMap.SimpleEntry<>(replica,
                                                                      new ReplicaMetadata(state.of(replica),
                                                                                          status.of(replica),
                                                                                          fqdn,
                                                                                          hap.getHost(),
                                                                                          hap.getPort(),
                                                                                          datacenter));
                             }
                             catch (UnknownHostException e)
                             {
                                 throw new RuntimeException(
                                 String.format("Failed to resolve fqdn for replica %s ", replica), e);
                             }
                         })
                         .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

    protected ReplicaGroupsInfoJmxOperations initializeReplicaGroupsInfoProxy()
    {
        return jmxClient.proxy(ReplicaGroupsInfoJmxOperations.class, REPLICA_GROUPS_INFO_OBJ_NAME);
    }

    protected EndpointSnitchJmxOperations initializeEndpointProxy()
    {
        return jmxClient.proxy(EndpointSnitchJmxOperations.class, ENDPOINT_SNITCH_INFO_OBJ_NAME);
    }

    protected StorageJmxOperations initializeStorageOps()
    {
        return new GossipDependentStorageJmxOperations(jmxClient.proxy(StorageJmxOperations.class,
                                                                       STORAGE_SERVICE_OBJ_NAME));
    }

    protected String getRawGossipInfo()
    {
        return jmxClient.proxy(ClusterMembershipJmxOperations.class, FAILURE_DETECTOR_OBJ_NAME)
                        .getAllEndpointStatesWithPort();
    }

    private List<ReplicaInfo> writeReplicasFromPendingRanges(List<TokenRangeReplicas> tokenRangeReplicaSet,
                                                             Map<String, String> hostToDatacenter)
    {
        // Candidate write-replica mappings are normalized by consolidating overlapping ranges
        return TokenRangeReplicas.normalize(tokenRangeReplicaSet).stream()
                                 .map(range -> buildReplicaInfo(hostToDatacenter, range))
                                 .collect(toList());
    }

    private List<ReplicaInfo> readReplicasFromReplicaMapping(List<TokenRangeReplicas> naturalTokenRangeReplicas,
                                                             Map<String, String> hostToDatacenter)
    {
        return naturalTokenRangeReplicas.stream()
                                        .sorted()
                                        .map(rep -> buildReplicaInfo(hostToDatacenter, rep))
                                        .collect(toList());
    }

    @NotNull
    private static ReplicaInfo buildReplicaInfo(Map<String, String> hostToDatacenter, TokenRangeReplicas rep)
    {
        Map<String, List<String>> replicasByDc = replicasByDataCenter(hostToDatacenter, rep.replicaSet());

        return new ReplicaInfo(rep.start().toBigInteger().toString(),
                               rep.end().toBigInteger().toString(),
                               replicasByDc);
    }

    private Map<String, String> buildHostToDatacenterMapping(List<TokenRangeReplicas> replicaSet)
    {
        EndpointSnitchJmxOperations endpointSnitchInfo = initializeEndpointProxy();

        return replicaSet.stream()
                         .map(TokenRangeReplicas::replicaSet)
                         .flatMap(Collection::stream)
                         .distinct()
                         .collect(Collectors.toMap(Function.identity(),
                                                   (String host) -> getDatacenter(endpointSnitchInfo, host)));
    }

    private String getDatacenter(EndpointSnitchJmxOperations endpointSnitchInfo, String host)
    {
        try
        {
            return endpointSnitchInfo.getDatacenter(host);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    private static Map<String, List<String>> replicasByDataCenter(Map<String, String> hostToDatacenter,
                                                                  Collection<String> replicas)
    {

        Map<String, List<String>> dcReplicaMapping = new HashMap<>();

        replicas.stream()
                .filter(hostToDatacenter::containsKey)
                .forEach(item -> dcReplicaMapping.computeIfAbsent(hostToDatacenter.get(item), v -> new ArrayList<>())
                                                 .add(item));
        return dcReplicaMapping;
    }

    /**
     * We want to identity a joining node, to replace a dead node, differently from a newly joining node. To
     * do this we analyze gossip info and set 'Replacing' state for node replacing a dead node.
     * {@link StateWithReplacement} is used to set replacing state for a node.
     *
     * <p>We are adding this state for token range replica provider endpoint. To send out replicas for a
     * range along with state of replicas including replacing state.
     */
    static class StateWithReplacement extends RingProvider.State
    {
        private final GossipInfoResponse gossipInfo;

        StateWithReplacement(List<String> joiningNodes, List<String> leavingNodes, List<String> movingNodes,
                             GossipInfoResponse gossipInfo)
        {
            super(joiningNodes, leavingNodes, movingNodes);
            this.gossipInfo = gossipInfo;
        }

        /**
         * This method returns state of a node and accounts for a new 'Replacing' state if the node is
         * replacing a dead node. For returning this state, the method checks status of the node in gossip
         * information.
         *
         * @param endpoint node information represented usually in form of 'ip:port'
         * @return Node status
         */
        @Override
        String of(String endpoint)
        {
            if (joiningNodes.contains(endpoint))
            {
                GossipInfoResponse.GossipInfo gossipInfoEntry = gossipInfo.get(endpoint);

                if (gossipInfoEntry != null)
                {
                    LOGGER.debug("Found gossipInfoEntry={}", gossipInfoEntry);
                    String hostStatus = gossipInfoEntry.status();
                    String hostStatusWithPort = gossipInfoEntry.statusWithPort();
                    if ((hostStatus != null && hostStatus.startsWith("BOOT_REPLACE,")) ||
                        (hostStatusWithPort != null && hostStatusWithPort.startsWith("BOOT_REPLACE,")))
                    {
                        return NodeState.REPLACING.displayName();
                    }
                }
            }
            return super.of(endpoint);
        }
    }
}
