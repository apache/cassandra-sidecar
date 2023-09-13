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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.adapters.base.NodeInfo.NodeState;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.data.GossipInfoResponse;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse.ReplicaInfo;
import org.apache.cassandra.sidecar.common.utils.GossipInfoParser;
import org.jetbrains.annotations.NotNull;

import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.sidecar.adapters.base.ClusterMembershipJmxOperations.FAILURE_DETECTOR_OBJ_NAME;
import static org.apache.cassandra.sidecar.adapters.base.EndpointSnitchJmxOperations.ENDPOINT_SNITCH_INFO_OBJ_NAME;
import static org.apache.cassandra.sidecar.adapters.base.StorageJmxOperations.STORAGE_SERVICE_OBJ_NAME;
import static org.apache.cassandra.sidecar.adapters.base.TokenRangeReplicas.generateTokenRangeReplicas;

/**
 * Aggregates the replica-set by token range
 */
public class TokenRangeReplicaProvider
{
    private interface KeyspaceToRangeMappingFunc extends Function<String, Map<List<String>, List<String>>>
    {
    }

    protected final JmxClient jmxClient;

    private static final Logger LOGGER = LoggerFactory.getLogger(TokenRangeReplicaProvider.class);

    public TokenRangeReplicaProvider(JmxClient jmxClient)
    {
        this.jmxClient = jmxClient;
    }

    public TokenRangeReplicasResponse tokenRangeReplicas(String keyspace, Partitioner partitioner)
    {
        Objects.requireNonNull(keyspace, "keyspace must be non-null");

        StorageJmxOperations storage = initializeStorageOps();

        List<TokenRangeReplicas> naturalTokenRangeReplicas =
            getTokenRangeReplicas("Natural", keyspace, partitioner, storage::getRangeToEndpointWithPortMap);
        // Pending ranges include bootstrap tokens and leaving endpoints as represented in the Cassandra TokenMetadata
        List<TokenRangeReplicas> pendingTokenRangeReplicas =
            getTokenRangeReplicas("Pending", keyspace, partitioner, storage::getPendingRangeToEndpointWithPortMap);

        // Merge natural and pending range replicas to generate candidates for write-replicas
        List<TokenRangeReplicas> allTokenRangeReplicas = new ArrayList<>(naturalTokenRangeReplicas);
        allTokenRangeReplicas.addAll(pendingTokenRangeReplicas);

        Map<String, String> hostToDatacenter = buildHostToDatacenterMapping(allTokenRangeReplicas);

        // Retrieve map of all token ranges (pending & primary) to endpoints
        List<ReplicaInfo> writeReplicas = writeReplicasFromPendingRanges(allTokenRangeReplicas, hostToDatacenter);

        List<ReplicaInfo> readReplicas = readReplicasFromReplicaMapping(naturalTokenRangeReplicas, hostToDatacenter);
        Map<String, String> replicaToStateMap = replicaToStateMap(allTokenRangeReplicas, storage);

        return new TokenRangeReplicasResponse(replicaToStateMap,
                                              writeReplicas,
                                              readReplicas);
    }

    private List<TokenRangeReplicas> getTokenRangeReplicas(String rangeType, String keyspace, Partitioner partitioner,
                                                           KeyspaceToRangeMappingFunc rangeMappingSupplier)
    {
        Map<List<String>, List<String>> rangeMappings = rangeMappingSupplier.apply(keyspace);
        LOGGER.debug(rangeType + " token range mappings for keyspace={}, rangeMappings={}", keyspace, rangeMappings);
        return transformRangeMappings(rangeMappings, partitioner);
    }

    private List<TokenRangeReplicas> transformRangeMappings(Map<List<String>, List<String>> replicaMappings,
                                                            Partitioner partitioner)
    {
        return replicaMappings.entrySet()
                              .stream()
                              .map(entry -> generateTokenRangeReplicas(new BigInteger(entry.getKey().get(0)),
                                                                       new BigInteger(entry.getKey().get(1)),
                                                                       partitioner,
                                                                       new HashSet<>(entry.getValue())))
                              .flatMap(Collection::stream)
                              .collect(toList());
    }

    private Map<String, String> replicaToStateMap(List<TokenRangeReplicas> replicaSet, StorageJmxOperations storage)
    {
        List<String> joiningNodes = storage.getJoiningNodesWithPort();
        List<String> leavingNodes = storage.getLeavingNodesWithPort();
        List<String> movingNodes = storage.getMovingNodesWithPort();

        String rawGossipInfo = getRawGossipInfo();
        GossipInfoResponse gossipInfo = GossipInfoParser.parse(rawGossipInfo);

        StateWithReplacement state = new StateWithReplacement(joiningNodes, leavingNodes, movingNodes, gossipInfo);

        return replicaSet.stream()
                         .map(TokenRangeReplicas::replicaSet)
                         .flatMap(Collection::stream)
                         .distinct()
                         .collect(Collectors.toMap(Function.identity(), state::of));
    }

    protected EndpointSnitchJmxOperations initializeEndpointProxy()
    {
        return jmxClient.proxy(EndpointSnitchJmxOperations.class, ENDPOINT_SNITCH_INFO_OBJ_NAME);
    }

    protected StorageJmxOperations initializeStorageOps()
    {
        return jmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME);
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

        return new ReplicaInfo(rep.start().toString(),
                               rep.end().toString(),
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
            .forEach(item ->
                     dcReplicaMapping.computeIfAbsent(hostToDatacenter.get(item), v -> new ArrayList<>())
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
                    if (hostStatus != null && hostStatus.startsWith("BOOT_REPLACE,"))
                    {
                        return NodeState.REPLACING.displayName();
                    }
                }
            }
            return super.of(endpoint);
        }
    }
}
