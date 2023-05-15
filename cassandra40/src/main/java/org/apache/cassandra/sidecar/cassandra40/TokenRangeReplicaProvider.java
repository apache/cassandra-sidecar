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

package org.apache.cassandra.sidecar.cassandra40;

import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.cassandra40.EndpointSnitchJmxOperations.ENDPOINT_SNITCH_INFO_OBJ_NAME;
import static org.apache.cassandra.sidecar.cassandra40.StorageJmxOperations.STORAGE_SERVICE_OBJ_NAME;

/**
 * Aggregates the replica-set by token range
 */
public class TokenRangeReplicaProvider
{
    private final JmxClient jmxClient;

    private static final Logger LOGGER = LoggerFactory.getLogger(TokenRangeReplicaProvider.class);

    public TokenRangeReplicaProvider(JmxClient jmxClient)
    {
        this.jmxClient = jmxClient;
    }

    public TokenRangeReplicasResponse tokenRangeReplicas(String keyspace, Partitioner partitioner)
    {
        Objects.requireNonNull(keyspace, "keyspace must be non-null");

        StorageJmxOperations storage = jmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME);

        // Retrieve map of primary token ranges to endpoints that describe the ring topology
        Map<List<String>, List<String>> rangeToEndpointMappings = storage.getRangeToEndpointWithPortMap(keyspace);
        // Pending ranges include bootstrap tokens and leaving endpoints as represented in the Cassandra TokenMetadata
        Map<List<String>, List<String>> pendingRangeMappings = storage.getPendingRangeToEndpointWithPortMap(keyspace);

        Stream<String> hostsStream = Stream.concat(rangeToEndpointMappings.values().stream().flatMap(List::stream),
                                                   pendingRangeMappings.values().stream().flatMap(List::stream));

        Map<String, String> hostToDatacenter = groupHostsByDatacenter(hostsStream);

        // Retrieve map of all token ranges (pending & primary) to endpoints
        List<TokenRangeReplicasResponse.ReplicaInfo> writeReplicas =
        writeReplicasFromPendingRanges(rangeToEndpointMappings,
                                       pendingRangeMappings,
                                       hostToDatacenter,
                                       partitioner,
                                       keyspace);

        return new TokenRangeReplicasResponse(
                writeReplicas,
                mappingsToUnwrappedReplicaSet(rangeToEndpointMappings, hostToDatacenter, partitioner));
    }

    private List<TokenRangeReplicasResponse.ReplicaInfo>
    writeReplicasFromPendingRanges(Map<List<String>, List<String>> naturalReplicaMappings,
                                   Map<List<String>, List<String>> pendingRangeMappings,
                                   Map<String, String> hostToDatacenter,
                                   Partitioner partitioner,
                                   String keyspace)
    {
        LOGGER.debug("Pending token ranges for keyspace={}, pendingRangeMappings={}", keyspace, pendingRangeMappings);
        // Merge natural and pending range replicas to generate candidates for write-replicas
        List<TokenRangeReplicas> replicas = Stream.concat(
                                                  naturalReplicaMappings.entrySet().stream(),
                                                  pendingRangeMappings.entrySet().stream())
                                                  .map(entry ->
                                                       new TokenRangeReplicas(
                                                               new BigInteger(entry.getKey().get(0)),
                                                               new BigInteger(entry.getKey().get(1)),
                                                               partitioner,
                                                               new HashSet<>(entry.getValue())))
                                                  .collect(Collectors.toList());

        // Candidate write-replica mappings (merged from natural and pending ranges) are normalized by consolidate
        // overlapping ranges. For an overlapping range that is included in both natural and pending ranges, say
        // R_natural and R_pending (where R_natural == R_pending), the replicas of both R_natural and R_pending should
        // receive writes. Therefore, the write-replicas of such range is the union of both replica sets.
        // The procedure (esp. normalize method) implements the consolidation process.
        return TokenRangeReplicas.normalize(replicas).stream()
                                 .map(range -> {
                                     Map<String, List<String>> replicasByDc =
                                     replicasByDataCenter(hostToDatacenter, range.replicaSet());
                                     return new TokenRangeReplicasResponse.ReplicaInfo(range.start().toString(),
                                                                                       range.end().toString(),
                                                                                       replicasByDc);
                                 })
                                 .collect(Collectors.toList());
    }

    private List<TokenRangeReplicasResponse.ReplicaInfo>
    mappingsToUnwrappedReplicaSet(Map<List<String>, List<String>> replicasByTokenRange,
                                  Map<String, String> hostToDatacenter,
                                  Partitioner partitioner)
    {
        return replicasByTokenRange.entrySet().stream()
                .map(entry -> new TokenRangeReplicas(new BigInteger(entry.getKey().get(0)),
                                                     new BigInteger(entry.getKey().get(1)),
                                                      partitioner,
                                                      new HashSet<>(entry.getValue())))
                .flatMap(r -> r.unwrap().stream())
                .map(rep -> {
                    Map<String, List<String>> replicasByDc = replicasByDataCenter(hostToDatacenter, rep.replicaSet());
                    return new TokenRangeReplicasResponse.ReplicaInfo(rep.start().toString(),
                            rep.end().toString(),
                            replicasByDc);
                })
                .collect(Collectors.toList());
    }

    private Map<String, String> groupHostsByDatacenter(Stream<String> hostsStream)
    {
        EndpointSnitchJmxOperations endpointSnitchInfo = jmxClient.proxy(EndpointSnitchJmxOperations.class,
                                                                         ENDPOINT_SNITCH_INFO_OBJ_NAME);

        return hostsStream.distinct()
                          .collect(Collectors.toMap(Function.identity(),
                                                    (String host) -> dataCenter(endpointSnitchInfo, host)));
    }

    private String dataCenter(EndpointSnitchJmxOperations endpointSnitchInfo, String host)
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
        return replicas.stream().collect(Collectors.groupingBy(hostToDatacenter::get));
    }
}
