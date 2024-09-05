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

package org.apache.cassandra.sidecar.routes.tokenrange;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for TokenRangeIntegrationJoining tests
 */
class JoiningBaseTest extends BaseTokenRangeIntegrationTest
{
    void runJoiningTestScenario(VertxTestContext context,
                                CountDownLatch transientStateStart,
                                CountDownLatch transientStateEnd,
                                UpgradeableCluster cluster,
                                List<Range<BigInteger>> expectedRanges,
                                Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings,
                                boolean isCrossDCKeyspace)
    throws Exception
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        try
        {
            Set<String> dcReplication;
            if (annotation.numDcs() > 1 && isCrossDCKeyspace)
            {
                createTestKeyspace(ImmutableMap.of("replication_factor", DEFAULT_RF));
                dcReplication = Sets.newHashSet(Arrays.asList("datacenter1", "datacenter2"));
            }
            else
            {
                createTestKeyspace(ImmutableMap.of("datacenter1", DEFAULT_RF));
                dcReplication = Collections.singleton("datacenter1");
            }

            IUpgradeableInstance seed = cluster.get(1);

            List<IUpgradeableInstance> newInstances = new ArrayList<>();
            // Go over new nodes and add them once for each DC
            for (int i = 0; i < annotation.newNodesPerDc(); i++)
            {
                int dcNodeIdx = 1; // Use node 2's DC
                for (int dc = 1; dc <= annotation.numDcs(); dc++)
                {
                    IUpgradeableInstance dcNode = cluster.get(dcNodeIdx++);
                    IUpgradeableInstance newInstance = ClusterUtils.addInstance(cluster,
                                                                                dcNode.config().localDatacenter(),
                                                                                dcNode.config().localRack(),
                                                                                inst -> {
                                                                                    inst.set("auto_bootstrap", true);
                                                                                    inst.with(Feature.GOSSIP,
                                                                                              Feature.JMX,
                                                                                              Feature.NATIVE_PROTOCOL);
                                                                                });
                    startAsync("Start new node node" + newInstance.config().num(),
                               () -> newInstance.startup(cluster));
                    newInstances.add(newInstance);
                }
            }

            awaitLatchOrThrow(transientStateStart, 2, TimeUnit.MINUTES, "transientStateStart");

            for (IUpgradeableInstance newInstance : newInstances)
            {
                ClusterUtils.awaitRingState(seed, newInstance, "Joining");
            }

            retrieveMappingWithKeyspace(context, TEST_KEYSPACE, response -> {
                assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                TokenRangeReplicasResponse mappingResponse = response.bodyAsJson(TokenRangeReplicasResponse.class);
                assertMappingResponseOK(mappingResponse,
                                        DEFAULT_RF,
                                        dcReplication);
                int finalNodeCount = (annotation.nodesPerDc() + annotation.newNodesPerDc()) * annotation.numDcs();
                TokenSupplier tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                        annotation.newNodesPerDc(),
                                                                                        annotation.numDcs(),
                                                                                        1);
                // New split ranges resulting from joining nodes and corresponding tokens
                List<Range<BigInteger>> splitRanges = extractSplitRanges(annotation.newNodesPerDc() *
                                                                         annotation.numDcs(),
                                                                         finalNodeCount,
                                                                         tokenSupplier,
                                                                         expectedRanges);

                List<Integer> newNodes = newInstances.stream().map(i -> i.config().num()).collect(Collectors.toList());
                validateNodeStates(mappingResponse,
                                   dcReplication,
                                   nodeNumber -> newNodes.contains(nodeNumber) ? "Joining" : "Normal");

                validateTokenRanges(mappingResponse, expectedRanges);
                validateReplicaMapping(mappingResponse,
                                       newInstances,
                                       isCrossDCKeyspace,
                                       splitRanges,
                                       expectedRangeMappings);

                completeContextOrThrow(context);
            });
        }
        finally
        {
            for (int i = 0;
                 i < (annotation.newNodesPerDc() * annotation.numDcs()); i++)
            {
                transientStateEnd.countDown();
            }
        }
    }

    private void validateReplicaMapping(TokenRangeReplicasResponse mappingResponse,
                                        List<IUpgradeableInstance> newInstances,
                                        boolean isCrossDCKeyspace,
                                        List<Range<BigInteger>> splitRanges,
                                        Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings)
    {

        if (!isCrossDCKeyspace)
        {
            newInstances = newInstances.stream()
                                       .filter(i -> i.config().localDatacenter().equals("datacenter1"))
                                       .collect(Collectors.toList());
        }

        List<String> transientNodeAddresses = newInstances.stream().map(i -> {
            InetSocketAddress address = i.config().broadcastAddress();
            return address.getAddress().getHostAddress() +
                   ":" +
                   address.getPort();
        }).collect(Collectors.toList());

        Set<String> writeReplicaInstances = instancesFromReplicaSet(mappingResponse.writeReplicas());
        Set<String> readReplicaInstances = instancesFromReplicaSet(mappingResponse.readReplicas());

        Set<String> splitRangeReplicas = mappingResponse.writeReplicas().stream()
                                                        .filter(w -> matchSplitRanges(w, splitRanges))
                                                        .map(r ->
                                                             r.replicasByDatacenter().values())
                                                        .flatMap(Collection::stream)
                                                        .flatMap(Collection::stream)
                                                        .collect(Collectors.toSet());

        assertThat(readReplicaInstances).doesNotContainAnyElementsOf(transientNodeAddresses);
        // Validate that the new nodes are mapped to the split ranges
        assertThat(splitRangeReplicas).containsAll(transientNodeAddresses);
        assertThat(writeReplicaInstances).containsAll(transientNodeAddresses);

        validateWriteReplicaMappings(mappingResponse.writeReplicas(), expectedRangeMappings, isCrossDCKeyspace);
    }

    private List<Range<BigInteger>> extractSplitRanges(int newNodes,
                                                       int finalNodeCount,
                                                       TokenSupplier tokenSupplier,
                                                       List<Range<BigInteger>> expectedRanges)
    {

        int newNode = 1;
        List<BigInteger> newNodeTokens = new ArrayList<>();
        while (newNode <= newNodes)
        {
            int nodeIdx = finalNodeCount - newNode;
            newNodeTokens.add(new BigInteger(tokenSupplier.tokens(nodeIdx).stream().findFirst().get()));
            newNode++;
        }

        return expectedRanges.stream()
                             .filter(r -> newNodeTokens.contains(r.upperEndpoint()) ||
                                          newNodeTokens.contains(r.lowerEndpoint()))
                             .collect(Collectors.toList());
    }

    private boolean matchSplitRanges(TokenRangeReplicasResponse.ReplicaInfo range,
                                     List<Range<BigInteger>> expectedSplitRanges)
    {
        return expectedSplitRanges.stream()
                                  .anyMatch(s -> range.start().equals(s.lowerEndpoint().toString()) &&
                                                 range.end().equals(s.upperEndpoint().toString()));
    }

    void runJoiningTestScenario(VertxTestContext context,
                                ConfigurableCassandraTestContext cassandraTestContext,
                                BiConsumer<ClassLoader, Integer> instanceInitializer,
                                CountDownLatch transientStateStart,
                                CountDownLatch transientStateEnd,
                                Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings)
    throws Exception
    {

        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                annotation.newNodesPerDc(),
                                                                                annotation.numDcs(),
                                                                                1);

        UpgradeableCluster cluster = cassandraTestContext.configureAndStartCluster(builder -> {
            builder.withInstanceInitializer(instanceInitializer);
            builder.withTokenSupplier(tokenSupplier);
        });

        runJoiningTestScenario(context,
                               transientStateStart,
                               transientStateEnd,
                               cluster,
                               generateExpectedRanges(),
                               expectedRangeMappings,
                               true);
    }
}
