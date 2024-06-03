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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Partitioner;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for TokenRangeIntegrationMoving Tests
 */
class MovingBaseTest extends BaseTokenRangeIntegrationTest
{
    public static final int MOVING_NODE_IDX = 5;
    public static final int MULTIDC_MOVING_NODE_IDX = 10;

    void runMovingTestScenario(VertxTestContext context,
                               CountDownLatch transientStateStart,
                               CountDownLatch transientStateEnd,
                               UpgradeableCluster cluster,
                               Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings,
                               long moveTargetToken) throws Exception
    {
        try
        {
            CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
            Set<String> dcReplication;
            if (annotation.numDcs() > 1)
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
            int movingNodeIndex = (annotation.numDcs() > 1) ? MULTIDC_MOVING_NODE_IDX : MOVING_NODE_IDX;

            IUpgradeableInstance movingNode = cluster.get(movingNodeIndex);
            startAsync("move token of node" + movingNode.config().num() + " to " + moveTargetToken,
                       () -> movingNode.nodetoolResult("move", "--", Long.toString(moveTargetToken))
                                       .asserts()
                                       .success());

            // Wait until nodes have reached expected state
            awaitLatchOrThrow(transientStateStart, 2, TimeUnit.MINUTES, "transientStateStart");
            ClusterUtils.awaitRingState(seed, movingNode, "Moving");

            retrieveMappingWithKeyspace(context, TEST_KEYSPACE, response -> {
                assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                TokenRangeReplicasResponse mappingResponse = response.bodyAsJson(TokenRangeReplicasResponse.class);
                assertMappingResponseOK(mappingResponse,
                                        DEFAULT_RF,
                                        dcReplication);

                validateNodeStates(mappingResponse,
                                   dcReplication,
                                   nodeNumber -> nodeNumber == movingNodeIndex ? "Moving" : "Normal");
                List<Range<BigInteger>> expectedRanges = getMovingNodesExpectedRanges(annotation.nodesPerDc(),
                                                                                      annotation.numDcs(),
                                                                                      moveTargetToken);
                validateTokenRanges(mappingResponse, expectedRanges);
                validateReplicaMapping(mappingResponse, movingNode, moveTargetToken, expectedRangeMappings);

                completeContextOrThrow(context);
            });
        }
        finally
        {
            transientStateEnd.countDown();
        }
    }


    private void validateReplicaMapping(TokenRangeReplicasResponse mappingResponse,
                                        IUpgradeableInstance movingNode,
                                        long moveTo,
                                        Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings)
    {
        InetSocketAddress address = movingNode.config().broadcastAddress();
        String expectedAddress = address.getAddress().getHostAddress() +
                                 ":" +
                                 address.getPort();

        Set<String> writeReplicaInstances = instancesFromReplicaSet(mappingResponse.writeReplicas());
        Set<String> readReplicaInstances = instancesFromReplicaSet(mappingResponse.readReplicas());

        Optional<TokenRangeReplicasResponse.ReplicaInfo> moveResultRange // Get ranges ending in move token
        = mappingResponse.writeReplicas()
                         .stream()
                         .filter(r -> r.end().equals(String.valueOf(moveTo)))
                         .findAny();
        assertThat(moveResultRange).isPresent();
        List<String> replicasInRange = moveResultRange.get().replicasByDatacenter().values()
                                                      .stream()
                                                      .flatMap(Collection::stream)
                                                      .collect(Collectors.toList());
        assertThat(replicasInRange).contains(expectedAddress);
        assertThat(readReplicaInstances).contains(expectedAddress);
        assertThat(writeReplicaInstances).contains(expectedAddress);

        validateWriteReplicaMappings(mappingResponse.writeReplicas(), expectedRangeMappings);
    }

    protected List<Range<BigInteger>> getMovingNodesExpectedRanges(int initialNodeCount, int numDcs, long moveTo)
    {
        boolean moveHandled = false;
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                annotation.newNodesPerDc(),
                                                                                annotation.numDcs(),
                                                                                1);

        List<Range<BigInteger>> expectedRanges = new ArrayList<>();
        BigInteger startToken = Partitioner.Murmur3.minToken;
        BigInteger endToken = Partitioner.Murmur3.maxToken;
        int node = 1;
        BigInteger prevToken = new BigInteger(tokenSupplier.tokens(node++).stream().findFirst().get());
        Range<BigInteger> firstRange = Range.openClosed(startToken, prevToken);
        expectedRanges.add(firstRange);
        while (node <= (initialNodeCount * numDcs))
        {

            BigInteger currentToken = new BigInteger(tokenSupplier.tokens(node).stream().findFirst().get());
            if (!moveHandled && currentToken.compareTo(BigInteger.valueOf(moveTo)) > 0)
            {
                expectedRanges.add(Range.openClosed(prevToken, BigInteger.valueOf(moveTo)));
                expectedRanges.add(Range.openClosed(BigInteger.valueOf(moveTo), currentToken));
                moveHandled = true;
            }
            else
            {
                expectedRanges.add(Range.openClosed(prevToken, currentToken));
            }

            prevToken = currentToken;
            node++;
        }
        expectedRanges.add(Range.openClosed(prevToken, endToken));

        return expectedRanges;
    }

    protected long getMoveTargetToken(UpgradeableCluster cluster)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        IUpgradeableInstance seed = cluster.get(1);
        // The target token to move the node to is calculated by adding an offset to the seed node token which
        // is half of the range between 2 tokens.
        // For multi-DC case (specifically 2 DCs), since neighbouring tokens can be consecutive, we use tokens 1
        // and 3 to calculate the offset
        int nextIndex = (annotation.numDcs() > 1) ? 3 : 2;
        long t2 = Long.parseLong(seed.config().getString("initial_token"));
        long t3 = Long.parseLong(cluster.get(nextIndex).config().getString("initial_token"));
        return (t2 + ((t3 - t2) / 2));
    }
}
