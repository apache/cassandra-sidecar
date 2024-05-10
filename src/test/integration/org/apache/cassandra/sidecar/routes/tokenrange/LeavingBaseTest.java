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
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for TokenRangeIntegrationLeaving Tests
 */
class LeavingBaseTest extends BaseTokenRangeIntegrationTest
{
    void runLeavingTestScenario(VertxTestContext context,
                                int leavingNodesPerDC,
                                CountDownLatch transientStateStart,
                                CountDownLatch transientStateEnd,
                                UpgradeableCluster cluster,
                                Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings)
    throws Exception
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

            List<IUpgradeableInstance> leavingNodes = new ArrayList<>();
            for (int i = 0; i < leavingNodesPerDC * annotation.numDcs(); i++)
            {
                IUpgradeableInstance node = cluster.get(cluster.size() - i);
                new Thread(() -> node.nodetoolResult("decommission").asserts().success()).start();
                leavingNodes.add(node);
            }

            // Wait until nodes have reached expected state
            awaitLatchOrTimeout(transientStateStart, 2, TimeUnit.MINUTES);

            for (IUpgradeableInstance node : leavingNodes)
            {
                ClusterUtils.awaitRingState(seed, node, "Leaving");
            }

            retrieveMappingWithKeyspace(context, TEST_KEYSPACE, response -> {
                assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                TokenRangeReplicasResponse mappingResponse = response.bodyAsJson(TokenRangeReplicasResponse.class);
                assertMappingResponseOK(mappingResponse,
                                        DEFAULT_RF,
                                        dcReplication);

                int initialNodeCount = annotation.nodesPerDc() * annotation.numDcs();
                validateNodeStates(mappingResponse,
                                   dcReplication,
                                   nodeNumber ->
                                   nodeNumber <= (initialNodeCount - (leavingNodesPerDC * annotation.numDcs())) ?
                                   "Normal" :
                                   "Leaving");
                validateTokenRanges(mappingResponse, generateExpectedRanges());
                validateReplicaMapping(mappingResponse, leavingNodes, expectedRangeMappings);

                context.completeNow();
            });
        }
        finally
        {
            for (int i = 0; i < leavingNodesPerDC; i++)
            {
                transientStateEnd.countDown();
            }
        }
    }

    private void validateReplicaMapping(TokenRangeReplicasResponse mappingResponse,
                                        List<IUpgradeableInstance> leavingNodes,
                                        Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings)
    {
        List<String> transientNodeAddresses = leavingNodes.stream().map(i -> {
            InetSocketAddress address = i.config().broadcastAddress();
            return address.getAddress().getHostAddress() +
                   ":" +
                   address.getPort();
        }).collect(Collectors.toList());

        Set<String> writeReplicaInstances = instancesFromReplicaSet(mappingResponse.writeReplicas());
        Set<String> readReplicaInstances = instancesFromReplicaSet(mappingResponse.readReplicas());
        assertThat(readReplicaInstances).containsAll(transientNodeAddresses);
        assertThat(writeReplicaInstances).containsAll(transientNodeAddresses);

        validateWriteReplicaMappings(mappingResponse.writeReplicas(), expectedRangeMappings);
    }
}
