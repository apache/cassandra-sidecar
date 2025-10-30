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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Range;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.IClusterExtension;

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
                                IClusterExtension<? extends IInstance> cluster,
                                Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings)
    throws Exception
    {
        try
        {
            maybeReconfigureCMS(cluster);
            CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
            Set<String> dcReplication = getDcReplication(annotation);

            IInstance seed = cluster.get(1);

            List<IInstance> leavingNodes = new ArrayList<>();
            for (int i = 0; i < leavingNodesPerDC * annotation.numDcs(); i++)
            {
                IInstance node = cluster.get(cluster.size() - i);
                startAsync("Decommission node" + node.config().num(),
                           () -> node.nodetoolResult("decommission").asserts().success());
                leavingNodes.add(node);
            }

            // Wait until nodes have reached expected state
            awaitLatchOrThrow(transientStateStart, 2, TimeUnit.MINUTES, "transientStateStart");

            for (IInstance node : leavingNodes)
            {
                cluster.awaitRingState(seed, node, "Leaving");
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

                completeContextOrThrow(context);
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
                                        List<IInstance> leavingNodes,
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
