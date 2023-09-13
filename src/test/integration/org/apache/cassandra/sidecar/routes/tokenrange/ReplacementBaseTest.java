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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for the TokenRangeIntegrationReplacement Tests
 */
class ReplacementBaseTest extends BaseTokenRangeIntegrationTest
{
    protected void runReplacementTestScenario(VertxTestContext context,
                                              CountDownLatch nodeStart,
                                              CountDownLatch transientStateStart,
                                              CountDownLatch transientStateEnd,
                                              UpgradeableCluster cluster,
                                              List<IUpgradeableInstance> nodesToRemove,
                                              Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings)
    throws Exception
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        try
        {
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
            List<String> removedNodeAddresses = nodesToRemove.stream()
                                                             .map(n ->
                                                                  n.config()
                                                                   .broadcastAddress()
                                                                   .getAddress()
                                                                   .getHostAddress())
                                                             .collect(Collectors.toList());

            List<ClusterUtils.RingInstanceDetails> ring = ClusterUtils.ring(seed);
            List<String> removedNodeTokens = ring.stream()
                                                 .filter(i -> removedNodeAddresses.contains(i.getAddress()))
                                                 .map(ClusterUtils.RingInstanceDetails::getToken)
                                                 .collect(Collectors.toList());

            stopNodes(seed, nodesToRemove);
            List<IUpgradeableInstance> newNodes = startReplacementNodes(nodeStart, cluster, nodesToRemove);

            // Wait until replacement nodes are in JOINING state
            Uninterruptibles.awaitUninterruptibly(transientStateStart, 2, TimeUnit.MINUTES);

            // Verify state of replacement nodes
            for (IUpgradeableInstance newInstance : newNodes)
            {
                ClusterUtils.awaitRingState(newInstance, newInstance, "Joining");
                ClusterUtils.awaitGossipStatus(newInstance, newInstance, "BOOT_REPLACE");

                String newAddress = newInstance.config().broadcastAddress().getAddress().getHostAddress();
                Optional<ClusterUtils.RingInstanceDetails> replacementInstance = ClusterUtils.ring(seed)
                                                                                             .stream()
                                                                                             .filter(
                                                                                             i -> i.getAddress()
                                                                                                   .equals(newAddress))
                                                                                             .findFirst();
                assertThat(replacementInstance).isPresent();
                // Verify that replacement node tokens match the removed nodes
                assertThat(removedNodeTokens).contains(replacementInstance.get().getToken());
            }

            retrieveMappingWithKeyspace(context, TEST_KEYSPACE, response -> {
                assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                TokenRangeReplicasResponse mappingResponse = response.bodyAsJson(TokenRangeReplicasResponse.class);
                assertMappingResponseOK(mappingResponse,
                                        DEFAULT_RF,
                                        dcReplication);

                List<Integer> nodeNums = newNodes.stream().map(i -> i.config().num()).collect(Collectors.toList());
                validateNodeStates(mappingResponse,
                                   dcReplication,
                                   nodeNumber -> nodeNums.contains(nodeNumber) ? "Joining" : "Normal");

                int nodeCount = annotation.nodesPerDc() * annotation.numDcs();
                validateTokenRanges(mappingResponse, generateExpectedRanges(nodeCount));

                validateReplicaMapping(mappingResponse, newNodes, expectedRangeMappings);
                context.completeNow();
            });
        }
        finally
        {
            for (int i = 0; i < (annotation.newNodesPerDc() * annotation.numDcs()); i++)
            {
                transientStateEnd.countDown();
            }
        }
    }

    private List<IUpgradeableInstance> startReplacementNodes(CountDownLatch nodeStart,
                                                             UpgradeableCluster cluster,
                                                             List<IUpgradeableInstance> nodesToRemove)
    {
        List<IUpgradeableInstance> newNodes = new ArrayList<>();
        // Launch replacements nodes with the config of the removed nodes
        for (IUpgradeableInstance removed : nodesToRemove)
        {
            // Add new instance for each removed instance as a replacement of its owned token
            IInstanceConfig removedConfig = removed.config();
            String remAddress = removedConfig.broadcastAddress().getAddress().getHostAddress();
            int remPort = removedConfig.getInt("storage_port");
            IUpgradeableInstance replacement =
            ClusterUtils.addInstance(cluster, removedConfig,
                                     c -> {
                                         c.set("auto_bootstrap", true);
                                         // explicitly DOES NOT set instances that failed startup as "shutdown"
                                         // so subsequent attempts to shut down the instance are honored
                                         c.set("dtest.api.startup.failure_as_shutdown", false);
                                         c.with(Feature.GOSSIP,
                                                Feature.JMX,
                                                Feature.NATIVE_PROTOCOL);
                                     });

            new Thread(() -> ClusterUtils.start(replacement, (properties) -> {
                properties.set(CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK, true);
                properties.set(CassandraRelevantProperties.BOOTSTRAP_SCHEMA_DELAY_MS,
                               TimeUnit.SECONDS.toMillis(10L));
                properties.with("cassandra.broadcast_interval_ms",
                                Long.toString(TimeUnit.SECONDS.toMillis(30L)));
                properties.with("cassandra.ring_delay_ms",
                                Long.toString(TimeUnit.SECONDS.toMillis(10L)));
                // This property tells cassandra that this new instance is replacing the node with
                // address remAddress and port remPort
                properties.with("cassandra.replace_address_first_boot", remAddress + ":" + remPort);
            })).start();

            Uninterruptibles.awaitUninterruptibly(nodeStart, 2, TimeUnit.MINUTES);
            newNodes.add(replacement);
        }
        return newNodes;
    }

    private void stopNodes(IUpgradeableInstance seed, List<IUpgradeableInstance> removedNodes)
    {
        for (IUpgradeableInstance nodeToRemove : removedNodes)
        {
            ClusterUtils.stopUnchecked(nodeToRemove);
            String remAddress = nodeToRemove.config().broadcastAddress().getAddress().getHostAddress();

            List<ClusterUtils.RingInstanceDetails> ring = ClusterUtils.ring(seed);
            List<ClusterUtils.RingInstanceDetails> match = ring.stream()
                                                               .filter((d) -> d.getAddress().equals(remAddress))
                                                               .collect(Collectors.toList());
            assertThat(match.stream().anyMatch(r -> r.getStatus().equals("Down"))).isTrue();
        }
    }

    private void validateReplicaMapping(TokenRangeReplicasResponse mappingResponse,
                                        List<IUpgradeableInstance> newInstances,
                                        Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings)
    {
        List<String> transientNodeAddresses = newInstances.stream().map(i -> {
            InetSocketAddress address = i.config().broadcastAddress();
            return address.getAddress().getHostAddress() +
                   ":" +
                   address.getPort();
        }).collect(Collectors.toList());

        Set<String> writeReplicaInstances = instancesFromReplicaSet(mappingResponse.writeReplicas());
        Set<String> readReplicaInstances = instancesFromReplicaSet(mappingResponse.readReplicas());
        assertThat(readReplicaInstances).doesNotContainAnyElementsOf(transientNodeAddresses);
        assertThat(writeReplicaInstances).containsAll(transientNodeAddresses);

        validateWriteReplicaMappings(mappingResponse.writeReplicas(), expectedRangeMappings);
    }
}
