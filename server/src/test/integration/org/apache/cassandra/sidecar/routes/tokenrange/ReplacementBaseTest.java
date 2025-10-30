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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.Range;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.testing.TestTokenSupplier;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.IClusterExtension;
import org.apache.cassandra.testing.IRingEntry;

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
                                              IClusterExtension<? extends IInstance> cluster,
                                              List<? extends IInstance> nodesToRemove,
                                              Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings,
                                              TestTokenSupplier tokenSupplier)
    throws Exception
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        try
        {
            maybeReconfigureCMS(cluster);
            Set<String> dcReplication = getDcReplication(annotation);

            IInstance seed = cluster.get(1);
            List<String> removedNodeAddresses = nodesToRemove.stream()
                                                             .map(n ->
                                                                  n.config()
                                                                   .broadcastAddress()
                                                                   .getAddress()
                                                                   .getHostAddress())
                                                             .collect(Collectors.toList());

            List<IRingEntry> ring = cluster.ring(seed);
            List<String> removedNodeTokens = ring.stream()
                                                 .filter(i -> removedNodeAddresses.contains(i.address()))
                                                 .map(IRingEntry::token)
                                                 .collect(Collectors.toList());

            stopNodes(cluster, seed, nodesToRemove);
            List<IInstance> newNodes = startReplacementNodes(nodeStart, cluster, nodesToRemove);
            sidecarTestContext.refreshInstancesMetadata();
            // Wait until replacement nodes are in JOINING state
            awaitLatchOrThrow(transientStateStart, 2, TimeUnit.MINUTES, "transientStateStart");

            // Verify state of replacement nodes
            for (IInstance newInstance : newNodes)
            {
                cluster.awaitRingState(newInstance, newInstance, "Joining");
                cluster.awaitGossipStatus(newInstance, newInstance, "BOOT_REPLACE");

                String newAddress = newInstance.config().broadcastAddress().getAddress().getHostAddress();
                Optional<IRingEntry> replacementInstance = cluster.ring(seed)
                                                                  .stream()
                                                                  .filter(i -> i.address().equals(newAddress))
                                                                  .findFirst();
                assertThat(replacementInstance).isPresent();
                // Verify that replacement node tokens match the removed nodes
                assertThat(removedNodeTokens).contains(replacementInstance.get().token());
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
                                   nodeNumber -> nodeNums.contains(nodeNumber) ? "Replacing" : "Normal");

                int nodeCount = annotation.nodesPerDc() * annotation.numDcs();
                TestTokenSupplier finalTokenSupplier = tokenSupplier != null ? tokenSupplier :
                                                       TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                                 annotation.newNodesPerDc(),
                                                                                                 annotation.numDcs(),
                                                                                                 1);
                validateTokenRanges(mappingResponse,
                                    generateExpectedRanges(nodeCount, finalTokenSupplier));
                validateReplicaMapping(mappingResponse, newNodes, expectedRangeMappings);

                completeContextOrThrow(context);
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

    private List<IInstance> startReplacementNodes(CountDownLatch nodeStart,
                                                  IClusterExtension<? extends IInstance> cluster,
                                                  List<? extends IInstance> nodesToRemove)
    {
        List<IInstance> newNodes = new ArrayList<>();
        // Launch replacement nodes with the config of the removed nodes
        for (IInstance removed : nodesToRemove)
        {
            // Add new instance for each removed instance as a replacement of its owned token
            IInstanceConfig removedConfig = removed.config();
            String remAddress = removedConfig.broadcastAddress().getAddress().getHostAddress();
            int remPort = removedConfig.getInt("storage_port");
            IInstance replacement =
            addInstanceLocal(cluster, removedConfig.localDatacenter(), removedConfig.localRack(),
                             c -> {
                                 c.set("auto_bootstrap", true)
                                 // explicitly DOES NOT set instances that failed startup as "shutdown"
                                 // so subsequent attempts to shut down the instance are honored
                                  .set("dtest.api.startup.failure_as_shutdown", false);
                                 c.with(Feature.GOSSIP,
                                        Feature.JMX,
                                        Feature.NATIVE_PROTOCOL);
                                 c.set("storage_port", remPort);
                             });

            startAsync("Start replacement node node" + replacement.config().set("progress_barrier_timeout", "10s").num(),
                       () -> cluster.start(replacement, Map.of("cassandra.skip_schema_check", "true",
                                                               "cassandra.schema_delay_ms", Long.toString(TimeUnit.SECONDS.toMillis(10L)),
                                                               "cassandra.broadcast_interval_ms", Long.toString(TimeUnit.SECONDS.toMillis(30L)),
                                                               "cassandra.ring_delay_ms", Long.toString(TimeUnit.SECONDS.toMillis(10L)),
                                                               // Allow progress with less than EACH_QUORUM
                                                               "progress_barrier_default_consistency_level", "QUORUM",
                                                               "progress_barrier_timeout", "30s",
                                                               "cms_await_timeout", "60s",
                                                               "accord.enabled", "false",
                                                               "write_request_timeout", "10s",
                                                               // This property tells cassandra that this new instance is replacing the node with
                                                               // address remAddress and port remPort
                                                               "cassandra.replace_address_first_boot", remAddress + ":" + remPort)));

            awaitLatchOrThrow(nodeStart, 2, TimeUnit.MINUTES, "nodeStart");
            newNodes.add(replacement);
        }
        return newNodes;
    }

    public static <I extends IInstance> I addInstanceLocal(IClusterExtension<I> cluster,
                                                           String dc,
                                                           String rack,
                                                           Consumer<IInstanceConfig> fn)
    {
        Objects.requireNonNull(dc, "dc");
        Objects.requireNonNull(rack, "rack");
        IInstanceConfig config = cluster.newInstanceConfig();
        fn.accept(config);
        config.networkTopology().put(config.broadcastAddress(), NetworkTopology.dcAndRack(dc, rack));
        return cluster.bootstrap(config);
    }

    private void stopNodes(IClusterExtension<? extends IInstance> cluster, IInstance seed, List<? extends IInstance> removedNodes)
    {
        for (IInstance nodeToRemove : removedNodes)
        {
            cluster.stopUnchecked(nodeToRemove);
            cluster.awaitRingStatus(seed, nodeToRemove, "Down");
        }
        sidecarTestContext.refreshInstancesMetadata();
    }

    private void validateReplicaMapping(TokenRangeReplicasResponse mappingResponse,
                                        List<IInstance> newInstances,
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
