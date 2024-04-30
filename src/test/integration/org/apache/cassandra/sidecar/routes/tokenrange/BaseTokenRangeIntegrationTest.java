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

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Range;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.sidecar.adapters.base.Partitioner;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.AbstractCassandraTestContext;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.assertj.core.api.InstanceOfAssertFactories;

import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.from;

/**
 * Test the token range replica mapping endpoint with the in-jvm dtest framework.
 */
public class BaseTokenRangeIntegrationTest extends IntegrationTestBase
{
    protected void validateTokenRanges(TokenRangeReplicasResponse mappingsResponse,
                                       List<Range<BigInteger>> expectedRanges)
    {
        List<TokenRangeReplicasResponse.ReplicaInfo> writeReplicaSet = mappingsResponse.writeReplicas();
        List<TokenRangeReplicasResponse.ReplicaInfo> readReplicaSet = mappingsResponse.readReplicas();
        List<Range<BigInteger>> writeRanges = writeReplicaSet.stream()
                                                             .map(r -> Range.openClosed(new BigInteger(r.start()),
                                                                                        new BigInteger(r.end())))
                                                             .collect(Collectors.toList());

        List<Range<BigInteger>> readRanges = readReplicaSet.stream()
                                                           .map(r -> Range.openClosed(new BigInteger(r.start()),
                                                                                      new BigInteger(r.end())))
                                                           .collect(Collectors.toList());

        assertThat(writeRanges).containsExactlyElementsOf(expectedRanges);

        //Sorted and Overlap check
        validateOrderAndOverlaps(writeRanges);
        validateOrderAndOverlaps(readRanges);
    }

    private void validateOrderAndOverlaps(List<Range<BigInteger>> ranges)
    {
        for (int r = 0; r < ranges.size() - 1; r++)
        {
            assertThat(ranges.get(r).upperEndpoint()).isLessThan(ranges.get(r + 1).upperEndpoint());
            assertThat(ranges.get(r).intersection(ranges.get(r + 1)).isEmpty()).isTrue();
        }
    }

    protected void validateNodeStates(TokenRangeReplicasResponse mappingResponse,
                                      Set<String> dcReplication,
                                      Function<Integer, String> stateFunction)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        int expectedReplicas = (annotation.nodesPerDc() + annotation.newNodesPerDc()) * dcReplication.size();

        AbstractCassandraTestContext cassandraTestContext = sidecarTestContext.cassandraTestContext();
        assertThat(mappingResponse.replicaMetadata().size()).isEqualTo(expectedReplicas);
        for (int i = 1; i <= cassandraTestContext.cluster().size(); i++)
        {
            IInstanceConfig config = cassandraTestContext.cluster().get(i).config();

            if (dcReplication.contains(config.localDatacenter()))
            {
                String ipAndPort = config.broadcastAddress().getAddress().getHostAddress() + ":"
                                   + config.broadcastAddress().getPort();

                String expectedStatus = stateFunction.apply(i);
                assertThat(mappingResponse.replicaMetadata().get(ipAndPort))
                .isNotNull()
                .extracting(from(TokenRangeReplicasResponse.ReplicaMetadata::state),
                            as(InstanceOfAssertFactories.STRING))
                .isEqualTo(expectedStatus);
            }
        }
    }

    protected UpgradeableCluster getMultiDCCluster(BiConsumer<ClassLoader, Integer> initializer,
                                                   ConfigurableCassandraTestContext cassandraTestContext)
    throws IOException
    {
        return getMultiDCCluster(initializer, cassandraTestContext, null);
    }

    protected UpgradeableCluster getMultiDCCluster(BiConsumer<ClassLoader, Integer> initializer,
                                                   ConfigurableCassandraTestContext cassandraTestContext,
                                                   Consumer<UpgradeableCluster.Builder> additionalConfigurator)
    throws IOException
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier mdcTokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                   annotation.newNodesPerDc(),
                                                                                   annotation.numDcs(),
                                                                                   1);

        int totalNodeCount = (annotation.nodesPerDc() + annotation.newNodesPerDc()) * annotation.numDcs();
        return cassandraTestContext.configureAndStartCluster(
        builder -> {
            builder.withInstanceInitializer(initializer);
            builder.withTokenSupplier(mdcTokenSupplier);
            builder.withNodeIdTopology(networkTopology(totalNodeCount,
                                                       (nodeId) -> nodeId % 2 != 0 ?
                                                                   dcAndRack("datacenter1", "rack1") :
                                                                   dcAndRack("datacenter2", "rack2")));

            if (additionalConfigurator != null)
            {
                additionalConfigurator.accept(builder);
            }
        });
    }

    protected List<Range<BigInteger>> generateExpectedRanges()
    {
        return generateExpectedRanges(true);
    }

    protected List<Range<BigInteger>> generateExpectedRanges(boolean isCrossDCKeyspace)
    {

        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        // For single DC keyspaces, the ranges are initially allocated replicas from both DCs. As a result,
        // we will take into account the node count across all DCs. It is only while accounting for the new/joining
        // nodes that we will limit the nodes to the single DC, as the pending nodes for the given keyspace will
        // exclude the nodes from other DCs.

        int nodeCount = isCrossDCKeyspace ?
                        (annotation.nodesPerDc() + annotation.newNodesPerDc()) * annotation.numDcs() :
                        (annotation.nodesPerDc() * annotation.numDcs()) + annotation.newNodesPerDc();

        return generateExpectedRanges(nodeCount);
    }

    protected List<Range<BigInteger>> generateExpectedRanges(int nodeCount)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                annotation.newNodesPerDc(),
                                                                                annotation.numDcs(),
                                                                                1);

        TreeSet<BigInteger> tokens = new TreeSet<>();
        int node = 1;
        while (node <= nodeCount)
        {
            tokens.add(new BigInteger(tokenSupplier.tokens(node++).stream().findFirst().get()));
        }

        List<Range<BigInteger>> expectedRanges = new ArrayList<>();
        BigInteger startToken = Partitioner.Murmur3.minToken;
        BigInteger endToken = Partitioner.Murmur3.maxToken;

        BigInteger prevToken = tokens.pollFirst();
        Range<BigInteger> firstRange = Range.openClosed(startToken, prevToken);
        expectedRanges.add(firstRange);

        for (BigInteger token : tokens)
        {
            BigInteger currentToken = token;
            expectedRanges.add(Range.openClosed(prevToken, currentToken));
            prevToken = currentToken;
        }

        expectedRanges.add(Range.openClosed(prevToken, endToken));
        return expectedRanges;
    }

    protected Set<String>
    instancesFromReplicaSet(List<TokenRangeReplicasResponse.ReplicaInfo> replicas)
    {
        return replicas.stream()
                       .flatMap(r -> r.replicasByDatacenter().values().stream())
                       .flatMap(Collection::stream)
                       .collect(Collectors.toSet());
    }

    protected void validateWriteReplicaMappings(List<TokenRangeReplicasResponse.ReplicaInfo> writeReplicas,
                                                Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMapping)
    {
        validateWriteReplicaMappings(writeReplicas, expectedRangeMapping, true);
    }

    protected void validateWriteReplicaMappings(List<TokenRangeReplicasResponse.ReplicaInfo> writeReplicas,
                                                Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMapping,
                                                boolean isCrossDCKeyspace)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        // Validates the no. of ranges in the write-replica mappings match the no. of expected ranges
        assertThat(writeReplicas).hasSize(expectedRangeMapping.get("datacenter1").size());
        for (TokenRangeReplicasResponse.ReplicaInfo r : writeReplicas)
        {
            Range<BigInteger> range = Range.openClosed(BigInteger.valueOf(Long.parseLong(r.start())),
                                                       BigInteger.valueOf(Long.parseLong(r.end())));
            assertThat(expectedRangeMapping).containsKey("datacenter1");
            assertThat(expectedRangeMapping.get("datacenter1")).containsKey(range);
            // Replicaset for the same range match expected
            List<String> replicaSetNoPort = r.replicasByDatacenter().get("datacenter1")
                                             .stream()
                                             .map(node -> node.split(":")[0])
                                             .collect(Collectors.toList());
            assertThat(replicaSetNoPort)
            .containsExactlyInAnyOrderElementsOf(expectedRangeMapping.get("datacenter1").get(range));

            if (annotation.numDcs() > 1 && isCrossDCKeyspace)
            {
                assertThat(expectedRangeMapping).containsKey("datacenter2");
                assertThat(expectedRangeMapping.get("datacenter2")).containsKey(range);

                List<String> replicaSetNoPortDc2 = r.replicasByDatacenter().get("datacenter2")
                                                    .stream()
                                                    .map(node -> node.split(":")[0])
                                                    .collect(Collectors.toList());
                assertThat(replicaSetNoPortDc2)
                .containsExactlyInAnyOrderElementsOf(expectedRangeMapping.get("datacenter2").get(range));
            }
        }
    }


    void retrieveMappingWithKeyspace(VertxTestContext context, String keyspace,
                                     Handler<HttpResponse<Buffer>> verifier) throws Exception
    {
        String testRoute = "/api/v1/keyspaces/" + keyspace + "/token-range-replicas";
        testWithClient(context, client -> client.get(server.actualPort(), "127.0.0.1", testRoute)
                                                .send(context.succeeding(verifier)));
    }

    void assertMappingResponseOK(TokenRangeReplicasResponse mappingResponse,
                                 int replicationFactor,
                                 Set<String> dcReplication)
    {
        assertThat(mappingResponse).isNotNull();
        assertThat(mappingResponse.readReplicas()).isNotNull();
        assertThat(mappingResponse.writeReplicas()).isNotNull();
        validateRanges(mappingResponse.writeReplicas());
        validateRanges(mappingResponse.readReplicas());
        TokenRangeReplicasResponse.ReplicaInfo readReplica = mappingResponse.readReplicas().get(0);
        assertThat(readReplica.replicasByDatacenter()).isNotNull().hasSize(dcReplication.size());
        TokenRangeReplicasResponse.ReplicaInfo writeReplica = mappingResponse.writeReplicas().get(0);
        assertThat(writeReplica.replicasByDatacenter()).isNotNull().hasSize(dcReplication.size());

        for (String dcName : dcReplication)
        {
            assertThat(readReplica.replicasByDatacenter().keySet()).isNotEmpty().contains(dcName);
            assertThat(readReplica.replicasByDatacenter().get(dcName)).isNotNull().hasSize(replicationFactor);

            assertThat(writeReplica.replicasByDatacenter().keySet()).isNotEmpty().contains(dcName);
            assertThat(writeReplica.replicasByDatacenter().get(dcName)).isNotNull();
            assertThat(writeReplica.replicasByDatacenter().get(dcName).size())
            .isGreaterThanOrEqualTo(replicationFactor);
        }
    }

    private void validateRanges(List<TokenRangeReplicasResponse.ReplicaInfo> replicaRanges)
    {
        // Ranges should not be empty
        replicaRanges.forEach(r -> assertThat(r.start()).isNotEqualTo(r.end()));
        // Ranges should include partitioner start and end
        assertThat(replicaRanges.stream()
                                .map(TokenRangeReplicasResponse.ReplicaInfo::start)
                                .anyMatch(s -> s.equals(Murmur3Partitioner.MINIMUM.toString()))).isTrue();
        assertThat(replicaRanges.stream()
                                .map(TokenRangeReplicasResponse.ReplicaInfo::end)
                                .anyMatch(s -> s.equals(Long.toString(Murmur3Partitioner.MAXIMUM)))).isTrue();
    }
}
