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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.google.common.collect.Range;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.sidecar.testing.TestTokenSupplier;
import org.apache.cassandra.sidecar.testing.bytebuddy.BBHelperLeavingNode;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.testing.IClusterExtension;

/**
 * Cluster shrink scenarios integration tests for token range replica mapping endpoint with the in-jvm dtest framework.
 */
@Tag("heavy")
@ExtendWith(VertxExtension.class)
class LeavingTest extends LeavingBaseTest
{
    @CassandraIntegrationTest(nodesPerDc = 5, network = true, buildCluster = false)
    void retrieveMappingWithKeyspaceLeavingNode(VertxTestContext context,
                                                ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        BBHelperLeavingNode.reset();
        runLeavingTestScenario(context,
                               cassandraTestContext,
                               1,
                               (cl, nodeNum) -> BBHelperLeavingNode.install(cl, nodeNum, 5),
                               BBHelperLeavingNode.transientStateStart,
                               BBHelperLeavingNode.transientStateEnd,
                               generateExpectedRangeMappingSingleLeavingNode(),
                               TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                         annotation.newNodesPerDc(),
                                                                         annotation.numDcs(),
                                                                         1));
    }

    @CassandraIntegrationTest(nodesPerDc = 10, network = true, buildCluster = false)
    void retrieveMappingWithMultipleLeavingNodes(VertxTestContext context,
                                                 ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        BBHelperMultipleLeavingNodes.reset();
        TestTokenSupplier tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                    annotation.newNodesPerDc(),
                                                                                    annotation.numDcs(),
                                                                                    1);
        tokenSupplier.swap(4, 8);
        runLeavingTestScenario(context,
                               cassandraTestContext,
                               2,
                               BBHelperMultipleLeavingNodes::install,
                               BBHelperMultipleLeavingNodes.transientStateStart,
                               BBHelperMultipleLeavingNodes.transientStateEnd,
                               generateExpectedRangeMappingMultipleLeavingNodes(tokenSupplier, annotation),
                               tokenSupplier);
    }

    void runLeavingTestScenario(VertxTestContext context,
                                ConfigurableCassandraTestContext cassandraTestContext,
                                int leavingNodesPerDC,
                                BiConsumer<ClassLoader, Integer> instanceInitializer,
                                CountDownLatch transientStateStart,
                                CountDownLatch transientStateEnd,
                                Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings, TokenSupplier tokenSupplier)
    throws Exception
    {

        IClusterExtension<? extends IInstance> cluster = cassandraTestContext.configureAndStartCluster(builder -> {
            builder.instanceInitializer(instanceInitializer);
            builder.tokenSupplier(tokenSupplier);
        });
        runLeavingTestScenario(context,
                               leavingNodesPerDC,
                               transientStateStart,
                               transientStateEnd,
                               cluster,
                               expectedRangeMappings
        );
    }

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 5 node cluster
     * with the last node leaving the cluster
     * <p>
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     * <p>
     * Ranges that including leaving node replicas will have [RF + no. leaving nodes in replica-set] replicas with
     * the new replicas being the existing nodes in ring-order.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D (with D being the leaving node)
     * Expected Range 2 - B, C, D, A (With A taking over the range of the leaving node)
     */
    private Map<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingSingleLeavingNode()
    {
        List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4"));

        mapping.put(expectedRanges.get(2),
                    Arrays.asList("127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.1"));
        mapping.put(expectedRanges.get(3),
                    Arrays.asList("127.0.0.4", "127.0.0.5", "127.0.0.1", "127.0.0.2"));
        mapping.put(expectedRanges.get(4),
                    Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.2", "127.0.0.3"));

        mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));

        return Map.of("datacenter1", mapping);
    }

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 5 node cluster
     * with the last 2 nodes leaving the cluster
     * <p>
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     * <p>
     * Ranges that including leaving node replicas will have [RF + no. leaving nodes in replica-set] replicas with
     * the new replicas being the existing nodes in ring-order.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D (with D being the leaving node)
     * Expected Range 2 - B, C, D, A (With A taking over the range of the leaving node)
     */

    private Map<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingMultipleLeavingNodes(TokenSupplier tokenSupplier, CassandraIntegrationTest annotation)
    {
        List<Range<BigInteger>> expectedRanges = generateExpectedRanges(false, tokenSupplier, annotation);
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.2"));
        mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.3", "127.0.0.2", "127.0.0.4"));
        mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.9", "127.0.0.3", "127.0.0.4", "127.0.0.6"));
        mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.7", "127.0.0.9", "127.0.0.4", "127.0.0.6"));
        mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.7", "127.0.0.9", "127.0.0.6", "127.0.0.8"));
        mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.7", "127.0.0.6", "127.0.0.8"));
        mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.7", "127.0.0.5", "127.0.0.8"));
        mapping.put(expectedRanges.get(7), Arrays.asList("127.0.0.1", "127.0.0.10", "127.0.0.5", "127.0.0.8"));
        mapping.put(expectedRanges.get(8), Arrays.asList("127.0.0.1", "127.0.0.10", "127.0.0.5", "127.0.0.2"));
        mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.1", "127.0.0.10", "127.0.0.3", "127.0.0.2"));
        mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.2"));

        return Map.of("datacenter1", mapping);
    }

    /**
     * ByteBuddy helper for multiple leaving nodes
     */
    public static class BBHelperMultipleLeavingNodes
    {
        static CountDownLatch transientStateStart = new CountDownLatch(2);
        static CountDownLatch transientStateEnd = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 12 node cluster with a 2 leaving nodes
            // We intercept the shutdown of the leaving nodes (11, 12) to validate token ranges
            if (nodeNumber > 8)
            {
                BBHelperLeavingNode.intercept(cl, BBHelperMultipleLeavingNodes.class);
            }
        }

        @SuppressWarnings("unused")
        public static void interceptedMethod(@SuperCall Callable<?> orig) throws Exception
        {
            transientStateStart.countDown();
            awaitLatchOrTimeout(transientStateEnd, 2, TimeUnit.MINUTES, "transientStateEnd");
            orig.call();
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(2);
            transientStateEnd = new CountDownLatch(2);
        }
    }
}
