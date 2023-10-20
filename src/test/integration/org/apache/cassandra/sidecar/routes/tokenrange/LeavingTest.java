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
import java.util.function.BiConsumer;

import com.google.common.collect.Range;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * Cluster shrink scenarios integration tests for token range replica mapping endpoint with the in-jvm dtest framework.
 */
@ExtendWith(VertxExtension.class)
class LeavingTest extends LeavingBaseTest
{
    @CassandraIntegrationTest(nodesPerDc = 5, network = true, buildCluster = false)
    void retrieveMappingWithKeyspaceLeavingNode(VertxTestContext context,
                                                ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperSingleLeavingNode.reset();
        runLeavingTestScenario(context,
                               cassandraTestContext,
                               1,
                               BBHelperSingleLeavingNode::install,
                               BBHelperSingleLeavingNode.transientStateStart,
                               BBHelperSingleLeavingNode.transientStateEnd,
                               generateExpectedRangeMappingSingleLeavingNode());
    }

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, buildCluster = false)
    void retrieveMappingWithMultipleLeavingNodes(VertxTestContext context,
                                                 ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperMultipleLeavingNodes.reset();
        runLeavingTestScenario(context,
                               cassandraTestContext,
                               2,
                               BBHelperMultipleLeavingNodes::install,
                               BBHelperMultipleLeavingNodes.transientStateStart,
                               BBHelperMultipleLeavingNodes.transientStateEnd,
                               generateExpectedRangeMappingMultipleLeavingNodes());
    }

    @CassandraIntegrationTest(nodesPerDc = 6, network = true, buildCluster = false)
    void retrieveMappingHalveClusterSize(VertxTestContext context,
                                         ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperHalveClusterSize.reset();
        runLeavingTestScenario(context,
                               cassandraTestContext,
                               3,
                               BBHelperHalveClusterSize::install,
                               BBHelperHalveClusterSize.transientStateStart,
                               BBHelperHalveClusterSize.transientStateEnd,
                               generateExpectedRangeMappingHalveClusterSize());
    }

    void runLeavingTestScenario(VertxTestContext context,
                                ConfigurableCassandraTestContext cassandraTestContext,
                                int leavingNodesPerDC,
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
        runLeavingTestScenario(context,
                               leavingNodesPerDC,
                               transientStateStart,
                               transientStateEnd,
                               cluster,
                               expectedRangeMappings);
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
    private HashMap<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingSingleLeavingNode()
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

        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", mapping);
            }
        };
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

    private HashMap<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingMultipleLeavingNodes()
    {
        List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(expectedRanges.get(1),
                    Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.1"));
        mapping.put(
        expectedRanges.get(2),
        Arrays.asList("127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.2", "127.0.0.1"));
        mapping.put(
        expectedRanges.get(3),
        Arrays.asList("127.0.0.4", "127.0.0.5", "127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(
        expectedRanges.get(4),
        Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));

        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", mapping);
            }
        };
    }

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 6 node cluster
     * with the last 3 nodes leaving the cluster
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

    private Map<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingHalveClusterSize()
    {
        List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(
        expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.1"));
        mapping.put(
        expectedRanges.get(2),
        Arrays.asList("127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.1", "127.0.0.2"));
        mapping.put(
        expectedRanges.get(3),
        Arrays.asList("127.0.0.4", "127.0.0.5", "127.0.0.6", "127.0.0.1", "127.0.0.2",
                      "127.0.0.3"));
        mapping.put(
        expectedRanges.get(4),
        Arrays.asList("127.0.0.5", "127.0.0.6", "127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(
        expectedRanges.get(5), Arrays.asList("127.0.0.6", "127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));

        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", mapping);
            }
        };
    }

    /**
     * ByteBuddy Helper for a single leaving node
     */
    @Shared
    public static class BBHelperSingleLeavingNode
    {
        static CountDownLatch transientStateStart = new CountDownLatch(1);
        static CountDownLatch transientStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster with 1 leaving node
            // We intercept the shutdown of the leaving node (5) to validate token ranges
            if (nodeNumber == 5)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperSingleLeavingNode.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            transientStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transientStateEnd);
            orig.call();
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(1);
            transientStateEnd = new CountDownLatch(1);
        }
    }

    /**
     * ByteBuddy helper for multiple leaving nodes
     */
    @Shared
    public static class BBHelperMultipleLeavingNodes
    {
        static CountDownLatch transientStateStart = new CountDownLatch(2);
        static CountDownLatch transientStateEnd = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster with a 2 leaving nodes
            // We intercept the shutdown of the leaving nodes (4, 5) to validate token ranges
            if (nodeNumber > 3)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperMultipleLeavingNodes.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            transientStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transientStateEnd);
            orig.call();
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(2);
            transientStateEnd = new CountDownLatch(2);
        }
    }

    /**
     * ByteBuddy helper for shrinking cluster by half its size
     */
    @Shared
    public static class BBHelperHalveClusterSize
    {
        static CountDownLatch transientStateStart = new CountDownLatch(3);
        static CountDownLatch transientStateEnd = new CountDownLatch(3);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves halving the size of a 6 node cluster
            // We intercept the shutdown of the removed nodes (4-6) to validate token ranges
            if (nodeNumber > 3)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperHalveClusterSize.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            transientStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transientStateEnd);
            orig.call();
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(3);
            transientStateEnd = new CountDownLatch(3);
        }
    }
}
