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
import java.util.Collection;
import java.util.Collections;
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
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

/**
 * Host replacement scenario integration tests for token range replica mapping endpoint with the in-jvm dtest framework.
 */
@ExtendWith(VertxExtension.class)
class ReplacementTest extends ReplacementBaseTest
{
    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, network = true, gossip = true, buildCluster = false)
    void retrieveMappingWithNodeReplacement(VertxTestContext context,
                                            ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperReplacementsNode.reset();
        runReplacementTestScenario(context,
                                   cassandraTestContext,
                                   BBHelperReplacementsNode::install,
                                   BBHelperReplacementsNode.nodeStart,
                                   BBHelperReplacementsNode.transientStateStart,
                                   BBHelperReplacementsNode.transientStateEnd,
                                   generateExpectedRangeMappingNodeReplacement());
    }

    private void runReplacementTestScenario(VertxTestContext context,
                                            ConfigurableCassandraTestContext cassandraTestContext,
                                            BiConsumer<ClassLoader, Integer> instanceInitializer,
                                            CountDownLatch nodeStart,
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

        List<IUpgradeableInstance> nodesToRemove = Collections.singletonList(cluster.get(cluster.size()));
        runReplacementTestScenario(context,
                                   nodeStart,
                                   transientStateStart,
                                   transientStateEnd,
                                   cluster,
                                   nodesToRemove,
                                   expectedRangeMappings);
    }

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 5 node cluster
     * with the last node replaced with a new node
     * <p>
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in
     * subsequent ranges cascade with the next range excluding the first replica, and including the next replica from
     * the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     * <p>
     * Ranges will have [RF] replicas with ranges containing the replacement node having [RF + no. replacement nodes].
     * <p>
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D (with D being replaced with E)
     * Expected Range 2 - B, C, D, E (With E taking over the range of the node being replaced)
     */
    private Map<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingNodeReplacement()
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        int nodeCount = annotation.nodesPerDc() * annotation.numDcs();
        List<Range<BigInteger>> expectedRanges = generateExpectedRanges(nodeCount);
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4"));
        mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.3", "127.0.0.4", "127.0.0.5",
                                                         "127.0.0.6"));
        mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.4", "127.0.0.5", "127.0.0.1",
                                                         "127.0.0.6"));
        mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.2",
                                                         "127.0.0.6"));
        mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", mapping);
            }
        };
    }

    /**
     * ByteBuddy helper for a single node replacement
     */
    @Shared
    public static class BBHelperReplacementsNode
    {
        // Additional latch used here to sequentially start the 2 new nodes to isolate the loading
        // of the shared Cassandra system property REPLACE_ADDRESS_FIRST_BOOT across instances
        static CountDownLatch nodeStart = new CountDownLatch(1);
        static CountDownLatch transientStateStart = new CountDownLatch(1);
        static CountDownLatch transientStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster with a replacement node
            // We intercept the bootstrap of the replacement (6th) node to validate token ranges
            if (nodeNumber == 6)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperReplacementsNode.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            nodeStart.countDown();
            // trigger bootstrap start and wait until bootstrap is ready from test
            transientStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transientStateEnd);
            return result;
        }

        public static void reset()
        {
            nodeStart = new CountDownLatch(1);
            transientStateStart = new CountDownLatch(1);
            transientStateEnd = new CountDownLatch(1);
        }
    }
}
