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

import com.google.common.collect.Range;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.BootstrapBBUtils;
import org.apache.cassandra.sidecar.testing.TestTokenSupplier;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.testing.IClusterExtension;
import org.jetbrains.annotations.NotNull;

/**
 * Multi-DC Host replacement scenario integration tests for token range replica mapping endpoint with the in-jvm
 * dtest framework.
 */
@Tag("heavy")
@ExtendWith(VertxExtension.class)
class ReplacementMultiDCTest extends ReplacementBaseTest
{
    @CassandraIntegrationTest(
    nodesPerDc = 7, newNodesPerDc = 1, numDcs = 2, network = true, buildCluster = false)
    void retrieveMappingWithNodeReplacementMultiDC(VertxTestContext context,
                                                   ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {
        BBHelperReplacementsMultiDC.reset();
        TestTokenSupplier tokenSupplier = getTestTokenSupplier();
        Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings = generateExpectedRangeMappingReplacementMultiDC();
        IClusterExtension<? extends IInstance> cluster = getMultiDCCluster(BBHelperReplacementsMultiDC::install, cassandraTestContext, tokenSupplier,
                                                                           builder -> builder.additionalInstanceConfig(Map.of("progress_barrier_default_consistency_level", "QUORUM",
                                                                                                                               "progress_barrier_timeout", "30s",
                                                                                                                               "cms_await_timeout", "60s",
                                                                                                                               "accord.enabled", "false",
                                                                                                                               "write_request_timeout", "10s")));

        int initialClusterSize = cluster.size();
        List<IInstance> nodesToRemove = Arrays.asList(cluster.get(initialClusterSize - 1), cluster.get(initialClusterSize));
        runReplacementTestScenario(context,
                                   BBHelperReplacementsMultiDC.nodeStart,
                                   BBHelperReplacementsMultiDC.transientStateStart,
                                   BBHelperReplacementsMultiDC.transientStateEnd,
                                   cluster,
                                   nodesToRemove,
                                   expectedRangeMappings, tokenSupplier);
    }

    private static @NotNull TestTokenSupplier getTestTokenSupplier()
    {
        // We'll manually swap around tokens, so use 0 as number of new DCs
        TestTokenSupplier tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(8, 0, 2, 1);
        // Swap 6 for 12 so we don't have overlapping ranges
        tokenSupplier.swap(6, 12);
        // duplicate tokens for nodes 11 & 12 in 13 & 14 (replacement nodes) as they should use the same token
        tokenSupplier.copyToken(12, 14);
        tokenSupplier.copyToken(13, 15);
        return tokenSupplier;
    }

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 10 node cluster
     * across 2 DCs with the last 2 nodes leaving the cluster (1 per DC), with RF 3
     * <p>
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in
     * subsequent ranges cascade with the next range excluding the first replica, and including the next replica from
     * the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     * <p>
     * In a multi-DC scenario, a single range will have nodes from both DCs. The replicas are grouped by DC here
     * to allow per-DC validation as returned from the sidecar endpoint.
     * <p>
     * Ranges that including leaving node replicas will have [RF + no. leaving nodes in replica-set] replicas with
     * the new replicas being the existing nodes in ring-order.
     * <p>
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D (with D being the leaving node)
     * Expected Range 2 - B, C, D, A (With A taking over the range of the leaving node)
     */
    private Map<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingReplacementMultiDC()
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        int nodeCount = annotation.nodesPerDc() * annotation.numDcs();
        // Get a copy so we can make modifications so it will generate the tokens for the final configuration
        TestTokenSupplier tokenSupplier = getTestTokenSupplier();
        List<Range<BigInteger>> expectedRanges = generateExpectedRanges(nodeCount, tokenSupplier);
        Map<Range<BigInteger>, List<String>> dc1Mapping = new HashMap<>();
        Map<Range<BigInteger>, List<String>> dc2Mapping = new HashMap<>();

        dc1Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.6", "127.0.0.4", "127.0.0.2"));

        dc1Mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.13", "127.0.0.15", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.6", "127.0.0.4", "127.0.0.2"));

        dc1Mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.13", "127.0.0.15", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.6", "127.0.0.4", "127.0.0.8"));

        dc1Mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.9", "127.0.0.13", "127.0.0.15", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.6", "127.0.0.4", "127.0.0.8"));

        dc1Mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.9", "127.0.0.13", "127.0.0.15", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.6", "127.0.0.10", "127.0.0.8"));

        dc1Mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.9", "127.0.0.13", "127.0.0.11", "127.0.0.15"));
        dc2Mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.6", "127.0.0.10", "127.0.0.8"));

        dc1Mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.9", "127.0.0.13", "127.0.0.11", "127.0.0.15"));
        dc2Mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.10", "127.0.0.12", "127.0.0.8"));

        dc1Mapping.put(expectedRanges.get(7), Arrays.asList("127.0.0.9", "127.0.0.11", "127.0.0.7"));
        dc2Mapping.put(expectedRanges.get(7), Arrays.asList("127.0.0.10", "127.0.0.12", "127.0.0.8"));

        dc1Mapping.put(expectedRanges.get(8), Arrays.asList("127.0.0.9", "127.0.0.11", "127.0.0.7"));
        dc2Mapping.put(expectedRanges.get(8), Arrays.asList("127.0.0.14", "127.0.0.10", "127.0.0.12", "127.0.0.16"));

        dc1Mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.1", "127.0.0.11", "127.0.0.7"));
        dc2Mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.14", "127.0.0.10", "127.0.0.12", "127.0.0.16"));

        dc1Mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.1", "127.0.0.11", "127.0.0.7"));
        dc2Mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.14", "127.0.0.12", "127.0.0.2", "127.0.0.16"));

        dc1Mapping.put(expectedRanges.get(11), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.7"));
        dc2Mapping.put(expectedRanges.get(11), Arrays.asList("127.0.0.14", "127.0.0.12", "127.0.0.2", "127.0.0.16"));

        dc1Mapping.put(expectedRanges.get(12), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.7"));
        dc2Mapping.put(expectedRanges.get(12), Arrays.asList("127.0.0.14", "127.0.0.4", "127.0.0.2", "127.0.0.16"));

        dc1Mapping.put(expectedRanges.get(13), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(13), Arrays.asList("127.0.0.14", "127.0.0.4", "127.0.0.2", "127.0.0.16"));

        dc1Mapping.put(expectedRanges.get(14), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(14), Arrays.asList("127.0.0.6", "127.0.0.4", "127.0.0.2"));

        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", dc1Mapping);
                put("datacenter2", dc2Mapping);
            }
        };
    }

    /**
     * ByteBuddy helper for multi-DC node replacement
     */
    public static class BBHelperReplacementsMultiDC
    {
        // Additional latch used here to sequentially start the 2 new nodes to isolate the loading
        // of the shared Cassandra system property REPLACE_ADDRESS_FIRST_BOOT across instances
        static CountDownLatch nodeStart = new CountDownLatch(1);
        static CountDownLatch transientStateStart = new CountDownLatch(2);
        static CountDownLatch transientStateEnd = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 14 node cluster (across 2 DCs) with a 2 replacement nodes
            // We intercept the bootstrap of the replacement nodes to validate token ranges
            if (nodeNumber > 14)
            {
                BootstrapBBUtils.installFinishJoiningRingInterceptor(cl, BBHelperReplacementsMultiDC.class);
            }
        }

        @SuppressWarnings("unused")
        @RuntimeType
        public static Object finishJoiningRing(@SuperCall Callable<?> orig) throws Exception
        {
            return intercept(orig, null);
        }

        @SuppressWarnings("unused")
        @RuntimeType
        public static boolean bootstrap(@SuperCall Callable<Boolean> orig) throws Exception
        {
            // In trunk, we want to skip the actual bootstrap as it hangs shutdown
            // therefore, just return false rather than `orig.call`
            return intercept(orig, false);
        }

        private static <T> T intercept(Callable<T> orig, T returnVal) throws Exception
        {
            nodeStart.countDown();
            // trigger bootstrap start and wait until bootstrap is ready from test
            transientStateStart.countDown();
            awaitLatchOrTimeout(transientStateEnd, 4, TimeUnit.MINUTES, "transientStateEnd");
            return returnVal != null ? returnVal : orig.call();
        }

        public static void reset()
        {
            nodeStart = new CountDownLatch(1);
            transientStateStart = new CountDownLatch(2);
            transientStateEnd = new CountDownLatch(2);
        }
    }
}
