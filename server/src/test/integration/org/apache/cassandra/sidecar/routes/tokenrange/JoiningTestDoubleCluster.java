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
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.sidecar.testing.BootstrapBBUtils;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

/**
 * Cluster expansion scenarios integration tests for token range replica mapping endpoint with the in-jvm
 * dtest framework.
 *
 * Note: Some related test classes are broken down to have a single test case to parallelize test execution and
 * therefore limit the instance size required to run the tests from CircleCI as the in-jvm-dtests tests are memory bound
 */
@Tag("heavy")
@ExtendWith(VertxExtension.class)
public class JoiningTestDoubleCluster extends JoiningBaseTest
{
    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 5, network = true, buildCluster = false)
    void retrieveMappingWithDoubleClusterSize(VertxTestContext context,
                                              ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperDoubleClusterSize.reset();
        runJoiningTestScenario(context,
                               cassandraTestContext,
                               BBHelperDoubleClusterSize::install,
                               BBHelperDoubleClusterSize.transientStateStart,
                               BBHelperDoubleClusterSize.transientStateEnd,
                               generateExpectedRangeMappingDoubleClusterSize());
    }

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 5 node cluster
     * doubling in size
     * <p>
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     * <p>
     * We generate the expected ranges by using
     * 1) the initial token allocations to nodes (prior to adding nodes) shown under "Initial Ranges"
     * (in the comment block below),
     * 2)the "pending node ranges" and
     * 3) the final token allocations per node.
     * <p>
     * Step 1: Prepare ranges starting from partitioner min-token, ending at partitioner max-token using (3) above
     * Step 2: Create the cascading list of replica-sets based on the RF (3) for each range using the initial node list
     * Step 3: Add replicas to ranges based on (1) and (2) above.
     */
    private Map<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingDoubleClusterSize()
    {

        /*
         *
         * Initial Ranges:
         * [-9223372036854775808, -6148914691236517205]:["127.0.0.3","127.0.0.2","127.0.0.1"]
         * [-6148914691236517205, -3074457345618258603]:["127.0.0.3","127.0.0.2","127.0.0.4"]
         * [-3074457345618258603, -1]:["127.0.0.3","127.0.0.5","127.0.0.4"]
         * [-1, 3074457345618258601]:["127.0.0.5","127.0.0.4","127.0.0.1"]
         * [3074457345618258601, 6148914691236517203]:["127.0.0.5","127.0.0.2","127.0.0.1"]
         * [6148914691236517203, 9223372036854775807]:["127.0.0.3","127.0.0.2","127.0.0.1"]
         *
         * New node tokens
         * 127.0.0.6 at token -4611686018427387904
         * 127.0.0.7 at token -1537228672809129302
         * 127.0.0.8 at token 1537228672809129300
         * 127.0.0.9 at token 4611686018427387902
         * 127.0.0.10 at token 7686143364045646504
         *
         * Pending Ranges:
         * [-3074457345618258603, -1]=[127.0.0.9:64060, 127.0.0.8:64055]
         * [6148914691236517203, -6148914691236517205]=[127.0.0.6:64047, 127.0.0.7:64050] (wrap-around)
         * [-6148914691236517205, -4611686018427387904]=[127.0.0.6:64047]
         * [6148914691236517203, 7686143364045646504]=[127.0.0.10:64068]
         * [-3074457345618258603, -1537228672809129302]=[127.0.0.7:64050]
         * [3074457345618258601, 6148914691236517203]=[127.0.0.6:64047, 127.0.0.10:64068]
         * [-1, 1537228672809129300]=[127.0.0.8:64055]
         * [-6148914691236517205, -3074457345618258603]=[127.0.0.7:64050, 127.0.0.8:64055]
         * [-1, 3074457345618258601]=[127.0.0.9:64060, 127.0.0.10:64068]
         * [3074457345618258601, 4611686018427387902]=[127.0.0.9:64060]
         *
         */

        List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        // [-9223372036854775808, -6148914691236517205]
        mapping.put(expectedRanges.get(0),
                    Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.6", "127.0.0.7"));
        // [-6148914691236517205, -4611686018427387904]
        mapping.put(expectedRanges.get(1),
                    Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.6", "127.0.0.7", "127.0.0.8"));
        // [-4611686018427387904, -3074457345618258603]
        mapping.put(expectedRanges.get(2),
                    Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.7", "127.0.0.8"));
        // [-3074457345618258603, -1537228672809129302]
        mapping.put(expectedRanges.get(3),
                    Arrays.asList("127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.7", "127.0.0.8", "127.0.0.9"));
        // [-1537228672809129302, -1]
        mapping.put(expectedRanges.get(4),
                    Arrays.asList("127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.8", "127.0.0.9"));
        // [-1, 1537228672809129300]
        mapping.put(expectedRanges.get(5),
                    Arrays.asList("127.0.0.4", "127.0.0.5", "127.0.0.1", "127.0.0.8", "127.0.0.9", "127.0.0.10"));
        // [1537228672809129300, 3074457345618258601]
        mapping.put(expectedRanges.get(6),
                    Arrays.asList("127.0.0.4", "127.0.0.5", "127.0.0.1", "127.0.0.9", "127.0.0.10"));
        // [3074457345618258601, 4611686018427387902]
        mapping.put(expectedRanges.get(7),
                    Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.2", "127.0.0.6", "127.0.0.9", "127.0.0.10"));
        // [4611686018427387902, 6148914691236517203]
        mapping.put(expectedRanges.get(8),
                    Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.2", "127.0.0.6", "127.0.0.10"));
        // [6148914691236517203, 7686143364045646504]
        mapping.put(expectedRanges.get(9),
                    Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.6", "127.0.0.7", "127.0.0.10"));
        // Un-wrapped wrap-around range with the nodes in the initial range
        // [7686143364045646504, 9223372036854775807]
        mapping.put(expectedRanges.get(10),
                    Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.6", "127.0.0.7"));

        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", mapping);
            }
        };
    }

    /**
     * ByteBuddy helper for doubling cluster size
     */
    public static class BBHelperDoubleClusterSize
    {
        static CountDownLatch transientStateStart = new CountDownLatch(5);
        static CountDownLatch transientStateEnd = new CountDownLatch(5);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster doubling in size
            // We intercept the bootstrap of the new nodes (6-10) to validate token ranges
            if (nodeNumber > 5)
            {
                BootstrapBBUtils.installSetBoostrapStateIntercepter(cl, BBHelperDoubleClusterSize.class);
            }
        }

        public static void setBootstrapState(SystemKeyspace.BootstrapState state, @SuperCall Callable<Void> orig) throws Exception
        {
            if (state == SystemKeyspace.BootstrapState.COMPLETED)
            {
                // trigger bootstrap start and wait until bootstrap is ready from test
                transientStateStart.countDown();
                awaitLatchOrTimeout(transientStateEnd, 2, TimeUnit.MINUTES, "transientStateEnd");
            }
            orig.call();
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(5);
            transientStateEnd = new CountDownLatch(5);
        }
    }
}