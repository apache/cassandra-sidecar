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

package org.apache.cassandra.sidecar.restore;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.cluster.ConsistencyVerifier;
import org.apache.cassandra.sidecar.cluster.ConsistencyVerifiers;
import org.apache.cassandra.sidecar.cluster.locator.InstanceSetByDc;
import org.apache.cassandra.sidecar.common.data.ConsistencyLevel;
import org.apache.cassandra.sidecar.common.data.ConsistencyVerificationResult;
import org.apache.cassandra.sidecar.common.data.RestoreJobProgressFetchPolicy;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse.ReplicaInfo;
import org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreRange;

import static org.apache.cassandra.sidecar.restore.RestoreJobConsistencyLevelChecker.concludeOneRangeUnsafe;
import static org.apache.cassandra.sidecar.restore.RestoreJobConsistencyLevelChecker.concludeRangesUnsafe;
import static org.apache.cassandra.sidecar.restore.RestoreJobConsistencyLevelChecker.normalizeStatus;
import static org.apache.cassandra.sidecar.restore.RestoreJobConsistencyLevelChecker.replicaSetForRangeUnsafe;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RestoreJobConsistencyLevelCheckerTest
{
    @Test
    void testReplicaSetForRangeNotFound()
    {
        RestoreRange range = RestoreRangeTest.createTestRange(1, 10);
        TokenRangeReplicasResponse topology = mock(TokenRangeReplicasResponse.class);
        ReplicaInfo r1 = new ReplicaInfo("100", "110", null);
        when(topology.writeReplicas()).thenReturn(Collections.singletonList(r1));
        InstanceSetByDc result = replicaSetForRangeUnsafe(range, topology);
        assertThat(result).isNull();

        assertThat(concludeOneRangeUnsafe(topology, ConsistencyVerifiers.ForOne.INSTANCE, RestoreRangeStatus.STAGED, range))
        .describedAs("When unable to find replica set for the range, pending should be returned")
        .isEqualTo(ConsistencyVerificationResult.PENDING);
    }

    @Test
    void testReplicaSetForRangeTopologyChanged()
    {
        RestoreRange range = RestoreRangeTest.createTestRange(1, 10);
        TokenRangeReplicasResponse topology = mock(TokenRangeReplicasResponse.class);
        ReplicaInfo r1 = new ReplicaInfo("5", "15", null);
        when(topology.writeReplicas()).thenReturn(Collections.singletonList(r1));
        InstanceSetByDc result = replicaSetForRangeUnsafe(range, topology);
        assertThat(result)
        .describedAs("When topology has changed, return null")
        .isNull();

        assertThat(concludeOneRangeUnsafe(topology, ConsistencyVerifiers.ForOne.INSTANCE, RestoreRangeStatus.STAGED, range))
        .describedAs("When unable to find replica set for the range, pending should be returned")
        .isEqualTo(ConsistencyVerificationResult.PENDING);
    }

    @Test
    void testReplicaSetForRange()
    {
        RestoreRange range = RestoreRangeTest.createTestRange(1, 10);
        TokenRangeReplicasResponse topology = mock(TokenRangeReplicasResponse.class);
        int dcCount = 2;
        int replicasPerDc = 3;
        ReplicaInfo r1 = new ReplicaInfo("0", "15", replicaByDc(dcCount, replicasPerDc));
        when(topology.writeReplicas()).thenReturn(Collections.singletonList(r1));
        InstanceSetByDc result = replicaSetForRangeUnsafe(range, topology);
        assertThat(result.size()).isEqualTo(dcCount);
        assertThat(result.get("dc-1")).hasSize(replicasPerDc);
        assertThat(result.get("dc-2")).hasSize(replicasPerDc);
    }

    @Test
    void testConcludeOneRangeWithConsistencyOne()
    {
        RestoreRange range = RestoreRangeTest.createTestRange(1, 10);
        TokenRangeReplicasResponse topology = mock(TokenRangeReplicasResponse.class);
        ReplicaInfo r1 = new ReplicaInfo("0", "15", replicaByDc(1, 3));
        when(topology.writeReplicas()).thenReturn(Collections.singletonList(r1));

        // range has no replica status
        Map<String, RestoreRangeStatus> replicaStatus = new HashMap<>();
        range = range.unbuild().replicaStatus(replicaStatus).build();
        assertThat(concludeOneRangeUnsafe(topology, ConsistencyVerifiers.ForOne.INSTANCE, RestoreRangeStatus.STAGED, range))
        .isEqualTo(ConsistencyVerificationResult.PENDING);

        replicaStatus.put("i-1", RestoreRangeStatus.FAILED);
        replicaStatus.put("i-2", RestoreRangeStatus.FAILED);
        replicaStatus.put("i-3", RestoreRangeStatus.CREATED);
        range = range.unbuild().replicaStatus(replicaStatus).build();
        assertThat(concludeOneRangeUnsafe(topology, ConsistencyVerifiers.ForOne.INSTANCE, RestoreRangeStatus.STAGED, range))
        .isEqualTo(ConsistencyVerificationResult.PENDING);

        replicaStatus.put("i-1", RestoreRangeStatus.STAGED);
        replicaStatus.put("i-2", RestoreRangeStatus.CREATED);
        replicaStatus.put("i-3", RestoreRangeStatus.FAILED);
        range = range.unbuild().replicaStatus(replicaStatus).build();
        assertThat(concludeOneRangeUnsafe(topology, ConsistencyVerifiers.ForOne.INSTANCE, RestoreRangeStatus.STAGED, range))
        .describedAs("As long as one replica reports STAGED, it is good for CL_ONE")
        .isEqualTo(ConsistencyVerificationResult.SATISFIED);

        replicaStatus.put("i-1", RestoreRangeStatus.FAILED);
        replicaStatus.put("i-2", RestoreRangeStatus.FAILED);
        replicaStatus.put("i-3", RestoreRangeStatus.FAILED);
        range = range.unbuild().replicaStatus(replicaStatus).build();
        assertThat(concludeOneRangeUnsafe(topology, ConsistencyVerifiers.ForOne.INSTANCE, RestoreRangeStatus.STAGED, range))
        .describedAs("When all replicas fail, it fails for CL_ONE")
        .isEqualTo(ConsistencyVerificationResult.FAILED);
    }

    @Test
    void testConcludeOneRangeWithConsistencyLocalQuorum()
    {
        RestoreRange range = RestoreRangeTest.createTestRange(1, 10);
        TokenRangeReplicasResponse topology = mock(TokenRangeReplicasResponse.class);
        ReplicaInfo r1 = new ReplicaInfo("0", "15", replicaByDc(1, 3));
        when(topology.writeReplicas()).thenReturn(Collections.singletonList(r1));

        ConsistencyVerifier localQuorumVerifier = ConsistencyVerifiers.forConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM, "dc-1");

        // range has no replica status
        Map<String, RestoreRangeStatus> replicaStatus = new HashMap<>();
        range = range.unbuild().replicaStatus(replicaStatus).build();
        assertThat(concludeOneRangeUnsafe(topology, localQuorumVerifier, RestoreRangeStatus.STAGED, range))
        .isEqualTo(ConsistencyVerificationResult.PENDING);

        replicaStatus.put("i-1", RestoreRangeStatus.FAILED);
        replicaStatus.put("i-2", RestoreRangeStatus.STAGED);
        replicaStatus.put("i-3", RestoreRangeStatus.CREATED);
        range = range.unbuild().replicaStatus(replicaStatus).build();
        assertThat(concludeOneRangeUnsafe(topology, localQuorumVerifier, RestoreRangeStatus.STAGED, range))
        .isEqualTo(ConsistencyVerificationResult.PENDING);

        replicaStatus.put("i-1", RestoreRangeStatus.STAGED);
        replicaStatus.put("i-2", RestoreRangeStatus.STAGED);
        replicaStatus.put("i-3", RestoreRangeStatus.FAILED);
        range = range.unbuild().replicaStatus(replicaStatus).build();
        assertThat(concludeOneRangeUnsafe(topology, localQuorumVerifier, RestoreRangeStatus.STAGED, range))
        .isEqualTo(ConsistencyVerificationResult.SATISFIED);

        replicaStatus.put("i-1", RestoreRangeStatus.STAGED);
        replicaStatus.put("i-2", RestoreRangeStatus.FAILED);
        replicaStatus.put("i-3", RestoreRangeStatus.FAILED);
        range = range.unbuild().replicaStatus(replicaStatus).build();
        assertThat(concludeOneRangeUnsafe(topology, localQuorumVerifier, RestoreRangeStatus.STAGED, range))
        .isEqualTo(ConsistencyVerificationResult.FAILED);
    }

    @Test
    void testConcludeRangesIgnoreDiscardedRanges()
    {
        RestoreRange range = RestoreRangeTest.createTestRange(1, 10);
        range.discard();
        RestoreJobProgressCollector collectAll = RestoreJobProgressCollectors.create(mock(RestoreJob.class),
                                                                                     RestoreJobProgressFetchPolicy.ALL_FAILED_AND_PENDING);
        concludeRangesUnsafe(Collections.singletonList(range),
                             mock(TokenRangeReplicasResponse.class),
                             mock(ConsistencyVerifier.class), RestoreRangeStatus.STAGED,
                             collectAll);
        RestoreJobProgress progress = collectAll.toRestoreJobProgress();
        assertThat(progress.allRanges())
        .describedAs("No range is collect since the only range is discarded")
        .isEmpty();
    }

    @Test
    void testNormalizeStatus()
    {
        List<RestoreRange> ranges = Arrays.asList(createRangeWithStatus(0, 10, "i1"),
                                                  createRangeWithStatus(10, 20, "i1", "i2"),
                                                  createRangeWithStatus(20, 30, "i2"));
        assertThat(normalizeStatus(ranges))
        .describedAs("Status normalization should be successful")
        .isTrue();
        assertRangeWithStatus(ranges.get(0), 0, 10, "i1");
        assertRangeWithStatus(ranges.get(1), 10, 20, "i1", "i2");
        assertRangeWithStatus(ranges.get(2), 20, 30, "i2");
    }

    @Test
    void testNormalizeStatusWithFullyEnclosedRanges()
    {
        List<RestoreRange> ranges = Arrays.asList(createRangeWithStatus(0, 10, "i1"), // r1 is fully enclosed by r2, r3
                                                  createRangeWithStatus(0, 20, "i2"), // r2 is fully enclosed by r3
                                                  createRangeWithStatus(0, 30, "i3"),
                                                  createRangeWithStatus(10, 20, "i4"), // r4 is fully enclosed by r2 and r3
                                                  createRangeWithStatus(20, 30, "i5"), // r5 is fully enclosed by r3
                                                  createRangeWithStatus(40, 50, "i6")); // standalone
        assertThat(normalizeStatus(ranges))
        .describedAs("Status normalization should be successful")
        .isTrue();
        assertRangeWithStatus(ranges.get(0), 0, 10, "i1", "i2", "i3");
        assertRangeWithStatus(ranges.get(1), 0, 20, "i2", "i3");
        assertRangeWithStatus(ranges.get(2), 0, 30, "i3");
        assertRangeWithStatus(ranges.get(3), 10, 20, "i2", "i3", "i4");
        assertRangeWithStatus(ranges.get(4), 20, 30, "i3", "i5");
        assertRangeWithStatus(ranges.get(5), 40, 50, "i6");
    }

    @Test
    void testNormalizeStatusFails()
    {
        List<RestoreRange> ranges = Arrays.asList(createRangeWithStatus(0, 10, "i1"),
                                                  createRangeWithStatus(5, 15, "i1", "i2"));
        assertThat(normalizeStatus(ranges))
        .describedAs("Ranges (0, 10] and (5, 15] overlaps and normalization cannot proceed")
        .isFalse();
    }

    private Map<String, List<String>> replicaByDc(int dcCount, int replicasPerDc)
    {
        Map<String, List<String>> result = new HashMap<>(dcCount);
        List<String> replicas = IntStream.rangeClosed(1, replicasPerDc).boxed().map(i -> "i-" + i).collect(Collectors.toList());
        for (int i = 1; i <= dcCount; i++)
        {
            result.put("dc-" + i, replicas);
        }
        return result;
    }

    private RestoreRange createRangeWithStatus(long start, long end, String... replicas)
    {
        return RestoreRangeTest.createTestRange(start, end)
                               .unbuild()
                               .replicaStatus(createTestStatusMap(replicas))
                               .build();
    }

    private void assertRangeWithStatus(RestoreRange range, long expectedStart, long expectedEnd, String... expectedReplicas)
    {
        assertThat(range.startToken()).isEqualTo(BigInteger.valueOf(expectedStart));
        assertThat(range.endToken()).isEqualTo(BigInteger.valueOf(expectedEnd));
        assertThat(range.statusByReplica()).isEqualTo(createTestStatusMap(expectedReplicas));
    }

    private static Map<String, RestoreRangeStatus> createTestStatusMap(String[] replicas)
    {
        Map<String, RestoreRangeStatus> status = new HashMap<>(replicas.length);
        for (String r : replicas)
        {
            status.put(r, RestoreRangeStatus.SUCCEEDED);
        }
        return status;
    }
}
