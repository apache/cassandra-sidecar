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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.cluster.ConsistencyVerifier;
import org.apache.cassandra.sidecar.cluster.ConsistencyVerifiers;
import org.apache.cassandra.sidecar.cluster.locator.InstanceSetByDc;
import org.apache.cassandra.sidecar.common.data.ConsistencyLevel;
import org.apache.cassandra.sidecar.common.data.ConsistencyVerificationResult;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse.ReplicaInfo;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Token;
import org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.assertj.core.api.MapAssert;

import static org.apache.cassandra.sidecar.restore.RestoreJobConsistencyChecker.concludeOneRangeUnsafe;
import static org.apache.cassandra.sidecar.restore.RestoreJobConsistencyChecker.populateStatusByReplica;
import static org.apache.cassandra.sidecar.restore.RestoreJobConsistencyChecker.replicaSetForRangeUnsafe;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RestoreJobConsistencyCheckerTest
{
    @Test
    void testReplicaSetForRangeNotFound()
    {
        RestoreRange range = RestoreRangeTest.createTestRange(1, 10);
        TokenRangeReplicasResponse topology = mock(TokenRangeReplicasResponse.class);
        ReplicaInfo r1 = new ReplicaInfo("100", "110", ImmutableMap.of("dc1", ImmutableList.of("i1", "i2")));
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
        ReplicaInfo r1 = new ReplicaInfo("5", "15", ImmutableMap.of("dc1", ImmutableList.of("i1", "i2")));
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
    void testConcludeOneRangeWithDiscarded()
    {
        RestoreRange range = RestoreRangeTest.createTestRange(1, 10);
        TokenRangeReplicasResponse topology = mock(TokenRangeReplicasResponse.class);
        ReplicaInfo r1 = new ReplicaInfo("0", "15", replicaByDc(1, 3));
        when(topology.writeReplicas()).thenReturn(Collections.singletonList(r1));

        ConsistencyVerifier localQuorumVerifier = ConsistencyVerifiers.forConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM, "dc-1");

        // has quorum replicas of desired status
        Map<String, RestoreRangeStatus> replicaStatus = new HashMap<>();
        replicaStatus.put("i-1", RestoreRangeStatus.STAGED);
        replicaStatus.put("i-2", RestoreRangeStatus.CREATED);
        replicaStatus.put("i-3", RestoreRangeStatus.FAILED);
        replicaStatus.put("i-4", RestoreRangeStatus.DISCARDED); // i-4 worked on the range, but discards it since it loses the ownership
        range = range.unbuild().replicaStatus(replicaStatus).build();
        assertThat(concludeOneRangeUnsafe(topology, localQuorumVerifier, RestoreRangeStatus.STAGED, range))
        .isEqualTo(ConsistencyVerificationResult.PENDING);

        replicaStatus.put("i-1", RestoreRangeStatus.STAGED);
        replicaStatus.put("i-2", RestoreRangeStatus.STAGED);
        replicaStatus.put("i-3", RestoreRangeStatus.FAILED);
        replicaStatus.put("i-4", RestoreRangeStatus.DISCARDED); // i-4 worked on the range, but discards it since it loses the ownership
        range = range.unbuild().replicaStatus(replicaStatus).build();
        assertThat(concludeOneRangeUnsafe(topology, localQuorumVerifier, RestoreRangeStatus.STAGED, range))
        .isEqualTo(ConsistencyVerificationResult.SATISFIED);
    }

    @Test
    void testPopulateStatusByReplica()
    {
        List<RestoreRange> restoreRanges = Arrays.asList(r(1, 10, ImmutableMap.of("i1", RestoreRangeStatus.STAGED,
                                                                                  "i2", RestoreRangeStatus.CREATED)),
                                                         r(5, 15, ImmutableMap.of("i3", RestoreRangeStatus.STAGED)),
                                                         r(15, 20, ImmutableMap.of("i3", RestoreRangeStatus.STAGED,
                                                                                   "i4", RestoreRangeStatus.CREATED)));
        List<Range<Token>> expectedKeys =
        Arrays.asList(Range.openClosed(Token.from(1), Token.from(5)),
                      Range.openClosed(Token.from(5), Token.from(10)), // overlapping range of (1, 10] and (5, 15]
                      Range.openClosed(Token.from(10), Token.from(15)),
                      Range.openClosed(Token.from(15), Token.from(20)));
        List<Map<String, RestoreRangeStatus>> expectedStatus =
        Arrays.asList(ImmutableMap.of("i1", RestoreRangeStatus.STAGED,
                                      "i2", RestoreRangeStatus.CREATED),
                      ImmutableMap.of("i1", RestoreRangeStatus.STAGED,
                                      "i2", RestoreRangeStatus.CREATED,
                                      "i3", RestoreRangeStatus.STAGED), // merged from range (5, 15]
                      ImmutableMap.of("i3", RestoreRangeStatus.STAGED),
                      ImmutableMap.of("i3", RestoreRangeStatus.STAGED,
                                      "i4", RestoreRangeStatus.CREATED));
        Map<Range<Token>, Pair<Map<String, RestoreRangeStatus>, RestoreRange>> result = populateStatusByReplica(restoreRanges);
        MapAssert<Range<Token>, ?> assertion = assertThat(result).hasSize(4);
        for (int i = 0; i < 4; i++)
        {
            Range<Token> expectedKey = expectedKeys.get(i);
            assertion.containsKey(expectedKey);
            Map<String, RestoreRangeStatus> status = result.get(expectedKey).getLeft();
            assertThat(status).isEqualTo(expectedStatus.get(i));
        }
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

    private static RestoreRange r(long start, long end, Map<String, RestoreRangeStatus> status)
    {
        return RestoreRangeTest.createTestRange(start, end).unbuild().replicaStatus(status).build();
    }
}
