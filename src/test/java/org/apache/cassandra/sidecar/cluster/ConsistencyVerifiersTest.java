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

package org.apache.cassandra.sidecar.cluster;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.cluster.ConsistencyVerifier.Result;
import org.apache.cassandra.sidecar.cluster.locator.InstanceSetByDc;
import org.apache.cassandra.sidecar.common.data.ConsistencyLevel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsistencyVerifiersTest
{
    private final InstanceSetByDc allReplicasByDc = replicaSet(2, 3);
    private final InstanceSetByDc dc1LocalReplicas = replicaSet(1, 3);

    @Test
    void testClOne()
    {
        ConsistencyVerifier verifier = ConsistencyVerifiers.forConsistencyLevel(ConsistencyLevel.ONE);
        assertThat(verifier.verify(Collections.emptySet(), // no passed instances
                                   replicas(1, 4),  // 4 instances fail
                                   allReplicasByDc))
        .isEqualTo(Result.PENDING);

        assertThat(verifier.verify(Collections.emptySet(),
                                   replicas(1, 6), // all instances fail
                                   allReplicasByDc))
        .isEqualTo(Result.FAILED);

        assertThat(verifier.verify(replicas(1, 1), // 1 instance passes
                                   replicas(2, 6), // the rest fail
                                   allReplicasByDc))
        .isEqualTo(Result.SATISFIED);
    }

    @Test
    void testClTwo()
    {
        ConsistencyVerifier verifier = ConsistencyVerifiers.forConsistencyLevel(ConsistencyLevel.TWO);
        assertThat(verifier.verify(replicas(1, 1), // 1 instance passes
                                   replicas(2, 4), // 3 instances fail
                                   allReplicasByDc))
        .isEqualTo(Result.PENDING);

        assertThat(verifier.verify(replicas(1, 1), // 1 instance passes
                                   replicas(1, 5), // 5 out of 6 instances fail
                                   allReplicasByDc))
        .isEqualTo(Result.FAILED);

        assertThat(verifier.verify(replicas(1, 2), // 2 instances pass
                                   replicas(3, 6), // the rest fail
                                   allReplicasByDc))
        .isEqualTo(Result.SATISFIED);
    }

    @Test
    void testClQuorum()
    {
        ConsistencyVerifier verifier = ConsistencyVerifiers.forConsistencyLevel(ConsistencyLevel.QUORUM);
        assertThat(verifier.verify(replicas(1, 1), // 1 instance passes
                                   replicas(2, 4), // 3 instances fail
                                   allReplicasByDc))
        .isEqualTo(Result.PENDING);

        assertThat(verifier.verify(replicas(1, 1), // 1 instance passes
                                   replicas(2, 5), // 4 out of 6 instances fail
                                   allReplicasByDc))
        .isEqualTo(Result.FAILED);

        assertThat(verifier.verify(replicas(1, 4), // 1 instance passed
                                   replicas(5, 6), // the rest fail
                                   allReplicasByDc))
        .isEqualTo(Result.SATISFIED);
    }

    @Test
    void testClLocalOne()
    {
        ConsistencyVerifier verifier = ConsistencyVerifiers.forConsistencyLevel(ConsistencyLevel.LOCAL_ONE, "dc-1");
        assertThat(verifier.verify(Collections.emptySet(),
                                   replicas(2, 3), // 2 out of 3 local instances fail
                                   allReplicasByDc))
        .isEqualTo(Result.PENDING);

        assertThat(verifier.verify(Collections.emptySet(),
                                   replicas(1, 3), // 3 out of 3 local instances fail
                                   allReplicasByDc))
        .isEqualTo(Result.FAILED);

        assertThat(verifier.verify(replicas(1, 1), // 1 instance passed
                                   replicas(2, 3), // the rest of local instances fail
                                   allReplicasByDc))
        .isEqualTo(Result.SATISFIED);

        ConsistencyVerifier dc2LocalVerifier = ConsistencyVerifiers.forConsistencyLevel(ConsistencyLevel.LOCAL_ONE, "dc-2");
        Set<String> set = Collections.emptySet();
        assertThatThrownBy(() -> dc2LocalVerifier.verify(set, set, dc1LocalReplicas))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Parameter 'all' should contain the local datacenter: dc-2");
    }

    @Test
    void testClLocalQuorum()
    {
        ConsistencyVerifier verifier = ConsistencyVerifiers.forConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM, "dc-1");
        assertThat(verifier.verify(replicas(1, 1), // 1 instance passes
                                   replicas(2, 2), // 1 instance fails
                                   allReplicasByDc))
        .isEqualTo(Result.PENDING);

        assertThat(verifier.verify(replicas(1, 1), // 1 instance passes
                                   replicas(2, 3), // 2 out of 3 local instances fail
                                   allReplicasByDc))
        .isEqualTo(Result.FAILED);

        assertThat(verifier.verify(replicas(1, 2), // 2 instances pass
                                   replicas(3, 3), // the rest fail
                                   allReplicasByDc))
        .isEqualTo(Result.SATISFIED);

        ConsistencyVerifier dc2LocalVerifier = ConsistencyVerifiers.forConsistencyLevel(ConsistencyLevel.LOCAL_ONE, "dc-2");
        Set<String> set = Collections.emptySet();
        assertThatThrownBy(() -> dc2LocalVerifier.verify(set, set, dc1LocalReplicas))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Parameter 'all' should contain the local datacenter: dc-2");
    }

    @Test
    void testClEachQuorum()
    {
        ConsistencyVerifier verifier = ConsistencyVerifiers.forConsistencyLevel(ConsistencyLevel.EACH_QUORUM);
        assertThat(verifier.verify(replicas(1, 1), // 1 instance passes
                                   replicas(3, 4), // 1 instance in each dc fails
                                   allReplicasByDc))
        .isEqualTo(Result.PENDING);

        assertThat(verifier.verify(replicas(1, 1), // 1 instance passes
                                   replicas(2, 5), // 4 out of 6 instances fail
                                   allReplicasByDc))
        .isEqualTo(Result.FAILED);

        assertThat(verifier.verify(replicas(2, 5), // 1 instance passes
                                   replicas(1, 1), // the rest fail
                                   allReplicasByDc))
        .isEqualTo(Result.SATISFIED);
    }

    @Test
    void testClAll()
    {
        ConsistencyVerifier verifier = ConsistencyVerifiers.forConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(verifier.verify(replicas(1, 1), // 1 instance passes
                                   Collections.emptySet(),
                                   allReplicasByDc))
        .isEqualTo(Result.PENDING);


        assertThat(verifier.verify(replicas(1, 1), // 1 instance passes
                                   replicas(2, 2), // any instance fails
                                   allReplicasByDc))
        .isEqualTo(Result.FAILED);

        assertThat(verifier.verify(replicas(1, 6), // all instances pass
                                   Collections.emptySet(),
                                   allReplicasByDc))
        .isEqualTo(Result.SATISFIED);
    }

    private Set<String> replicas(int start, int end)
    {
        return IntStream.rangeClosed(start, end).boxed().map(i -> "i-" + i).collect(Collectors.toSet());
    }

    private InstanceSetByDc replicaSet(int dcCount, int replicaPerDc)
    {
        Map<String, Set<String>> mapping = new HashMap<>(dcCount);
        for (int i = 1; i <= dcCount; i++)
        {
            Set<String> replicas = IntStream.rangeClosed(1 + (i - 1) * replicaPerDc, i * replicaPerDc)
                                            .boxed().map(id -> "i-" + id)
                                            .collect(Collectors.toSet());
            mapping.put("dc-" + i, replicas);
        }

        return new InstanceSetByDc(mapping);
    }
}
