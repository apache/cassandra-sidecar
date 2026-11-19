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

package org.apache.cassandra.sidecar.job.restart;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.response.data.RingEntry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ReplicaScopeHealthChecker}
 */
class ReplicaScopeHealthCheckerTest
{
    private ReplicationStrategyAnalyzer strategyAnalyzer;
    private ReplicaScopeHealthChecker checker;

    @BeforeEach
    void setup()
    {
        strategyAnalyzer = mock(ReplicationStrategyAnalyzer.class);
        when(strategyAnalyzer.replicaScopeForDatacenter(any(), any())).thenReturn(ReplicaScope.RACK);
        checker = new ReplicaScopeHealthChecker(strategyAnalyzer);
    }

    @Test
    void testHealthyRingReturnsHealthy()
    {
        UUID local = UUID.randomUUID();
        RingResponse ring = new RingResponse();
        ring.add(ringEntry("dc1", "rackA", "Up", local));
        ring.add(ringEntry("dc1", "rackB", "Up", UUID.randomUUID()));
        ring.add(ringEntry("dc1", "rackC", "Up", UUID.randomUUID()));
        assertThat(checker.isHealthy(ring, local)).isTrue();
    }

    @ParameterizedTest(name = "status={0} state={1}")
    @CsvSource({"Down, Normal", "?, Normal", "Up, Joining", "Up, Leaving"})
    void testCoReplicaNotUpAndNormalReturnsUnhealthy(String status, String state)
    {
        UUID local = UUID.randomUUID();
        RingResponse ring = new RingResponse();
        ring.add(ringEntry("dc1", "rackA", "Up", local));
        ring.add(ringEntry("dc1", "rackB", status, state, UUID.randomUUID()));
        ring.add(ringEntry("dc1", "rackC", "Up", UUID.randomUUID()));
        assertThat(checker.isHealthy(ring, local)).isFalse();
    }

    @ParameterizedTest(name = "status={0} state={1}")
    @CsvSource({"Down, Normal", "Up, Joining", "Up, Leaving", "Up, Moving"})
    void testLocalNodeNotUpAndNormalReturnsUnhealthy(String status, String state)
    {
        UUID local = UUID.randomUUID();
        RingResponse ring = new RingResponse();
        ring.add(ringEntry("dc1", "rackA", status, state, local));
        ring.add(ringEntry("dc1", "rackB", "Up", UUID.randomUUID()));
        ring.add(ringEntry("dc1", "rackC", "Up", UUID.randomUUID()));
        assertThat(checker.isHealthy(ring, local)).isFalse();
    }

    @Test
    void testNodeDownInSameRackReturnsHealthy()
    {
        UUID local = UUID.randomUUID();
        RingResponse ring = new RingResponse();
        ring.add(ringEntry("dc1", "rackA", "Up", local));
        ring.add(ringEntry("dc1", "rackA", "Down", UUID.randomUUID()));
        ring.add(ringEntry("dc1", "rackB", "Up", UUID.randomUUID()));
        assertThat(checker.isHealthy(ring, local)).isTrue();
    }

    @Test
    void testNodeDownInDifferentDatacenterReturnsHealthy()
    {
        UUID local = UUID.randomUUID();
        RingResponse ring = new RingResponse();
        ring.add(ringEntry("dc1", "rackA", "Up", local));
        ring.add(ringEntry("dc1", "rackB", "Up", UUID.randomUUID()));
        ring.add(ringEntry("dc2", "rackA", "Down", UUID.randomUUID()));
        assertThat(checker.isHealthy(ring, local)).isTrue();
    }

    @Test
    void testLocalNodeNotInRingReturnsUnhealthy()
    {
        RingResponse ring = new RingResponse();
        ring.add(ringEntry("dc1", "rackA", "Up", UUID.randomUUID()));
        assertThat(checker.isHealthy(ring, UUID.randomUUID())).isFalse();
    }

    @Test
    void testDatacenterScopeTreatsSameRackDownNodeAsUnhealthy()
    {
        when(strategyAnalyzer.replicaScopeForDatacenter(any(), any())).thenReturn(ReplicaScope.DATACENTER);
        UUID local = UUID.randomUUID();
        RingResponse ring = new RingResponse();
        ring.add(ringEntry("dc1", "rackA", "Up", local));
        ring.add(ringEntry("dc1", "rackA", "Down", UUID.randomUUID()));
        assertThat(checker.isHealthy(ring, local)).isFalse();
    }

    @Test
    void testDatacenterScopeIgnoresOtherDatacenterDownNode()
    {
        when(strategyAnalyzer.replicaScopeForDatacenter(any(), any())).thenReturn(ReplicaScope.DATACENTER);
        UUID local = UUID.randomUUID();
        RingResponse ring = new RingResponse();
        ring.add(ringEntry("dc1", "rackA", "Up", local));
        ring.add(ringEntry("dc1", "rackB", "Up", UUID.randomUUID()));
        ring.add(ringEntry("dc2", "rackA", "Down", UUID.randomUUID()));
        assertThat(checker.isHealthy(ring, local)).isTrue();
    }

    @Test
    void testClusterScopeTreatsOtherDatacenterDownNodeAsUnhealthy()
    {
        when(strategyAnalyzer.replicaScopeForDatacenter(any(), any())).thenReturn(ReplicaScope.CLUSTER);
        UUID local = UUID.randomUUID();
        RingResponse ring = new RingResponse();
        ring.add(ringEntry("dc1", "rackA", "Up", local));
        ring.add(ringEntry("dc2", "rackB", "Down", UUID.randomUUID()));
        assertThat(checker.isHealthy(ring, local)).isFalse();
    }

    @Test
    void testClusterScopeReturnsHealthyWhenAllUp()
    {
        when(strategyAnalyzer.replicaScopeForDatacenter(any(), any())).thenReturn(ReplicaScope.CLUSTER);
        UUID local = UUID.randomUUID();
        RingResponse ring = new RingResponse();
        ring.add(ringEntry("dc1", "rackA", "Up", local));
        ring.add(ringEntry("dc1", "rackA", "Up", UUID.randomUUID()));
        ring.add(ringEntry("dc2", "rackB", "Up", UUID.randomUUID()));
        assertThat(checker.isHealthy(ring, local)).isTrue();
    }

    private static int tokenCounter = 0;

    private RingEntry ringEntry(String datacenter, String rack, String status, UUID hostId)
    {
        return ringEntry(datacenter, rack, status, "Normal", hostId);
    }

    private RingEntry ringEntry(String datacenter, String rack, String status, String state, UUID hostId)
    {
        return new RingEntry.Builder()
               .datacenter(datacenter)
               .rack(rack)
               .status(status)
               .address("127.0.0." + (++tokenCounter))
               .port(9042)
               .token(String.valueOf(tokenCounter * 1000L))
               .state(state)
               .load("100 KiB")
               .owns("")
               .fqdn("localhost")
               .hostId(hostId.toString())
               .build();
    }
}
