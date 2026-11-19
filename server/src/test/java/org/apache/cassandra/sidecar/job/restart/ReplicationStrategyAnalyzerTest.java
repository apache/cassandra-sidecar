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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.response.data.RingEntry;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ReplicationStrategyAnalyzer}
 */
class ReplicationStrategyAnalyzerTest
{
    private static final String NTS = "org.apache.cassandra.locator.NetworkTopologyStrategy";
    private static final String SIMPLE = "org.apache.cassandra.locator.SimpleStrategy";
    private static final String LOCAL = "org.apache.cassandra.locator.LocalStrategy";

    @Test
    void classifyReturnsRackWhenAllNtsFitOneReplicaPerRack()
    {
        List<Map<String, String>> replications = List.of(
        Map.of("class", NTS, "dc1", "3"),
        Map.of("class", NTS, "dc1", "2"));
        assertThat(ReplicationStrategyAnalyzer.classify(3, "dc1", replications)).isEqualTo(ReplicaScope.RACK);
    }

    @Test
    void classifyReturnsDatacenterWhenNtsReplicationFactorExceedsRacks()
    {
        List<Map<String, String>> replications = List.of(
        Map.of("class", NTS, "dc1", "2"),
        Map.of("class", NTS, "dc1", "4"));
        assertThat(ReplicationStrategyAnalyzer.classify(3, "dc1", replications)).isEqualTo(ReplicaScope.DATACENTER);
    }

    @Test
    void classifyReturnsClusterWhenAnyKeyspaceIsNotNetworkTopologyStrategy()
    {
        List<Map<String, String>> replications = List.of(
        Map.of("class", NTS, "dc1", "3"),
        Map.of("class", SIMPLE, "replication_factor", "3"));
        assertThat(ReplicationStrategyAnalyzer.classify(3, "dc1", replications)).isEqualTo(ReplicaScope.CLUSTER);
    }

    @Test
    void classifyReturnsClusterEvenWhenSimpleStrategyReplicationFitsRacks()
    {
        List<Map<String, String>> replications = List.of(Map.of("class", SIMPLE, "replication_factor", "1"));
        assertThat(ReplicationStrategyAnalyzer.classify(3, "dc1", replications)).isEqualTo(ReplicaScope.CLUSTER);
    }

    @Test
    void classifyReturnsRackWhenNoUserKeyspaces()
    {
        assertThat(ReplicationStrategyAnalyzer.classify(3, "dc1", List.of())).isEqualTo(ReplicaScope.RACK);
    }

    @Test
    void classifyIgnoresReplicationFactorForOtherDatacenters()
    {
        List<Map<String, String>> replications = List.of(Map.of("class", NTS, "dc1", "2", "dc2", "9"));
        assertThat(ReplicationStrategyAnalyzer.classify(3, "dc1", replications)).isEqualTo(ReplicaScope.RACK);
    }

    @Test
    void replicaScopeForDatacenterReadsSchemaAndCountsRacks()
    {
        RingResponse ring = new RingResponse();
        ring.add(ringEntry("dc1", "rackA"));
        ring.add(ringEntry("dc1", "rackB"));
        ring.add(ringEntry("dc2", "rackA"));

        ReplicationStrategyAnalyzer analyzer = analyzerWithKeyspaces(
        keyspace("app", Map.of("class", NTS, "dc1", "3")));

        assertThat(analyzer.replicaScopeForDatacenter(ring, "dc1")).isEqualTo(ReplicaScope.DATACENTER);
    }

    @Test
    void replicaScopeForDatacenterExcludesLocalStrategyKeyspaces()
    {
        RingResponse ring = new RingResponse();
        ring.add(ringEntry("dc1", "rackA"));
        ring.add(ringEntry("dc1", "rackB"));
        ring.add(ringEntry("dc1", "rackC"));

        ReplicationStrategyAnalyzer analyzer = analyzerWithKeyspaces(
        keyspace("system", Map.of("class", LOCAL)),
        keyspace("app", Map.of("class", NTS, "dc1", "3")));

        assertThat(analyzer.replicaScopeForDatacenter(ring, "dc1")).isEqualTo(ReplicaScope.RACK);
    }

    @Test
    void replicaScopeForDatacenterIncludesReplicatedSystemKeyspaces()
    {
        RingResponse ring = new RingResponse();
        ring.add(ringEntry("dc1", "rackA"));
        ring.add(ringEntry("dc1", "rackB"));
        ring.add(ringEntry("dc1", "rackC"));

        ReplicationStrategyAnalyzer analyzer = analyzerWithKeyspaces(
        keyspace("system_auth", Map.of("class", SIMPLE, "replication_factor", "1")),
        keyspace("app", Map.of("class", NTS, "dc1", "3")));

        assertThat(analyzer.replicaScopeForDatacenter(ring, "dc1")).isEqualTo(ReplicaScope.CLUSTER);
    }

    @Test
    void replicaScopeForDatacenterSkipsVirtualKeyspaces()
    {
        RingResponse ring = new RingResponse();
        ring.add(ringEntry("dc1", "rackA"));
        ring.add(ringEntry("dc1", "rackB"));
        ring.add(ringEntry("dc1", "rackC"));

        ReplicationStrategyAnalyzer analyzer = analyzerWithKeyspaces(
        virtualKeyspace("system_virtual_schema"),
        keyspace("app", Map.of("class", NTS, "dc1", "3")));

        assertThat(analyzer.replicaScopeForDatacenter(ring, "dc1")).isEqualTo(ReplicaScope.RACK);
    }

    @Test
    void replicaScopeForDatacenterFallsBackToClusterWhenMetadataUnavailable()
    {
        InstanceMetadataFetcher fetcher = mock(InstanceMetadataFetcher.class);
        when(fetcher.callOnFirstAvailableInstance(any())).thenThrow(new RuntimeException("metadata unavailable"));
        ReplicationStrategyAnalyzer analyzer = new ReplicationStrategyAnalyzer(fetcher);

        RingResponse ring = new RingResponse();
        ring.add(ringEntry("dc1", "rackA"));

        assertThat(analyzer.replicaScopeForDatacenter(ring, "dc1")).isEqualTo(ReplicaScope.CLUSTER);
    }

    private static ReplicationStrategyAnalyzer analyzerWithKeyspaces(KeyspaceMetadata... keyspaces)
    {
        Metadata metadata = mock(Metadata.class);
        when(metadata.getKeyspaces()).thenReturn(List.of(keyspaces));
        InstanceMetadataFetcher fetcher = mock(InstanceMetadataFetcher.class);
        when(fetcher.callOnFirstAvailableInstance(any())).thenReturn(metadata);
        return new ReplicationStrategyAnalyzer(fetcher);
    }

    private static KeyspaceMetadata keyspace(String name, Map<String, String> replication)
    {
        KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        when(keyspace.getName()).thenReturn(name);
        when(keyspace.getReplication()).thenReturn(replication);
        return keyspace;
    }

    private static KeyspaceMetadata virtualKeyspace(String name)
    {
        KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        when(keyspace.getName()).thenReturn(name);
        when(keyspace.isVirtual()).thenReturn(true);
        Map<String, String> nullStrategy = new HashMap<>();
        nullStrategy.put("class", null);
        when(keyspace.getReplication()).thenReturn(nullStrategy);
        return keyspace;
    }

    private static int tokenCounter = 0;

    private static RingEntry ringEntry(String datacenter, String rack)
    {
        return new RingEntry.Builder()
               .datacenter(datacenter)
               .rack(rack)
               .status("Up")
               .address("127.0.0." + (++tokenCounter))
               .port(9042)
               .token(String.valueOf(tokenCounter * 1000L))
               .state("Normal")
               .load("100 KiB")
               .owns("")
               .fqdn("localhost")
               .hostId(UUID.randomUUID().toString())
               .build();
    }
}
