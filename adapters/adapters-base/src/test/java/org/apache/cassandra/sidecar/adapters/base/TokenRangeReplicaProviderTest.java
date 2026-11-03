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

package org.apache.cassandra.sidecar.adapters.base;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.management.InstanceNotFoundException;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeData;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.adapters.base.jmx.ClusterMembershipJmxOperations;
import org.apache.cassandra.sidecar.adapters.base.jmx.EndpointSnitchJmxOperations;
import org.apache.cassandra.sidecar.adapters.base.jmx.ReplicaGroupsInfoJmxOperations;
import org.apache.cassandra.sidecar.adapters.base.jmx.StorageJmxOperations;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Partitioners;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.assertj.core.api.InstanceOfAssertFactories;

import static org.apache.cassandra.sidecar.adapters.base.data.ReplicaGroupsFixture.datacenter;
import static org.apache.cassandra.sidecar.adapters.base.data.ReplicaGroupsFixture.peer;
import static org.apache.cassandra.sidecar.adapters.base.data.ReplicaGroupsFixture.range;
import static org.apache.cassandra.sidecar.adapters.base.data.ReplicaGroupsFixture.ranges;
import static org.apache.cassandra.sidecar.adapters.base.data.ReplicaGroupsFixture.replica;
import static org.apache.cassandra.sidecar.adapters.base.data.ReplicaGroupsFixture.replicaGroups;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.from;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for TokenRangeReplicaProvider
 */
public class TokenRangeReplicaProviderTest
{
    private static final String TEST_DC1 = "test_dc_1";
    private static final String TEST_DC2 = "test_dc_2";
    private static final List<String> TOKEN_RANGE1 = Arrays.asList("0", "100");
    private static final List<String> TOKEN_RANGE2 = Arrays.asList("200", "500");
    private static final List<String> TEST_ENDPOINTS1 = Arrays.asList("127.0.0.1:7000",
                                                                      "127.0.0.2:7000",
                                                                      "127.0.0.3:7000");
    private static final List<String> TEST_ENDPOINTS2 = Arrays.asList("128.0.0.1:7000", "128.0.0.2:7000");
    private static final List<String> TEST_MULTI_DC_ENDPOINTS = Arrays.asList("128.0.0.1:7000",
                                                                              "128.0.0.2:7000",
                                                                              "127.0.0.4:7000");
    public static final Name TEST_KEYSPACE = new Name("test_keyspace");

    private static final String TEST_HOSTNAME = "Hostname";
    private static final String MIN_TOKEN = String.valueOf(Long.MIN_VALUE);
    private static final String MAX_TOKEN = String.valueOf(Long.MAX_VALUE);

    StorageJmxOperations storageOperations;
    ReplicaGroupsInfoJmxOperations replicaGroupsOperations;
    EndpointSnitchJmxOperations endpointOperations;
    ClusterMembershipJmxOperations clusterMembershipOperations;
    JmxClient jmxClient;
    DnsResolver dnsResolver;
    TokenRangeReplicaProvider instance;

    @BeforeEach
    void setup() throws UnknownHostException
    {
        storageOperations = mock(StorageJmxOperations.class);
        replicaGroupsOperations = mock(ReplicaGroupsInfoJmxOperations.class);
        endpointOperations = mock(EndpointSnitchJmxOperations.class);
        clusterMembershipOperations = mock(ClusterMembershipJmxOperations.class);
        jmxClient = mock(JmxClient.class);
        dnsResolver = mock(DnsResolver.class);
        instance = new TokenRangeReplicaProvider(jmxClient, dnsResolver);

        when(storageOperations.isGossipRunning()).thenReturn(true);
        when(jmxClient.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService"))
        .thenReturn(storageOperations);
        when(jmxClient.proxy(EndpointSnitchJmxOperations.class, "org.apache.cassandra.db:type=EndpointSnitchInfo"))
        .thenReturn(endpointOperations);
        when(jmxClient.proxy(ClusterMembershipJmxOperations.class, "org.apache.cassandra.net:type=FailureDetector"))
        .thenReturn(clusterMembershipOperations);
        when(dnsResolver.reverseResolve(any())).thenReturn(TEST_HOSTNAME);
        // Cassandra versions without witness replicas do not register the ReplicaGroupsInfo MBean
        when(jmxClient.proxy(ReplicaGroupsInfoJmxOperations.class, "org.apache.cassandra.locator:type=ReplicaGroupsInfo"))
        .thenReturn(replicaGroupsOperations);
        when(replicaGroupsOperations.replicaGroupsForKeyspaceByDatacenter(anyString()))
        .thenThrow(new UndeclaredThrowableException(new InstanceNotFoundException()));
    }

    @Test
    public void replicasByTokenRangeTest() throws UnknownHostException
    {
        Map<List<String>, List<String>> readReplicaMappings = new HashMap<>();
        readReplicaMappings.put(TOKEN_RANGE1, TEST_ENDPOINTS1);

        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE.name())).thenReturn(readReplicaMappings);
        Map<List<String>, List<String>> writeReplicaMappings = new HashMap<>();
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE.name()))
        .thenReturn(writeReplicaMappings);
        when(endpointOperations.getDatacenter(anyString())).thenReturn(TEST_DC1);
        when(storageOperations.getLiveNodesWithPort()).thenReturn(TEST_ENDPOINTS1);
        when(storageOperations.getUnreachableNodesWithPort()).thenReturn(Collections.emptyList());
        when(clusterMembershipOperations.getAllEndpointStatesWithPort()).thenReturn("");

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioners.RANDOM);
        assertThat(result).isNotNull();
        assertThat(result.writeReplicas().size()).isEqualTo(1);
        // Single token range
        assertThat(result.readReplicas().size()).isEqualTo(1);
        // Single DC
        assertThat(result.readReplicas().get(0).replicasByDatacenter().size()).isEqualTo(1);
        assertThat(result.readReplicas().get(0).replicasByDatacenter().get(TEST_DC1)).containsAll(TEST_ENDPOINTS1);
        assertThat(result.replicaMetadata().size()).isEqualTo(3);
        TokenRangeReplicasResponse.ReplicaMetadata nodeMetadata = result.replicaMetadata().get("127.0.0.1:7000");
        assertThat(nodeMetadata.state()).isEqualTo("Normal");
        assertThat(nodeMetadata.status()).isEqualTo("Up");
        assertThat(nodeMetadata.datacenter()).isEqualTo(TEST_DC1);
        assertState(result.replicaMetadata(), "127.0.0.2:7000", "Normal");
        assertState(result.replicaMetadata(), "127.0.0.3:7000", "Normal");
    }

    @Test
    public void replicasByTokenRangeTestMultipleDCs() throws UnknownHostException
    {
        Map<List<String>, List<String>> readReplicaMappings = new HashMap<>();
        readReplicaMappings.put(TOKEN_RANGE1, TEST_ENDPOINTS1);
        readReplicaMappings.put(TOKEN_RANGE2, TEST_ENDPOINTS2);
        List<String> allLiveNodes = Stream.concat(TEST_ENDPOINTS1.stream(), TEST_ENDPOINTS2.stream())
                                          .collect(Collectors.toList());
        when(storageOperations.getLiveNodesWithPort()).thenReturn(allLiveNodes);
        when(storageOperations.getUnreachableNodesWithPort()).thenReturn(Collections.emptyList());

        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE.name())).thenReturn(readReplicaMappings);
        Map<List<String>, List<String>> writeReplicaMappings = new HashMap<>();
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE.name()))
        .thenReturn(writeReplicaMappings);
        when(storageOperations.getLeavingNodesWithPort()).thenReturn(Arrays.asList("128.0.0.1:7000", "127.0.0.2:7000"));
        when(storageOperations.getUnreachableNodesWithPort()).thenReturn(Collections.emptyList());
        when(clusterMembershipOperations.getAllEndpointStatesWithPort()).thenReturn(generateSampleGossip("NORMAL",
                                                                                                         "LEAVING",
                                                                                                         "NORMAL",
                                                                                                         "NORMAL",
                                                                                                         "LEAVING",
                                                                                                         "NORMAL"));


        when(endpointOperations.getDatacenter(startsWith("127"))).thenReturn(TEST_DC1);
        when(endpointOperations.getDatacenter(startsWith("128"))).thenReturn(TEST_DC2);

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioners.RANDOM);
        assertThat(result).isNotNull();
        assertThat(result.writeReplicas().size()).isEqualTo(2);
        // Single token range
        assertThat(result.readReplicas().size()).isEqualTo(2);
        // Single DC per token range
        assertThat(result.readReplicas().get(0).replicasByDatacenter().size()).isEqualTo(1);
        assertThat(result.readReplicas().get(1).replicasByDatacenter().size()).isEqualTo(1);
        assertThat(result.readReplicas().get(0).replicasByDatacenter().get(TEST_DC1)).containsAll(TEST_ENDPOINTS1);
        assertThat(result.readReplicas().get(1).replicasByDatacenter().get(TEST_DC2)).containsAll(TEST_ENDPOINTS2);
        assertThat(result.replicaMetadata().size()).isEqualTo(5);
        TokenRangeReplicasResponse.ReplicaMetadata nodeMetadataDc1 = result.replicaMetadata().get("127.0.0.1:7000");
        assertThat(nodeMetadataDc1.state()).isEqualTo("Normal");
        assertThat(nodeMetadataDc1.status()).isEqualTo("Up");
        assertThat(nodeMetadataDc1.datacenter()).isEqualTo(TEST_DC1);
        assertState(result.replicaMetadata(), "127.0.0.2:7000", "Leaving");
        assertState(result.replicaMetadata(), "127.0.0.3:7000", "Normal");

        TokenRangeReplicasResponse.ReplicaMetadata nodeMetadatDc2 = result.replicaMetadata().get("128.0.0.1:7000");
        assertThat(nodeMetadatDc2.state()).isEqualTo("Leaving");
        assertThat(nodeMetadatDc2.status()).isEqualTo("Up");
        assertThat(nodeMetadatDc2.datacenter()).isEqualTo(TEST_DC2);

        assertState(result.replicaMetadata(), "128.0.0.2:7000", "Normal");
    }

    @Test
    public void replicasByTokenRangeTestMultipleDCsPerTokenRange() throws UnknownHostException
    {
        Map<List<String>, List<String>> readReplicaMappings = new HashMap<>();
        readReplicaMappings.put(TOKEN_RANGE1, TEST_ENDPOINTS1);
        readReplicaMappings.put(TOKEN_RANGE2, TEST_MULTI_DC_ENDPOINTS);
        List<String> allLiveNodes = Stream.concat(TEST_ENDPOINTS1.stream(), TEST_MULTI_DC_ENDPOINTS.stream())
                                          .collect(Collectors.toList());
        when(storageOperations.getLiveNodesWithPort()).thenReturn(allLiveNodes);
        when(storageOperations.getUnreachableNodesWithPort()).thenReturn(Collections.emptyList());

        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE.name())).thenReturn(readReplicaMappings);
        Map<List<String>, List<String>> writeReplicaMappings = new HashMap<>();
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE.name()))
        .thenReturn(writeReplicaMappings);
        when(storageOperations.getLeavingNodesWithPort()).thenReturn(Arrays.asList("127.0.0.1:7000",
                                                                                   "127.0.0.2:7000",
                                                                                   "128.0.0.1:7000"));
        when(clusterMembershipOperations.getAllEndpointStatesWithPort())
        .thenReturn(generateSampleGossip("LEAVING",
                                         "LEAVING",
                                         "NORMAL",
                                         "NORMAL",
                                         "LEAVING",
                                         "NORMAL"));

        when(endpointOperations.getDatacenter(startsWith("127"))).thenReturn(TEST_DC1);
        when(endpointOperations.getDatacenter(startsWith("128"))).thenReturn(TEST_DC2);

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioners.RANDOM);
        assertThat(result).isNotNull();
        assertThat(result.writeReplicas().size()).isEqualTo(2);
        // 2 token ranges
        assertThat(result.readReplicas().size()).isEqualTo(2);

        // Validate token range has entries from both DCs
        TokenRangeReplicasResponse.ReplicaInfo replicaInfoWithMultipleDCs =
        result.readReplicas().stream().filter(r -> TOKEN_RANGE2.get(0).equals(r.start())).findAny().get();
        assertThat(replicaInfoWithMultipleDCs.replicasByDatacenter().size()).isEqualTo(2);
        assertThat(result.readReplicas().get(0).replicasByDatacenter().get(TEST_DC1)).containsAll(TEST_ENDPOINTS1);
        assertThat(result.readReplicas().get(1).replicasByDatacenter().get(TEST_DC2)).containsAll(TEST_ENDPOINTS2);
        assertState(result.replicaMetadata(), "127.0.0.1:7000", "Leaving");
        assertState(result.replicaMetadata(), "127.0.0.2:7000", "Leaving");
        assertState(result.replicaMetadata(), "127.0.0.3:7000", "Normal");
        assertState(result.replicaMetadata(), "127.0.0.4:7000", "Normal");
        assertState(result.replicaMetadata(), "128.0.0.1:7000", "Leaving");
        assertState(result.replicaMetadata(), "128.0.0.2:7000", "Normal");
    }

    @Test
    public void readAndWriteReplicasByTokenRangeTest() throws UnknownHostException
    {
        Map<List<String>, List<String>> readReplicaMappings = new HashMap<>();
        readReplicaMappings.put(TOKEN_RANGE1, TEST_ENDPOINTS1);

        Map<List<String>, List<String>> writeReplicaMappings = new HashMap<>();
        writeReplicaMappings.put(TOKEN_RANGE2, TEST_ENDPOINTS2);

        when(storageOperations.getLiveNodesWithPort()).thenReturn(TEST_ENDPOINTS1);
        when(storageOperations.getUnreachableNodesWithPort()).thenReturn(Collections.emptyList());


        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE.name()))
        .thenReturn(readReplicaMappings);
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE.name()))
        .thenReturn(writeReplicaMappings);
        when(storageOperations.getLeavingNodesWithPort()).thenReturn(Arrays.asList("127.0.0.3:7000", "128.0.0.1:7000"));
        when(clusterMembershipOperations.getAllEndpointStatesWithPort())
        .thenReturn(generateSampleGossip("NORMAL",
                                         "NORMAL",
                                         "LEAVING",
                                         "NORMAL",
                                         "LEAVING",
                                         "NORMAL"));

        when(endpointOperations.getDatacenter(startsWith("127"))).thenReturn(TEST_DC1);
        when(endpointOperations.getDatacenter(startsWith("128"))).thenReturn(TEST_DC2);

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioners.RANDOM);
        assertThat(result).isNotNull();
        assertThat(result.writeReplicas().size()).isEqualTo(2);
        assertThat(result.readReplicas().size()).isEqualTo(1);
        assertThat(result.readReplicas().get(0).replicasByDatacenter().size()).isEqualTo(1);
        assertThat(result.readReplicas().get(0).replicasByDatacenter().get(TEST_DC1)).containsAll(TEST_ENDPOINTS1);
        assertState(result.replicaMetadata(), "127.0.0.1:7000", "Normal");
        assertState(result.replicaMetadata(), "127.0.0.2:7000", "Normal");
        assertState(result.replicaMetadata(), "127.0.0.3:7000", "Leaving");
        assertState(result.replicaMetadata(), "128.0.0.1:7000", "Leaving");
        assertState(result.replicaMetadata(), "128.0.0.2:7000", "Normal");
    }

    @Test
    public void tokenRangeBeforeNodeJoins() throws UnknownHostException
    {
        Map<List<String>, List<String>> rangeToEndpointWithPortMap = ImmutableMap.of(
        Arrays.asList("3074457345618258602", "-9223372036854775808"),
        Arrays.asList("127.0.0.1:7000", "127.0.0.2:7000", "127.0.0.3:7000"),
        Arrays.asList("-3074457345618258603", "3074457345618258602"),
        Arrays.asList("127.0.0.3:7000", "127.0.0.1:7000", "127.0.0.2:7000"),
        Arrays.asList("-9223372036854775808", "-3074457345618258603"),
        Arrays.asList("127.0.0.2:7000", "127.0.0.3:7000", "127.0.0.1:7000")
        );
        Map<List<String>, List<String>> pendingRangeToEndpointWithPortMap = Collections.emptyMap();

        when(storageOperations.getLiveNodesWithPort()).thenReturn(TEST_ENDPOINTS1);
        when(storageOperations.getUnreachableNodesWithPort()).thenReturn(Collections.emptyList());
        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE.name()))
        .thenReturn(rangeToEndpointWithPortMap);
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE.name()))
        .thenReturn(pendingRangeToEndpointWithPortMap);
        when(endpointOperations.getDatacenter(anyString())).thenReturn(TEST_DC1);
        when(storageOperations.getLeavingNodesWithPort()).thenReturn(Arrays.asList("127.0.0.4:7000", "128.0.0.2:7000"));
        when(clusterMembershipOperations.getAllEndpointStatesWithPort())
        .thenReturn(generateSampleGossip("NORMAL",
                                         "NORMAL",
                                         "NORMAL",
                                         "LEAVING",
                                         "NORMAL",
                                         "LEAVING"));

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioners.MURMUR3);
        assertThat(result).isNotNull();
        assertThat(result.readReplicas()).hasSize(3);
        assertThat(validateRangeExists(result.readReplicas(), "3074457345618258602",
                                       Long.toString(Long.MAX_VALUE))).isTrue();
        assertThat(validateRangeExists(result.writeReplicas(), "3074457345618258602",
                                       Long.toString(Long.MAX_VALUE))).isTrue();
        assertState(result.replicaMetadata(), "127.0.0.1:7000", "Normal");
        assertState(result.replicaMetadata(), "127.0.0.2:7000", "Normal");
        assertState(result.replicaMetadata(), "127.0.0.3:7000", "Normal");
    }

    @Test
    public void tokenRangeDuringNodeJoin() throws UnknownHostException
    {
        Map<List<String>, List<String>> rangeToEndpointWithPortMap = ImmutableMap.of(
        Arrays.asList("3074457345618258602", "-9223372036854775808"),
        Arrays.asList("127.0.0.1:7000", "127.0.0.2:7000", "127.0.0.3:7000"),
        Arrays.asList("-3074457345618258603", "3074457345618258602"),
        Arrays.asList("127.0.0.3:7000", "127.0.0.1:7000", "127.0.0.2:7000"),
        Arrays.asList("-9223372036854775808", "-3074457345618258603"),
        Arrays.asList("127.0.0.2:7000", "127.0.0.3:7000", "127.0.0.1:7000")
        );
        Map<List<String>, List<String>> pendingRangeToEndpointWithPortMap = ImmutableMap.of(
        Arrays.asList("6148914691236517204", "9223372036854775807"),
        Collections.singletonList("127.0.0.4:7000"),
        Arrays.asList("-3074457345618258603", "3074457345618258602"),
        Collections.singletonList("127.0.0.4:7000"),
        Arrays.asList("-9223372036854775808", "-3074457345618258603"),
        Collections.singletonList("127.0.0.4:7000")
        );

        when(storageOperations.getLiveNodesWithPort()).thenReturn(TEST_ENDPOINTS1);
        when(storageOperations.getUnreachableNodesWithPort()).thenReturn(Collections.emptyList());

        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE.name()))
        .thenReturn(rangeToEndpointWithPortMap);
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE.name()))
        .thenReturn(pendingRangeToEndpointWithPortMap);
        when(endpointOperations.getDatacenter(anyString())).thenReturn(TEST_DC1);
        when(storageOperations.getLeavingNodesWithPort()).thenReturn(Arrays.asList("127.0.0.2:7000", "127.0.0.4:7000"));
        when(clusterMembershipOperations.getAllEndpointStatesWithPort()).thenReturn(generateSampleGossip("NORMAL",
                                                                                                         "LEAVING",
                                                                                                         "NORMAL",
                                                                                                         "LEAVING",
                                                                                                         "NORMAL",
                                                                                                         "NORMAL"));

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioners.MURMUR3);
        assertThat(result).isNotNull();
        assertThat(result.readReplicas()).hasSize(3);
        assertThat(result.writeReplicas()).hasSize(4);
        // Write replicas includes the new range ending at "maxToken"
        assertThat(validateRangeExists(result.writeReplicas(), "6148914691236517204",
                                       Long.toString(Long.MAX_VALUE))).isTrue();
        // Read replicas does NOT include the new range from pending ranges
        assertThat(validateRangeExists(result.readReplicas(), "6148914691236517204",
                                       Long.toString(Long.MAX_VALUE))).isFalse();
        // Existing read replicas wrap-around range ends at "maxToken"
        assertThat(validateRangeExists(result.readReplicas(), "3074457345618258602",
                                       Long.toString(Long.MAX_VALUE))).isTrue();
        assertState(result.replicaMetadata(), "127.0.0.1:7000", "Normal");
        assertState(result.replicaMetadata(), "127.0.0.2:7000", "Leaving");
        assertState(result.replicaMetadata(), "127.0.0.3:7000", "Normal");
        assertState(result.replicaMetadata(), "127.0.0.4:7000", "Leaving");
    }

    @Test
    public void tokenRangeAfterNodeJoins() throws UnknownHostException
    {
        Map<List<String>, List<String>> rangeToEndpointWithPortMap = ImmutableMap.of(
        Arrays.asList("6148914691236517204", "-9223372036854775808"),
        Arrays.asList("127.0.0.1:7000", "127.0.0.2:7000", "127.0.0.3:7000"),
        Arrays.asList("3074457345618258602", "6148914691236517204"),
        Arrays.asList("127.0.0.4:7000", "127.0.0.1:7000", "127.0.0.2:7000"),
        Arrays.asList("-3074457345618258603", "3074457345618258602"),
        Arrays.asList("127.0.0.3:7000", "127.0.0.4:7000", "127.0.0.1:7000"),
        Arrays.asList("-9223372036854775808", "-3074457345618258603"),
        Arrays.asList("127.0.0.2:7000", "127.0.0.3:7000", "127.0.0.4:7000")
        );
        Map<List<String>, List<String>> pendingRangeToEndpointWithPortMap = Collections.emptyMap();

        when(storageOperations.getLiveNodesWithPort())
        .thenReturn(Arrays.asList("127.0.0.1:7000", "127.0.0.2:7000", "127.0.0.3:7000", "127.0.0.4:7000"));
        when(storageOperations.getUnreachableNodesWithPort()).thenReturn(Collections.emptyList());

        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE.name()))
        .thenReturn(rangeToEndpointWithPortMap);
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE.name()))
        .thenReturn(pendingRangeToEndpointWithPortMap);
        when(endpointOperations.getDatacenter(anyString())).thenReturn(TEST_DC1);
        when(storageOperations.getLeavingNodesWithPort()).thenReturn(Arrays.asList("127.0.0.1:7000", "128.0.0.1:7000"));
        when(storageOperations.getJoiningNodesWithPort()).thenReturn(Collections.singletonList("127.0.0.4:7000"));
        when(clusterMembershipOperations.getAllEndpointStatesWithPort()).thenReturn(generateSampleGossip("LEAVING",
                                                                                                         "NORMAL",
                                                                                                         "NORMAL",
                                                                                                         "BOOT_REPLACE",
                                                                                                         "NORMAL",
                                                                                                         "NORMAL"));

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioners.MURMUR3);
        assertThat(result).isNotNull();
        assertThat(result.readReplicas()).hasSize(4);
        assertThat(result.writeReplicas()).hasSize(4);
        assertThat(validateRangeExists(result.readReplicas(), "6148914691236517204",
                                       Long.toString(Long.MAX_VALUE))).isTrue();
        assertThat(validateRangeExists(result.writeReplicas(), "6148914691236517204",
                                       Long.toString(Long.MAX_VALUE))).isTrue();

        assertState(result.replicaMetadata(), "127.0.0.1:7000", "Leaving");
        assertState(result.replicaMetadata(), "127.0.0.2:7000", "Normal");
        assertState(result.replicaMetadata(), "127.0.0.3:7000", "Normal");
        assertState(result.replicaMetadata(), "127.0.0.4:7000", "Replacing");
    }

    @Test
    public void tokenRangeAfterNodeJoinsGossipVariant() throws UnknownHostException
    {
        Map<List<String>, List<String>> rangeToEndpointWithPortMap = ImmutableMap.of(
        Arrays.asList("6148914691236517204", "-9223372036854775808"),
        Arrays.asList("127.0.0.1:7000", "127.0.0.2:7000", "127.0.0.3:7000"),
        Arrays.asList("3074457345618258602", "6148914691236517204"),
        Arrays.asList("127.0.0.4:7000", "127.0.0.1:7000", "127.0.0.2:7000"),
        Arrays.asList("-3074457345618258603", "3074457345618258602"),
        Arrays.asList("127.0.0.3:7000", "127.0.0.4:7000", "127.0.0.1:7000"),
        Arrays.asList("-9223372036854775808", "-3074457345618258603"),
        Arrays.asList("127.0.0.2:7000", "127.0.0.3:7000", "127.0.0.4:7000")
        );
        Map<List<String>, List<String>> pendingRangeToEndpointWithPortMap = Collections.emptyMap();

        when(storageOperations.getLiveNodesWithPort())
        .thenReturn(Arrays.asList("127.0.0.1:7000", "127.0.0.2:7000", "127.0.0.3:7000", "127.0.0.4:7000"));
        when(storageOperations.getUnreachableNodesWithPort()).thenReturn(Collections.emptyList());

        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE.name()))
        .thenReturn(rangeToEndpointWithPortMap);
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE.name()))
        .thenReturn(pendingRangeToEndpointWithPortMap);
        when(endpointOperations.getDatacenter(anyString())).thenReturn(TEST_DC1);
        when(storageOperations.getLeavingNodesWithPort()).thenReturn(Arrays.asList("127.0.0.1:7000", "128.0.0.1:7000"));
        when(storageOperations.getJoiningNodesWithPort()).thenReturn(Collections.singletonList("127.0.0.4:7000"));
        when(clusterMembershipOperations.getAllEndpointStatesWithPort()).thenReturn(generateSampleGossip("LEAVING",
                                                                                                         "NORMAL",
                                                                                                         "NORMAL",
                                                                                                         "BOOT_REPLACE",
                                                                                                         "NORMAL",
                                                                                                         "NORMAL",
                                                                                                         true));

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioners.MURMUR3);
        assertThat(result).isNotNull();
        assertThat(result.readReplicas()).hasSize(4);
        assertThat(result.writeReplicas()).hasSize(4);
        assertThat(validateRangeExists(result.readReplicas(), "6148914691236517204",
                                       Long.toString(Long.MAX_VALUE))).isTrue();
        assertThat(validateRangeExists(result.writeReplicas(), "6148914691236517204",
                                       Long.toString(Long.MAX_VALUE))).isTrue();

        Map<String, TokenRangeReplicasResponse.ReplicaMetadata> map = result.replicaMetadata();
        assertState(map, "127.0.0.1:7000", "Leaving");
        assertState(map, "127.0.0.2:7000", "Normal");
        assertState(map, "127.0.0.3:7000", "Normal");
        assertState(map, "127.0.0.4:7000", "Replacing");
    }

    private boolean validateRangeExists(List<TokenRangeReplicasResponse.ReplicaInfo> ranges, String start, String end)
    {
        return ranges.stream().anyMatch(r -> (r.start().equals(start) && r.end().equals(end)));
    }

    private String generateSampleGossip(String dc1Node1Status,
                                        String dc1Node2Status,
                                        String dc1Node3Status,
                                        String dc1Node4Status,
                                        String dc2Node1Status,
                                        String dc2Node2Status)
    {
        return generateSampleGossip(dc1Node1Status,
                                    dc1Node2Status,
                                    dc1Node3Status,
                                    dc1Node4Status,
                                    dc2Node1Status,
                                    dc2Node2Status,
                                    false);
    }

    private String generateSampleGossip(String dc1Node1Status,
                                        String dc1Node2Status,
                                        String dc1Node3Status,
                                        String dc1Node4Status,
                                        String dc2Node1Status,
                                        String dc2Node2Status,
                                        boolean excludeStatus)
    {
        return String.format("/127.0.0.1:7000%n" +
                             getStatus(excludeStatus, "  STATUS:16:%s,9223372036854775805%n", dc1Node1Status) +
                             "  HOST_ID:21:00000000-0000-4000-8000-000000000003%n" +
                             "  STATUS_WITH_PORT:17:%s,9223372036854775805%n" +
                             "/127.0.0.2:7000%n" +
                             getStatus(excludeStatus, "  STATUS:9:%s,3074457345618258601%n", dc1Node2Status) +
                             "  HOST_ID:14:00000000-0000-4000-8000-000000000002%n" +
                             "  STATUS_WITH_PORT:10:%s,3074457345618258601%n" +
                             "/127.0.0.3:7000%n" +
                             getStatus(excludeStatus, "  STATUS:2:%s,-3074457345618258603%n", dc1Node3Status) +
                             "  HOST_ID:7:00000000-0000-4000-8000-000000000001%n" +
                             "  STATUS_WITH_PORT:3:%s,-3074457345618258603%n" +
                             "/127.0.0.4:7000%n" +
                             getStatus(excludeStatus, "  STATUS:2:%s,-3074457345618258603%n", dc1Node4Status) +
                             "  HOST_ID:7:00000000-0000-4000-8000-000000000004%n" +
                             "  STATUS_WITH_PORT:3:%s,-3074457345618258603%n" +
                             "/128.0.0.1:7000%n" +
                             getStatus(excludeStatus, "  STATUS:2:%s,-3074457345618258603%n", dc2Node1Status) +
                             "  HOST_ID:7:00000000-0000-4000-8000-000000000001%n" +
                             "  STATUS_WITH_PORT:3:%s,-3074457345618258603%n" +
                             "/128.0.0.2:7000%n" +
                             getStatus(excludeStatus, "  STATUS:2:%s,-3074457345618258603%n", dc2Node2Status) +
                             "  HOST_ID:7:00000000-0000-4000-8000-000000000002%n" +
                             "  STATUS_WITH_PORT:3:%s,-3074457345618258603%n",
                             dc1Node1Status, dc1Node2Status,
                             dc1Node3Status, dc1Node4Status,
                             dc2Node1Status, dc1Node2Status);
    }

    private String getStatus(boolean exclude, String status, String value)
    {
        return exclude ? "" : String.format(status, value);
    }

    @Test
    public void replicasAndWitnessesAreTakenFromReplicaGroups()
    {
        CompositeData[] placement = ranges(range("100", MIN_TOKEN, datacenter(TEST_DC1, full(1), full(2), full(3))),
                                           range(MIN_TOKEN, "0", datacenter(TEST_DC1, full(2), full(3), full(1))),
                                           range("0", "100", datacenter(TEST_DC1, full(3), full(1), witness(2))));
        stubReplicaGroups(replicaGroups(placement,
                                        placement,
                                        peer(1, TEST_DC1, "127.0.0.1:7000", "JOINED", "UP"),
                                        peer(2, TEST_DC1, "127.0.0.2:7000", "JOINED", "UP"),
                                        peer(3, TEST_DC1, "127.0.0.3:7000", "JOINED", "DOWN")));

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioners.MURMUR3);

        // sorted by start, with the range that wraps reported ending at the maximum token
        assertThat(result.readReplicas()).extracting(TokenRangeReplicasResponse.ReplicaInfo::start)
                                         .containsExactly(MIN_TOKEN, "0", "100");
        assertThat(result.readReplicas()).extracting(TokenRangeReplicasResponse.ReplicaInfo::end)
                                         .containsExactly("0", "100", MAX_TOKEN);
        TokenRangeReplicasResponse.ReplicaInfo withWitness = result.readReplicas().get(1);
        assertThat(withWitness.replicasByDatacenter().get(TEST_DC1))
        .containsExactlyInAnyOrder("127.0.0.1:7000", "127.0.0.2:7000", "127.0.0.3:7000");
        assertThat(withWitness.witnessesByDatacenter()).isEqualTo(ImmutableMap.of(TEST_DC1, Collections.singletonList("127.0.0.2:7000")));
        // ranges without witnesses report none, rather than leaving them unreported
        assertThat(result.readReplicas().get(0).witnessesByDatacenter()).isEmpty();
        assertThat(result.readReplicas().get(2).witnessesByDatacenter()).isEmpty();
        // witnesses are reported for write replicas too
        assertThat(result.writeReplicas()).isEqualTo(result.readReplicas());

        assertThat(result.replicaMetadata()).hasSize(3);
        TokenRangeReplicasResponse.ReplicaMetadata metadata = result.replicaMetadata().get("127.0.0.3:7000");
        assertThat(metadata.state()).isEqualTo("Normal");
        assertThat(metadata.status()).isEqualTo("Down");
        assertThat(metadata.fqdn()).isEqualTo(TEST_HOSTNAME);
        assertThat(metadata.address()).isEqualTo("127.0.0.3");
        assertThat(metadata.port()).isEqualTo(7000);
        assertThat(metadata.datacenter()).isEqualTo(TEST_DC1);
        // the StorageService MBean is not consulted
        verify(storageOperations, never()).getRangeToEndpointWithPortMap(anyString());
    }

    @Test
    public void pendingReplicasAreOnlyWriteReplicas()
    {
        CompositeData[] readPlacement = ranges(range(MIN_TOKEN, MIN_TOKEN, datacenter(TEST_DC1, full(1), full(2))));
        CompositeData[] writePlacement = ranges(range(MIN_TOKEN, MIN_TOKEN, datacenter(TEST_DC1, full(1), full(2), full(3))));
        stubReplicaGroups(replicaGroups(readPlacement,
                                        writePlacement,
                                        peer(1, TEST_DC1, "127.0.0.1:7000", "JOINED", "UP"),
                                        peer(2, TEST_DC1, "127.0.0.2:7000", "JOINED", "UP"),
                                        peer(3, TEST_DC1, "127.0.0.3:7000", "BOOTSTRAPPING", "UP")));

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioners.MURMUR3);

        assertThat(result.readReplicas().get(0).replicasByDatacenter().get(TEST_DC1))
        .containsExactlyInAnyOrder("127.0.0.1:7000", "127.0.0.2:7000");
        assertThat(result.writeReplicas().get(0).replicasByDatacenter().get(TEST_DC1))
        .containsExactlyInAnyOrder("127.0.0.1:7000", "127.0.0.2:7000", "127.0.0.3:7000");
        assertState(result.replicaMetadata(), "127.0.0.3:7000", "Joining");
    }

    @Test
    public void replicasPerDatacenterAreTakenFromReplicaGroups()
    {
        CompositeData[] placement = ranges(range(MIN_TOKEN, MIN_TOKEN,
                                                 datacenter(TEST_DC1, full(1), witness(2)),
                                                 datacenter(TEST_DC2, full(3), witness(4))));
        stubReplicaGroups(replicaGroups(placement,
                                        placement,
                                        peer(1, TEST_DC1, "127.0.0.1:7000", "JOINED", "UP"),
                                        peer(2, TEST_DC1, "127.0.0.2:7000", "JOINED", "UP"),
                                        peer(3, TEST_DC2, "128.0.0.1:7000", "JOINED", "UP"),
                                        peer(4, TEST_DC2, "128.0.0.2:7000", "BOOT_REPLACING", "UP")));

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioners.MURMUR3);

        assertThat(result.readReplicas().get(0).witnessesByDatacenter())
        .isEqualTo(ImmutableMap.of(TEST_DC1, Collections.singletonList("127.0.0.2:7000"),
                                   TEST_DC2, Collections.singletonList("128.0.0.2:7000")));
        assertThat(result.replicaMetadata().get("128.0.0.1:7000").datacenter()).isEqualTo(TEST_DC2);
        assertState(result.replicaMetadata(), "128.0.0.2:7000", "Replacing");
    }

    @Test
    public void witnessesAreNotReportedWhenCassandraDoesNotSupportThem() throws UnknownHostException
    {
        Map<List<String>, List<String>> readReplicaMappings = new HashMap<>();
        readReplicaMappings.put(TOKEN_RANGE1, TEST_ENDPOINTS1);
        stubSingleDatacenter(readReplicaMappings);

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioners.RANDOM);

        assertThat(result.readReplicas()).hasSize(1);
        assertThat(result.readReplicas().get(0).replicasByDatacenter().get(TEST_DC1)).containsAll(TEST_ENDPOINTS1);
        assertThat(result.readReplicas().get(0).witnessesByDatacenter()).isNull();
        assertThat(result.writeReplicas()).allSatisfy(replicaInfo -> assertThat(replicaInfo.witnessesByDatacenter()).isNull());
    }

    @Test
    public void replicasAreStillReportedWhenReplicaGroupsAreUnusable() throws UnknownHostException
    {
        Map<List<String>, List<String>> readReplicaMappings = new HashMap<>();
        readReplicaMappings.put(TOKEN_RANGE1, TEST_ENDPOINTS1);
        stubSingleDatacenter(readReplicaMappings);
        // node 2 is a replica but missing from peer_metadata
        CompositeData[] placement = ranges(range(MIN_TOKEN, MIN_TOKEN, datacenter(TEST_DC1, full(1), witness(2))));
        stubReplicaGroups(replicaGroups(placement, placement, peer(1, TEST_DC1, "127.0.0.1:7000", "JOINED", "UP")));

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioners.RANDOM);

        // built from the StorageService MBean instead, without witnesses
        assertThat(result.readReplicas().get(0).replicasByDatacenter().get(TEST_DC1)).containsAll(TEST_ENDPOINTS1);
        assertThat(result.readReplicas().get(0).witnessesByDatacenter()).isNull();
    }

    @Test
    public void missingJmxOperationIsRecognized()
    {
        assertThat(TokenRangeReplicaProvider.isMissingOperation(
        new UndeclaredThrowableException(new InstanceNotFoundException()))).isTrue();
        assertThat(TokenRangeReplicaProvider.isMissingOperation(
        new UndeclaredThrowableException(new ReflectionException(new NoSuchMethodException())))).isTrue();
        assertThat(TokenRangeReplicaProvider.isMissingOperation(new IllegalStateException("failure inside Cassandra")))
        .isFalse();
        assertThat(TokenRangeReplicaProvider.isMissingOperation(
        new UndeclaredThrowableException(new IOException("connection reset")))).isFalse();
    }

    @Test
    public void nodeStatesAndStatusesMapToTheReportedOnes()
    {
        assertThat(TokenRangeReplicaProvider.state("JOINED")).isEqualTo("Normal");
        assertThat(TokenRangeReplicaProvider.state("REGISTERED")).isEqualTo("Joining");
        assertThat(TokenRangeReplicaProvider.state("BOOTSTRAPPING")).isEqualTo("Joining");
        assertThat(TokenRangeReplicaProvider.state("BOOT_REPLACING")).isEqualTo("Replacing");
        assertThat(TokenRangeReplicaProvider.state("LEAVING")).isEqualTo("Leaving");
        assertThat(TokenRangeReplicaProvider.state("LEFT")).isEqualTo("Leaving");
        assertThat(TokenRangeReplicaProvider.state("MOVING")).isEqualTo("Moving");
        assertThat(TokenRangeReplicaProvider.status("UP")).isEqualTo("Up");
        assertThat(TokenRangeReplicaProvider.status("DOWN")).isEqualTo("Down");
        assertThat(TokenRangeReplicaProvider.status("SOMETHING_ELSE")).isEqualTo("Unknown");
    }

    private void stubReplicaGroups(CompositeData replicaGroups)
    {
        reset(replicaGroupsOperations);
        when(replicaGroupsOperations.replicaGroupsForKeyspaceByDatacenter(TEST_KEYSPACE.name())).thenReturn(replicaGroups);
    }

    private void stubSingleDatacenter(Map<List<String>, List<String>> readReplicaMappings) throws UnknownHostException
    {
        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE.name())).thenReturn(readReplicaMappings);
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE.name())).thenReturn(new HashMap<>());
        when(endpointOperations.getDatacenter(anyString())).thenReturn(TEST_DC1);
        when(storageOperations.getLiveNodesWithPort()).thenReturn(TEST_ENDPOINTS1);
        when(storageOperations.getUnreachableNodesWithPort()).thenReturn(Collections.emptyList());
        when(clusterMembershipOperations.getAllEndpointStatesWithPort()).thenReturn("");
    }

    private static CompositeData full(int nodeId)
    {
        return replica(nodeId, "FULL");
    }

    private static CompositeData witness(int nodeId)
    {
        return replica(nodeId, "TRANSIENT");
    }

    private static void assertState(Map<String, TokenRangeReplicasResponse.ReplicaMetadata> map,
                                    String ipAndPort,
                                    String expectedState)
    {
        assertThat(map.get(ipAndPort)).isNotNull()
                                      .extracting(from(TokenRangeReplicasResponse.ReplicaMetadata::state),
                                                  as(InstanceOfAssertFactories.STRING))
                                      .isEqualTo(expectedState);
    }
}
