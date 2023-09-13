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

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
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
    public static final String TEST_KEYSPACE = "test_keyspace";

    StorageJmxOperations storageOperations;
    EndpointSnitchJmxOperations endpointOperations;
    ClusterMembershipJmxOperations clusterMembershipOperations;
    JmxClient jmxClient;
    TokenRangeReplicaProvider instance;

    @BeforeEach
    void setup()
    {
        storageOperations = mock(StorageJmxOperations.class);
        endpointOperations = mock(EndpointSnitchJmxOperations.class);
        clusterMembershipOperations = mock(ClusterMembershipJmxOperations.class);
        jmxClient = mock(JmxClient.class);
        instance = new TokenRangeReplicaProvider(jmxClient);

        when(jmxClient.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService"))
        .thenReturn(storageOperations);
        when(jmxClient.proxy(EndpointSnitchJmxOperations.class, "org.apache.cassandra.db:type=EndpointSnitchInfo"))
        .thenReturn(endpointOperations);
        when(jmxClient.proxy(ClusterMembershipJmxOperations.class, "org.apache.cassandra.net:type=FailureDetector"))
        .thenReturn(clusterMembershipOperations);
    }

    @Test
    public void replicasByTokenRangeTest() throws UnknownHostException
    {
        Map<List<String>, List<String>> readReplicaMappings = new HashMap<>();
        readReplicaMappings.put(TOKEN_RANGE1, TEST_ENDPOINTS1);

        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(readReplicaMappings);
        Map<List<String>, List<String>> writeReplicaMappings = new HashMap<>();
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(writeReplicaMappings);
        when(endpointOperations.getDatacenter(anyString())).thenReturn(TEST_DC1);
        when(clusterMembershipOperations.getAllEndpointStatesWithPort()).thenReturn("");

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioner.Random);
        assertThat(result).isNotNull();
        assertThat(result.writeReplicas().size()).isEqualTo(1);
        // Single token range
        assertThat(result.readReplicas().size()).isEqualTo(1);
        // Single DC
        assertThat(result.readReplicas().get(0).replicasByDatacenter().size()).isEqualTo(1);
        assertThat(result.readReplicas().get(0).replicasByDatacenter().get(TEST_DC1)).containsAll(TEST_ENDPOINTS1);
        assertThat(result.replicaState().size()).isEqualTo(3);
        assertThat(result.replicaState().get("127.0.0.1:7000")).isEqualTo("Normal");
        assertThat(result.replicaState().get("127.0.0.2:7000")).isEqualTo("Normal");
        assertThat(result.replicaState().get("127.0.0.3:7000")).isEqualTo("Normal");
    }

    @Test
    public void replicasByTokenRangeTestMultipleDCs() throws UnknownHostException
    {
        Map<List<String>, List<String>> readReplicaMappings = new HashMap<>();
        readReplicaMappings.put(TOKEN_RANGE1, TEST_ENDPOINTS1);
        readReplicaMappings.put(TOKEN_RANGE2, TEST_ENDPOINTS2);

        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(readReplicaMappings);
        Map<List<String>, List<String>> writeReplicaMappings = new HashMap<>();
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(writeReplicaMappings);
        when(storageOperations.getLeavingNodesWithPort()).thenReturn(Arrays.asList("128.0.0.1:7000", "127.0.0.2:7000"));
        when(clusterMembershipOperations.getAllEndpointStatesWithPort()).thenReturn(generateSampleGossip("NORMAL",
                                                                                                         "LEAVING",
                                                                                                         "NORMAL",
                                                                                                         "NORMAL",
                                                                                                         "LEAVING",
                                                                                                         "NORMAL"));


        when(endpointOperations.getDatacenter(startsWith("127"))).thenReturn(TEST_DC1);
        when(endpointOperations.getDatacenter(startsWith("128"))).thenReturn(TEST_DC2);

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas("test_keyspace", Partitioner.Random);
        assertThat(result).isNotNull();
        assertThat(result.writeReplicas().size()).isEqualTo(2);
        // Single token range
        assertThat(result.readReplicas().size()).isEqualTo(2);
        // Single DC per token range
        assertThat(result.readReplicas().get(0).replicasByDatacenter().size()).isEqualTo(1);
        assertThat(result.readReplicas().get(1).replicasByDatacenter().size()).isEqualTo(1);
        assertThat(result.readReplicas().get(0).replicasByDatacenter().get(TEST_DC1)).containsAll(TEST_ENDPOINTS1);
        assertThat(result.readReplicas().get(1).replicasByDatacenter().get(TEST_DC2)).containsAll(TEST_ENDPOINTS2);
        assertThat(result.replicaState().size()).isEqualTo(5);
        assertThat(result.replicaState().get("127.0.0.1:7000")).isEqualTo("Normal");
        assertThat(result.replicaState().get("127.0.0.2:7000")).isEqualTo("Leaving");
        assertThat(result.replicaState().get("127.0.0.3:7000")).isEqualTo("Normal");
        assertThat(result.replicaState().get("128.0.0.1:7000")).isEqualTo("Leaving");
        assertThat(result.replicaState().get("128.0.0.2:7000")).isEqualTo("Normal");
    }

    @Test
    public void replicasByTokenRangeTestMultipleDCsPerTokenRange() throws UnknownHostException
    {
        Map<List<String>, List<String>> readReplicaMappings = new HashMap<>();
        readReplicaMappings.put(TOKEN_RANGE1, TEST_ENDPOINTS1);
        readReplicaMappings.put(TOKEN_RANGE2, TEST_MULTI_DC_ENDPOINTS);

        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(readReplicaMappings);
        Map<List<String>, List<String>> writeReplicaMappings = new HashMap<>();
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(writeReplicaMappings);
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

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas("test_keyspace", Partitioner.Random);
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
        assertThat(result.replicaState().get("127.0.0.1:7000")).isEqualTo("Leaving");
        assertThat(result.replicaState().get("127.0.0.2:7000")).isEqualTo("Leaving");
        assertThat(result.replicaState().get("127.0.0.3:7000")).isEqualTo("Normal");
        assertThat(result.replicaState().get("127.0.0.4:7000")).isEqualTo("Normal");
        assertThat(result.replicaState().get("128.0.0.1:7000")).isEqualTo("Leaving");
        assertThat(result.replicaState().get("128.0.0.2:7000")).isEqualTo("Normal");
    }

    @Test
    public void readAndWriteReplicasByTokenRangeTest() throws UnknownHostException
    {
        Map<List<String>, List<String>> readReplicaMappings = new HashMap<>();
        readReplicaMappings.put(TOKEN_RANGE1, TEST_ENDPOINTS1);

        Map<List<String>, List<String>> writeReplicaMappings = new HashMap<>();
        writeReplicaMappings.put(TOKEN_RANGE2, TEST_ENDPOINTS2);

        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(readReplicaMappings);
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(writeReplicaMappings);
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

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas("test_keyspace", Partitioner.Random);
        assertThat(result).isNotNull();
        assertThat(result.writeReplicas().size()).isEqualTo(2);
        assertThat(result.readReplicas().size()).isEqualTo(1);
        assertThat(result.readReplicas().get(0).replicasByDatacenter().size()).isEqualTo(1);
        assertThat(result.readReplicas().get(0).replicasByDatacenter().get(TEST_DC1)).containsAll(TEST_ENDPOINTS1);
        assertThat(result.replicaState().get("127.0.0.1:7000")).isEqualTo("Normal");
        assertThat(result.replicaState().get("127.0.0.2:7000")).isEqualTo("Normal");
        assertThat(result.replicaState().get("127.0.0.3:7000")).isEqualTo("Leaving");
        assertThat(result.replicaState().get("128.0.0.1:7000")).isEqualTo("Leaving");
        assertThat(result.replicaState().get("128.0.0.2:7000")).isEqualTo("Normal");
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

        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(rangeToEndpointWithPortMap);
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE))
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

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioner.Murmur3);
        assertThat(result).isNotNull();
        assertThat(result.readReplicas()).hasSize(3);
        assertThat(validateRangeExists(result.readReplicas(), "3074457345618258602",
                                       Long.toString(Long.MAX_VALUE))).isTrue();
        assertThat(validateRangeExists(result.writeReplicas(), "3074457345618258602",
                                       Long.toString(Long.MAX_VALUE))).isTrue();
        assertThat(result.replicaState().get("127.0.0.1:7000")).isEqualTo("Normal");
        assertThat(result.replicaState().get("127.0.0.2:7000")).isEqualTo("Normal");
        assertThat(result.replicaState().get("127.0.0.3:7000")).isEqualTo("Normal");
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
//        Arrays.asList("3074457345618258602", "6148914691236517204"),
        Collections.singletonList("127.0.0.4:7000"),
        Arrays.asList("-3074457345618258603", "3074457345618258602"),
        Collections.singletonList("127.0.0.4:7000"),
        Arrays.asList("-9223372036854775808", "-3074457345618258603"),
        Collections.singletonList("127.0.0.4:7000")
        );

        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(rangeToEndpointWithPortMap);
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE))
        .thenReturn(pendingRangeToEndpointWithPortMap);
        when(endpointOperations.getDatacenter(anyString())).thenReturn(TEST_DC1);
        when(storageOperations.getLeavingNodesWithPort()).thenReturn(Arrays.asList("127.0.0.2:7000", "127.0.0.4:7000"));
        when(clusterMembershipOperations.getAllEndpointStatesWithPort()).thenReturn(generateSampleGossip("NORMAL",
                                                                                                         "LEAVING",
                                                                                                         "NORMAL",
                                                                                                         "LEAVING",
                                                                                                         "NORMAL",
                                                                                                         "NORMAL"));

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioner.Murmur3);
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
        assertThat(result.replicaState().get("127.0.0.1:7000")).isEqualTo("Normal");
        assertThat(result.replicaState().get("127.0.0.2:7000")).isEqualTo("Leaving");
        assertThat(result.replicaState().get("127.0.0.3:7000")).isEqualTo("Normal");
        assertThat(result.replicaState().get("127.0.0.4:7000")).isEqualTo("Leaving");
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

        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(rangeToEndpointWithPortMap);
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE))
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

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioner.Murmur3);
        assertThat(result).isNotNull();
        assertThat(result.readReplicas()).hasSize(4);
        assertThat(result.writeReplicas()).hasSize(4);
        assertThat(validateRangeExists(result.readReplicas(), "6148914691236517204",
                                       Long.toString(Long.MAX_VALUE))).isTrue();
        assertThat(validateRangeExists(result.writeReplicas(), "6148914691236517204",
                                       Long.toString(Long.MAX_VALUE))).isTrue();
        assertThat(result.replicaState().get("127.0.0.1:7000")).isEqualTo("Leaving");
        assertThat(result.replicaState().get("127.0.0.2:7000")).isEqualTo("Normal");
        assertThat(result.replicaState().get("127.0.0.3:7000")).isEqualTo("Normal");
        assertThat(result.replicaState().get("127.0.0.4:7000")).isEqualTo("Replacing");
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
        return String.format("/127.0.0.1:7000%n" +
                             "  STATUS:16:%s,9223372036854775805%n" +
                             "  HOST_ID:21:00000000-0000-4000-8000-000000000003%n" +
                             "  STATUS_WITH_PORT:17:%s,9223372036854775805%n" +
                             "/127.0.0.2:7000%n" +
                             "  STATUS:9:%s,3074457345618258601%n" +
                             "  HOST_ID:14:00000000-0000-4000-8000-000000000002%n" +
                             "  STATUS_WITH_PORT:10:%s,3074457345618258601%n" +
                             "/127.0.0.3:7000%n" +
                             "  STATUS:2:%s,-3074457345618258603%n" +
                             "  HOST_ID:7:00000000-0000-4000-8000-000000000001%n" +
                             "  STATUS_WITH_PORT:3:%s,-3074457345618258603%n" +
                             "/127.0.0.4:7000%n" +
                             "  STATUS:2:%s,-3074457345618258603%n" +
                             "  HOST_ID:7:00000000-0000-4000-8000-000000000004%n" +
                             "  STATUS_WITH_PORT:3:%s,-3074457345618258603%n" +
                             "/128.0.0.1:7000%n" +
                             "  STATUS:2:%s,-3074457345618258603%n" +
                             "  HOST_ID:7:00000000-0000-4000-8000-000000000001%n" +
                             "  STATUS_WITH_PORT:3:%s,-3074457345618258603%n" +
                             "/128.0.0.2:7000%n" +
                             "  STATUS:2:%s,-3074457345618258603%n" +
                             "  HOST_ID:7:00000000-0000-4000-8000-000000000002%n" +
                             "  STATUS_WITH_PORT:3:%s,-3074457345618258603%n",
                             dc1Node1Status, dc1Node1Status, dc1Node2Status, dc1Node2Status,
                             dc1Node3Status, dc1Node3Status, dc1Node4Status, dc1Node4Status,
                             dc2Node1Status, dc2Node1Status, dc2Node2Status, dc1Node2Status);
    }
}
