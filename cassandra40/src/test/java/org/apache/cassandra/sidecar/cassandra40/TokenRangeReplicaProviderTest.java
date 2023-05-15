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

package org.apache.cassandra.sidecar.cassandra40;

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
    private static final List<String> TEST_ENDPOINTS1 = Arrays.asList("dc1_host1", "dc1_host2", "dc1_host3");
    private static final List<String> TEST_ENDPOINTS2 = Arrays.asList("dc2_host1", "dc2_host2");
    private static final List<String> TEST_MULTI_DC_ENDPOINTS = Arrays.asList("dc2_host1", "dc2_host2", "dc1_host4");
    public static final String TEST_KEYSPACE = "test_keyspace";

    StorageJmxOperations storageOperations;
    EndpointSnitchJmxOperations endpointOperations;
    JmxClient jmxClient;
    TokenRangeReplicaProvider instance;

    @BeforeEach
    void setup()
    {
        storageOperations = mock(StorageJmxOperations.class);
        endpointOperations = mock(EndpointSnitchJmxOperations.class);
        jmxClient = mock(JmxClient.class);
        instance = new TokenRangeReplicaProvider(jmxClient);

        when(jmxClient.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService"))
                .thenReturn(storageOperations);
        when(jmxClient.proxy(EndpointSnitchJmxOperations.class, "org.apache.cassandra.db:type=EndpointSnitchInfo"))
                .thenReturn(endpointOperations);
    }

    @Test
    public void replicasByTokenRangeTest() throws UnknownHostException
    {
        Map<List<String>, List<String>> naturalReplicaMappings = new HashMap<>();
        naturalReplicaMappings.put(TOKEN_RANGE1, TEST_ENDPOINTS1);

        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(naturalReplicaMappings);
        Map<List<String>, List<String>> writeReplicaMappings = new HashMap<>();
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(writeReplicaMappings);
        when(endpointOperations.getDatacenter(anyString())).thenReturn(TEST_DC1);

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioner.Random);
        assertThat(result).isNotNull();
        assertThat(result.writeReplicas().size()).isEqualTo(1);
        // Single token range
        assertThat(result.naturalReplicas().size()).isEqualTo(1);
        // Single DC
        assertThat(result.naturalReplicas().get(0).replicasByDatacenter().size()).isEqualTo(1);
        assertThat(result.naturalReplicas().get(0).replicasByDatacenter().get(TEST_DC1)).containsAll(TEST_ENDPOINTS1);
    }

    @Test
    public void replicasByTokenRangeTestMultipleDCs() throws UnknownHostException
    {
        Map<List<String>, List<String>> naturalReplicaMappings = new HashMap<>();
        naturalReplicaMappings.put(TOKEN_RANGE1, TEST_ENDPOINTS1);
        naturalReplicaMappings.put(TOKEN_RANGE2, TEST_ENDPOINTS2);

        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(naturalReplicaMappings);
        Map<List<String>, List<String>> writeReplicaMappings = new HashMap<>();
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(writeReplicaMappings);

        when(endpointOperations.getDatacenter(startsWith("dc1_"))).thenReturn(TEST_DC1);
        when(endpointOperations.getDatacenter(startsWith("dc2_"))).thenReturn(TEST_DC2);

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas("test_keyspace", Partitioner.Random);
        assertThat(result).isNotNull();
        assertThat(result.writeReplicas().size()).isEqualTo(2);
        // Single token range
        assertThat(result.naturalReplicas().size()).isEqualTo(2);
        // Single DC per token range
        assertThat(result.naturalReplicas().get(0).replicasByDatacenter().size()).isEqualTo(1);
        assertThat(result.naturalReplicas().get(1).replicasByDatacenter().size()).isEqualTo(1);
        assertThat(result.naturalReplicas().get(0).replicasByDatacenter().get(TEST_DC1)).containsAll(TEST_ENDPOINTS1);
        assertThat(result.naturalReplicas().get(1).replicasByDatacenter().get(TEST_DC2)).containsAll(TEST_ENDPOINTS2);
    }

    @Test
    public void replicasByTokenRangeTestMultipleDCsPerTokenRange() throws UnknownHostException
    {
        Map<List<String>, List<String>> naturalReplicaMappings = new HashMap<>();
        naturalReplicaMappings.put(TOKEN_RANGE1, TEST_ENDPOINTS1);
        naturalReplicaMappings.put(TOKEN_RANGE2, TEST_MULTI_DC_ENDPOINTS);

        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(naturalReplicaMappings);
        Map<List<String>, List<String>> writeReplicaMappings = new HashMap<>();
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(writeReplicaMappings);

        when(endpointOperations.getDatacenter(startsWith("dc1_"))).thenReturn(TEST_DC1);
        when(endpointOperations.getDatacenter(startsWith("dc2_"))).thenReturn(TEST_DC2);

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas("test_keyspace", Partitioner.Random);
        assertThat(result).isNotNull();
        assertThat(result.writeReplicas().size()).isEqualTo(2);
        // 2 token ranges
        assertThat(result.naturalReplicas().size()).isEqualTo(2);

        // Validate token range has entries from both DCs
        TokenRangeReplicasResponse.ReplicaInfo replicaInfoWithMultipleDCs =
                result.naturalReplicas().stream().filter(r -> TOKEN_RANGE2.get(0).equals(r.start())).findAny().get();
        assertThat(replicaInfoWithMultipleDCs.replicasByDatacenter().size()).isEqualTo(2);
        assertThat(result.naturalReplicas().get(0).replicasByDatacenter().get(TEST_DC1)).containsAll(TEST_ENDPOINTS1);
        assertThat(result.naturalReplicas().get(1).replicasByDatacenter().get(TEST_DC2)).containsAll(TEST_ENDPOINTS2);
    }

    @Test
    public void naturalAndWriteReplicasByTokenRangeTest() throws UnknownHostException
    {
        Map<List<String>, List<String>> naturalReplicaMappings = new HashMap<>();
        naturalReplicaMappings.put(TOKEN_RANGE1, TEST_ENDPOINTS1);

        Map<List<String>, List<String>> writeReplicaMappings = new HashMap<>();
        writeReplicaMappings.put(TOKEN_RANGE2, TEST_ENDPOINTS2);

        when(storageOperations.getRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(naturalReplicaMappings);
        when(storageOperations.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE)).thenReturn(writeReplicaMappings);

        when(endpointOperations.getDatacenter(startsWith("dc1_"))).thenReturn(TEST_DC1);
        when(endpointOperations.getDatacenter(startsWith("dc2_"))).thenReturn(TEST_DC2);

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas("test_keyspace", Partitioner.Random);
        assertThat(result).isNotNull();
        assertThat(result.writeReplicas().size()).isEqualTo(2);
        assertThat(result.naturalReplicas().size()).isEqualTo(1);
        assertThat(result.naturalReplicas().get(0).replicasByDatacenter().size()).isEqualTo(1);
        assertThat(result.naturalReplicas().get(0).replicasByDatacenter().get(TEST_DC1)).containsAll(TEST_ENDPOINTS1);
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

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioner.Murmur3);
        assertThat(result).isNotNull();
        assertThat(result.naturalReplicas().size()).isEqualTo(3);
        assertThat(validateRangeExists(result.naturalReplicas(), "3074457345618258602",
                                       Long.toString(Long.MAX_VALUE))).isTrue();
        assertThat(validateRangeExists(result.writeReplicas(), "3074457345618258602",
                Long.toString(Long.MAX_VALUE))).isTrue();
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
                Arrays.asList("3074457345618258602", "6148914691236517204"),
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

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioner.Murmur3);
        assertThat(result).isNotNull();
        assertThat(result.naturalReplicas().size()).isEqualTo(3);
        assertThat(result.writeReplicas().size()).isEqualTo(4);
        // Write replicas includes the new range ending at "maxToken"
        assertThat(validateRangeExists(result.writeReplicas(), "6148914691236517204",
                Long.toString(Long.MAX_VALUE))).isTrue();
        // Natural replicas does NOT include the new range from pending ranges
        assertThat(validateRangeExists(result.naturalReplicas(), "6148914691236517204",
                                       Long.toString(Long.MAX_VALUE))).isFalse();
        // Existing natural replicas wrap-around range ends at "maxToken"
        assertThat(validateRangeExists(result.naturalReplicas(), "3074457345618258602",
                                       Long.toString(Long.MAX_VALUE))).isTrue();
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

        TokenRangeReplicasResponse result = instance.tokenRangeReplicas(TEST_KEYSPACE, Partitioner.Murmur3);
        assertThat(result).isNotNull();
        assertThat(result.naturalReplicas().size()).isEqualTo(4);
        assertThat(result.writeReplicas().size()).isEqualTo(4);
        assertThat(validateRangeExists(result.naturalReplicas(), "6148914691236517204",
                                       Long.toString(Long.MAX_VALUE))).isTrue();
        assertThat(validateRangeExists(result.writeReplicas(), "6148914691236517204",
                Long.toString(Long.MAX_VALUE))).isTrue();
    }

    private boolean validateRangeExists(List<TokenRangeReplicasResponse.ReplicaInfo> ranges, String start, String end)
    {
        return ranges.stream().anyMatch(r -> (r.start().equals(start) && r.end().equals(end)));
    }
}
