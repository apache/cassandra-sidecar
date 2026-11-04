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

package org.apache.cassandra.sidecar.common.response;

import java.util.Arrays;
import java.util.Collections;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test {@link TokenRangeReplicasResponse}
 */
class TokenRangeReplicasResponseTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testWitnessesAreDeserialized() throws Exception
    {
        String json = "{\"start\":\"0\",\"end\":\"100\","
                      + "\"replicas\":{\"dc1\":[\"1.1.1.1:9042\",\"1.1.1.2:9042\",\"1.1.1.3:9042\"]},"
                      + "\"witnessesByDatacenter\":{\"dc1\":[\"1.1.1.3:9042\"]}}";

        TokenRangeReplicasResponse.ReplicaInfo info = MAPPER.readValue(json, TokenRangeReplicasResponse.ReplicaInfo.class);

        assertThat(info.replicasByDatacenter()).containsEntry("dc1", Arrays.asList("1.1.1.1:9042", "1.1.1.2:9042", "1.1.1.3:9042"));
        assertThat(info.witnessesByDatacenter()).containsEntry("dc1", Collections.singletonList("1.1.1.3:9042"));
    }

    @Test
    void testMissingWitnessesAreNull() throws Exception
    {
        // Unreported witnesses must be distinguishable from a range having none
        String json = "{\"start\":\"0\",\"end\":\"100\",\"replicas\":{\"dc1\":[\"1.1.1.1:9042\"]}}";

        TokenRangeReplicasResponse.ReplicaInfo info = MAPPER.readValue(json, TokenRangeReplicasResponse.ReplicaInfo.class);

        assertThat(info.witnessesByDatacenter()).isNull();
    }

    @Test
    void testEmptyWitnessesAreNotNull() throws Exception
    {
        String json = "{\"start\":\"0\",\"end\":\"100\",\"replicas\":{\"dc1\":[\"1.1.1.1:9042\"]},"
                      + "\"witnessesByDatacenter\":{}}";

        TokenRangeReplicasResponse.ReplicaInfo info = MAPPER.readValue(json, TokenRangeReplicasResponse.ReplicaInfo.class);

        assertThat(info.witnessesByDatacenter()).isEmpty();
    }

    @Test
    void testWitnessesAreSerialized() throws Exception
    {
        TokenRangeReplicasResponse.ReplicaInfo info = new TokenRangeReplicasResponse.ReplicaInfo(
        "0", "100",
        Collections.singletonMap("dc1", Arrays.asList("1.1.1.1:9042", "1.1.1.2:9042")),
        Collections.singletonMap("dc1", Collections.singletonList("1.1.1.2:9042")));

        String json = MAPPER.writeValueAsString(info);

        assertThat(json).contains("\"witnessesByDatacenter\":{\"dc1\":[\"1.1.1.2:9042\"]}");
    }

    @Test
    void testUnreportedWitnessesAreNotSerialized() throws Exception
    {
        // keeps the response unchanged where witnesses are not reported
        TokenRangeReplicasResponse.ReplicaInfo info = new TokenRangeReplicasResponse.ReplicaInfo(
        "0", "100", Collections.singletonMap("dc1", Collections.singletonList("1.1.1.1:9042")));

        String json = MAPPER.writeValueAsString(info);

        assertThat(json).doesNotContain("witnessesByDatacenter");
    }
}
