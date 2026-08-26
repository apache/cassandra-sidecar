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

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse.ReplicaMetadata;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TokenRangeReplicasResponse.ReplicaMetadata} JSON serialization and deserialization.
 */
class TokenRangeReplicasResponseTest
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testReplicaMetadataRoundTripWithSidecarInstanceId() throws Exception
    {
        ReplicaMetadata original = new ReplicaMetadata("Normal", "Up", "host1.local",
                                                       "127.0.0.1", 7000, "dc1", 5);

        String json = objectMapper.writeValueAsString(original);
        assertThat(json).contains("\"sidecarInstanceId\":5");

        ReplicaMetadata deserialized = objectMapper.readValue(json, ReplicaMetadata.class);
        assertThat(deserialized.sidecarInstanceId()).isEqualTo(5);
        assertThat(deserialized.address()).isEqualTo("127.0.0.1");
        assertThat(deserialized.datacenter()).isEqualTo("dc1");
    }

    @Test
    void testReplicaMetadataDeserializeLegacyJsonWithoutSidecarInstanceId() throws Exception
    {
        // A response produced before this field existed must still deserialize, leaving the id null
        String legacyJson = "{\"state\":\"Normal\",\"status\":\"Up\",\"fqdn\":\"host1.local\","
                            + "\"address\":\"127.0.0.1\",\"port\":7000,\"datacenter\":\"dc1\"}";

        ReplicaMetadata deserialized = objectMapper.readValue(legacyJson, ReplicaMetadata.class);
        assertThat(deserialized.sidecarInstanceId()).isNull();
        assertThat(deserialized.address()).isEqualTo("127.0.0.1");
    }
}
