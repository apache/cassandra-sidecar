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

package org.apache.cassandra.sidecar.common.response.data;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RingEntry} JSON serialization and deserialization.
 */
class RingEntryTest
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testRoundTripWithSidecarInstanceId() throws Exception
    {
        RingEntry original = new RingEntry.Builder()
                             .datacenter("dc1")
                             .address("127.0.0.1")
                             .port(7000)
                             .rack("rack1")
                             .status("Up")
                             .state("Normal")
                             .load("1 KiB")
                             .owns("100%")
                             .token("42")
                             .fqdn("host1.local")
                             .hostId("host-id-1")
                             .sidecarInstanceId(7)
                             .build();

        String json = objectMapper.writeValueAsString(original);
        assertThat(json).contains("\"sidecarInstanceId\":7");

        RingEntry deserialized = objectMapper.readValue(json, RingEntry.class);
        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.sidecarInstanceId()).isEqualTo(7);
    }

    @Test
    void testDeserializeLegacyJsonWithoutSidecarInstanceId() throws Exception
    {
        // A response produced before this field existed must still deserialize, leaving the id null
        String legacyJson = "{\"datacenter\":\"dc1\",\"address\":\"127.0.0.1\",\"port\":7000,"
                            + "\"rack\":\"rack1\",\"status\":\"Up\",\"state\":\"Normal\",\"load\":\"1 KiB\","
                            + "\"owns\":\"100%\",\"token\":\"42\",\"fqdn\":\"host1.local\",\"hostId\":\"host-id-1\"}";

        RingEntry deserialized = objectMapper.readValue(legacyJson, RingEntry.class);
        assertThat(deserialized.sidecarInstanceId()).isNull();
        assertThat(deserialized.address()).isEqualTo("127.0.0.1");
    }
}
