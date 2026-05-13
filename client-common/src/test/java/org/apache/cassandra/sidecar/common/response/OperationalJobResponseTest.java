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

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link OperationalJobResponse} JSON serialization and deserialization.
 */
class OperationalJobResponseTest
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testSerializeStartTimeAsIso8601() throws Exception
    {
        Instant startTime = Instant.parse("2026-05-12T14:30:00Z");
        OperationalJobResponse response = OperationalJobResponse.builder()
                                                                .jobId(UUID.randomUUID())
                                                                .status(OperationalJobStatus.RUNNING)
                                                                .operation("drain")
                                                                .startTime(startTime)
                                                                .build();

        String json = objectMapper.writeValueAsString(response);

        assertThat(json).contains("\"startTime\":\"2026-05-12T14:30:00Z\"");
    }

    @Test
    void testSerializeLastUpdateAsIso8601() throws Exception
    {
        Instant lastUpdate = Instant.parse("2026-05-12T14:35:00Z");
        OperationalJobResponse response = OperationalJobResponse.builder()
                                                                .jobId(UUID.randomUUID())
                                                                .status(OperationalJobStatus.RUNNING)
                                                                .operation("drain")
                                                                .lastUpdate(lastUpdate)
                                                                .build();

        String json = objectMapper.writeValueAsString(response);

        assertThat(json).contains("\"lastUpdate\":\"2026-05-12T14:35:00Z\"");
    }

    @Test
    void testDeserializeIso8601ToInstant() throws Exception
    {
        String json = "{\"jobId\":\"6ba7b810-9dad-11d1-80b4-00c04fd430c8\","
                      + "\"jobStatus\":\"RUNNING\","
                      + "\"operation\":\"drain\","
                      + "\"startTime\":\"2026-05-12T14:30:00Z\","
                      + "\"lastUpdate\":\"2026-05-12T14:35:00Z\"}";

        OperationalJobResponse response = objectMapper.readValue(json, OperationalJobResponse.class);

        assertThat(response.startTime()).isEqualTo(Instant.parse("2026-05-12T14:30:00Z"));
        assertThat(response.lastUpdate()).isEqualTo(Instant.parse("2026-05-12T14:35:00Z"));
    }

    @Test
    void testRoundTripSerialization() throws Exception
    {
        Instant startTime = Instant.parse("2026-05-12T14:30:00Z");
        Instant lastUpdate = Instant.parse("2026-05-12T14:35:00Z");
        UUID jobId = UUID.randomUUID();
        UUID nodeId = UUID.randomUUID();
        List<UUID> nodesSucceeded = Arrays.asList(nodeId);

        OperationalJobResponse original = OperationalJobResponse.builder()
                                                                 .jobId(jobId)
                                                                 .status(OperationalJobStatus.SUCCEEDED)
                                                                 .operation("decommission")
                                                                 .startTime(startTime)
                                                                 .lastUpdate(lastUpdate)
                                                                 .nodesSucceeded(nodesSucceeded)
                                                                 .build();

        String json = objectMapper.writeValueAsString(original);
        OperationalJobResponse deserialized = objectMapper.readValue(json, OperationalJobResponse.class);

        assertThat(deserialized.jobId()).isEqualTo(jobId);
        assertThat(deserialized.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertThat(deserialized.operation()).isEqualTo("decommission");
        assertThat(deserialized.startTime()).isEqualTo(startTime);
        assertThat(deserialized.lastUpdate()).isEqualTo(lastUpdate);
        assertThat(deserialized.nodesSucceeded()).isEqualTo(nodesSucceeded);
    }

    @Test
    void testNullTimesOmittedFromJson() throws Exception
    {
        OperationalJobResponse response = OperationalJobResponse.builder()
                                                                .jobId(UUID.randomUUID())
                                                                .status(OperationalJobStatus.CREATED)
                                                                .operation("move")
                                                                .build();

        String json = objectMapper.writeValueAsString(response);

        assertThat(json).doesNotContain("startTime");
        assertThat(json).doesNotContain("lastUpdate");
    }

    @Test
    void testDeserializeUnknownFieldsIgnored() throws Exception
    {
        String json = "{\"jobId\":\"6ba7b810-9dad-11d1-80b4-00c04fd430c8\","
                      + "\"jobStatus\":\"SUCCEEDED\","
                      + "\"operation\":\"drain\","
                      + "\"unknownField\":\"someValue\","
                      + "\"startTime\":\"2026-05-12T14:30:00Z\"}";

        OperationalJobResponse response = objectMapper.readValue(json, OperationalJobResponse.class);

        assertThat(response.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertThat(response.startTime()).isEqualTo(Instant.parse("2026-05-12T14:30:00Z"));
    }
}
