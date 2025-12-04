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

package org.apache.cassandra.sidecar.common.request.data;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.sidecar.common.data.CompactionType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link CompactionStopRequestPayload} serialization and deserialization
 */
class CompactionStopRequestPayloadTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    @Test
    void testSerDeserWithBothFields() throws JsonProcessingException
    {
        CompactionStopRequestPayload payload = new CompactionStopRequestPayload(CompactionType.COMPACTION, "abc-123");
        String json = MAPPER.writeValueAsString(payload);
        assertThat(json).isEqualTo("{\"compaction_type\":\"COMPACTION\",\"compaction_id\":\"abc-123\"}");

        CompactionStopRequestPayload deser = MAPPER.readValue(json, CompactionStopRequestPayload.class);
        assertThat(deser.compactionType()).isEqualTo(payload.compactionType());
        assertThat(deser.compactionId()).isEqualTo(payload.compactionId());
    }

    @Test
    void testSerDeserWithTypeOnly() throws JsonProcessingException
    {
        CompactionStopRequestPayload payload = new CompactionStopRequestPayload(CompactionType.VALIDATION, null);
        String json = MAPPER.writeValueAsString(payload);
        assertThat(json).isEqualTo("{\"compaction_type\":\"VALIDATION\"}");

        CompactionStopRequestPayload deser = MAPPER.readValue(json, CompactionStopRequestPayload.class);
        assertThat(deser.compactionType()).isEqualTo(CompactionType.VALIDATION);
        assertThat(deser.compactionId()).isNull();
    }

    @Test
    void testSerDeserWithIdOnly() throws JsonProcessingException
    {
        CompactionStopRequestPayload payload = new CompactionStopRequestPayload(null, "xyz-456");
        String json = MAPPER.writeValueAsString(payload);
        assertThat(json).isEqualTo("{\"compaction_id\":\"xyz-456\"}");

        CompactionStopRequestPayload deser = MAPPER.readValue(json, CompactionStopRequestPayload.class);
        assertThat(deser.compactionType()).isNull();
        assertThat(deser.compactionId()).isEqualTo("xyz-456");
    }

    @Test
    void testSerDeserWithBothNull() throws JsonProcessingException
    {
        CompactionStopRequestPayload payload = new CompactionStopRequestPayload(null, null);
        String json = MAPPER.writeValueAsString(payload);
        assertThat(json).isEqualTo("{}");

        CompactionStopRequestPayload deser = MAPPER.readValue(json, CompactionStopRequestPayload.class);
        assertThat(deser.compactionType()).isNull();
        assertThat(deser.compactionId()).isNull();
    }

    @Test
    void testDeserFromJsonWithBothFields() throws JsonProcessingException
    {
        String json = "{\"compaction_type\":\"CLEANUP\",\"compaction_id\":\"test-123\"}";
        CompactionStopRequestPayload payload = MAPPER.readValue(json, CompactionStopRequestPayload.class);
        assertThat(payload.compactionType()).isEqualTo(CompactionType.CLEANUP);
        assertThat(payload.compactionId()).isEqualTo("test-123");
    }

    @Test
    void testDeserFromJsonWithTypeOnly() throws JsonProcessingException
    {
        String json = "{\"compaction_type\":\"SCRUB\"}";
        CompactionStopRequestPayload payload = MAPPER.readValue(json, CompactionStopRequestPayload.class);
        assertThat(payload.compactionType()).isEqualTo(CompactionType.SCRUB);
        assertThat(payload.compactionId()).isNull();
    }

    @Test
    void testDeserFromJsonWithIdOnly() throws JsonProcessingException
    {
        String json = "{\"compaction_id\":\"unique-compaction-id\"}";
        CompactionStopRequestPayload payload = MAPPER.readValue(json, CompactionStopRequestPayload.class);
        assertThat(payload.compactionType()).isNull();
        assertThat(payload.compactionId()).isEqualTo("unique-compaction-id");
    }

    @Test
    void testDeserFromEmptyJson() throws JsonProcessingException
    {
        String json = "{}";
        CompactionStopRequestPayload payload = MAPPER.readValue(json, CompactionStopRequestPayload.class);
        assertThat(payload.compactionType()).isNull();
        assertThat(payload.compactionId()).isNull();
    }

    @Test
    void testDeserializeWithEmptyStrings() throws JsonProcessingException
    {
        String json = "{\"compaction_type\":\"\",\"compaction_id\":\"\"}";
        CompactionStopRequestPayload payload = MAPPER.readValue(json, CompactionStopRequestPayload.class);
        assertThat(payload.compactionType()).isNull();
        assertThat(payload.compactionId()).isEmpty();
    }

    @Test
    void testDeserializeWithWhitespace() throws JsonProcessingException
    {
        String json = "{\"compaction_type\":\"  COMPACTION  \",\"compaction_id\":\"  test-id  \"}";
        CompactionStopRequestPayload payload = MAPPER.readValue(json, CompactionStopRequestPayload.class);
        assertThat(payload.compactionType()).isEqualTo(CompactionType.COMPACTION);
        assertThat(payload.compactionId()).isEqualTo("  test-id  ");
    }

    @Test
    void testToString()
    {
        CompactionStopRequestPayload payload = new CompactionStopRequestPayload(CompactionType.COMPACTION, "abc-123");
        String toString = payload.toString();
        assertThat(toString).contains("compaction");
        assertThat(toString).contains("abc-123");
        assertThat(toString).contains("CompactionStopRequestPayload");
    }

    @Test
    void testAllSupportedCompactionTypes() throws JsonProcessingException
    {

        // Check each compactionType field for CompactionStopRequestPayload is recognized as a supported compaction type
        for (CompactionType type : CompactionType.values()) {
            CompactionStopRequestPayload payload = new CompactionStopRequestPayload(type, null);
            String json = MAPPER.writeValueAsString(payload);
            assertThat(json).contains(type.toString().toUpperCase());

            CompactionStopRequestPayload deser = MAPPER.readValue(json, CompactionStopRequestPayload.class);
            assertThat(deser.compactionType()).isEqualTo(type);
        }
    }

    @Test
    void testCasePreservation() throws JsonProcessingException
    {
        // Test preserved in serialization/deserialization
        CompactionStopRequestPayload lowerCase = new CompactionStopRequestPayload(CompactionType.COMPACTION, "Test-ID-123");
        String json = MAPPER.writeValueAsString(lowerCase);
        assertThat(json).contains("compaction");
        assertThat(json).contains("Test-ID-123");

        CompactionStopRequestPayload deser = MAPPER.readValue(json, CompactionStopRequestPayload.class);
        assertThat(deser.compactionType()).isEqualTo(CompactionType.COMPACTION);
        assertThat(deser.compactionId()).isEqualTo("Test-ID-123");
    }
}