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

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AbortRestoreJobRequestPayloadTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

    @Test
    void testSerDeser() throws JsonProcessingException
    {
        AbortRestoreJobRequestPayload payload = new AbortRestoreJobRequestPayload("Expired");
        String json = MAPPER.writeValueAsString(payload);
        assertThat(json).isEqualTo("{\"reason\":\"Expired\"}");
        AbortRestoreJobRequestPayload deser = MAPPER.readValue(json, AbortRestoreJobRequestPayload.class);
        assertThat(deser.reason()).isEqualTo(payload.reason());

        AbortRestoreJobRequestPayload nullPayload = new AbortRestoreJobRequestPayload(null);
        json = MAPPER.writeValueAsString(nullPayload);
        assertThat(json).isEqualTo("{}");
        deser = MAPPER.readValue(json, AbortRestoreJobRequestPayload.class);
        assertThat(deser.reason()).isNull();
    }

    @Test
    void testValidation()
    {
        String longString = Stream.generate(() -> "a").limit(2048).collect(Collectors.joining());
        assertThatThrownBy(() -> new AbortRestoreJobRequestPayload(longString))
        .hasMessage("Reason string is too long");

        String disallowedChars = "! cat /super/secrets";
        assertThatThrownBy(() -> new AbortRestoreJobRequestPayload(disallowedChars))
        .hasMessage("Reason string cannot contain non-alphanumeric-blank characters");

        assertThatThrownBy(() -> MAPPER.readValue(String.format("{\"reason\":\"%s\"}", longString),
                                                  AbortRestoreJobRequestPayload.class))
        .hasMessageContaining("Reason string is too long");

        assertThatThrownBy(() -> MAPPER.readValue(String.format("{\"reason\":\"%s\"}", disallowedChars),
                                                  AbortRestoreJobRequestPayload.class))
        .hasMessageContaining("Reason string cannot contain non-alphanumeric-blank characters");
    }
}
