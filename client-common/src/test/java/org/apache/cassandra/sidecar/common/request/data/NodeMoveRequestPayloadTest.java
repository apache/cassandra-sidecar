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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link NodeMoveRequestPayload}
 */
public class NodeMoveRequestPayloadTest
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testValidTokens()
    {
        NodeMoveRequestPayload payload1 = new NodeMoveRequestPayload("123456789");
        assertThat(payload1.newToken()).isEqualTo("123456789");

        NodeMoveRequestPayload payload2 = new NodeMoveRequestPayload("-9223372036854775808");
        assertThat(payload2.newToken()).isEqualTo("-9223372036854775808");

        NodeMoveRequestPayload payload3 = new NodeMoveRequestPayload("0");
        assertThat(payload3.newToken()).isEqualTo("0");
    }

    @Test
    void testInvalidTokens()
    {
        assertThatThrownBy(() -> new NodeMoveRequestPayload("invalid"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("newToken parameter must be a valid integer");

        assertThatThrownBy(() -> new NodeMoveRequestPayload(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("newToken must be provided and non-empty");

        assertThatThrownBy(() -> new NodeMoveRequestPayload(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("newToken must be provided and non-empty");
    }

    @Test
    void testTokenTrimming()
    {
        NodeMoveRequestPayload payload = new NodeMoveRequestPayload("  123456789  ");
        assertThat(payload.newToken()).isEqualTo("123456789");
    }

    @Test
    void testJsonSerialization() throws JsonProcessingException
    {
        NodeMoveRequestPayload payload = new NodeMoveRequestPayload("123456789");
        String json = objectMapper.writeValueAsString(payload);
        assertThat(json).contains("\"newToken\":\"123456789\"");

        NodeMoveRequestPayload deserialized = objectMapper.readValue(json, NodeMoveRequestPayload.class);
        assertThat(deserialized.newToken()).isEqualTo("123456789");
    }

    @Test
    void testToString()
    {
        NodeMoveRequestPayload payload = new NodeMoveRequestPayload("123456789");
        assertThat(payload.toString()).contains("123456789");
    }
}
