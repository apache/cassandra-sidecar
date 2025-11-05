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

/**
 * Tests for {@link NodeMoveRequestPayload}
 */
public class NodeMoveRequestPayloadTest
{
    private final ObjectMapper objectMapper = new ObjectMapper();

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
