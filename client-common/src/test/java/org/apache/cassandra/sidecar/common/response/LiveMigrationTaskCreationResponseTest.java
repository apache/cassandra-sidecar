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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LiveMigrationTaskCreationResponseTest
{
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void testSerializationRoundTrip() throws Exception
    {
        LiveMigrationTaskCreationResponse original = new LiveMigrationTaskCreationResponse(
        "task-123-abc",
        "/api/v1/live-migration/status/task-123-abc");

        // Serialize to JSON
        String json = mapper.writeValueAsString(original);

        // Deserialize back to object
        LiveMigrationTaskCreationResponse deserialized =
        mapper.readValue(json, LiveMigrationTaskCreationResponse.class);

        assertThat(deserialized.taskId()).isEqualTo(original.taskId());
        assertThat(deserialized.statusUrl()).isEqualTo(original.statusUrl());
    }

    @Test
    void testConstructorInvalidValues()
    {
        assertThatThrownBy(() -> new LiveMigrationTaskCreationResponse("task-123", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("statusUrl cannot be null");

        assertThatThrownBy(() -> new LiveMigrationTaskCreationResponse(
        null,
        "/api/v1/live-migration/status/task-123"
        ))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("taskId cannot be null");
    }
}
