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
import org.apache.cassandra.sidecar.common.response.LiveMigrationStatus.MigrationState;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link LiveMigrationStatus} JSON serialization and deserialization.
 */
class LiveMigrationStatusTest
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testSerializationDeserializationRoundTrip() throws Exception
    {
        LiveMigrationStatus original = new LiveMigrationStatus(MigrationState.COMPLETED, 1234567890L);

        String json = objectMapper.writeValueAsString(original);
        LiveMigrationStatus deserialized = objectMapper.readValue(json, LiveMigrationStatus.class);

        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.state()).isEqualTo(original.state());
        assertThat(deserialized.endTime()).isEqualTo(original.endTime());
    }
}
