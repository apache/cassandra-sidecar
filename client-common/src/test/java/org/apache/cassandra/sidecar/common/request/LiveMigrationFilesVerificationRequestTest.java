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

package org.apache.cassandra.sidecar.common.request;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LiveMigrationFilesVerificationRequestTest
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testValidConstruction()
    {
        LiveMigrationFilesVerificationRequest request =
        new LiveMigrationFilesVerificationRequest(10, "XXHash32", 42);

        assertThat(request.maxConcurrency()).isEqualTo(10);
        assertThat(request.digestAlgorithm()).isEqualTo("XXHash32");
        assertThat(request.seed()).isEqualTo(42);
    }

    @Test
    void testValidConstructionWithNullSeed()
    {
        LiveMigrationFilesVerificationRequest request =
        new LiveMigrationFilesVerificationRequest(5, "MD5", null);

        assertThat(request.maxConcurrency()).isEqualTo(5);
        assertThat(request.digestAlgorithm()).isEqualTo("MD5");
        assertThat(request.seed()).isNull();
    }

    @Test
    void testSerializationDeserializationRoundTrip() throws Exception
    {
        LiveMigrationFilesVerificationRequest original =
        new LiveMigrationFilesVerificationRequest(8, "XXHash32", 12345);

        String json = objectMapper.writeValueAsString(original);
        LiveMigrationFilesVerificationRequest deserialized =
        objectMapper.readValue(json, LiveMigrationFilesVerificationRequest.class);

        assertThat(deserialized.maxConcurrency()).isEqualTo(original.maxConcurrency());
        assertThat(deserialized.digestAlgorithm()).isEqualTo(original.digestAlgorithm());
        assertThat(deserialized.seed()).isEqualTo(original.seed());
    }

    @Test
    void testValidationFailures()
    {
        // Invalid maxConcurrency values
        assertThatThrownBy(() -> new LiveMigrationFilesVerificationRequest(0, "MD5", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxConcurrency must be >= 1");

        assertThatThrownBy(() -> new LiveMigrationFilesVerificationRequest(-5, "XXHash32", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxConcurrency must be >= 1");

        // Invalid digestAlgorithm values
        assertThatThrownBy(() -> new LiveMigrationFilesVerificationRequest(10, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("digestAlgorithm cannot be null or empty");

        assertThatThrownBy(() -> new LiveMigrationFilesVerificationRequest(10, "", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("digestAlgorithm cannot be null or empty");

        assertThatThrownBy(() -> new LiveMigrationFilesVerificationRequest(10, "   ", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("digestAlgorithm cannot be null or empty");
    }

    @Test
    void testBoundaryValues()
    {
        LiveMigrationFilesVerificationRequest minRequest =
        new LiveMigrationFilesVerificationRequest(1, "MD5", Integer.MIN_VALUE);
        assertThat(minRequest.maxConcurrency()).isEqualTo(1);
        assertThat(minRequest.seed()).isEqualTo(Integer.MIN_VALUE);

        LiveMigrationFilesVerificationRequest maxRequest =
        new LiveMigrationFilesVerificationRequest(Integer.MAX_VALUE, "XXHash32", Integer.MAX_VALUE);
        assertThat(maxRequest.maxConcurrency()).isEqualTo(Integer.MAX_VALUE);
        assertThat(maxRequest.seed()).isEqualTo(Integer.MAX_VALUE);
    }
}
