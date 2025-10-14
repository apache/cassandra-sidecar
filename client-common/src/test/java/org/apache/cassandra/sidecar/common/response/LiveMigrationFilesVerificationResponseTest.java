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

class LiveMigrationFilesVerificationResponseTest
{
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void testSerializationRoundTrip() throws Exception
    {
        LiveMigrationFilesVerificationResponse original = new LiveMigrationFilesVerificationResponse(
        "test-id-123",
        "MD5",
        null,
        "COMPLETED",
        "192.168.1.100",
        9042,
        5,
        10,
        100,
        15,
        20,
        3,
        85
        );

        // Serialize to JSON
        String json = mapper.writeValueAsString(original);

        // Deserialize back to object
        LiveMigrationFilesVerificationResponse deserialized =
        mapper.readValue(json, LiveMigrationFilesVerificationResponse.class);

        // Verify all fields match
        assertThat(deserialized.id()).isEqualTo(original.id());
        assertThat(deserialized.digestAlgorithm()).isEqualTo(original.digestAlgorithm());
        assertThat(deserialized.seed()).isEqualTo(original.seed());
        assertThat(deserialized.state()).isEqualTo(original.state());
        assertThat(deserialized.source()).isEqualTo(original.source());
        assertThat(deserialized.port()).isEqualTo(original.port());
        assertThat(deserialized.filesNotFoundAtSource()).isEqualTo(original.filesNotFoundAtSource());
        assertThat(deserialized.filesNotFoundAtDestination()).isEqualTo(original.filesNotFoundAtDestination());
        assertThat(deserialized.metadataMatched()).isEqualTo(original.metadataMatched());
        assertThat(deserialized.metadataMismatches()).isEqualTo(original.metadataMismatches());
        assertThat(deserialized.digestMismatches()).isEqualTo(original.digestMismatches());
        assertThat(deserialized.digestVerificationFailures()).isEqualTo(original.digestVerificationFailures());
        assertThat(deserialized.filesMatched()).isEqualTo(original.filesMatched());
        assertThat(deserialized.isVerificationSuccessful()).isEqualTo(original.isVerificationSuccessful());
    }

    @Test
    void testSerializationRoundTripInProgressState() throws Exception
    {
        LiveMigrationFilesVerificationResponse original = new LiveMigrationFilesVerificationResponse(
        "test-id-456",
        "XXHash32",
        123456,
        "IN_PROGRESS",
        "192.168.1.200",
        7000,
        0,
        0,
        50,
        0,
        0,
        0,
        50
        );

        // Serialize to JSON
        String json = mapper.writeValueAsString(original);

        // Deserialize back to object
        LiveMigrationFilesVerificationResponse deserialized =
        mapper.readValue(json, LiveMigrationFilesVerificationResponse.class);

        // Verify all fields match
        assertThat(deserialized.id()).isEqualTo(original.id());
        assertThat(deserialized.digestAlgorithm()).isEqualTo(original.digestAlgorithm());
        assertThat(deserialized.seed()).isEqualTo(original.seed());
        assertThat(deserialized.state()).isEqualTo(original.state());
        assertThat(deserialized.source()).isEqualTo(original.source());
        assertThat(deserialized.port()).isEqualTo(original.port());
        assertThat(deserialized.filesNotFoundAtSource()).isEqualTo(original.filesNotFoundAtSource());
        assertThat(deserialized.filesNotFoundAtDestination()).isEqualTo(original.filesNotFoundAtDestination());
        assertThat(deserialized.metadataMatched()).isEqualTo(original.metadataMatched());
        assertThat(deserialized.metadataMismatches()).isEqualTo(original.metadataMismatches());
        assertThat(deserialized.digestMismatches()).isEqualTo(original.digestMismatches());
        assertThat(deserialized.digestVerificationFailures()).isEqualTo(original.digestVerificationFailures());
        assertThat(deserialized.filesMatched()).isEqualTo(original.filesMatched());
        assertThat(deserialized.isVerificationSuccessful()).isEqualTo(original.isVerificationSuccessful());
    }

    @Test
    void testIsVerificationSuccessfulAllConditionsMet()
    {
        LiveMigrationFilesVerificationResponse response = new LiveMigrationFilesVerificationResponse(
        "test-id",
        "MD5",
        null,
        "COMPLETED",
        "192.168.1.1",
        9042,
        0,  // filesNotFoundAtSource
        0,  // filesNotFoundAtDestination
        100, // metadataMatched
        0,  // metadataMismatches
        0,  // digestMismatches
        0,  // digestVerificationFailures
        100 // filesMatched
        );

        assertThat(response.isVerificationSuccessful()).isTrue();
    }

    @Test
    void testIsVerificationSuccessfulStateNotCompleted()
    {
        LiveMigrationFilesVerificationResponse response = new LiveMigrationFilesVerificationResponse(
        "test-id",
        "MD5",
        null,
        "IN_PROGRESS",
        "192.168.1.1",
        9042,
        0,
        0,
        100,
        0,
        0,
        0,
        100
        );

        assertThat(response.isVerificationSuccessful()).isFalse();
    }

    @Test
    void testIsVerificationSuccessfulFilesNotFoundAtSource()
    {
        LiveMigrationFilesVerificationResponse response = new LiveMigrationFilesVerificationResponse(
        "test-id",
        "MD5",
        null,
        "COMPLETED",
        "192.168.1.1",
        9042,
        5,  // filesNotFoundAtSource > 0
        0,
        100,
        0,
        0,
        0,
        100
        );

        assertThat(response.isVerificationSuccessful()).isFalse();
    }

    @Test
    void testIsVerificationSuccessfulFilesNotFoundAtDestination()
    {
        LiveMigrationFilesVerificationResponse response = new LiveMigrationFilesVerificationResponse(
        "test-id",
        "MD5",
        null,
        "COMPLETED",
        "192.168.1.1",
        9042,
        0,
        3,  // filesNotFoundAtDestination > 0
        100,
        0,
        0,
        0,
        100
        );

        assertThat(response.isVerificationSuccessful()).isFalse();
    }

    @Test
    void testIsVerificationSuccessfulMetadataMismatches()
    {
        LiveMigrationFilesVerificationResponse response = new LiveMigrationFilesVerificationResponse(
        "test-id",
        "MD5",
        null,
        "COMPLETED",
        "192.168.1.1",
        9042,
        0,
        0,
        95,
        5,  // metadataMismatches > 0
        0,
        0,
        100
        );

        assertThat(response.isVerificationSuccessful()).isFalse();
    }

    @Test
    void testIsVerificationSuccessfulDigestMismatches()
    {
        LiveMigrationFilesVerificationResponse response = new LiveMigrationFilesVerificationResponse(
        "test-id",
        "MD5",
        null,
        "COMPLETED",
        "192.168.1.1",
        9042,
        0,
        0,
        100,
        0,
        10, // digestMismatches > 0
        0,
        90
        );

        assertThat(response.isVerificationSuccessful()).isFalse();
    }

    @Test
    void testIsVerificationSuccessfulDigestVerificationFailures()
    {
        LiveMigrationFilesVerificationResponse response = new LiveMigrationFilesVerificationResponse(
        "test-id",
        "MD5",
        null,
        "COMPLETED",
        "192.168.1.1",
        9042,
        0,
        0,
        100,
        0,
        0,
        2,  // digestVerificationFailures > 0
        98
        );

        assertThat(response.isVerificationSuccessful()).isFalse();
    }

    @Test
    void testIsVerificationSuccessfulMultipleFailureConditions()
    {
        LiveMigrationFilesVerificationResponse response = new LiveMigrationFilesVerificationResponse(
        "test-id",
        "MD5",
        null,
        "COMPLETED",
        "192.168.1.1",
        9042,
        2,  // filesNotFoundAtSource > 0
        3,  // filesNotFoundAtDestination > 0
        90,
        5,  // metadataMismatches > 0
        8,  // digestMismatches > 0
        1,  // digestVerificationFailures > 0
        85
        );

        assertThat(response.isVerificationSuccessful()).isFalse();
    }

    @Test
    void testConstructorThrowsNullPointerExceptionForNullId()
    {
        assertThatThrownBy(() -> new LiveMigrationFilesVerificationResponse(
        null,  // null id
        "MD5",
        null,
        "COMPLETED",
        "192.168.1.1",
        9042,
        0,
        0,
        100,
        0,
        0,
        0,
        100
        )).isInstanceOf(NullPointerException.class)
          .hasMessageContaining("id of files verification task must be specified");
    }

    @Test
    void testIsVerificationSuccessfulFailedState()
    {
        LiveMigrationFilesVerificationResponse response = new LiveMigrationFilesVerificationResponse(
        "test-id",
        "MD5",
        null,
        "FAILED",  // FAILED state
        "192.168.1.1",
        9042,
        0,  // all counters are perfect
        0,
        100,
        0,
        0,
        0,
        100
        );

        assertThat(response.isVerificationSuccessful()).isFalse();
    }
}
