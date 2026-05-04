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

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LiveMigrationFilesVerificationResponseTest
{
    private final ObjectMapper mapper = new ObjectMapper();

    static Stream<Arguments> serializationRoundTripParams()
    {
        return Stream.of(
        Arguments.of("test-id-123", "MD5", "COMPLETED", "192.168.1.100", 9042, 5, 10, 100, 15, 20, 3, 85),
        Arguments.of("test-id-456", "XXHash32", "IN_PROGRESS", "192.168.1.200", 7000, 0, 0, 50, 0, 0, 0, 50)
        );
    }

    static Stream<Arguments> verificationSuccessParams()
    {
        return Stream.of(
        // all conditions met
        Arguments.of("COMPLETED", 0, 0, 0, 0, 0, true),
        // state not completed
        Arguments.of("IN_PROGRESS", 0, 0, 0, 0, 0, false),
        // failed state
        Arguments.of("FAILED", 0, 0, 0, 0, 0, false),
        // filesNotFoundAtSource > 0
        Arguments.of("COMPLETED", 5, 0, 0, 0, 0, false),
        // filesNotFoundAtDestination > 0
        Arguments.of("COMPLETED", 0, 3, 0, 0, 0, false),
        // metadataMismatches > 0
        Arguments.of("COMPLETED", 0, 0, 5, 0, 0, false),
        // digestMismatches > 0
        Arguments.of("COMPLETED", 0, 0, 0, 10, 0, false),
        // digestVerificationFailures > 0
        Arguments.of("COMPLETED", 0, 0, 0, 0, 2, false),
        // multiple failure conditions
        Arguments.of("COMPLETED", 2, 3, 5, 8, 1, false)
        );
    }

    @ParameterizedTest
    @MethodSource("serializationRoundTripParams")
    void testSerializationRoundTrip(String id, String digestAlgorithm, String state, String source,
                                    int port, int filesNotFoundAtSource, int filesNotFoundAtDestination,
                                    int metadataMatched, int metadataMismatches, int digestMismatches,
                                    int digestVerificationFailures, int filesMatched) throws Exception
    {
        LiveMigrationFilesVerificationResponse original = new LiveMigrationFilesVerificationResponse(
        id, digestAlgorithm, state, source, port,
        filesNotFoundAtSource, filesNotFoundAtDestination,
        metadataMatched, metadataMismatches,
        digestMismatches, digestVerificationFailures, filesMatched
        );

        String json = mapper.writeValueAsString(original);
        LiveMigrationFilesVerificationResponse deserialized =
        mapper.readValue(json, LiveMigrationFilesVerificationResponse.class);

        assertThat(deserialized.id()).isEqualTo(original.id());
        assertThat(deserialized.digestAlgorithm()).isEqualTo(original.digestAlgorithm());
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

    @ParameterizedTest
    @MethodSource("verificationSuccessParams")
    void testIsVerificationSuccessful(String state, int filesNotFoundAtSource, int filesNotFoundAtDestination,
                                      int metadataMismatches, int digestMismatches,
                                      int digestVerificationFailures, boolean expectedResult)
    {
        LiveMigrationFilesVerificationResponse response = new LiveMigrationFilesVerificationResponse(
        "test-id", "MD5", state, "192.168.1.1", 9042,
        filesNotFoundAtSource, filesNotFoundAtDestination,
        100, metadataMismatches, digestMismatches,
        digestVerificationFailures, 100
        );

        assertThat(response.isVerificationSuccessful()).isEqualTo(expectedResult);
    }

    @Test
    void testConstructorThrowsNullPointerExceptionForNullId()
    {
        assertThatThrownBy(() -> new LiveMigrationFilesVerificationResponse(
        null, "MD5", "COMPLETED", "192.168.1.1", 9042,
        0, 0, 100, 0, 0, 0, 100
        )).isInstanceOf(NullPointerException.class)
          .hasMessageContaining("id of files verification task must be specified");
    }
}
