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

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LiveMigrationFilesVerificationRequestTest
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    static Stream<Arguments> concurrencyAndAlgorithm()
    {
        return Stream.of(
        Arguments.of(1, "MD5"),
        Arguments.of(8, "XXHash32"),
        Arguments.of(10, "MD5"),
        Arguments.of(Integer.MAX_VALUE, "XXHash32")
        );
    }

    static Stream<String> invalidDigestAlgorithms()
    {
        return Stream.of(null, "", "   ");
    }

    @ParameterizedTest
    @MethodSource("concurrencyAndAlgorithm")
    void testSerializationDeserializationRoundTrip(int maxConcurrency, String digestAlgorithm) throws Exception
    {
        LiveMigrationFilesVerificationRequest original =
        new LiveMigrationFilesVerificationRequest(maxConcurrency, digestAlgorithm);

        String json = objectMapper.writeValueAsString(original);
        LiveMigrationFilesVerificationRequest deserialized =
        objectMapper.readValue(json, LiveMigrationFilesVerificationRequest.class);

        assertThat(deserialized.maxConcurrency()).isEqualTo(original.maxConcurrency());
        assertThat(deserialized.digestAlgorithm()).isEqualTo(original.digestAlgorithm());
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, -1, Integer.MIN_VALUE })
    void testInvalidMaxConcurrency(int maxConcurrency)
    {
        assertThatThrownBy(() -> new LiveMigrationFilesVerificationRequest(maxConcurrency, "MD5"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxConcurrency must be >= 1");
    }

    @ParameterizedTest
    @MethodSource("invalidDigestAlgorithms")
    void testInvalidDigestAlgorithm(String digestAlgorithm)
    {
        assertThatThrownBy(() -> new LiveMigrationFilesVerificationRequest(10, digestAlgorithm))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("digestAlgorithm cannot be null or empty");
    }
}
