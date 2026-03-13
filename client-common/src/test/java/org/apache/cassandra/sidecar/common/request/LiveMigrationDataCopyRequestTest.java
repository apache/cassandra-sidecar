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

class LiveMigrationDataCopyRequestTest
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testSerializationDeserializationRoundTrip() throws Exception
    {
        LiveMigrationDataCopyRequest original = new LiveMigrationDataCopyRequest(5, 0.95, 10);

        String json = objectMapper.writeValueAsString(original);
        LiveMigrationDataCopyRequest deserialized = objectMapper.readValue(json, LiveMigrationDataCopyRequest.class);

        assertThat(deserialized.maxIterations).isEqualTo(original.maxIterations);
        assertThat(deserialized.successThreshold).isEqualTo(original.successThreshold);
        assertThat(deserialized.maxConcurrency).isEqualTo(original.maxConcurrency);
    }

    @Test
    void testConstructorWithInvalidMaxIterationsThrowsException()
    {
        assertThatThrownBy(() -> new LiveMigrationDataCopyRequest(0, 0.95, 10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid maxIterations 0. It cannot be less than or equal to zero.");
    }

    @Test
    void testConstructorWithNegativeMaxIterationsThrowsException()
    {
        assertThatThrownBy(() -> new LiveMigrationDataCopyRequest(-5, 0.95, 10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid maxIterations -5. It cannot be less than or equal to zero.");
    }

    @Test
    void testConstructorWithInvalidSuccessThresholdBelowZeroThrowsException()
    {
        assertThatThrownBy(() -> new LiveMigrationDataCopyRequest(5, -0.1, 10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid successThreshold -0.1. It cannot be less than zero or greater than one.");
    }

    @Test
    void testConstructorWithInvalidSuccessThresholdAboveOneThrowsException()
    {
        assertThatThrownBy(() -> new LiveMigrationDataCopyRequest(5, 1.5, 10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid successThreshold 1.5. It cannot be less than zero or greater than one.");
    }

    @Test
    void testConstructorWithInvalidMaxConcurrencyThrowsException()
    {
        assertThatThrownBy(() -> new LiveMigrationDataCopyRequest(5, 0.95, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid maxConcurrency 0. It cannot be less than or equal to zero.");
    }

    @Test
    void testConstructorWithNegativeMaxConcurrencyThrowsException()
    {
        assertThatThrownBy(() -> new LiveMigrationDataCopyRequest(5, 0.95, -3))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid maxConcurrency -3. It cannot be less than or equal to zero.");
    }

    @Test
    void testValidBoundaryValues()
    {
        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 0.0, 1);

        assertThat(request.maxIterations).isEqualTo(1);
        assertThat(request.successThreshold).isEqualTo(0.0);
        assertThat(request.maxConcurrency).isEqualTo(1);
    }

    @Test
    void testValidBoundaryValuesUpperBound()
    {
        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(Integer.MAX_VALUE,
                                                                                1.0,
                                                                                Integer.MAX_VALUE);

        assertThat(request.maxIterations).isEqualTo(Integer.MAX_VALUE);
        assertThat(request.successThreshold).isEqualTo(1.0);
        assertThat(request.maxConcurrency).isEqualTo(Integer.MAX_VALUE);
    }
}
