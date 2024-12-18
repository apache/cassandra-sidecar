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

package org.apache.cassandra.sidecar.coordination;

import java.util.Collections;
import java.util.HashMap;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BestEffortSingleConditionalExecutor.Parameters}
 */
class BestEffortSingleConditionalExecutorParametersTest
{
    @DisplayName("Default values are returned when no configuration is provided")
    @Test
    void testDefaults()
    {
        BestEffortSingleConditionalExecutor.Parameters params = BestEffortSingleConditionalExecutor.Parameters.from(Collections.emptyMap());
        assertThat(params.isEnabled()).as("Enabled by default").isTrue();
        assertThat(params.delayMillis()).as("Run every minute by default").isEqualTo(60_000L);
        assertThat(params.initialDelayMillis()).as("Wait 1 second before starting the process by default").isEqualTo(1_000L);
    }

    @DisplayName("Ensures that is_enabled returns the default value when an invalid string value is configured")
    @Test
    void testInvalidIsEnabledConfiguration()
    {
        BestEffortSingleConditionalExecutor.Parameters params
        = BestEffortSingleConditionalExecutor.Parameters.from(Collections.singletonMap("is_enabled", "please-disable"));
        assertThat(params.isEnabled()).as("The default value is expected when misconfigured").isEqualTo(true);
    }

    @DisplayName("Enabled when the configuration is_enabled is true")
    @Test
    void testIsEnabledConfiguration()
    {
        BestEffortSingleConditionalExecutor.Parameters params
        = BestEffortSingleConditionalExecutor.Parameters.from(Collections.singletonMap("is_enabled", "true"));
        assertThat(params.isEnabled()).as("Configured to enabled").isEqualTo(true);
    }

    @DisplayName("Ensures that the delay millis cannot be configured with a value less than the minimum value")
    @Test
    void testDelayMillisMinimum()
    {
        BestEffortSingleConditionalExecutor.Parameters params
        = BestEffortSingleConditionalExecutor.Parameters.from(Collections.singletonMap("frequency_millis", "1000"));
        assertThat(params.delayMillis()).as("Guarantee the minimum value").isEqualTo(30_000L);
    }

    @DisplayName("Ensures that the delay millis cannot be configured with an invalid string value")
    @Test
    void testInvalidDelayMillisConfiguration()
    {
        BestEffortSingleConditionalExecutor.Parameters params
        = BestEffortSingleConditionalExecutor.Parameters.from(Collections.singletonMap("frequency_millis", "one-thousand"));
        assertThat(params.delayMillis()).as("The default value is expected when misconfigured").isEqualTo(60_000L);
    }

    @DisplayName("Ensures that the initial delay millis cannot be configured with a value less than the minimum value")
    @Test
    void testInitialDelayMillisMinimum()
    {
        BestEffortSingleConditionalExecutor.Parameters params
        = BestEffortSingleConditionalExecutor.Parameters.from(Collections.singletonMap("initial_delay_millis", "-10"));
        assertThat(params.initialDelayMillis()).as("Guarantee the minimum value").isEqualTo(0);
    }

    @DisplayName("Ensures that the initial delay millis cannot be configured with an invalid string value")
    @Test
    void testInvalidInitialDelayMillisConfiguration()
    {
        BestEffortSingleConditionalExecutor.Parameters params
        = BestEffortSingleConditionalExecutor.Parameters.from(Collections.singletonMap("initial_delay_millis", "one_hundred"));
        assertThat(params.initialDelayMillis()).as("The default value is expected when misconfigured").isEqualTo(1_000);
    }

    @DisplayName("Configures non-default values")
    @Test
    void testConfiguration()
    {
        BestEffortSingleConditionalExecutor.Parameters params
        = BestEffortSingleConditionalExecutor.Parameters.from(new HashMap<String, String>()
        {{
            put("is_enabled", "false");
            put("initial_delay_millis", "25987");
            put("frequency_millis", "600000");
        }});
        assertThat(params.isEnabled()).isFalse();
        assertThat(params.delayMillis()).isEqualTo(600_000L);
        assertThat(params.initialDelayMillis()).isEqualTo(25987);
    }
}
