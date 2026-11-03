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

package org.apache.cassandra.sidecar.modules;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.config.OperationalJobConfiguration;
import org.apache.cassandra.sidecar.config.yaml.OperationalJobConfigurationImpl;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Tests for {@link CassandraOperationsModule} focusing on configuration validation on startup.
 */
class CassandraOperationsModuleTest
{
    @Test
    void passesWhenSchemaDisabledAndAllStorageBackedFeaturesOff()
    {
        assertThatCode(() -> CassandraOperationsModule.validateOperationalJobConfiguration(
        config(false, false), false)).doesNotThrowAnyException();
    }

    @Test
    void passesWhenSchemaEnabledAndAllStorageBackedFeaturesOn()
    {
        assertThatCode(() -> CassandraOperationsModule.validateOperationalJobConfiguration(
        config(true, true), true)).doesNotThrowAnyException();
    }

    @Test
    void failsWhenSchemaDisabledButDurableTrackingOn()
    {
        assertThatIllegalArgumentException()
        .isThrownBy(() -> CassandraOperationsModule.validateOperationalJobConfiguration(config(false, true), false))
        .withMessageContaining("sidecar.schema.is_enabled")
        .withMessageContaining("durable_tracking_enabled");
    }

    @Test
    void failsWhenSchemaDisabledButCoordinationOn()
    {
        assertThatIllegalArgumentException()
        .isThrownBy(() -> CassandraOperationsModule.validateOperationalJobConfiguration(config(true, false), false))
        .withMessageContaining("sidecar.schema.is_enabled")
        .withMessageContaining("coordination_enabled");
    }

    private static OperationalJobConfiguration config(boolean coordinationEnabled, boolean durableTrackingEnabled)
    {
        return OperationalJobConfigurationImpl.builder()
                                              .coordinationEnabled(coordinationEnabled)
                                              .durableTrackingEnabled(durableTrackingEnabled)
                                              .build();
    }
}
