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

package org.apache.cassandra.sidecar.job;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.data.OperationType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link DisabledOperationalJobCoordinator}, the coordinator used when a Sidecar instance
 * does not support coordinated cluster-wide operations.
 */
class DisabledOperationalJobCoordinatorTest
{
    private final OperationalJobCoordinator coordinator = new DisabledOperationalJobCoordinator();

    @Test
    void testTrySetActiveThrows()
    {
        UUID operationId = UUIDs.timeBased();
        assertThatThrownBy(() -> coordinator.trySetActive(OperationType.MOVE, operationId))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("coordination is not supported by this Sidecar instance");
    }

    @Test
    void testClearActiveThrows()
    {
        UUID operationId = UUIDs.timeBased();
        assertThatThrownBy(() -> coordinator.clearActive(OperationType.MOVE, operationId))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("coordination is not supported by this Sidecar instance");
    }

    @Test
    void testGetActiveOperationReturnsNull()
    {
        assertThat(coordinator.getActiveOperation(OperationType.MOVE)).isNull();
    }

    @Test
    void testGetActiveOperationsReturnsEmptyMap()
    {
        assertThat(coordinator.getActiveOperations()).isEmpty();
    }
}
