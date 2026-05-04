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

package org.apache.cassandra.sidecar.db;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link ActiveClusterOpsDatabaseAccessor}
 */
class ActiveClusterOpsDatabaseAccessorIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void testLwtOperations()
    {
        waitForSchemaReady(10, TimeUnit.SECONDS);

        ActiveClusterOpsDatabaseAccessor accessor = injector.getInstance(ActiveClusterOpsDatabaseAccessor.class);
        String clusterName = maybeGetSession().getCluster().getMetadata().getClusterName();

        UUID operationId1 = UUID.randomUUID();
        UUID operationId2 = UUID.randomUUID();

        assertThat(accessor.getActiveOperation(clusterName, "restart"))
                .withFailMessage("getActiveOperation should return null when no active operation exists")
                .isNull();
        assertThat(accessor.getActiveOperations(clusterName))
                .withFailMessage("getActiveOperations should return an empty map when no active operation exists")
                .isEmpty();

        assertThat(accessor)
                .withFailMessage("trySetActiveOperation should succeed when no active operation, and getActiveOperation should return the operation")
                .satisfies(a -> {
                    assertThat(a.trySetActiveOperation(clusterName, "restart", operationId1)).isTrue();
                    assertThat(a.getActiveOperation(clusterName, "restart")).isEqualTo(operationId1);
                });

        assertThat(accessor)
                .withFailMessage("trySetActiveOperation should fail when an operation of the same type is active")
                .satisfies(a -> {
                    assertThat(a.trySetActiveOperation(clusterName, "restart", operationId2)).isFalse();
                    assertThat(a.getActiveOperation(clusterName, "restart")).isEqualTo(operationId1);
                });

        assertThat(accessor.trySetActiveOperation(clusterName, "restart", operationId1))
                .withFailMessage("trySetActiveOperation should fail when retried with same active operation ID")
                .isFalse();

        UUID decommissionId = UUID.randomUUID();
        assertThat(accessor.trySetActiveOperation(clusterName, "decommission", decommissionId))
                .withFailMessage("trySetActiveOperation should succeed for a different operation type")
                .isTrue();
        assertThat(accessor.getActiveOperations(clusterName))
                .withFailMessage("getActiveOperations should return all concurrently active operations")
                .hasSize(2)
                .containsEntry("restart", operationId1)
                .containsEntry("decommission", decommissionId);

        assertThat(accessor)
                .withFailMessage("clearActiveOperation should fail when a non-matching operation ID is supplied")
                .satisfies(a -> {
                    assertThat(a.clearActiveOperation(clusterName, "restart", operationId2)).isFalse();
                    assertThat(a.getActiveOperation(clusterName, "restart")).isEqualTo(operationId1);
                });

        assertThat(accessor)
                .withFailMessage("clearActiveOperation should succeed when the operation ID matches the active operation")
                .satisfies(a -> {
                    assertThat(a.clearActiveOperation(clusterName, "restart", operationId1)).isTrue();
                    assertThat(a.getActiveOperation(clusterName, "restart")).isNull();
                });

        assertThat(accessor.clearActiveOperation(clusterName, "restart", operationId1))
                .withFailMessage("clearActiveOperation should be a safe no-op when retried after operation is already cleared")
                .isFalse();

        assertThat(accessor)
                .withFailMessage("trySetActiveOperation should succeed after active operation is cleared, and other operation types should be unaffected")
                .satisfies(a -> {
                    assertThat(a.trySetActiveOperation(clusterName, "restart", operationId2)).isTrue();
                    assertThat(a.getActiveOperation(clusterName, "restart")).isEqualTo(operationId2);
                    assertThat(a.getActiveOperation(clusterName, "decommission")).isEqualTo(decommissionId);
                });
    }
}
