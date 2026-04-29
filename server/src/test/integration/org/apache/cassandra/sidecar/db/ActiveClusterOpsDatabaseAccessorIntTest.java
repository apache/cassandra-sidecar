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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link ActiveClusterOpsDatabaseAccessor}
 */
class ActiveClusterOpsDatabaseAccessorIntTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void testLwtOperations()
    {
        waitForSchemaReady(10, TimeUnit.SECONDS);

        ActiveClusterOpsDatabaseAccessor accessor = injector.getInstance(ActiveClusterOpsDatabaseAccessor.class);
        String clusterName = maybeGetSession().getCluster().getMetadata().getClusterName();

        UUID operationId1 = UUID.randomUUID();
        UUID operationId2 = UUID.randomUUID();

        // getActiveOperation returns null when no active operation exists
        assertThat(accessor.getActiveOperation(clusterName, "restart")).isNull();

        // getActiveOperations returns empty when no active operations exist
        assertThat(accessor.getActiveOperations(clusterName)).isEmpty();

        // trySetActiveOperation succeeds when no active operation exists
        assertThat(accessor.trySetActiveOperation(clusterName, "restart", operationId1)).isTrue();

        // getActiveOperation returns the active operation
        assertThat(accessor.getActiveOperation(clusterName, "restart")).isEqualTo(operationId1);

        // trySetActiveOperation fails when an active operation already exists
        assertThat(accessor.trySetActiveOperation(clusterName, "restart", operationId2)).isFalse();
        // original operation is still active after failed attempt
        assertThat(accessor.getActiveOperation(clusterName, "restart")).isEqualTo(operationId1);

        // trySetActiveOperation with the same ID also returns false 
        assertThat(accessor.trySetActiveOperation(clusterName, "restart", operationId1)).isFalse();
        assertThat(accessor.getActiveOperation(clusterName, "restart")).isEqualTo(operationId1);

        // different operation types are independent
        UUID decommissionId = UUID.randomUUID();
        assertThat(accessor.trySetActiveOperation(clusterName, "decommission", decommissionId)).isTrue();

        // getActiveOperations returns all active operations
        Map<String, UUID> activeOps = accessor.getActiveOperations(clusterName);
        assertThat(activeOps).hasSize(2);
        assertThat(activeOps).containsEntry("restart", operationId1);
        assertThat(activeOps).containsEntry("decommission", decommissionId);

        // clearActiveOperation fails with wrong operation ID 
        assertThat(accessor.clearActiveOperation(clusterName, "restart", operationId2)).isFalse();
        // operation is still active after failed clear
        assertThat(accessor.getActiveOperation(clusterName, "restart")).isEqualTo(operationId1);

        // clearActiveOperation succeeds with matching operation ID
        assertThat(accessor.clearActiveOperation(clusterName, "restart", operationId1)).isTrue();
        assertThat(accessor.getActiveOperation(clusterName, "restart")).isNull();

        // clearActiveOperation is safe to retry after already cleared 
        assertThat(accessor.clearActiveOperation(clusterName, "restart", operationId1)).isFalse();

        // acquire-release-reacquire: can set a new active operation after clearing, other operation types unaffected
        assertThat(accessor.trySetActiveOperation(clusterName, "restart", operationId2)).isTrue();
        assertThat(accessor.getActiveOperation(clusterName, "restart")).isEqualTo(operationId2);
        assertThat(accessor.getActiveOperation(clusterName, "decommission")).isEqualTo(decommissionId);
    }
}
