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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link ClusterOpsNodeStateDatabaseAccessor}
 */
class ClusterOpsNodeStateDatabaseAccessorIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void testCrudOperations()
    {
        waitForSchemaReady(10, TimeUnit.SECONDS);

        ClusterOpsNodeStateDatabaseAccessor accessor = injector.getInstance(ClusterOpsNodeStateDatabaseAccessor.class);
        String clusterName = maybeGetSession().getCluster().getMetadata().getClusterName();

        UUID operationId = UUIDs.timeBased();
        UUID nodeId1 = UUID.randomUUID();
        UUID nodeId2 = UUID.randomUUID();
        UUID nodeId3 = UUID.randomUUID();

        assertThat(accessor.getNodeStatus(clusterName, operationId, nodeId1))
                .withFailMessage("getNodeStatus should return null for a non-existent node")
                .isNull();

        assertThat(accessor.getNodeStatusesForOperation(clusterName, operationId))
                .withFailMessage("getNodeStatusesForOperation should return an empty map for a non-existent operation")
                .isEmpty();

        accessor.updateNodeStatus(clusterName, operationId, nodeId1, OperationalJobStatus.CREATED);
        assertThat(accessor.getNodeStatus(clusterName, operationId, nodeId1))
                .withFailMessage("getNodeStatus should return correct node status after updateNodeStatus")
                .isEqualTo(OperationalJobStatus.CREATED);

        accessor.updateNodeStatus(clusterName, operationId, nodeId1, OperationalJobStatus.RUNNING);
        assertThat(accessor.getNodeStatus(clusterName, operationId, nodeId1))
                .withFailMessage("getNodeStatus should return updated status after updateNodeStatus overwrites previous status")
                .isEqualTo(OperationalJobStatus.RUNNING);

        accessor.updateNodeStatus(clusterName, operationId, nodeId2, OperationalJobStatus.CREATED);
        accessor.updateNodeStatus(clusterName, operationId, nodeId3, OperationalJobStatus.CREATED);
        Map<UUID, OperationalJobStatus> allStatuses =
                accessor.getNodeStatusesForOperation(clusterName, operationId);
        assertThat(allStatuses)
                .withFailMessage("getNodeStatusesForOperation should return all nodes with their latest statuses")
                .hasSize(3)
                .containsEntry(nodeId1, OperationalJobStatus.RUNNING)
                .containsEntry(nodeId2, OperationalJobStatus.CREATED)
                .containsEntry(nodeId3, OperationalJobStatus.CREATED);

        accessor.updateNodeStatuses(clusterName, operationId,
                                    Arrays.asList(nodeId2, nodeId3),
                                    OperationalJobStatus.CREATED);
        Map<UUID, OperationalJobStatus> afterRetry =
                accessor.getNodeStatusesForOperation(clusterName, operationId);
        assertThat(afterRetry)
                .withFailMessage("updateNodeStatuses should be idempotent")
                .hasSize(3)
                .containsEntry(nodeId2, OperationalJobStatus.CREATED)
                .containsEntry(nodeId3, OperationalJobStatus.CREATED);

        UUID otherOperationId = UUIDs.timeBased();
        accessor.updateNodeStatus(clusterName, otherOperationId, nodeId1, OperationalJobStatus.SUCCEEDED);
        assertThat(accessor.getNodeStatusesForOperation(clusterName, otherOperationId))
                .withFailMessage("getNodeStatusesForOperation should only return nodes for the queried operation")
                .hasSize(1);
        assertThat(accessor.getNodeStatus(clusterName, operationId, nodeId1))
                .withFailMessage("getNodeStatus should be unaffected by node updates in a different operation")
                .isEqualTo(OperationalJobStatus.RUNNING);

        UUID chunkOperationId = UUIDs.timeBased();
        List<UUID> nodeIds = new ArrayList<>();
        for (int i = 0; i < 250; i++)
        {
            nodeIds.add(UUID.randomUUID());
        }
        accessor.updateNodeStatuses(clusterName, chunkOperationId, nodeIds, OperationalJobStatus.CREATED);
        Map<UUID, OperationalJobStatus> chunkStatuses =
                accessor.getNodeStatusesForOperation(clusterName, chunkOperationId);
        assertThat(chunkStatuses)
                .withFailMessage("getNodeStatusesForOperation should return all nodes persisted across chunked UNLOGGED batches")
                .hasSize(250);
        for (UUID nodeId : nodeIds)
        {
            assertThat(chunkStatuses)
                    .withFailMessage("getNodeStatusesForOperation should show correct status for node %s after chunked batch write", nodeId)
                    .containsEntry(nodeId, OperationalJobStatus.CREATED);
        }
    }
}
