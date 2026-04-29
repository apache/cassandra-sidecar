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

import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link ClusterOpsNodeStateDatabaseAccessor}
 */
class ClusterOpsNodeStateDatabaseAccessorIntTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void testCrudOperations()
    {
        waitForSchemaReady(10, TimeUnit.SECONDS);

        ClusterOpsNodeStateDatabaseAccessor accessor = injector.getInstance(ClusterOpsNodeStateDatabaseAccessor.class);
        String clusterName = maybeGetSession().getCluster().getMetadata().getClusterName();

        UUID operationId = UUID.randomUUID();
        UUID nodeId1 = UUID.randomUUID();
        UUID nodeId2 = UUID.randomUUID();
        UUID nodeId3 = UUID.randomUUID();

        // getNodeStatus returns null for non-existent node
        assertThat(accessor.getNodeStatus(clusterName, operationId, nodeId1)).isNull();

        // getNodeStatusesForOperation returns empty for non-existent operation
        assertThat(accessor.getNodeStatusesForOperation(clusterName, operationId)).isEmpty();

        // updateNodeStatus + getNodeStatus
        accessor.updateNodeStatus(clusterName, operationId, nodeId1, OperationalJobStatus.CREATED);
        assertThat(accessor.getNodeStatus(clusterName, operationId, nodeId1))
                .isEqualTo(OperationalJobStatus.CREATED);

        // updateNodeStatus overwrites existing status
        accessor.updateNodeStatus(clusterName, operationId, nodeId1, OperationalJobStatus.RUNNING);
        assertThat(accessor.getNodeStatus(clusterName, operationId, nodeId1))
                .isEqualTo(OperationalJobStatus.RUNNING);

        // updateNodeStatuses batch writes multiple nodes
        accessor.updateNodeStatuses(clusterName, operationId,
                                    Arrays.asList(nodeId2, nodeId3),
                                    OperationalJobStatus.CREATED);
        assertThat(accessor.getNodeStatus(clusterName, operationId, nodeId2))
                .isEqualTo(OperationalJobStatus.CREATED);
        assertThat(accessor.getNodeStatus(clusterName, operationId, nodeId3))
                .isEqualTo(OperationalJobStatus.CREATED);

        // getNodeStatusesForOperation returns all nodes for the operation
        Map<UUID, OperationalJobStatus> allStatuses =
                accessor.getNodeStatusesForOperation(clusterName, operationId);
        assertThat(allStatuses).hasSize(3);
        assertThat(allStatuses).containsEntry(nodeId1, OperationalJobStatus.RUNNING);
        assertThat(allStatuses).containsEntry(nodeId2, OperationalJobStatus.CREATED);
        assertThat(allStatuses).containsEntry(nodeId3, OperationalJobStatus.CREATED);

        // updateNodeStatuses is safe to retry and idempotent with the same inputs
        accessor.updateNodeStatuses(clusterName, operationId,
                                    Arrays.asList(nodeId2, nodeId3),
                                    OperationalJobStatus.CREATED);
        Map<UUID, OperationalJobStatus> afterRetry =
                accessor.getNodeStatusesForOperation(clusterName, operationId);
        assertThat(afterRetry).hasSize(3);
        assertThat(afterRetry).containsEntry(nodeId2, OperationalJobStatus.CREATED);
        assertThat(afterRetry).containsEntry(nodeId3, OperationalJobStatus.CREATED);

        // statuses from different operations are isolated
        UUID otherOperationId = UUID.randomUUID();
        accessor.updateNodeStatus(clusterName, otherOperationId, nodeId1, OperationalJobStatus.SUCCEEDED);
        assertThat(accessor.getNodeStatusesForOperation(clusterName, otherOperationId)).hasSize(1);
        // original operation unaffected
        assertThat(accessor.getNodeStatus(clusterName, operationId, nodeId1))
                .isEqualTo(OperationalJobStatus.RUNNING);
    }

    @CassandraIntegrationTest
    void testUpdateNodeStatusesLargeBatchChunking()
    {
        waitForSchemaReady(10, TimeUnit.SECONDS);

        ClusterOpsNodeStateDatabaseAccessor accessor = injector.getInstance(ClusterOpsNodeStateDatabaseAccessor.class);
        String clusterName = maybeGetSession().getCluster().getMetadata().getClusterName();

        UUID operationId = UUID.randomUUID();
        List<UUID> nodeIds = new ArrayList<>();
        for (int i = 0; i < 250; i++)
        {
            nodeIds.add(UUID.randomUUID());
        }

        accessor.updateNodeStatuses(clusterName, operationId, nodeIds, OperationalJobStatus.CREATED);

        Map<UUID, OperationalJobStatus> allStatuses =
                accessor.getNodeStatusesForOperation(clusterName, operationId);
        assertThat(allStatuses).hasSize(250);
        for (UUID nodeId : nodeIds)
        {
            assertThat(allStatuses).containsEntry(nodeId, OperationalJobStatus.CREATED);
        }
    }
}
