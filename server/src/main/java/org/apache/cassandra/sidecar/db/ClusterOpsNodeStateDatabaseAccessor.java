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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.ClusterOpsNodeStateSchema;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Database accessor for the {@code cluster_ops_node_state} table.
 */
@Singleton
public class ClusterOpsNodeStateDatabaseAccessor extends DatabaseAccessor<ClusterOpsNodeStateSchema>
{
    private static final int DEFAULT_BATCH_CHUNK_SIZE = 100;

    private final int batchChunkSize;

    @Inject
    public ClusterOpsNodeStateDatabaseAccessor(SidecarSchema sidecarSchema, CQLSessionProvider sessionProvider)
    {
        this(sidecarSchema.tableSchema(ClusterOpsNodeStateSchema.class), sessionProvider, DEFAULT_BATCH_CHUNK_SIZE);
    }

    @VisibleForTesting
    public ClusterOpsNodeStateDatabaseAccessor(ClusterOpsNodeStateSchema schema, CQLSessionProvider sessionProvider,
                                               int batchChunkSize)
    {
        super(schema, sessionProvider);
        this.batchChunkSize = batchChunkSize;
    }

    public void updateNodeStatus(String clusterName, UUID operationId, UUID nodeId, OperationalJobStatus nodeStatus)
    {
        BoundStatement statement = tableSchema.insertNodeStatus()
                                              .bind(clusterName, operationId, nodeId, nodeStatus.name());
        statement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        execute(statement);
    }

    public void updateNodeStatuses(String clusterName, UUID operationId,
                                   List<UUID> nodeIds, OperationalJobStatus nodeStatus)
    {
        for (int i = 0; i < nodeIds.size(); i += batchChunkSize)
        {
            List<UUID> chunk = nodeIds.subList(i, Math.min(i + batchChunkSize, nodeIds.size()));
            BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            for (UUID nodeId : chunk)
            {
                BoundStatement statement = tableSchema.insertNodeStatus()
                                                      .bind(clusterName, operationId, nodeId, nodeStatus.name());
                batch.add(statement);
            }
            batch.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            execute(batch);
        }
    }

    @Nullable
    public OperationalJobStatus getNodeStatus(String clusterName, UUID operationId, UUID nodeId)
    {
        BoundStatement statement = tableSchema.selectNodeStatus().bind(clusterName, operationId, nodeId);
        statement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        ResultSet resultSet = execute(statement);
        Row row = resultSet.one();
        if (row == null)
        {
            return null;
        }
        return OperationalJobStatus.valueOf(row.getString("node_status"));
    }

    @NotNull
    public Map<UUID, OperationalJobStatus> getNodeStatusesForOperation(String clusterName, UUID operationId)
    {
        BoundStatement statement = tableSchema.selectAllNodeStatuses().bind(clusterName, operationId);
        statement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        ResultSet resultSet = execute(statement);
        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        for (Row row : resultSet)
        {
            UUID nodeId = row.getUUID("node_id");
            OperationalJobStatus status = OperationalJobStatus.valueOf(row.getString("node_status"));
            statuses.put(nodeId, status);
        }
        return statuses;
    }
}
