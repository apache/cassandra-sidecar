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
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.ActiveClusterOpsSchema;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Database accessor for the {@code active_cluster_ops} table.
 * Provides mutual exclusion for active operations via lightweight transactions (LWT).
 */
@Singleton
public class ActiveClusterOpsDatabaseAccessor extends DatabaseAccessor<ActiveClusterOpsSchema>
{
    @Inject
    public ActiveClusterOpsDatabaseAccessor(SidecarSchema sidecarSchema, CQLSessionProvider sessionProvider)
    {
        this(sidecarSchema.tableSchema(ActiveClusterOpsSchema.class), sessionProvider);
    }

    @VisibleForTesting
    public ActiveClusterOpsDatabaseAccessor(ActiveClusterOpsSchema schema, CQLSessionProvider sessionProvider)
    {
        super(schema, sessionProvider);
    }

    public boolean trySetActiveOperation(String clusterName, String datacenter,
                                         OperationType operationType, UUID operationId)
    {
        BoundStatement statement = tableSchema.trySetActive()
                                              .bind(clusterName, datacenter, operationType.name(), operationId);
        statement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        statement.setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
        ResultSet resultSet = execute(statement);
        return resultSet.wasApplied();
    }

    @Nullable
    public UUID getActiveOperation(String clusterName, String datacenter, OperationType operationType)
    {
        BoundStatement statement = tableSchema.getActiveByType().bind(clusterName, datacenter, operationType.name());
        statement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        ResultSet resultSet = execute(statement);
        Row row = resultSet.one();
        return row == null ? null : row.getUUID("operation_id");
    }

    @NotNull
    public Map<OperationType, UUID> getActiveOperations(String clusterName, String datacenter)
    {
        BoundStatement statement = tableSchema.getActive().bind(clusterName, datacenter);
        statement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        ResultSet resultSet = execute(statement);
        Map<OperationType, UUID> activeOps = new HashMap<>();
        for (Row row : resultSet)
        {
            activeOps.put(OperationType.valueOf(row.getString("operation_type")), row.getUUID("operation_id"));
        }
        return activeOps;
    }

    public boolean clearActiveOperation(String clusterName, String datacenter,
                                        OperationType operationType, UUID operationId)
    {
        BoundStatement statement = tableSchema.clearActive()
                                              .bind(clusterName, datacenter, operationType.name(), operationId);
        statement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        statement.setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
        ResultSet resultSet = execute(statement);
        return resultSet.wasApplied();
    }
}
