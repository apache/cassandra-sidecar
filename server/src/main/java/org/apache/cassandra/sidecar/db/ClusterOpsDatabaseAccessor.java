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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.reflect.TypeToken;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.ClusterOpsSchema;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Database accessor for the {@code cluster_ops} table.
 */
@Singleton
public class ClusterOpsDatabaseAccessor extends DatabaseAccessor<ClusterOpsSchema>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterOpsDatabaseAccessor.class);
    @Inject
    public ClusterOpsDatabaseAccessor(SidecarSchema sidecarSchema, CQLSessionProvider sessionProvider)
    {
        this(sidecarSchema.tableSchema(ClusterOpsSchema.class), sessionProvider);
    }

    @VisibleForTesting
    public ClusterOpsDatabaseAccessor(ClusterOpsSchema schema, CQLSessionProvider sessionProvider)
    {
        super(schema, sessionProvider);
    }

    public void persistJob(String clusterName, OperationalJobRecord job)
    {
        BoundStatement statement = tableSchema.insertJob()
                                              .bind(clusterName,
                                                    job.jobId(),
                                                    job.operationType(),
                                                    job.status().name(),
                                                    job.nodeExecutionOrder(),
                                                    job.operationMetadata());
        execute(statement);
    }

    @Nullable
    public OperationalJobRecord findJob(String clusterName, UUID jobId)
    {
        BoundStatement statement = tableSchema.selectJob().bind(clusterName, jobId);
        ResultSet resultSet = execute(statement);
        Row row = resultSet.one();
        if (row == null)
        {
            return null;
        }
        if (!resultSet.isExhausted())
        {
            LOGGER.warn("Multiple rows found for operation_id={} in cluster={}. Using first match.", jobId, clusterName);
        }
        return recordFromRow(row);
    }

    public void updateJobStatus(String clusterName, UUID jobId, String operationType, OperationalJobStatus status)
    {
        BoundStatement statement = tableSchema.updateStatus()
                                              .bind(status.name(), clusterName, jobId, operationType);
        execute(statement);
    }

    @NotNull
    public List<OperationalJobRecord> findAllJobs(String clusterName, int limit)
    {
        BoundStatement statement = tableSchema.findAllJobs().bind(clusterName, limit);
        ResultSet resultSet = execute(statement);
        List<OperationalJobRecord> records = new ArrayList<>();
        for (Row row : resultSet)
        {
            records.add(recordFromRow(row));
        }
        return records;
    }

    private static final TypeToken<List<List<String>>> NODES_ORDER_TYPE = new TypeToken<List<List<String>>>() {};

    private OperationalJobRecord recordFromRow(Row row)
    {
        UUID operationId = row.getUUID("operation_id");
        String operationType = row.getString("operation_type");
        OperationalJobStatus status = OperationalJobStatus.valueOf(row.getString("status"));
        List<List<String>> nodeExecutionOrder = row.get("node_execution_order", NODES_ORDER_TYPE);
        Map<String, String> operationMetadata = row.getMap("operation_metadata", String.class, String.class);
        return new OperationalJobRecord(operationId, operationType, status,
                                        nodeExecutionOrder == null || nodeExecutionOrder.isEmpty() ? null : nodeExecutionOrder,
                                        operationMetadata.isEmpty() ? null : operationMetadata);
    }
}
