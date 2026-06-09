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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.reflect.TypeToken;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.data.OperationType;
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
    private static final TypeToken<List<List<UUID>>> NODES_ORDER_TYPE = new TypeToken<List<List<UUID>>>() {};

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
        Date lastUpdate = Date.from(Instant.now());
        Date startTime = job.startTime() != null ? Date.from(job.startTime()) : null;
        BoundStatement statement = tableSchema.insertJob()
                                              .bind(clusterName,
                                                    job.jobId(),
                                                    job.operationType().name(),
                                                    job.status().name(),
                                                    startTime,
                                                    lastUpdate,
                                                    job.failureReason(),
                                                    job.nodeExecutionOrder(),
                                                    job.operationMetadata());
        statement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        execute(statement);
    }

    @Nullable
    public OperationalJobRecord findJob(String clusterName, UUID jobId)
    {
        BoundStatement statement = tableSchema.selectJob().bind(clusterName, jobId);
        statement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
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

    public void updateJobStatus(String clusterName, UUID jobId, OperationType operationType,
                                OperationalJobStatus status, @Nullable String failureReason)
    {
        Date lastUpdate = Date.from(Instant.now());
        BoundStatement statement;
        if (failureReason != null)
        {
            statement = tableSchema.updateStatusWithFailure()
                                   .bind(status.name(), lastUpdate, failureReason,
                                         clusterName, jobId, operationType.name());
        }
        else if (status == OperationalJobStatus.RUNNING && shouldSetStartTime(clusterName, jobId))
        {
            statement = tableSchema.updateStatusWithStartTime()
                                   .bind(status.name(), lastUpdate, lastUpdate,
                                         clusterName, jobId, operationType.name());
        }
        else
        {
            statement = tableSchema.updateStatus()
                                   .bind(status.name(), lastUpdate,
                                         clusterName, jobId, operationType.name());
        }
        statement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        execute(statement);
    }

    private boolean shouldSetStartTime(String clusterName, UUID jobId)
    {
        OperationalJobRecord existing = findJob(clusterName, jobId);
        return existing != null && existing.startTime() == null;
    }

    @NotNull
    public List<OperationalJobRecord> findAllJobs(String clusterName, int limit)
    {
        BoundStatement statement = tableSchema.findAllJobs().bind(clusterName, limit);
        statement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        ResultSet resultSet = execute(statement);
        List<OperationalJobRecord> records = new ArrayList<>();
        for (Row row : resultSet)
        {
            records.add(recordFromRow(row));
        }
        return records;
    }

    private OperationalJobRecord recordFromRow(Row row)
    {
        UUID operationId = row.getUUID("operation_id");
        OperationType operationType = OperationType.valueOf(row.getString("operation_type"));
        OperationalJobStatus status = OperationalJobStatus.valueOf(row.getString("status"));
        Date startTimeDate = row.getTimestamp("start_time");
        Instant startTime = startTimeDate != null ? startTimeDate.toInstant() : null;
        Date lastUpdateDate = row.getTimestamp("last_update");
        Instant lastUpdate = lastUpdateDate != null ? lastUpdateDate.toInstant() : null;
        String failureReason = row.getString("failure_reason");
        List<List<UUID>> nodeExecutionOrder = row.get("node_execution_order", NODES_ORDER_TYPE);
        if (nodeExecutionOrder != null && nodeExecutionOrder.isEmpty())
        {
            nodeExecutionOrder = null;
        }
        Map<String, String> operationMetadata = row.getMap("operation_metadata", String.class, String.class);
        if (operationMetadata.isEmpty())
        {
            operationMetadata = null;
        }
        return new OperationalJobRecord(operationId, operationType, status,
                                        startTime, lastUpdate, failureReason,
                                        nodeExecutionOrder, operationMetadata);
    }
}
