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

package org.apache.cassandra.sidecar.db.schema;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * Schema for the {@code cluster_ops_node_state} table, which tracks the status of an operation for a given node.
 * Sidecar instances query this table to check the status of nodes being operated on before their local nodes,
 * enabling distributed coordination of cluster-wide operations.
 * <p>
 * The {@code cluster_name} partition key identifies the cluster being operated on, allowing a single
 * Cassandra cluster to store operational job state for multiple managed clusters.
 */
public class ClusterOpsNodeStateSchema extends TableSchema
{
    private static final String TABLE_NAME = "cluster_ops_node_state";

    private final SchemaKeyspaceConfiguration keyspaceConfig;
    private final SecondBoundConfiguration tableTtl;

    private PreparedStatement insertNodeStatus;
    private PreparedStatement selectNodeStatus;
    private PreparedStatement selectAllNodeStatuses;

    public ClusterOpsNodeStateSchema(SchemaKeyspaceConfiguration keyspaceConfig, SecondBoundConfiguration tableTtl)
    {
        this.keyspaceConfig = keyspaceConfig;
        this.tableTtl = tableTtl;
    }

    @Override
    protected String keyspaceName()
    {
        return keyspaceConfig.keyspace();
    }

    @Override
    protected String tableName()
    {
        return TABLE_NAME;
    }

    @Override
    protected String createSchemaStatement()
    {
        return String.format("CREATE TABLE IF NOT EXISTS %s.%s (" +
                             "  cluster_name text," +
                             "  operation_id timeuuid," +
                             "  node_id uuid," +
                             "  node_status text," +
                             "  PRIMARY KEY ((cluster_name, operation_id), node_id)" +
                             ") WITH compaction = {'class': 'LeveledCompactionStrategy'}" +
                             "  AND default_time_to_live = %s",
                             keyspaceConfig.keyspace(), TABLE_NAME, tableTtl.toSeconds());
    }

    @Override
    protected void prepareStatements(@NotNull Session session)
    {
        insertNodeStatus = prepare(insertNodeStatus, session, CqlLiterals.insertNodeStatus(keyspaceConfig));
        selectNodeStatus = prepare(selectNodeStatus, session, CqlLiterals.selectNodeStatus(keyspaceConfig));
        selectAllNodeStatuses = prepare(selectAllNodeStatuses, session, CqlLiterals.selectAllNodeStatuses(keyspaceConfig));
    }

    public PreparedStatement insertNodeStatus()
    {
        return insertNodeStatus;
    }

    public PreparedStatement selectNodeStatus()
    {
        return selectNodeStatus;
    }

    public PreparedStatement selectAllNodeStatuses()
    {
        return selectAllNodeStatuses;
    }

    private static class CqlLiterals
    {
        static String insertNodeStatus(SchemaKeyspaceConfiguration config)
        {
            return withTable("INSERT INTO %s.%s (" +
                             "  cluster_name," +
                             "  operation_id," +
                             "  node_id," +
                             "  node_status" +
                             ") VALUES (?, ?, ?, ?)", config);
        }

        static String selectNodeStatus(SchemaKeyspaceConfiguration config)
        {
            return withTable("SELECT node_status " +
                             "FROM %s.%s " +
                             "WHERE cluster_name = ? AND operation_id = ? AND node_id = ?", config);
        }

        static String selectAllNodeStatuses(SchemaKeyspaceConfiguration config)
        {
            return withTable("SELECT node_id, node_status " +
                             "FROM %s.%s " +
                             "WHERE cluster_name = ? AND operation_id = ?", config);
        }

        private static String withTable(String format, SchemaKeyspaceConfiguration config)
        {
            return String.format(format, config.keyspace(), TABLE_NAME);
        }
    }
}
