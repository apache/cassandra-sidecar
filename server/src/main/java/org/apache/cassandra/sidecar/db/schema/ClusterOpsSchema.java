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
 * Schema for the {@code cluster_ops} table, which persists and tracks operational jobs.
 */
public class ClusterOpsSchema extends TableSchema
{
    private static final String TABLE_NAME = "cluster_ops";

    private final SchemaKeyspaceConfiguration keyspaceConfig;
    private final SecondBoundConfiguration tableTtl;

    private PreparedStatement insertJob;
    private PreparedStatement selectJob;
    private PreparedStatement updateStatus;
    private PreparedStatement findAllJobs;

    public ClusterOpsSchema(SchemaKeyspaceConfiguration keyspaceConfig, SecondBoundConfiguration tableTtl)
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
                             "  operation_type text," +
                             "  status text," +
                             "  node_execution_order frozen<list<frozen<list<text>>>>," +
                             "  operation_metadata frozen<map<text, text>>," +
                             "  PRIMARY KEY ((cluster_name), operation_id, operation_type)" +
                             ") WITH CLUSTERING ORDER BY (operation_id DESC, operation_type ASC)" +
                             "  AND default_time_to_live = %s",
                             keyspaceConfig.keyspace(), TABLE_NAME, tableTtl.toSeconds());
    }

    @Override
    protected void prepareStatements(@NotNull Session session)
    {
        insertJob = prepare(insertJob, session, CqlLiterals.insertJob(keyspaceConfig));
        selectJob = prepare(selectJob, session, CqlLiterals.selectJob(keyspaceConfig));
        updateStatus = prepare(updateStatus, session, CqlLiterals.updateStatus(keyspaceConfig));
        findAllJobs = prepare(findAllJobs, session, CqlLiterals.findAllJobs(keyspaceConfig));
    }

    public PreparedStatement insertJob()
    {
        return insertJob;
    }

    public PreparedStatement selectJob()
    {
        return selectJob;
    }

    public PreparedStatement updateStatus()
    {
        return updateStatus;
    }

    public PreparedStatement findAllJobs()
    {
        return findAllJobs;
    }

    private static class CqlLiterals
    {
        static String insertJob(SchemaKeyspaceConfiguration config)
        {
            return withTable("INSERT INTO %s.%s (" +
                             "  cluster_name," +
                             "  operation_id," +
                             "  operation_type," +
                             "  status," +
                             "  node_execution_order," +
                             "  operation_metadata" +
                             ") VALUES (?, ?, ?, ?, ?, ?)", config);
        }

        static String selectJob(SchemaKeyspaceConfiguration config)
        {
            return withTable("SELECT cluster_name, " +
                             "operation_id, " +
                             "operation_type, " +
                             "status, " +
                             "node_execution_order, " +
                             "operation_metadata " +
                             "FROM %s.%s " +
                             "WHERE cluster_name = ? AND operation_id = ?", config);
        }

        static String updateStatus(SchemaKeyspaceConfiguration config)
        {
            return withTable("UPDATE %s.%s SET status = ? " +
                             "WHERE cluster_name = ? AND operation_id = ? AND operation_type = ?", config);
        }

        static String findAllJobs(SchemaKeyspaceConfiguration config)
        {
            return withTable("SELECT cluster_name, " +
                             "operation_id, " +
                             "operation_type, " +
                             "status, " +
                             "node_execution_order, " +
                             "operation_metadata " +
                             "FROM %s.%s " +
                             "WHERE cluster_name = ? LIMIT ?", config);
        }

        private static String withTable(String format, SchemaKeyspaceConfiguration config)
        {
            return String.format(format, config.keyspace(), TABLE_NAME);
        }
    }
}
