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
 * Schema for the {@code active_cluster_ops} table, which ensures that only one active operation of a given type
 * is running at a time across the cluster. When a Sidecar instance receives a request to set an operation to
 * {@code RUNNING}, it inserts the operation into this table using a lightweight transaction (LWT) to guarantee 
 * mutual exclusion. Sidecar instances also periodically query this table to discover when an operation has been 
 * activated.
 */
public class ActiveClusterOpsSchema extends TableSchema
{
    private static final String TABLE_NAME = "active_cluster_ops";

    private final SchemaKeyspaceConfiguration keyspaceConfig;
    private final SecondBoundConfiguration tableTtl;

    private PreparedStatement trySetActive;
    private PreparedStatement getActive;
    private PreparedStatement getActiveByType;
    private PreparedStatement clearActive;

    public ActiveClusterOpsSchema(SchemaKeyspaceConfiguration keyspaceConfig, SecondBoundConfiguration tableTtl)
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
                             "  datacenter text," +
                             "  operation_type text," +
                             "  operation_id timeuuid," +
                             "  PRIMARY KEY ((cluster_name, datacenter), operation_type)" +
                             ") WITH compaction = {'class': 'LeveledCompactionStrategy'}" +
                             "  AND default_time_to_live = %s",
                             keyspaceConfig.keyspace(), TABLE_NAME, tableTtl.toSeconds());
    }

    @Override
    protected void prepareStatements(@NotNull Session session)
    {
        trySetActive = prepare(trySetActive, session, CqlLiterals.trySetActive(keyspaceConfig));
        getActive = prepare(getActive, session, CqlLiterals.getActive(keyspaceConfig));
        getActiveByType = prepare(getActiveByType, session, CqlLiterals.getActiveByType(keyspaceConfig));
        clearActive = prepare(clearActive, session, CqlLiterals.clearActive(keyspaceConfig));
    }

    public PreparedStatement trySetActive()
    {
        return trySetActive;
    }

    public PreparedStatement getActive()
    {
        return getActive;
    }

    public PreparedStatement getActiveByType()
    {
        return getActiveByType;
    }

    public PreparedStatement clearActive()
    {
        return clearActive;
    }

    private static class CqlLiterals
    {
        static String trySetActive(SchemaKeyspaceConfiguration config)
        {
            return withTable("INSERT INTO %s.%s (cluster_name, datacenter, operation_type, operation_id) " +
                             "VALUES (?, ?, ?, ?) IF NOT EXISTS", config);
        }

        static String getActive(SchemaKeyspaceConfiguration config)
        {
            return withTable("SELECT operation_type, operation_id FROM %s.%s " +
                             "WHERE cluster_name = ? AND datacenter = ?", config);
        }

        static String getActiveByType(SchemaKeyspaceConfiguration config)
        {
            return withTable("SELECT operation_id FROM %s.%s " +
                             "WHERE cluster_name = ? AND datacenter = ? AND operation_type = ?", config);
        }

        static String clearActive(SchemaKeyspaceConfiguration config)
        {
            return withTable("DELETE FROM %s.%s " +
                             "WHERE cluster_name = ? AND datacenter = ? AND operation_type = ? " +
                             "IF operation_id = ?", config);
        }

        private static String withTable(String format, SchemaKeyspaceConfiguration config)
        {
            return String.format(format, config.keyspace(), TABLE_NAME);
        }
    }
}
