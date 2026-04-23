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
 * Schema for the {@code active_cluster_ops} table, which tracks active operations
 * and provides mutual exclusion via lightweight transactions (LWT).
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
                             "  operation_type text," +
                             "  operation_id uuid," +
                             "  PRIMARY KEY ((cluster_name), operation_type)" +
                             ") WITH default_time_to_live = %s",
                             keyspaceConfig.keyspace(), TABLE_NAME, tableTtl.toSeconds());
    }

    @Override
    protected void prepareStatements(@NotNull Session session)
    {
        trySetActive = prepare(trySetActive, session,
                               String.format("INSERT INTO %s.%s (cluster_name, operation_type, operation_id) " +
                                             "VALUES (?, ?, ?) IF NOT EXISTS",
                                             keyspaceName(), tableName()));
        getActive = prepare(getActive, session,
                            String.format("SELECT operation_type, operation_id FROM %s.%s " +
                                          "WHERE cluster_name = ?",
                                          keyspaceName(), tableName()));
        getActiveByType = prepare(getActiveByType, session,
                                  String.format("SELECT operation_id FROM %s.%s " +
                                                "WHERE cluster_name = ? AND operation_type = ?",
                                                keyspaceName(), tableName()));
        clearActive = prepare(clearActive, session,
                              String.format("DELETE FROM %s.%s " +
                                            "WHERE cluster_name = ? AND operation_type = ? IF operation_id = ?",
                                            keyspaceName(), tableName()));
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
}
