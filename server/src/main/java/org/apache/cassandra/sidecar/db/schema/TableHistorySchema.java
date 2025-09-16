/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.db.schema;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.coordination.ExecuteOnClusterLeaseholderOnly;
import org.jetbrains.annotations.NotNull;

/**
 * Schema definition and management for tracking table schema evolution history.
 * This class extends {@link TableSchema} to provide specialized schema management for storing
 * historical versions of table schemas in Cassandra Sidecar.
 */
@Singleton
public class TableHistorySchema extends TableSchema implements ExecuteOnClusterLeaseholderOnly
{
    private static final String TABLE_SCHEMA_HISTORY = "table_schema_history";

    private final SchemaKeyspaceConfiguration keyspaceConfig;

    // prepared statements
    private PreparedStatement insertTableSchema;
    private PreparedStatement selectVersionTableSchema;

    @Inject
    public TableHistorySchema(ServiceConfiguration configuration)
    {
        this.keyspaceConfig = configuration.schemaKeyspaceConfiguration();
    }

    @Override
    protected void prepareStatements(@NotNull Session session)
    {
        insertTableSchema = prepare(insertTableSchema, session, CqlLiterals.insertTableSchema(keyspaceConfig));
        selectVersionTableSchema = prepare(selectVersionTableSchema, session, CqlLiterals.selectVersionTableSchema(keyspaceConfig));
    }

    @Override
    protected String keyspaceName()
    {
        return keyspaceConfig.keyspace();
    }

    @Override
    protected String tableName()
    {
        return TABLE_SCHEMA_HISTORY;
    }

    @Override
    protected boolean exists(@NotNull Metadata metadata)
    {
        KeyspaceMetadata ksMetadata = metadata.getKeyspace(keyspaceConfig.keyspace());
        if (ksMetadata == null)
        {
            return false;
        }

        return ksMetadata.getTable(TABLE_SCHEMA_HISTORY) != null;
    }

    @Override
    protected String createSchemaStatement()
    {
        return String.format("CREATE TABLE IF NOT EXISTS %s.%s (" +
                             "  keyspace_name text," +
                             "  table_name text," +
                             "  version uuid," +
                             "  created_at timeuuid," +
                             "  table_schema text," +
                             "  PRIMARY KEY ((ks, tb), version)" +
                             ")",
                             keyspaceConfig.keyspace(), TABLE_SCHEMA_HISTORY);
    }

    public PreparedStatement insertTableSchema()
    {
        return insertTableSchema;
    }

    public PreparedStatement selectVersionTableSchema()
    {
        return selectVersionTableSchema;
    }

    private static class CqlLiterals
    {
        static String insertTableSchema(SchemaKeyspaceConfiguration config)
        {
            return withTable("INSERT INTO %s.%s (keyspace_name, table_name, version, created_at, table_schema) " +
                             "VALUES (?, ?, ?, NOW(), ?)", config);
        }

        static String selectVersionTableSchema(SchemaKeyspaceConfiguration config)
        {
            return withTable("SELECT table_schema FROM %s.%s WHERE keyspace_name = ? AND table_name = ? AND version = ?", config);
        }

        private static String withTable(String format, SchemaKeyspaceConfiguration config)
        {
            return String.format(format, config.keyspace(), TABLE_SCHEMA_HISTORY);
        }
    }
}
