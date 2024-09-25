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
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * {@link RestoreRangesSchema} holds all prepared statements needed for talking to Cassandra for various actions
 * related to restore progress tracking in terms of each token range
 */
public class RestoreRangesSchema extends TableSchema
{
    private static final String TABLE_NAME = "restore_range_v1";

    private final SchemaKeyspaceConfiguration keyspaceConfig;
    private final long tableTtlSeconds;

    private PreparedStatement insert;
    private PreparedStatement findAll;
    private PreparedStatement update;

    public RestoreRangesSchema(SchemaKeyspaceConfiguration keyspaceConfig, long tableTtlSeconds)
    {
        this.keyspaceConfig = keyspaceConfig;
        this.tableTtlSeconds = tableTtlSeconds;
    }

    @Override
    protected String keyspaceName()
    {
        return keyspaceConfig.keyspace();
    }

    @Override
    protected void prepareStatements(@NotNull Session session)
    {
        insert = prepare(insert, session, CqlLiterals.insert(keyspaceConfig));
        findAll = prepare(findAll, session, CqlLiterals.findAll(keyspaceConfig));
        update = prepare(update, session, CqlLiterals.update(keyspaceConfig));
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
                             "  job_id timeuuid," +
                             "  bucket_id smallint," + // same bucket_id as in the slice row
                             "  start_token varint," +
                             "  end_token varint," +
                             "  slice_id text," +
                             "  slice_bucket text," +
                             "  slice_key text," +
                             "  status_by_replica map<text, text>," +
                             "  PRIMARY KEY ((job_id, bucket_id), start_token, end_token)" +
                             ") WITH default_time_to_live = %s",
                             keyspaceConfig.keyspace(), TABLE_NAME, tableTtlSeconds);
    }

    public PreparedStatement insert()
    {
        return insert;
    }

    public PreparedStatement findAll()
    {
        return findAll;
    }

    public PreparedStatement updateStatus()
    {
        return update;
    }

    private static class CqlLiterals
    {
        static String insert(SchemaKeyspaceConfiguration config)
        {
            return withTable("INSERT INTO %s.%s (" +
                             "  job_id," +
                             "  bucket_id," +
                             "  start_token," +
                             "  end_token," +
                             "  slice_id," +
                             "  slice_bucket," +
                             "  slice_key," +
                             "  status_by_replica" +
                             ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)", config);
        }

        // ALLOW FILTERING within the same partition should have minimum impact on read performance.
        static String findAll(SchemaKeyspaceConfiguration config)
        {
            return withTable("SELECT job_id, bucket_id, slice_id, slice_bucket, slice_key, " +
                             "start_token, end_token, status_by_replica " +
                             "FROM %s.%s " +
                             "WHERE job_id = ? AND bucket_id = ? ALLOW FILTERING", config);
        }

        static String update(SchemaKeyspaceConfiguration config)
        {
            return withTable("UPDATE %s.%s " +
                             "SET status_by_replica = status_by_replica + ? " +
                             "WHERE job_id = ? AND bucket_id = ? AND start_token = ? AND end_token = ?", config);
        }

        private static String withTable(String format, SchemaKeyspaceConfiguration config)
        {
            return String.format(format, config.keyspace(), TABLE_NAME);
        }
    }
}
