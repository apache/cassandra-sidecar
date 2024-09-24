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
import org.apache.cassandra.sidecar.common.server.data.TableSchema;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * {@link RestoreSlicesSchema} holds all prepared statements needed for talking to Cassandra for various actions
 * related to {@link org.apache.cassandra.sidecar.db.RestoreSlice} like inserting a new restore slice,
 * updating status of a slice, finding restore slices and more
 */
public class RestoreSlicesSchema extends TableSchema
{
    private static final String RESTORE_SLICE_TABLE_NAME = "restore_slice_v3";

    private final SchemaKeyspaceConfiguration keyspaceConfig;
    private final long tableTtlSeconds;

    // prepared statements
    private PreparedStatement insertSlice;
    private PreparedStatement findAllByTokenRange;

    public RestoreSlicesSchema(SchemaKeyspaceConfiguration keyspaceConfig, long tableTtlSeconds)
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
        insertSlice = prepare(insertSlice, session, CqlLiterals.insertSlice(keyspaceConfig));
        findAllByTokenRange = prepare(findAllByTokenRange, session, CqlLiterals.findAllByTokenRange(keyspaceConfig));
    }

    @Override
    protected String tableName()
    {
        return RESTORE_SLICE_TABLE_NAME;
    }

    @Override
    protected String createSchemaStatement()
    {
        return String.format("CREATE TABLE IF NOT EXISTS %s.%s (" +
                             "  job_id timeuuid," +
                             "  bucket_id smallint," +
                             "  slice_id text," +
                             "  bucket text," +
                             "  key text," +
                             "  checksum text," +
                             "  start_token varint," +
                             "  end_token varint," +
                             "  compressed_size bigint," +
                             "  uncompressed_size bigint," +
                             "  PRIMARY KEY ((job_id, bucket_id), start_token, end_token, slice_id)" +
                             ") WITH default_time_to_live = %s",
                             keyspaceConfig.keyspace(), RESTORE_SLICE_TABLE_NAME, tableTtlSeconds);
    }

    public PreparedStatement insertSlice()
    {
        return insertSlice;
    }

    public PreparedStatement findAllByTokenRange()
    {
        return findAllByTokenRange;
    }

    private static class CqlLiterals
    {
        static String insertSlice(SchemaKeyspaceConfiguration config)
        {
            return withTable("INSERT INTO %s.%s (" +
                             "  job_id," +
                             "  bucket_id," +
                             "  slice_id," +
                             "  bucket," +
                             "  key," +
                             "  checksum," +
                             "  start_token," +
                             "  end_token," +
                             "  compressed_size," +
                             "  uncompressed_size" +
                             ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", config);
        }

        // ALLOW FILTERING within the same partition should have minimum impact on read performance.
        // To locate all qualified (intersecting) ranges, for Range [T1, T2],
        // the conditions is `start_token <= T2 AND end_token >= T1`
        static String findAllByTokenRange(SchemaKeyspaceConfiguration config)
        {
            return withTable("SELECT job_id, bucket_id, slice_id, bucket, key, checksum, " +
                             "start_token, end_token, compressed_size, uncompressed_size " +
                             "FROM %s.%s " +
                             "WHERE job_id = ? AND bucket_id = ? AND " +
                             "end_token >= ? AND start_token <= ? ALLOW FILTERING", config);
        }

        private static String withTable(String format, SchemaKeyspaceConfiguration config)
        {
            return String.format(format, config.keyspace(), RESTORE_SLICE_TABLE_NAME);
        }
    }
}
