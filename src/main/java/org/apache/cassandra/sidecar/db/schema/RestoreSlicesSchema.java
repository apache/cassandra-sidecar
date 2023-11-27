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

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * {@link RestoreSlicesSchema} holds all prepared statements needed for talking to Cassandra for various actions
 * related to {@link org.apache.cassandra.sidecar.db.RestoreSlice} like inserting a new restore slice,
 * updating status of a slice, finding restore slices and more
 */
public class RestoreSlicesSchema extends AbstractSchema.TableSchema
{
    private static final String RESTORE_SLICE_TABLE_NAME = "restore_slice_v2";

    private final SchemaKeyspaceConfiguration keyspaceConfig;
    private final long tableTtlSeconds;

    // prepared statements
    private PreparedStatement insertSlice;
    private PreparedStatement findAllByTokenRange;
    private PreparedStatement updateStatus;

    public RestoreSlicesSchema(SchemaKeyspaceConfiguration keyspaceConfig, long tableTtlSeconds)
    {
        this.keyspaceConfig = keyspaceConfig;
        this.tableTtlSeconds = tableTtlSeconds;
    }

    protected void prepareStatements(@NotNull Session session)
    {
        insertSlice = prepare(insertSlice, session, CqlLiterals.insertSlice(keyspaceConfig));
        findAllByTokenRange = prepare(findAllByTokenRange, session, CqlLiterals.findAllByTokenRange(keyspaceConfig));
        updateStatus = prepare(updateStatus, session, CqlLiterals.updateStatus(keyspaceConfig));
    }

    @Override
    protected boolean exists(@NotNull Metadata metadata)
    {
        KeyspaceMetadata ksMetadata = metadata.getKeyspace(keyspaceConfig.keyspace());
        if (ksMetadata == null)
            return false;
        return ksMetadata.getTable(RESTORE_SLICE_TABLE_NAME) != null;
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
                             "  status_by_replica map<text, text>," +
                             "  all_replicas set<text>," +
                             "  PRIMARY KEY ((job_id, bucket_id), start_token, slice_id)" +
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

    public PreparedStatement updateStatus()
    {
        return updateStatus;
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
                             "  uncompressed_size," +
                             "  status_by_replica," +
                             "  all_replicas" +
                             ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", config);
        }

        // ALLOW FILTERING within the same partition should have minimum impact on read performance.
        // To locate all qualified (intersecting) ranges, for Range[T1, T2],
        // the conditions is `end_token >= T1 AND start_token < T2`
        static String findAllByTokenRange(SchemaKeyspaceConfiguration config)
        {
            return withTable("SELECT job_id, bucket_id, slice_id, bucket, key, checksum, " +
                             "start_token, end_token, compressed_size, uncompressed_size, " +
                             "status_by_replica, all_replicas " +
                             "FROM %s.%s " +
                             "WHERE job_id = ? AND bucket_id = ? AND " +
                             "end_token >= ? AND start_token < ? ALLOW FILTERING", config);
        }

        static String updateStatus(SchemaKeyspaceConfiguration config)
        {
            return withTable("UPDATE %s.%s " +
                             "SET status_by_replica = status_by_replica + ?, " +
                             "all_replicas = all_replicas + ? " +
                             "WHERE job_id = ? AND bucket_id = ? AND start_token = ? AND slice_id = ?", config);
        }

        private static String withTable(String format, SchemaKeyspaceConfiguration config)
        {
            return String.format(format, config.keyspace(), RESTORE_SLICE_TABLE_NAME);
        }
    }
}
