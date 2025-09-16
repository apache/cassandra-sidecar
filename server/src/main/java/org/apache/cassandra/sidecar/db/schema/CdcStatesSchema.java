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

import java.util.concurrent.TimeUnit;

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
 * Schema definition and management for the CDC (Change Data Capture) state persistence table.
 * This class extends {@link TableSchema} to provide specialized schema management for storing
 * and retrieving CDC state data in Cassandra Sidecar. The CDC state table is used to persist
 * the current processing state of CDC consumers across token ranges.
 */
@Singleton
public class CdcStatesSchema extends TableSchema implements ExecuteOnClusterLeaseholderOnly
{
    private static final String CDC_STATE_TABLE_NAME = "cdc_state_v2";
    private static final String CDC_STATE_TABLE_TTL = Long.toString(TimeUnit.DAYS.toSeconds(30));

    private final SchemaKeyspaceConfiguration keyspaceConfig;

    // prepared statements
    private PreparedStatement insertState;
    private PreparedStatement selectState;

    @Inject
    public CdcStatesSchema(ServiceConfiguration configuration)
    {
        this.keyspaceConfig = configuration.schemaKeyspaceConfiguration();
    }

    @Override
    protected String keyspaceName()
    {
        return keyspaceConfig.keyspace();
    }

    @Override
    protected void prepareStatements(@NotNull Session session)
    {
        this.insertState = prepare(insertState, session, CqlLiterals.insertState(keyspaceConfig));
        this.selectState = prepare(selectState, session, CqlLiterals.select(keyspaceConfig));
    }

    @Override
    protected String tableName()
    {
        return CDC_STATE_TABLE_NAME;
    }

    @Override
    protected boolean exists(@NotNull Metadata metadata)
    {
        KeyspaceMetadata ksMetadata = metadata.getKeyspace(keyspaceConfig.keyspace());
        if (ksMetadata == null)
            return false;
        return ksMetadata.getTable(CDC_STATE_TABLE_NAME) != null;
    }

    @Override
    protected String createSchemaStatement()
    {
        return String.format("CREATE TABLE IF NOT EXISTS %s.%s (" +
                             "  job_id text," +
                             "  split smallint," +
                             "  start varint," +
                             "  end varint," +
                             "  state blob," +
                             "  PRIMARY KEY ((job_id, split), start, end)" +
                             ") WITH CLUSTERING ORDER BY (start ASC, end ASC) " +
                             " AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', " +
                             "'compaction_window_size': '1', 'compaction_window_unit': 'DAYS', 'enabled': 'true'}" +
                             " AND default_time_to_live = %s" +
                             " AND gc_grace_seconds = 0", // data is TTL'ed and no tombstones, so we can drop immediately without gc_grace_seconds
                             keyspaceConfig.keyspace(), CDC_STATE_TABLE_NAME, CDC_STATE_TABLE_TTL);
    }

    public PreparedStatement insertState()
    {
        return insertState;
    }

    public PreparedStatement select()
    {
        return selectState;
    }

    private static class CqlLiterals
    {
        static String insertState(SchemaKeyspaceConfiguration config)
        {
            return withTable("INSERT INTO %s.%s (job_id, split, start, end, state) " +
                             "VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?", config);
        }

        static String select(SchemaKeyspaceConfiguration config)
        {
            return withTable("SELECT start, end, state FROM %s.%s WHERE job_id = ? AND split = ?", config);
        }

        private static String withTable(String format, SchemaKeyspaceConfiguration config)
        {
            return String.format(format, config.keyspace(), CDC_STATE_TABLE_NAME);
        }
    }
}
