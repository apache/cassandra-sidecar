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
 * {@link RestoreJobsSchema} holds all prepared statements needed for talking to Cassandra for various actions related
 * to {@link org.apache.cassandra.sidecar.db.RestoreJob} like inserting a restore job, updating a restore job,
 * finding restore jobs and more
 */
public class RestoreJobsSchema extends TableSchema
{
    private static final String RESTORE_JOB_TABLE_NAME = "restore_job_v3";

    private final SchemaKeyspaceConfiguration keyspaceConfig;
    private final long tableTtlSeconds;

    // prepared statements
    private PreparedStatement insertJob;
    private PreparedStatement updateBlobSecrets;
    private PreparedStatement updateStatus;
    private PreparedStatement updateJobAgent;
    private PreparedStatement updateExpireAt;
    private PreparedStatement selectJob;
    private PreparedStatement findAllByCreatedAt;

    public RestoreJobsSchema(SchemaKeyspaceConfiguration keyspaceConfig, long tableTtlSeconds)
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
        insertJob = prepare(insertJob, session, CqlLiterals.insertJob(keyspaceConfig));
        updateBlobSecrets = prepare(updateBlobSecrets, session, CqlLiterals.updateBlobSecrets(keyspaceConfig));
        updateStatus = prepare(updateStatus, session, CqlLiterals.updateStatus(keyspaceConfig));
        updateJobAgent = prepare(updateJobAgent, session, CqlLiterals.updateJobAgent(keyspaceConfig));
        updateExpireAt = prepare(updateExpireAt, session, CqlLiterals.updateExpireAt(keyspaceConfig));
        selectJob = prepare(selectJob, session, CqlLiterals.selectJob(keyspaceConfig));
        findAllByCreatedAt = prepare(findAllByCreatedAt, session, CqlLiterals.findAllByCreatedAt(keyspaceConfig));
    }

    @Override
    protected String tableName()
    {
        return RESTORE_JOB_TABLE_NAME;
    }

    @Override
    protected String createSchemaStatement()
    {
        return String.format("CREATE TABLE IF NOT EXISTS %s.%s (" +
                             "  created_at date," +
                             "  job_id timeuuid," +
                             "  keyspace_name text," +
                             "  table_name text," +
                             "  job_agent text," +
                             "  status text," +
                             "  blob_secrets blob," +
                             "  import_options blob," +
                             "  expire_at timestamp," +
                             "  bucket_count smallint," +
                             "  consistency_level text," +
                             "  local_datacenter text," +
                             "  PRIMARY KEY (created_at, job_id)" +
                             ") WITH default_time_to_live = %s",
                             keyspaceConfig.keyspace(), RESTORE_JOB_TABLE_NAME, tableTtlSeconds);
    }

    public PreparedStatement insertJob()
    {
        return insertJob;
    }

    public PreparedStatement updateBlobSecrets()
    {
        return updateBlobSecrets;
    }

    public PreparedStatement updateStatus()
    {
        return updateStatus;
    }

    public PreparedStatement updateJobAgent()
    {
        return updateJobAgent;
    }

    public PreparedStatement updateExpireAt()
    {
        return updateExpireAt;
    }

    public PreparedStatement selectJob()
    {
        return selectJob;
    }

    public PreparedStatement findAllByCreatedAt()
    {
        return findAllByCreatedAt;
    }

    private static class CqlLiterals
    {
        static String insertJob(SchemaKeyspaceConfiguration config)
        {
            return withTable("INSERT INTO %s.%s (" +
                             "  created_at," +
                             "  job_id," +
                             "  keyspace_name," +
                             "  table_name," +
                             "  job_agent," +
                             "  status," +
                             "  blob_secrets," +
                             "  import_options," +
                             "  consistency_level," +
                             "  local_datacenter," +
                             "  expire_at" +
                             ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", config);
        }

        static String updateBlobSecrets(SchemaKeyspaceConfiguration config)
        {
            return withTable("INSERT INTO %s.%s (" +
                             "  created_at," +
                             "  job_id," +
                             "  blob_secrets" +
                             ") VALUES (?, ? ,?)", config);
        }

        static String updateStatus(SchemaKeyspaceConfiguration config)
        {
            return withTable("INSERT INTO %s.%s (" +
                             "  created_at," +
                             "  job_id," +
                             "  status" +
                             ") VALUES (?, ?, ?)", config);
        }

        static String updateJobAgent(SchemaKeyspaceConfiguration config)
        {
            return withTable("INSERT INTO %s.%s (" +
                             "  created_at," +
                             "  job_id," +
                             "  job_agent" +
                             ") VALUES (?, ?, ?)", config);
        }


        static String updateExpireAt(SchemaKeyspaceConfiguration config)
        {
            return withTable("INSERT INTO %s.%s (" +
                             "  created_at," +
                             "  job_id," +
                             "  expire_at" +
                             ") VALUES (?, ?, ?)", config);
        }

        static String selectJob(SchemaKeyspaceConfiguration config)
        {
            return withTable("SELECT created_at, " +
                             "job_id, " +
                             "keyspace_name, " +
                             "table_name, " +
                             "job_agent, " +
                             "status, " +
                             "blob_secrets, " +
                             "import_options, " +
                             "consistency_level, " +
                             "local_datacenter, " +
                             "expire_at " +
                             "FROM %s.%s " +
                             "WHERE created_at = ? AND job_id = ?", config);
        }

        static String findAllByCreatedAt(SchemaKeyspaceConfiguration config)
        {
            return withTable("SELECT created_at, " +
                             "job_id, " +
                             "keyspace_name, " +
                             "table_name, " +
                             "job_agent, " +
                             "status, " +
                             "blob_secrets, " +
                             "import_options, " +
                             "consistency_level, " +
                             "local_datacenter, " +
                             "expire_at " +
                             "FROM %s.%s " +
                             "WHERE created_at = ?", config);
        }

        private static String withTable(String format, SchemaKeyspaceConfiguration config)
        {
            return String.format(format, config.keyspace(), RESTORE_JOB_TABLE_NAME);
        }
    }
}
