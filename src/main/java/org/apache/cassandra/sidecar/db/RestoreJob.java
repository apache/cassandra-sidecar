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

package org.apache.cassandra.sidecar.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.data.SSTableImportOptions;
import org.jetbrains.annotations.NotNull;

/**
 * RestoreJob is the in-memory representation of a restore job
 */
public class RestoreJob
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public final LocalDate createdAt;
    public final UUID jobId;
    public final String keyspaceName;
    public final String tableName;
    public final String jobAgent;
    public final RestoreJobStatus status;
    public final RestoreJobSecrets secrets;
    public final SSTableImportOptions importOptions;
    public final Date expireAt;
    public final short bucketCount;
    public final String consistencyLevel;

    private final boolean isMaterialized; // When true, the job object is materialized from database

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Create from a row read from Cassandra.
     * Read {@code SidecarSchema.CqlLiterals#RESTORE_JOB_TABLE_SCHEMA} for the schema.
     *
     * @param row cannot be null
     */
    public static RestoreJob from(@NotNull Row row) throws DataObjectMappingException
    {
        Builder builder = new Builder();
        builder.createdAt(row.getDate("created_at"))
               .jobId(row.getUUID("job_id")).jobAgent(row.getString("job_agent"))
               .keyspace(row.getString("keyspace_name")).table(row.getString("table_name"))
               .jobStatus(decodeJobStatus(row.getString("status")))
               .jobSecrets(decodeJobSecrets(row.getBytes("blob_secrets")))
               .expireAt(row.getTimestamp("expire_at"))
               .sstableImportOptions(decodeSSTableImportOptions(row.getBytes("import_options")));
        // todo: Yifan, add them back when the cql statement is updated to reflect the new columns.
        //  Add new fields to CreateRestoreJobRequestPayload too
//               .bucketCount(row.getShort("bucket_count"))
//               .consistencyLevel(row.getString("consistency_level"));
        return builder.build();
    }

    private static RestoreJobStatus decodeJobStatus(String status)
    {
        return status == null ? null : RestoreJobStatus.valueOf(status.toUpperCase());
    }

    private static RestoreJobSecrets decodeJobSecrets(ByteBuffer secretsBytes)
    {
        return secretsBytes == null
               ? null
               : deserializeJsonBytes(secretsBytes,
                                      RestoreJobSecrets.class,
                                      "secrets");
    }

    private static SSTableImportOptions decodeSSTableImportOptions(ByteBuffer importOptionsBytes)
    {
        return importOptionsBytes == null
               ? null
               : deserializeJsonBytes(importOptionsBytes,
                                      SSTableImportOptions.class,
                                      "importOptions");
    }

    private RestoreJob(Builder builder)
    {
        this.createdAt = builder.createdAt;
        this.jobId = builder.jobId;
        this.keyspaceName = builder.keyspaceName;
        this.tableName = builder.tableName;
        this.jobAgent = builder.jobAgent;
        this.status = builder.status;
        this.secrets = builder.secrets;
        this.importOptions = builder.importOptions == null
                             ? SSTableImportOptions.defaults()
                             : builder.importOptions;
        this.expireAt = builder.expireAt;
        this.bucketCount = builder.bucketCount;
        this.consistencyLevel = builder.consistencyLevel;
        this.isMaterialized = builder.isMaterialized;
    }

    public Builder unbuild()
    {
        return new Builder(this);
    }

    /**
     * When a job object is materialized from database, and it has non-null consistencyLevel, the range of the slices
     * of the restore is managed by Sidecar server, meaning that server should assign slices to sidecar instances and
     * check whether the job has met the consistency level to complete the job; otherwise, sidecar instances are just
     * simple workers and rely on client for decision-making.
     * Depending on the return value, call-sites select the correct handling for the restore jobs and slices
     * @return true when sidecar server manages the slices based on the token range; otherwise, return false.
     */
    public boolean isRangeManagedByServer()
    {
        // a full job that has consistency level specified
        return isMaterialized && consistencyLevel != null;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return String.format("RestoreJob{" +
                             "createdAt='%s', jobId='%s', keyspaceName='%s', " +
                             "tableName='%s', status='%s', secrets='%s', importOptions='%s', " +
                             "expireAt='%s', bucketCount='%s', consistencyLevel='%s'}",
                             createdAt.toString(), jobId.toString(),
                             keyspaceName, tableName,
                             status, secrets, importOptions,
                             expireAt, bucketCount, consistencyLevel);
    }

    public static LocalDate toLocalDate(UUID jobId)
    {
        return LocalDate.fromMillisSinceEpoch(UUIDs.unixTimestamp(jobId));
    }

    private static <T> T deserializeJsonBytes(ByteBuffer byteBuffer, Class<T> type, String fieldNameHint)
    {
        try
        {
            return MAPPER.readValue(Bytes.getArray(byteBuffer), type);
        }
        catch (IOException e)
        {
            throw new DataObjectMappingException("Failed to deserialize " + fieldNameHint, e);
        }
    }

    /**
     * Builder for building a {@link RestoreJob}
     */
    public static class Builder implements DataObjectBuilder<Builder, RestoreJob>
    {
        private LocalDate createdAt;
        private UUID jobId;
        private String keyspaceName;
        private String tableName;
        private String jobAgent;
        private RestoreJobStatus status;
        private RestoreJobSecrets secrets;
        private SSTableImportOptions importOptions;
        private Date expireAt;
        private short bucketCount;
        private String consistencyLevel;
        private boolean isMaterialized;

        private Builder()
        {
        }

        // used by unbuild
        private Builder(RestoreJob restoreJob)
        {
            this.createdAt = restoreJob.createdAt;
            this.jobId = restoreJob.jobId;
            this.keyspaceName = restoreJob.keyspaceName;
            this.tableName = restoreJob.tableName;
            this.jobAgent = restoreJob.jobAgent;
            this.status = restoreJob.status;
            this.secrets = restoreJob.secrets;
            this.importOptions = restoreJob.importOptions;
            this.expireAt = restoreJob.expireAt;
            this.bucketCount = restoreJob.bucketCount;
            this.consistencyLevel = restoreJob.consistencyLevel;
            this.isMaterialized = restoreJob.isMaterialized;
        }

        public Builder createdAt(LocalDate createdAt)
        {
            return update(b -> b.createdAt = createdAt);
        }

        public Builder jobId(UUID jobId)
        {
            return update(b -> b.jobId = jobId);
        }

        public Builder keyspace(String keyspace)
        {
            return update(b -> b.keyspaceName = keyspace);
        }

        public Builder table(String table)
        {
            return update(b -> b.tableName = table);
        }

        public Builder jobAgent(String jobAgent)
        {
            return update(b -> b.jobAgent = jobAgent);
        }

        public Builder jobStatus(RestoreJobStatus jobStatus)
        {
            return update(b -> b.status = jobStatus);
        }

        public Builder jobSecrets(RestoreJobSecrets jobSecrets)
        {
            return update(b -> b.secrets = jobSecrets);
        }

        public Builder sstableImportOptions(SSTableImportOptions options)
        {
            return update(b -> b.importOptions = options);
        }

        public Builder expireAt(Date expireAt)
        {
            return update(b -> b.expireAt = expireAt);
        }

        public Builder bucketCount(short bucketCount)
        {
            return update(b -> b.bucketCount = bucketCount);
        }

        public Builder consistencyLevel(String consistencyLevel)
        {
            return update(b -> b.consistencyLevel = consistencyLevel);
        }

        public Builder isMaterialized(boolean isMaterialized)
        {
            return update(b -> b.isMaterialized = isMaterialized);
        }

        @Override
        public Builder self()
        {
            return this;
        }

        @Override
        public RestoreJob build()
        {
            return new RestoreJob(this);
        }
    }
}
