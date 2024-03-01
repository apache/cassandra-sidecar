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

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.data.ConsistencyLevel;
import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.data.SSTableImportOptions;
import org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
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
    public final ConsistencyLevel consistencyLevel;
    public final Manager restoreJobManager;

    private final String statusText;

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
               .jobStatusText(row.getString("status"))
               .jobSecrets(decodeJobSecrets(row.getBytes("blob_secrets")))
               .expireAt(row.getTimestamp("expire_at"))
               .sstableImportOptions(decodeSSTableImportOptions(row.getBytes("import_options")))
               .consistencyLevel(row.getString("consistency_level"));

        // todo: Yifan, add them back when the cql statement is updated to reflect the new columns.
        //  Add new fields to CreateRestoreJobRequestPayload too
//               .bucketCount(row.getShort("bucket_count"))
        return builder.build();
    }

    private static RestoreJobStatus decodeJobStatus(String status)
    {
        if (status == null)
        {
            return null;
        }

        String enumLiteral = status.split(":")[0];
        return RestoreJobStatus.valueOf(enumLiteral.toUpperCase());
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
        this.statusText = builder.statusText;
        this.secrets = builder.secrets;
        this.importOptions = builder.importOptions == null
                             ? SSTableImportOptions.defaults()
                             : builder.importOptions;
        this.expireAt = builder.expireAt;
        this.bucketCount = builder.bucketCount;
        this.consistencyLevel = builder.consistencyLevel;
        this.restoreJobManager = builder.manager;
    }

    public Builder unbuild()
    {
        return new Builder(this);
    }

    public boolean isManagedBySidecar()
    {
        return restoreJobManager == Manager.SIDECAR;
    }

    public String statusWithOptionalDescription()
    {
        return statusText;
    }

    public RestoreRangeStatus expectedNextRangeStatus()
    {
        Preconditions.checkState(status.isReady(), "The restore job is not in a ready status. jobId: " + jobId + " status: " + status);

        return status == RestoreJobStatus.STAGE_READY
               ? RestoreRangeStatus.STAGED
               : RestoreRangeStatus.SUCCEEDED;
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
                             statusText, secrets, importOptions,
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
        private String statusText;
        private RestoreJobSecrets secrets;
        private SSTableImportOptions importOptions;
        private Date expireAt;
        private short bucketCount;
        private ConsistencyLevel consistencyLevel;
        private Manager manager;

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
            this.statusText = restoreJob.statusText;
            this.secrets = restoreJob.secrets;
            this.importOptions = restoreJob.importOptions;
            this.expireAt = restoreJob.expireAt;
            this.bucketCount = restoreJob.bucketCount;
            this.consistencyLevel = restoreJob.consistencyLevel;
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

        public Builder jobStatus(@NotNull RestoreJobStatus jobStatus)
        {
            return update(b -> {
                b.status = jobStatus;
                b.statusText = jobStatus.name();
            });
        }

        /**
         * Assign the job status; primarily used when loading the restore job from database
         * Note that the status text might contain additional description than the status enum
         * @param statusText status text read from database
         */
        public Builder jobStatusText(String statusText)
        {
            return update(b -> {
                b.status = decodeJobStatus(statusText);
                b.statusText = statusText;
            });
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
            return update(b -> {
                b.consistencyLevel = ConsistencyLevel.fromString(consistencyLevel);
                b.manager = resolveJobManager();
            });
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

        /**
         * Resolve the manager of the restore job based on the existence of consistencyLevel
         * @return the resolved Manager
         */
        private Manager resolveJobManager()
        {
            // If spark is the manager, the restore job is created w/o specifying consistency level
            // If the manager of the restore job is sidecar, consistency level must present
            return consistencyLevel == null ? Manager.SPARK : Manager.SIDECAR;
        }
    }

    /**
     * The manager of the restore job. The variant could change the code path a restore job runs.
     * It is a feature switch essentially.
     */
    public enum Manager
    {
        /**
         * The restore job is managed by Spark. Sidecar instances are just simple workers. They rely on client/Spark
         * for decision-making.
         */
        SPARK,

        /**
         * The restore job is managed by Sidecar. Sidecar instances should assign slices to sidecar instances
         * and check whether the job has met the consistency level to complete the job.
         */
        SIDECAR,
    }
}
