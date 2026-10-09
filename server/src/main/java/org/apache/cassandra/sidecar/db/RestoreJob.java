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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.data.ConsistencyConfig;
import org.apache.cassandra.sidecar.common.data.ConsistencyLevel;
import org.apache.cassandra.sidecar.common.data.CredentialType;
import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.data.SSTableImportOptions;
import org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.common.utils.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * RestoreJob is the in-memory representation of a restore job
 */
public class RestoreJob
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreJob.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public final LocalDate createdAt;
    public final UUID jobId;
    public final String keyspaceName;
    public final String tableName;
    public final String jobAgent;
    public final RestoreJobStatus status;
    public final RestoreJobSecrets secrets;
    public final CredentialType credentialType;
    public final SSTableImportOptions importOptions;
    public final Date expireAt;
    public final short bucketCount;
    public final @Nullable ConsistencyLevel consistencyLevel;
    public final @Nullable String localDatacenter;
    // whether a restore job should restore to the local Cassandra nodes only; default is false
    public final boolean shouldRestoreToLocalDatacenterOnly;
    // whether the staging and the importing phases of the job are eagerly pipelined, i.e. slices are staged while the
    // job is in CREATED status and staged slices are imported once the job is in STAGED status; default is false
    public final boolean fastForwardEnabled;
    public final Manager restoreJobManager;
    public final Long sliceCount;

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
        ConsistencyConfig consistencyConfig = ConsistencyConfig.parseString(row.getString("consistency_level"),
                                                                            row.getString("local_datacenter"));
        builder.createdAt(row.getDate("created_at"))
               .jobId(row.getUUID("job_id")).jobAgent(row.getString("job_agent"))
               .bucketCount((short) 0) // always use 0 for now; TODO - Add bucketCount field to CreateRestoreJobRequestPayload
               .keyspace(row.getString("keyspace_name")).table(row.getString("table_name"))
               .jobStatusText(row.getString("status"))
               .jobSecrets(decodeJobSecrets(row.getBytes("blob_secrets")))
               .credentialType(decodeCredentialType(row.getString("credential_type")))
               .expireAt(row.getTimestamp("expire_at"))
               .sstableImportOptions(decodeSSTableImportOptions(row.getBytes("import_options")))
               .consistencyLevel(consistencyConfig.consistencyLevel)
               .localDatacenter(consistencyConfig.localDatacenter)
               .shouldRestoreToLocalDatacenterOnly(row.getBool("local_datacenter_only"))
               .fastForwardEnabled(row.getBool("fast_forward_enabled"))
               .sliceCount(row.get("slice_count", Long.class));

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

    private static CredentialType decodeCredentialType(String value)
    {
        try
        {
            return CredentialType.valueOf(value);
        }
        catch (IllegalArgumentException e)
        {
            throw new DataObjectMappingException("Failed to decode credentialType: " + value, e);
        }
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
        Preconditions.checkArgument(builder.consistencyLevel == null
                                    || !builder.consistencyLevel.isLocalDcOnly
                                    || StringUtils.isNotEmpty(builder.localDatacenter),
                                    "When local consistency level is used, localDatacenter must also present");
        boolean hasEffectiveLocalDC = StringUtils.isNotEmpty(builder.localDatacenter);
        // log a warning when consistency level is absent or no local, but localDatacenter is defined
        if ((builder.consistencyLevel == null || !builder.consistencyLevel.isLocalDcOnly) && hasEffectiveLocalDC)
        {
            LOGGER.warn("'localDatacenter' is defined but ignored. consistencyLevel={} localDatacenter={}",
                        builder.consistencyLevel, builder.localDatacenter);
            hasEffectiveLocalDC = false;
        }

        if (builder.shouldRestoreToLocalDatacenterOnly && !hasEffectiveLocalDC)
        {
            this.shouldRestoreToLocalDatacenterOnly = false;
            LOGGER.warn("shouldRestoreToLocalDatacenterOnly is true but 'localDatacenter' is not defined or invalid. " +
                        "Resetting shouldRestoreToLocalDatacenterOnly to false");
        }
        else
        {
            this.shouldRestoreToLocalDatacenterOnly = builder.shouldRestoreToLocalDatacenterOnly;
        }
        this.createdAt = builder.createdAt;
        this.jobId = builder.jobId;
        this.keyspaceName = builder.keyspaceName;
        this.tableName = builder.tableName;
        this.jobAgent = builder.jobAgent;
        this.status = builder.status;
        this.statusText = builder.statusText;
        this.secrets = builder.secrets;
        this.credentialType = builder.credentialType;
        this.importOptions = builder.importOptions == null
                             ? SSTableImportOptions.defaults()
                             : builder.importOptions;
        this.expireAt = builder.expireAt;
        this.bucketCount = builder.bucketCount;
        this.consistencyLevel = builder.consistencyLevel;
        this.localDatacenter = builder.localDatacenter;
        this.restoreJobManager = builder.manager;
        this.sliceCount = builder.sliceCount;
        this.fastForwardEnabled = builder.fastForwardEnabled;
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

    /**
     * Check whether the {@link RestoreJob} has expired at the referenceTimestampMillis
     * @param referenceTimestampMillis the number of milliseconds since January 1, 1970, 00:00:00 GMT
     * @return true if the job expires at the referenceTimestampMillis; false, otherwise
     */
    public boolean hasExpired(long referenceTimestampMillis)
    {
        return expireAt != null && referenceTimestampMillis >= expireAt.getTime();
    }

    /**
     * Determine whether the restore ranges of the job should be staged now.
     *
     * <p>Staging is normally triggered by the {@link RestoreJobStatus#STAGE_READY} signal from the external
     * controller. When fast forward is enabled, staging starts as soon as the job is created; the individual slices
     * are already available on the storage cloud as they are uploaded progressively. {@code STAGE_READY} then becomes
     * a fence, which guarantees that all slices of the job have been uploaded, rather than the trigger of the phase.
     *
     * @return true if the ranges of this job can be staged in the current job status
     */
    public boolean shouldStageNow()
    {
        return status == RestoreJobStatus.STAGE_READY
               || (fastForwardEnabled && status == RestoreJobStatus.CREATED);
    }

    /**
     * Determine whether the staged restore ranges of the job should be imported now.
     *
     * <p>Importing is normally triggered by the {@link RestoreJobStatus#IMPORT_READY} signal from the external
     * controller. When fast forward is enabled, importing starts once the job enters {@link RestoreJobStatus#STAGED},
     * i.e. all clusters have staged their data and satisfied the consistency requirement of the job.
     * {@code IMPORT_READY} then becomes a confirmation rather than the trigger of the phase.
     *
     * @return true if the staged ranges of this job can be imported in the current job status
     */
    public boolean shouldImportNow()
    {
        return status == RestoreJobStatus.IMPORT_READY
               || (fastForwardEnabled && status == RestoreJobStatus.STAGED);
    }

    /**
     * Determine the expected range status based on the job status
     * @return the expected next range status in order to succeed
     */
    public RestoreRangeStatus expectedNextRangeStatus()
    {
        // Ranges of a fast forward job are already staging while the job is in CREATED status, so their progress can
        // be examined. For all other jobs, no range exists yet in CREATED status.
        Preconditions.checkArgument(fastForwardEnabled || status != RestoreJobStatus.CREATED,
                                    "Cannot check progress for restore job in CREATED status. jobId: " + jobId);

        // The job is still staging its ranges when shouldStageNow() holds, i.e. in CREATED status for a fast forward
        // job and in STAGE_READY status for any job.
        // The STAGED status maps to STAGED as well, but only in the normal flow, where importing waits for the
        // IMPORT_READY signal. With fast forward, importing has already started in STAGED status, hence SUCCEEDED.
        return shouldStageNow() || (status == RestoreJobStatus.STAGED && !fastForwardEnabled)
               ? RestoreRangeStatus.STAGED
               : RestoreRangeStatus.SUCCEEDED;
    }

    @Nullable
    public String consistencyLevelText()
    {
        return consistencyLevel == null ? null : consistencyLevel.name();
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return String.format("RestoreJob{" +
                             "createdAt='%s', jobId='%s', keyspaceName='%s', " +
                             "tableName='%s', status='%s', secrets='%s', importOptions='%s', " +
                             "expireAt='%s', bucketCount='%s', consistencyLevel='%s', localDatacenter='%s', " +
                             "shouldRestoreToLocalDatacenterOnly='%s', fastForwardEnabled='%s'}",
                             createdAt.toString(), jobId.toString(),
                             keyspaceName, tableName,
                             statusText, secrets, importOptions,
                             expireAt, bucketCount,
                             consistencyLevel, localDatacenter,
                             shouldRestoreToLocalDatacenterOnly,
                             fastForwardEnabled);
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
        private CredentialType credentialType;
        private SSTableImportOptions importOptions;
        private Date expireAt;
        private short bucketCount;
        private ConsistencyLevel consistencyLevel;
        private String localDatacenter;
        private boolean shouldRestoreToLocalDatacenterOnly = false;
        private boolean fastForwardEnabled = false;
        private Manager manager;
        private Long sliceCount;

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
            this.credentialType = restoreJob.credentialType;
            this.importOptions = restoreJob.importOptions;
            this.expireAt = restoreJob.expireAt;
            this.bucketCount = restoreJob.bucketCount;
            this.consistencyLevel = restoreJob.consistencyLevel;
            this.localDatacenter = restoreJob.localDatacenter;
            this.manager = restoreJob.restoreJobManager;
            this.sliceCount = restoreJob.sliceCount;
            this.fastForwardEnabled = restoreJob.fastForwardEnabled;
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

        public Builder credentialType(CredentialType credentialType)
        {
            return update(b -> b.credentialType = credentialType);
        }

        public Builder sstableImportOptions(SSTableImportOptions options)
        {
            return update(b -> b.importOptions = options);
        }

        public Builder expireAt(Date expireAt)
        {
            return update(b -> b.expireAt = expireAt);
        }

        public Builder sliceCount(Long sliceCount)
        {
            return update(b -> b.sliceCount = sliceCount);
        }

        public Builder bucketCount(short bucketCount)
        {
            return update(b -> b.bucketCount = bucketCount);
        }

        public Builder consistencyLevel(@Nullable ConsistencyLevel consistencyLevel)
        {
            return update(b -> {
                b.consistencyLevel = consistencyLevel;
                b.manager = resolveJobManager();
            });
        }

        public Builder localDatacenter(@Nullable String localDatacenter)
        {
            return update(b -> b.localDatacenter = localDatacenter);
        }

        public Builder shouldRestoreToLocalDatacenterOnly(boolean localDatacenterOnly)
        {
            return update(b -> b.shouldRestoreToLocalDatacenterOnly = localDatacenterOnly);
        }

        public Builder fastForwardEnabled(boolean fastForwardEnabled)
        {
            return update(b -> b.fastForwardEnabled = fastForwardEnabled);
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
