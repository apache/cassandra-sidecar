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

package org.apache.cassandra.sidecar.common.request.data;

import java.util.Date;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.data.SSTableImportOptions;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_AGENT;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_CONSISTENCY_LEVEL;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_EXPIRE_AT;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_ID;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_IMPORT_OPTIONS;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_SECRETS;

/**
 * Request payload for creating restore jobs.
 */
public class CreateRestoreJobRequestPayload
{
    private final UUID jobId;
    private final String jobAgent;
    private final RestoreJobSecrets secrets;
    private final SSTableImportOptions importOptions;
    private final long expireAtInMillis;
    @Nullable
    private final String consistencyLevel; // optional field

    /**
     * Builder to build a CreateRestoreJobRequest
     *
     * @param secrets          secrets for access objects on storage cloud
     * @param expireAtInMillis time in future that the job expires,
     *                         i.e. fail the restore job if it is not in a final {@link RestoreJobStatus} yet
     * @return builder
     */
    public static Builder builder(RestoreJobSecrets secrets, long expireAtInMillis)
    {
        return new Builder(secrets, expireAtInMillis);
    }

    /**
     * CreateRestoreJobRequest deserializer
     *
     * @param jobId            job id of restore job
     * @param jobAgent         arbitrary text a job can put, which can be used to identity itself during Http request
     * @param secrets          secrets to be used by restore job to download data
     * @param importOptions    the configured options for SSTable import
     * @param expireAtInMillis a timestamp in the future when the job is considered expired
     * @param consistencyLevel consistency level a job should satisfy
     */
    @JsonCreator
    public CreateRestoreJobRequestPayload(@JsonProperty(JOB_ID) UUID jobId,
                                          @JsonProperty(JOB_AGENT) String jobAgent,
                                          @JsonProperty(JOB_SECRETS) RestoreJobSecrets secrets,
                                          @JsonProperty(JOB_IMPORT_OPTIONS) SSTableImportOptions importOptions,
                                          @JsonProperty(JOB_EXPIRE_AT) long expireAtInMillis,
                                          @JsonProperty(JOB_CONSISTENCY_LEVEL) String consistencyLevel)
    {
        Preconditions.checkArgument(jobId == null || jobId.version() == 1,
                                    "Only time based UUIDs allowed for jobId");
        Preconditions.checkArgument(expireAtInMillis != 0 && expireAtInMillis > System.currentTimeMillis(),
                                    "expireAt cannot be absent or a time in past");
        Objects.requireNonNull(secrets, "secrets cannot be null");
        this.jobId = jobId;
        this.jobAgent = jobAgent;
        this.secrets = secrets;
        this.importOptions = importOptions == null
                             ? SSTableImportOptions.defaults()
                             : importOptions;
        this.expireAtInMillis = expireAtInMillis;
        this.consistencyLevel = consistencyLevel;
    }

    private CreateRestoreJobRequestPayload(Builder builder)
    {
        this.jobId = builder.jobId;
        this.jobAgent = builder.jobAgent;
        this.secrets = builder.secrets;
        this.importOptions = builder.importOptions;
        this.expireAtInMillis = builder.expireAtInMillis;
        this.consistencyLevel = builder.consistencyLevel;
    }

    /**
     * @return job id of restore job
     */
    @JsonProperty(JOB_ID)
    public UUID jobId()
    {
        return jobId;
    }

    /**
     * @return arbitrary text a job can put, which can be used to identity itself during Http request
     */
    @JsonProperty(JOB_AGENT)
    public String jobAgent()
    {
        return jobAgent;
    }

    /**
     * @return secrets to be used by restore job to download data
     */
    @JsonProperty(JOB_SECRETS)
    public RestoreJobSecrets secrets()
    {
        return secrets;
    }

    /**
     * @return the options used for importing SSTables
     */
    @JsonProperty(JOB_IMPORT_OPTIONS)
    public SSTableImportOptions importOptions()
    {
        return importOptions;
    }

    /**
     * @return timestamp the job expires, i.e. fail the restore job if it is not in a final {@link RestoreJobStatus} yet
     */
    @JsonProperty(JOB_EXPIRE_AT)
    public long expireAtInMillis()
    {
        return expireAtInMillis;
    }

    /**
     * Convert the expireAtInMillis timestamp as {@link Date}
     *
     * @return date
     */
    public Date expireAtAsDate()
    {
        return new Date(expireAtInMillis);
    }

    /**
     * @return the consistency level a job should satisfy
     */
    @JsonProperty(JOB_CONSISTENCY_LEVEL)
    @Nullable
    public String consistencyLevel()
    {
        return consistencyLevel;
    }

    @Override
    public String toString()
    {
        return "CreateRestoreJobRequest{" +
               JOB_ID + "='" + jobId + "', " +
               JOB_AGENT + "='" + jobAgent + "', " +
               JOB_SECRETS + "='" + secrets + "', " +
               JOB_EXPIRE_AT + "='" + expireAtInMillis + "', " +
               JOB_CONSISTENCY_LEVEL + "='" + consistencyLevel + "', " +
               JOB_IMPORT_OPTIONS + "='" + importOptions + "'}";
    }

    /**
     * Builds the CreateRestoreJobRequest
     */
    public static class Builder
    {
        private final RestoreJobSecrets secrets;
        private final SSTableImportOptions importOptions = SSTableImportOptions.defaults();
        private final long expireAtInMillis;

        private UUID jobId = null;
        private String jobAgent = null;
        private String consistencyLevel = null;

        Builder(RestoreJobSecrets secrets, long expireAtInMillis)
        {
            this.secrets = secrets;
            this.expireAtInMillis = expireAtInMillis;
        }

        public Builder jobId(UUID jobId)
        {
            this.jobId = jobId;
            return this;
        }

        public Builder jobAgent(String jobAgent)
        {
            this.jobAgent = jobAgent;
            return this;
        }

        public Builder updateImportOptions(Consumer<SSTableImportOptions> updater)
        {
            updater.accept(importOptions);
            return this;
        }

        public Builder consistencyLevel(String consistencyLevel)
        {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        public CreateRestoreJobRequestPayload build()
        {
            return new CreateRestoreJobRequestPayload(this);
        }
    }
}
