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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_AGENT;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_EXPIRE_AT;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_SECRETS;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_SLICE_COUNT;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_STATUS;

/**
 * Request payload for updating existing restore job
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UpdateRestoreJobRequestPayload
{
    private final String jobAgent;
    private final RestoreJobSecrets secrets;
    private final RestoreJobStatus status;
    private final Long expireAtInMillis; // timestamp; use Long because the field can be absent
    private final Long sliceCount; // number of slices (s3 objects) produced by the job; use Long as the field can be absent

    public static Builder builder()
    {
        return new Builder();
    }

    // NOTE: if adding a new field, update isEmpty() and toString()
    @JsonCreator
    public UpdateRestoreJobRequestPayload(@JsonProperty(JOB_AGENT) String jobAgent,
                                          @JsonProperty(JOB_SECRETS) RestoreJobSecrets secrets,
                                          @JsonProperty(JOB_STATUS) RestoreJobStatus status,
                                          @JsonProperty(JOB_EXPIRE_AT) Long expireAtInMillis,
                                          @JsonProperty(JOB_SLICE_COUNT) Long sliceCount)
    {
        this.jobAgent = jobAgent;
        this.secrets = secrets;
        this.status = status;
        this.expireAtInMillis = expireAtInMillis;
        this.sliceCount = sliceCount;
    }

    private UpdateRestoreJobRequestPayload(Builder builder)
    {
        this.jobAgent = builder.jobAgent;
        this.secrets = builder.secrets;
        this.status = builder.status;
        this.expireAtInMillis = builder.expireAtInMillis;
        this.sliceCount = builder.sliceCount;
    }

    /**
     * @return arbitrary text a job can put, which can be used to identity itself during Http request
     */
    @Nullable @JsonProperty(JOB_AGENT)
    public String jobAgent()
    {
        return jobAgent;
    }

    /**
     * @return secrets to be used by restore job to download data
     */
    @Nullable @JsonProperty(JOB_SECRETS)
    public RestoreJobSecrets secrets()
    {
        return secrets;
    }

    /**
     * @return status of the restore job
     */
    @Nullable @JsonProperty(JOB_STATUS)
    public RestoreJobStatus status()
    {
        return status;
    }

    /**
     * @return timestamp the job expires, i.e. fail the restore job if it is not in a final {@link RestoreJobStatus} yet
     */
    @Nullable @JsonProperty(JOB_EXPIRE_AT)
    public Long expireAtInMillis()
    {
        return expireAtInMillis;
    }

    /**
     * @return number of total slices (s3 objects) produced. The field is only present when the restore job is managed by Sidecar
     */
    @Nullable @JsonProperty(JOB_SLICE_COUNT)
    public Long sliceCount()
    {
        return sliceCount;
    }

    /**
     * Convert the expireAtInMillis timestamp as {@link Date}
     * @return date or null
     */
    public Date expireAtAsDate()
    {
        return expireAtInMillis == null
               ? null
               : new Date(expireAtInMillis);
    }

    @JsonIgnore
    public boolean isEmpty()
    {
        return jobAgent == null && secrets == null && status == null && expireAtInMillis == null && sliceCount == null;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "UpdateRestoreJobRequest{" +
               JOB_AGENT + "='" + jobAgent + "', " +
               JOB_STATUS + "='" + status + "', " +
               JOB_SECRETS + "='" + secrets + "', " +
               JOB_EXPIRE_AT + "='" + expireAtInMillis + "', " +
               JOB_SLICE_COUNT + "='" + sliceCount + "'}";
    }

    /**
     * Builder for {@link UpdateRestoreJobRequestPayload}
     */
    public static class Builder implements DataObjectBuilder<Builder, UpdateRestoreJobRequestPayload>
    {
        private String jobAgent;
        private RestoreJobSecrets secrets;
        private RestoreJobStatus status;
        private Long expireAtInMillis; // timestamp; use Long because the field can be absent
        private Long sliceCount; // number of slices (s3 objects) produced by the job; use Long as the field can be absent

        public Builder withJobAgent(String jobAgent)
        {
            return update(b -> b.jobAgent = jobAgent);
        }

        public Builder withSecrets(RestoreJobSecrets secrets)
        {
            return update(b -> b.secrets = secrets);
        }

        public Builder withStatus(RestoreJobStatus status)
        {
            return update(b -> b.status = status);
        }

        public Builder withExpireAtInMillis(Long expireAtInMillis)
        {
            return update(b -> b.expireAtInMillis = expireAtInMillis);
        }

        public Builder withSliceCount(Long sliceCount)
        {
            return update(b -> b.sliceCount = sliceCount);
        }

        @Override
        public Builder self()
        {
            return this;
        }

        @Override
        public UpdateRestoreJobRequestPayload build()
        {
            return new UpdateRestoreJobRequestPayload(this);
        }
    }
}
