/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.common.response.data;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.request.RestoreJobProgressRequest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_STATUS_ABORTED_RANGES;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_STATUS_FAILED_RANGES;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_STATUS_MESSAGE;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_STATUS_PENDING_RANGES;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_STATUS_SUCCEEDED_RANGES;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_STATUS_SUMMARY;

/**
 * A class representing a response for the {@link RestoreJobProgressRequest}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RestoreJobProgressResponsePayload
{
    private final String message;
    private final RestoreJobSummaryResponsePayload summary;
    // the ranges can be null/absent, if they are not required to be included in the response payload, depending on the fetch policy
    @Nullable // failed due to unrecoverable exceptions
    private final List<RestoreJobRange> failedRanges;
    @Nullable // aborted due to the job has failed/aborted
    private final List<RestoreJobRange> abortedRanges;
    @Nullable
    private final List<RestoreJobRange> pendingRanges;
    @Nullable
    private final List<RestoreJobRange> succeededRanges;

    /**
     * @return builder to build the {@link RestoreJobProgressResponsePayload}
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Constructor for json deserialization
     */
    @JsonCreator
    public RestoreJobProgressResponsePayload(@NotNull @JsonProperty(JOB_STATUS_MESSAGE) String message,
                                             @NotNull @JsonProperty(JOB_STATUS_SUMMARY) RestoreJobSummaryResponsePayload summary,
                                             @Nullable @JsonProperty(JOB_STATUS_FAILED_RANGES) List<RestoreJobRange> failedRanges,
                                             @Nullable @JsonProperty(JOB_STATUS_ABORTED_RANGES) List<RestoreJobRange> abortedRanges,
                                             @Nullable @JsonProperty(JOB_STATUS_PENDING_RANGES) List<RestoreJobRange> pendingRanges,
                                             @Nullable @JsonProperty(JOB_STATUS_SUCCEEDED_RANGES) List<RestoreJobRange> succeededRanges)
    {
        this.message = message;
        this.summary = summary;
        this.failedRanges = failedRanges;
        this.abortedRanges = abortedRanges;
        this.pendingRanges = pendingRanges;
        this.succeededRanges = succeededRanges;
    }

    private RestoreJobProgressResponsePayload(Builder builder)
    {
        this(builder.message,
             builder.summary,
             builder.failedRanges,
             builder.abortedRanges,
             builder.pendingRanges,
             builder.succeededRanges);
    }

    @NotNull
    @JsonProperty(JOB_STATUS_MESSAGE)
    public String message()
    {
        return this.message;
    }

    @NotNull
    @JsonProperty(JOB_STATUS_SUMMARY)
    public RestoreJobSummaryResponsePayload summary()
    {
        return this.summary;
    }

    @Nullable
    @JsonProperty(JOB_STATUS_FAILED_RANGES)
    public List<RestoreJobRange> failedRanges()
    {
        return this.failedRanges;
    }

    @Nullable
    @JsonProperty(JOB_STATUS_ABORTED_RANGES)
    public List<RestoreJobRange> abortedRanges()
    {
        return this.abortedRanges;
    }

    @Nullable
    @JsonProperty(JOB_STATUS_PENDING_RANGES)
    public List<RestoreJobRange> pendingRanges()
    {
        return this.pendingRanges;
    }

    @Nullable
    @JsonProperty(JOB_STATUS_SUCCEEDED_RANGES)
    public List<RestoreJobRange> succeededRanges()
    {
        return this.succeededRanges;
    }

    /**
     * Builds {@link RestoreJobProgressResponsePayload}
     */
    public static class Builder implements DataObjectBuilder<Builder, RestoreJobProgressResponsePayload>
    {
        String message;
        RestoreJobSummaryResponsePayload summary;
        List<RestoreJobRange> failedRanges = null;
        List<RestoreJobRange> abortedRanges = null;
        List<RestoreJobRange> pendingRanges = null;
        List<RestoreJobRange> succeededRanges = null;

        public Builder withMessage(String message)
        {
            return update(b -> b.message = message);
        }

        public Builder withJobSummary(String createdAt,
                                      UUID jobId,
                                      String jobAgent,
                                      String keyspace,
                                      String table,
                                      String status)
        {
            return update(b -> b.summary = new RestoreJobSummaryResponsePayload(createdAt, jobId, jobAgent, keyspace, table, null, status));
        }

        public Builder withFailedRanges(List<RestoreJobRange> failedRanges)
        {
            return update(b -> b.failedRanges = copyNullableList(failedRanges));
        }

        public Builder withAbortedRanges(List<RestoreJobRange> abortedRanges)
        {
            return update(b -> b.abortedRanges = copyNullableList(abortedRanges));
        }

        public Builder withPendingRanges(List<RestoreJobRange> pendingRanges)
        {
            return update(b -> b.pendingRanges = copyNullableList(pendingRanges));
        }

        public Builder withSucceededRanges(List<RestoreJobRange> succeededRanges)
        {
            return update(b -> b.succeededRanges = copyNullableList(succeededRanges));
        }

        private static List<RestoreJobRange> copyNullableList(List<RestoreJobRange> list)
        {
            return list == null ? null : new ArrayList<>(list);
        }

        @Override
        public Builder self()
        {
            return this;
        }

        @Override
        public RestoreJobProgressResponsePayload build()
        {
            return new RestoreJobProgressResponsePayload(self());
        }
    }
}
