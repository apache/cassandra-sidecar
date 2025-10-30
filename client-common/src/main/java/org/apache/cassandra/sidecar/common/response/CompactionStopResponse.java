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

package org.apache.cassandra.sidecar.common.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;

/**
 * Response class for the Compaction Stop API
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CompactionStopResponse
{
    public static final String COMPACTION_TYPE = "compaction_type";
    public static final String COMPACTION_ID = "compaction_id";
    public static final String STATUS = "status";
    public static final String ERROR_CODE = "error_code";
    public static final String REASON = "reason";

    private final String compactionType;
    private final String compactionId;
    private final String status;
    private final String errorCode;
    private final String reason;

    private CompactionStopResponse(Builder builder)
    {
        this.compactionType = builder.compactionType;
        this.compactionId = builder.compactionId;
        this.status = builder.status;
        this.errorCode = builder.errorCode;
        this.reason = builder.reason;
    }

    /**
     * Constructs a new {@link CompactionStopResponse}.
     *
     * @param compactionType the type of compaction that was requested to stop
     * @param compactionId   the ID of the compaction that was requested to stop
     * @param status         the status of the stop operation (e.g., "PENDING", "FAILED")
     * @param errorCode      the error code (e.g., "200 OK", "400 BAD REQUEST", "404 NOT FOUND", "503 SERVICE UNAVAILABLE")
     * @param reason         the reason for the status (e.g., "Operation Succeeded", "Malformed Request", "Server-side error")
     */
    @JsonCreator
    public CompactionStopResponse(@JsonProperty(COMPACTION_TYPE) String compactionType,
                                  @JsonProperty(COMPACTION_ID) String compactionId,
                                  @JsonProperty(STATUS) String status,
                                  @JsonProperty(ERROR_CODE) String errorCode,
                                  @JsonProperty(REASON) String reason)
    {
        this.compactionType = compactionType;
        this.compactionId = compactionId;
        this.status = status;
        this.errorCode = errorCode;
        this.reason = reason;
    }

    /**
     * @return the type of compaction that was requested to stop
     */
    @JsonProperty(COMPACTION_TYPE)
    public String compactionType()
    {
        return compactionType;
    }

    /**
     * @return the ID of the compaction that was requested to stop
     */
    @JsonProperty(COMPACTION_ID)
    public String compactionId()
    {
        return compactionId;
    }

    /**
     * @return the status of the stop operation
     */
    @JsonProperty(STATUS)
    public String status()
    {
        return status;
    }

    /**
     * @return the error code
     */
    @JsonProperty(ERROR_CODE)
    public String errorCode()
    {
        return errorCode;
    }

    /**
     * @return the reason for the status
     */
    @JsonProperty(REASON)
    public String reason()
    {
        return reason;
    }

    @Override
    public String toString()
    {
        return String.format("CompactionStopResponse{" +
                             "compactionType='%s', " +
                             "compactionId='%s', " +
                             "status='%s', " +
                             "errorCode='%s', " +
                             "reason='%s'}",
                             compactionType, compactionId, status, errorCode, reason);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code CompactionStopResponse} builder static inner class.
     */
    public static final class Builder implements DataObjectBuilder<Builder, CompactionStopResponse>
    {
        private String compactionType;
        private String compactionId;
        private String status;
        private String errorCode;
        private String reason;

        private Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code compactionType} and returns a reference to this Builder enabling method chaining.
         *
         * @param compactionType the {@code compactionType} to set
         * @return a reference to this Builder
         */
        public Builder compactionType(String compactionType)
        {
            return update(b -> b.compactionType = compactionType);
        }

        /**
         * Sets the {@code compactionId} and returns a reference to this Builder enabling method chaining.
         *
         * @param compactionId the {@code compactionId} to set
         * @return a reference to this Builder
         */
        public Builder compactionId(String compactionId)
        {
            return update(b -> b.compactionId = compactionId);
        }

        /**
         * Sets the {@code status} and returns a reference to this Builder enabling method chaining.
         *
         * @param status the {@code status} to set
         * @return a reference to this Builder
         */
        public Builder status(String status)
        {
            return update(b -> b.status = status);
        }

        /**
         * Sets the {@code errorCode} and returns a reference to this Builder enabling method chaining.
         *
         * @param errorCode the {@code errorCode} to set
         * @return a reference to this Builder
         */
        public Builder errorCode(String errorCode)
        {
            return update(b -> b.errorCode = errorCode);
        }

        /**
         * Sets the {@code reason} and returns a reference to this Builder enabling method chaining.
         *
         * @param reason the {@code reason} to set
         * @return a reference to this Builder
         */
        public Builder reason(String reason)
        {
            return update(b -> b.reason = reason);
        }

        /**
         * Returns a {@code CompactionStopResponse} built from the parameters previously set.
         *
         * @return a {@code CompactionStopResponse} built with parameters of this {@code CompactionStopResponse.Builder}
         */
        @Override
        public CompactionStopResponse build()
        {
            return new CompactionStopResponse(this);
        }
    }
}
