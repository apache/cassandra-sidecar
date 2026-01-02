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
import org.apache.cassandra.sidecar.common.data.CompactionStopStatus;

/**
 * Response class for the Compaction Stop API
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CompactionStopResponse
{
    public static final String COMPACTION_TYPE_KEY = "compactionType";
    public static final String COMPACTION_ID_KEY = "compactionId";
    public static final String STATUS_KEY = "status";
    private final String compactionType;
    private final String compactionId;
    private final CompactionStopStatus status;

    private CompactionStopResponse(Builder builder)
    {
        this.compactionType = builder.compactionType;
        this.compactionId = builder.compactionId;
        this.status = builder.status;
    }

    /**
     * Constructs a new {@link CompactionStopResponse}.
     *
     * @param compactionType the type of compaction that was requested to stop
     * @param compactionId   the ID of the compaction that was requested to stop
     * @param status         the status of the stop operation (e.g., "PENDING", "FAILED")
     */
    @JsonCreator
    public CompactionStopResponse(@JsonProperty(COMPACTION_TYPE_KEY) String compactionType,
                                  @JsonProperty(COMPACTION_ID_KEY) String compactionId,
                                  @JsonProperty(STATUS_KEY) CompactionStopStatus status)
    {
        this.compactionType = compactionType;
        this.compactionId = compactionId;
        this.status = status;
    }

    /**
     * @return the type of compaction that was requested to stop
     */
    @JsonProperty(COMPACTION_TYPE_KEY)
    public String compactionType()
    {
        return compactionType;
    }

    /**
     * @return the ID of the compaction that was requested to stop
     */
    @JsonProperty(COMPACTION_ID_KEY)
    public String compactionId()
    {
        return compactionId;
    }

    /**
     * @return the status of the stop operation
     */
    @JsonProperty(STATUS_KEY)
    public CompactionStopStatus status()
    {
        return status;
    }

    @Override
    public String toString()
    {
        return String.format(
                "CompactionStopResponse{compactionType='%s', compactionId='%s', status='%s'}",
                compactionType, compactionId, status
        );
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
        private CompactionStopStatus status;

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
        public Builder status(CompactionStopStatus status)
        {
            return update(b -> b.status = status);
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
