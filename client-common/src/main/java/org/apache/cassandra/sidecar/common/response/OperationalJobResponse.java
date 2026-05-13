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

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.utils.InstantIso8601Deserializer;
import org.apache.cassandra.sidecar.common.utils.InstantIso8601Serializer;

/**
 * Response structure of the operational jobs API
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OperationalJobResponse
{
    private final UUID jobId;
    private final OperationalJobStatus status;
    private final String operation;
    private final String reason;
    @JsonSerialize(using = InstantIso8601Serializer.class)
    @JsonDeserialize(using = InstantIso8601Deserializer.class)
    private final Instant startTime;
    private final List<UUID> nodesPending;
    private final List<UUID> nodesExecuting;
    private final List<UUID> nodesSucceeded;
    private final List<UUID> nodesFailed;
    @JsonSerialize(using = InstantIso8601Serializer.class)
    @JsonDeserialize(using = InstantIso8601Deserializer.class)
    private final Instant lastUpdate;

    @JsonCreator
    public OperationalJobResponse(@JsonProperty("jobId") UUID jobId,
                                  @JsonProperty("jobStatus") OperationalJobStatus status,
                                  @JsonProperty("operation") String operation,
                                  @JsonProperty("reason") String reason,
                                  @JsonProperty("startTime") Instant startTime,
                                  @JsonProperty("nodesPending") List<UUID> nodesPending,
                                  @JsonProperty("nodesExecuting") List<UUID> nodesExecuting,
                                  @JsonProperty("nodesSucceeded") List<UUID> nodesSucceeded,
                                  @JsonProperty("nodesFailed") List<UUID> nodesFailed,
                                  @JsonProperty("lastUpdate") Instant lastUpdate)
    {
        this.jobId = jobId;
        this.status = status;
        this.operation = operation;
        this.reason = reason;
        this.startTime = startTime;
        this.nodesPending = nodesPending;
        this.nodesExecuting = nodesExecuting;
        this.nodesSucceeded = nodesSucceeded;
        this.nodesFailed = nodesFailed;
        this.lastUpdate = lastUpdate;
    }

    private OperationalJobResponse(Builder builder)
    {
        this.jobId = builder.jobId;
        this.status = builder.status;
        this.operation = builder.operation;
        this.reason = builder.reason;
        this.startTime = builder.startTime;
        this.nodesPending = builder.nodesPending;
        this.nodesExecuting = builder.nodesExecuting;
        this.nodesSucceeded = builder.nodesSucceeded;
        this.nodesFailed = builder.nodesFailed;
        this.lastUpdate = builder.lastUpdate;
    }

    /**
     * @return a new {@link Builder} instance
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * @return job id of operational job
     */
    @JsonProperty("jobId")
    public UUID jobId()
    {
        return jobId;
    }

    /**
     * @return status of the job
     */
    @JsonProperty("jobStatus")
    public OperationalJobStatus status()
    {
        return status;
    }

    /**
     * @return operation of the job
     */
    @JsonProperty("operation")
    public String operation()
    {
        return operation;
    }

    /**
     * @return reason for job failure
     */
    @JsonProperty("reason")
    public String reason()
    {
        return reason;
    }

    /**
     * @return the time this job started execution
     */
    @JsonProperty("startTime")
    public Instant startTime()
    {
        return startTime;
    }

    /**
     * @return the list of nodes pending execution
     */
    @JsonProperty("nodesPending")
    public List<UUID> nodesPending()
    {
        return nodesPending;
    }

    /**
     * @return the list of nodes currently executing
     */
    @JsonProperty("nodesExecuting")
    public List<UUID> nodesExecuting()
    {
        return nodesExecuting;
    }

    /**
     * @return the list of nodes that have succeeded
     */
    @JsonProperty("nodesSucceeded")
    public List<UUID> nodesSucceeded()
    {
        return nodesSucceeded;
    }

    /**
     * @return the list of nodes that have failed
     */
    @JsonProperty("nodesFailed")
    public List<UUID> nodesFailed()
    {
        return nodesFailed;
    }

    /**
     * @return the time of the last status update
     */
    @JsonProperty("lastUpdate")
    public Instant lastUpdate()
    {
        return lastUpdate;
    }

    /**
     * {@code OperationalJobResponse} builder static inner class.
     */
    public static final class Builder implements DataObjectBuilder<Builder, OperationalJobResponse>
    {
        private UUID jobId;
        private OperationalJobStatus status;
        private String operation;
        private String reason;
        private Instant startTime;
        private List<UUID> nodesPending;
        private List<UUID> nodesExecuting;
        private List<UUID> nodesSucceeded;
        private List<UUID> nodesFailed;
        private Instant lastUpdate;

        private Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code jobId} and returns a reference to this Builder enabling method chaining.
         *
         * @param jobId the {@code jobId} to set
         * @return a reference to this Builder
         */
        public Builder jobId(UUID jobId)
        {
            return update(b -> b.jobId = jobId);
        }

        /**
         * Sets the {@code status} and returns a reference to this Builder enabling method chaining.
         *
         * @param status the {@code status} to set
         * @return a reference to this Builder
         */
        public Builder status(OperationalJobStatus status)
        {
            return update(b -> b.status = status);
        }

        /**
         * Sets the {@code operation} and returns a reference to this Builder enabling method chaining.
         *
         * @param operation the {@code operation} to set
         * @return a reference to this Builder
         */
        public Builder operation(String operation)
        {
            return update(b -> b.operation = operation);
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
         * Sets the {@code startTime} and returns a reference to this Builder enabling method chaining.
         *
         * @param startTime the {@code startTime} to set
         * @return a reference to this Builder
         */
        public Builder startTime(Instant startTime)
        {
            return update(b -> b.startTime = startTime);
        }

        /**
         * Sets the {@code nodesPending} and returns a reference to this Builder enabling method chaining.
         *
         * @param nodesPending the {@code nodesPending} to set
         * @return a reference to this Builder
         */
        public Builder nodesPending(List<UUID> nodesPending)
        {
            return update(b -> b.nodesPending = nodesPending);
        }

        /**
         * Sets the {@code nodesExecuting} and returns a reference to this Builder enabling method chaining.
         *
         * @param nodesExecuting the {@code nodesExecuting} to set
         * @return a reference to this Builder
         */
        public Builder nodesExecuting(List<UUID> nodesExecuting)
        {
            return update(b -> b.nodesExecuting = nodesExecuting);
        }

        /**
         * Sets the {@code nodesSucceeded} and returns a reference to this Builder enabling method chaining.
         *
         * @param nodesSucceeded the {@code nodesSucceeded} to set
         * @return a reference to this Builder
         */
        public Builder nodesSucceeded(List<UUID> nodesSucceeded)
        {
            return update(b -> b.nodesSucceeded = nodesSucceeded);
        }

        /**
         * Sets the {@code nodesFailed} and returns a reference to this Builder enabling method chaining.
         *
         * @param nodesFailed the {@code nodesFailed} to set
         * @return a reference to this Builder
         */
        public Builder nodesFailed(List<UUID> nodesFailed)
        {
            return update(b -> b.nodesFailed = nodesFailed);
        }

        /**
         * Sets the {@code lastUpdate} and returns a reference to this Builder enabling method chaining.
         *
         * @param lastUpdate the {@code lastUpdate} to set
         * @return a reference to this Builder
         */
        public Builder lastUpdate(Instant lastUpdate)
        {
            return update(b -> b.lastUpdate = lastUpdate);
        }

        /**
         * Returns a {@code OperationalJobResponse} built from the parameters previously set.
         *
         * @return a {@code OperationalJobResponse} built with parameters of this {@code Builder}
         */
        @Override
        public OperationalJobResponse build()
        {
            return new OperationalJobResponse(this);
        }
    }
}
