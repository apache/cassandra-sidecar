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

package org.apache.cassandra.sidecar.job.storage;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import java.util.Objects;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.job.OperationalJob;
import org.apache.cassandra.sidecar.job.OperationalJobInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A data transfer object representing the persisted state of an operational job.
 * Implements {@link OperationalJobInfo} so that completed jobs stored in a persistent storage
 * can be returned directly through the tracker without requiring a live {@link OperationalJob}.
 */
public class OperationalJobRecord implements OperationalJobInfo
{
    private final UUID jobId;
    private final OperationType operationType;
    private final OperationalJobStatus status;
    private final long creationTimeMillis;
    @Nullable
    private final Instant startTime;
    @Nullable
    private final Instant lastUpdate;
    @Nullable
    private final String failureReason;
    @Nullable
    private final List<List<UUID>> nodeExecutionOrder;
    @Nullable
    private final Map<String, String> operationMetadata;
    @NotNull
    private final List<UUID> nodesPending;
    @NotNull
    private final List<UUID> nodesExecuting;
    @NotNull
    private final List<UUID> nodesSucceeded;
    @NotNull
    private final List<UUID> nodesFailed;

    /**
     * Constructs an OperationalJobRecord from its {@link Builder}.
     *
     * @param builder the builder holding the field values
     */
    private OperationalJobRecord(Builder builder)
    {
        Preconditions.checkArgument(builder.jobId != null, "jobId must not be null");
        Preconditions.checkArgument(builder.jobId.version() == 1, "jobId must be a time-based (v1) UUID");
        Preconditions.checkArgument(builder.operationType != null, "operationType must not be null");
        Preconditions.checkArgument(builder.status != null, "status must not be null");
        jobId = builder.jobId;
        operationType = builder.operationType;
        status = builder.status;
        creationTimeMillis = UUIDs.unixTimestamp(builder.jobId);
        startTime = builder.startTime;
        lastUpdate = builder.lastUpdate;
        failureReason = builder.failureReason;
        nodeExecutionOrder = builder.nodeExecutionOrder;
        operationMetadata = builder.operationMetadata;
        nodesPending = builder.nodesPending;
        nodesExecuting = builder.nodesExecuting;
        nodesSucceeded = builder.nodesSucceeded;
        nodesFailed = builder.nodesFailed;
    }

    /**
     * @return the time-based v1 UUID identifying this job
     */
    public UUID jobId()
    {
        return jobId;
    }

    /**
     * @return the operation type
     */
    public OperationType operationType()
    {
        return operationType;
    }

    /**
     * @return the current status of the job
     */
    public OperationalJobStatus status()
    {
        return status;
    }

    /**
     * @return the unix timestamp in milliseconds when the job was created, extracted from the time-based UUID
     */
    public long creationTimeMillis()
    {
        return creationTimeMillis;
    }

    /**
     * @return the timestamp when execution started, or null if not yet started
     */
    @Nullable
    public Instant startTime()
    {
        return startTime;
    }

    /**
     * @return the timestamp of the last status update, or null for pre-existing rows
     */
    @Nullable
    public Instant lastUpdate()
    {
        return lastUpdate;
    }

    /**
     * @return the failure reason if the job failed, or null otherwise
     */
    @Nullable
    public String failureReason()
    {
        return failureReason;
    }

    /**
     * @return the ordered list of parallel node groups for execution, or null if not set
     */
    @Nullable
    public List<List<UUID>> nodeExecutionOrder()
    {
        return nodeExecutionOrder;
    }

    /**
     * @return the operation parameters, or null if not set
     */
    @Nullable
    public Map<String, String> operationMetadata()
    {
        return operationMetadata;
    }

    @Override
    @Nullable
    public UUID nodeId()
    {
        return null;
    }

    @Override
    public String name()
    {
        return operationType.name().toLowerCase();
    }

    @Override
    public long creationTime()
    {
        return creationTimeMillis;
    }

    @Override
    @NotNull
    public List<UUID> nodesPending()
    {
        return nodesPending;
    }

    @Override
    @NotNull
    public List<UUID> nodesExecuting()
    {
        return nodesExecuting;
    }

    @Override
    @NotNull
    public List<UUID> nodesSucceeded()
    {
        return nodesSucceeded;
    }

    @Override
    @NotNull
    public List<UUID> nodesFailed()
    {
        return nodesFailed;
    }

    @Override
    public boolean isExecuting()
    {
        return status == OperationalJobStatus.RUNNING;
    }

    /**
     * Creates an {@link OperationalJobRecord} from a live {@link OperationalJob}.
     *
     * @param job the operational job to convert
     * @return a new record capturing the job's current state
     */
    public static OperationalJobRecord fromOperationalJob(OperationalJob job)
    {
        return builder().jobId(job.jobId())
                        .operationType(job.operationType())
                        .status(job.status())
                        .lastUpdate(Instant.now())
                        .build();
    }

    @Override
    public String toString()
    {
        return "OperationalJobRecord{" +
               "jobId=" + jobId +
               ", operationType=" + operationType +
               ", status=" + status +
               ", creationTimeMillis=" + creationTimeMillis +
               ", startTime=" + startTime +
               ", lastUpdate=" + lastUpdate +
               ", failureReason=" + failureReason +
               ", nodeExecutionOrder=" + nodeExecutionOrder +
               ", operationMetadata=" + operationMetadata +
               ", nodesPending=" + nodesPending +
               ", nodesExecuting=" + nodesExecuting +
               ", nodesSucceeded=" + nodesSucceeded +
               ", nodesFailed=" + nodesFailed +
               '}';
    }

    /**
     * @return a new {@link Builder} for constructing an {@link OperationalJobRecord}
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@link DataObjectBuilder} for {@link OperationalJobRecord}.
     */
    public static final class Builder implements DataObjectBuilder<Builder, OperationalJobRecord>
    {
        private UUID jobId;
        private OperationType operationType;
        private OperationalJobStatus status;
        private @Nullable Instant startTime;
        private @Nullable Instant lastUpdate;
        private @Nullable String failureReason;
        private @Nullable List<List<UUID>> nodeExecutionOrder;
        private @Nullable Map<String, String> operationMetadata;
        private @NotNull List<UUID> nodesPending = Collections.emptyList();
        private @NotNull List<UUID> nodesExecuting = Collections.emptyList();
        private @NotNull List<UUID> nodesSucceeded = Collections.emptyList();
        private @NotNull List<UUID> nodesFailed = Collections.emptyList();

        private Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        public Builder jobId(UUID jobId)
        {
            return update(b -> b.jobId = jobId);
        }

        public Builder operationType(OperationType operationType)
        {
            return update(b -> b.operationType = operationType);
        }

        public Builder status(OperationalJobStatus status)
        {
            return update(b -> b.status = status);
        }

        public Builder startTime(@Nullable Instant startTime)
        {
            return update(b -> b.startTime = startTime);
        }

        public Builder lastUpdate(@Nullable Instant lastUpdate)
        {
            return update(b -> b.lastUpdate = lastUpdate);
        }

        public Builder failureReason(@Nullable String failureReason)
        {
            return update(b -> b.failureReason = failureReason);
        }

        public Builder nodeExecutionOrder(@Nullable List<List<UUID>> nodeExecutionOrder)
        {
            return update(b -> b.nodeExecutionOrder = nodeExecutionOrder);
        }

        public Builder operationMetadata(@Nullable Map<String, String> operationMetadata)
        {
            return update(b -> b.operationMetadata = operationMetadata);
        }

        public Builder nodesPending(@NotNull List<UUID> nodesPending)
        {
            return update(b -> b.nodesPending = Objects.requireNonNull(nodesPending, "nodesPending cannot be null"));
        }

        public Builder nodesExecuting(@NotNull List<UUID> nodesExecuting)
        {
            return update(b -> b.nodesExecuting = Objects.requireNonNull(nodesExecuting, "nodesExecuting cannot be null"));
        }

        public Builder nodesSucceeded(@NotNull List<UUID> nodesSucceeded)
        {
            return update(b -> b.nodesSucceeded = Objects.requireNonNull(nodesSucceeded, "nodesSucceeded cannot be null"));
        }

        public Builder nodesFailed(@NotNull List<UUID> nodesFailed)
        {
            return update(b -> b.nodesFailed = Objects.requireNonNull(nodesFailed, "nodesFailed cannot be null"));
        }

        public OperationalJobRecord build()
        {
            return new OperationalJobRecord(this);
        }
    }
}
