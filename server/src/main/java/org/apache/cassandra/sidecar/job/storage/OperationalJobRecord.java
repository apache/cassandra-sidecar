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
import java.util.List;
import java.util.Map;

import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.jetbrains.annotations.Nullable;

/**
 * A data transfer object representing the persisted state of an operational job.
 */
public class OperationalJobRecord
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

    /**
     * Constructs an OperationalJobRecord with the given fields.
     *
     * @param jobId         time-based v1 UUID identifying the job
     * @param operationType the operation type
     * @param status        the current status of the job
     */
    public OperationalJobRecord(UUID jobId, OperationType operationType, OperationalJobStatus status)
    {
        this(jobId, operationType, status, null, Instant.now(), null, null, null);
    }

    /**
     * Constructs an OperationalJobRecord with all fields.
     *
     * @param jobId             time-based v1 UUID identifying the job
     * @param operationType     the operation type
     * @param status            the current status of the job
     * @param startTime         the timestamp when execution started, or null if not yet started
     * @param lastUpdate        the timestamp of the last status update, or null for pre-existing rows
     * @param failureReason     the failure reason if the job failed, or null
     * @param nodeExecutionOrder        the ordered list of parallel node groups for execution, or null
     * @param operationMetadata the operation parameters, or null
     */
    public OperationalJobRecord(UUID jobId, OperationType operationType, OperationalJobStatus status,
                                @Nullable Instant startTime,
                                @Nullable Instant lastUpdate,
                                @Nullable String failureReason,
                                @Nullable List<List<UUID>> nodeExecutionOrder,
                                @Nullable Map<String, String> operationMetadata)
    {
        Preconditions.checkArgument(jobId != null, "jobId must not be null");
        Preconditions.checkArgument(jobId.version() == 1, "jobId must be a time-based (v1) UUID");
        Preconditions.checkArgument(operationType != null, "operationType must not be null");
        Preconditions.checkArgument(status != null, "status must not be null");
        this.jobId = jobId;
        this.operationType = operationType;
        this.status = status;
        this.creationTimeMillis = UUIDs.unixTimestamp(jobId);
        this.startTime = startTime;
        this.lastUpdate = lastUpdate;
        this.failureReason = failureReason;
        this.nodeExecutionOrder = nodeExecutionOrder;
        this.operationMetadata = operationMetadata;
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
               '}';
    }
}
