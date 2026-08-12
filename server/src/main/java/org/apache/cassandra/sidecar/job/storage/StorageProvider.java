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

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A provider-agnostic storage abstraction for durable operational job state.
 * Each {@code StorageProvider} instance is scoped to a single cluster and datacenter.
 * The cluster and datacenter identity is configuration, not a per-call parameter.
 * <p>
 * This interface defines a data access pattern for persisting, modifying, and querying OperationalJobs.
 * Higher-level coordination logic, such as clearing the active operation lock when a job reaches
 * a terminal state, belongs in the layers that depend on this interface.
 */
public interface StorageProvider extends Closeable
{
    // --- Operational Job Storage ---

    /**
     * Persist a new operational job record.
     * <p>
     * Implementations must provide upsert semantics as a retry safety net: if called again with the
     * same job ID (e.g., due to a network timeout where the caller does not know if the first write
     * succeeded), the existing record should be overwritten rather than throwing.
     * <p>
     * Implementations should throw {@link StorageProviderException} on write failure.
     *
     * @param job the job record to store
     */
    void persistJob(OperationalJobRecord job);

    /**
     * Find a job by its ID.
     *
     * @param jobId the job identifier
     * @return the job record, or {@code null} if not found
     */
    @Nullable
    OperationalJobRecord findJob(UUID jobId);

    /**
     * Update the status of an existing job.
     * <p>
     * Implementations should throw {@link StorageProviderException} on write failure.
     *
     * @param jobId         the job identifier
     * @param operationType the operation type
     * @param status        the new status
     * @param failureReason the failure reason if the job has failed, or {@code null} otherwise
     */
    void updateJobStatus(UUID jobId, OperationType operationType, OperationalJobStatus status,
                         @Nullable String failureReason);

    /**
     * Retrieve stored job records, up to the specified limit. Implementations should return
     * records in descending time order. 
     *
     * @param limit the maximum number of job records to return
     * @return list of job records, never null
     */
    @NotNull
    List<OperationalJobRecord> findAllJobs(int limit);

    // --- Active Operation Coordination ---

    /**
     * Set an operation as active if no other operation of the same type is currently active.
     * <p>
     * Implementations must provide compare-and-set (CAS) semantics to ensure only one active
     * operation of a given type runs at a time across the cluster. This lock is per operation
     * plan, not per node; within a single operation, multiple nodes may be operated on
     * concurrently as defined by the node execution order.
     *
     * @param operationType the operation type
     * @param operationId   the unique identifier for this operation
     * @return {@code true} if the operation was successfully set as active, {@code false} if
     *         an operation of the same type is already active (including the same operation ID)
     */
    boolean trySetActiveOperation(OperationType operationType, UUID operationId);

    /**
     * Get the active operation ID for a given operation type.
     *
     * @param operationType the operation type
     * @return the active operation ID, or {@code null} if no operation of this type is active
     */
    @Nullable
    UUID getActiveOperation(OperationType operationType);

    /**
     * Get all active operations for the local datacenter.
     *
     * @return a map of operation type to operation ID for all currently active operations
     *         in the local datacenter, never null
     */
    @NotNull
    Map<OperationType, UUID> getActiveOperations();

    /**
     * Clear the active operation lock, but only if the provided operation ID matches
     * the currently active one.
     *
     * @param operationType the operation type
     * @param operationId   the operation ID to clear
     * @return {@code true} if the active operation was cleared, {@code false} if the provided
     *         operation ID did not match the active one
     */
    boolean clearActiveOperation(OperationType operationType, UUID operationId);

    // --- Node Status Tracking ---

    /**
     * Update the status of multiple nodes for an operation in a single call.
     * <p>
     * Implementations should optimize with batch writes where possible. This method is not
     * guaranteed to be atomic; if any write fails, implementations should throw
     * {@link StorageProviderException}. Callers can safely retry the full list since 
     * writing the same status to a node is idempotent.
     *
     * @param operationId the operation identifier
     * @param nodeIds     the list of node identifiers to update
     * @param nodeStatus  the status to set for all specified nodes
     */
    void updateNodeStatuses(UUID operationId, List<UUID> nodeIds, OperationalJobStatus nodeStatus);

    /**
     * Update a node's status within an operation.
     * <p>
     * Implementations should throw {@link StorageProviderException} on write failure.
     *
     * @param operationId the operation identifier
     * @param nodeId      the node identifier
     * @param nodeStatus  the new status for the node
     */
    void updateNodeStatus(UUID operationId, UUID nodeId, OperationalJobStatus nodeStatus);

    /**
     * Get a node's current status within an operation.
     *
     * @param operationId the operation identifier
     * @param nodeId      the node identifier
     * @return the node's current status, or {@code null} if not found
     */
    @Nullable
    OperationalJobStatus getNodeStatus(UUID operationId, UUID nodeId);

    /**
     * Get all node statuses for an operation.
     *
     * @param operationId the operation identifier
     * @return a map of node IDs to their current status, never null
     */
    @NotNull
    Map<UUID, OperationalJobStatus> getNodeStatusesForOperation(UUID operationId);

    // --- Lifecycle ---

    /**
     * Initialize the storage provider (e.g. schema creation, connection setup).
     * <p>
     * Implementations must use this method to prepare the StorageProvider for accepting users
     * requests, and ensure that this is idempotent. No other methods should be used to do
     * this activity.
     * <p>
     * Callers must invoke this method before calling any other method on this interface.
     * <p>
     * Implementations must ensure that stored records are eventually pruned. The default
     * Cassandra implementation relies on table-level TTL. Other implementations should
     * establish their own pruning strategy (e.g., scheduled deletes of old records).
     */
    void initialize();

    /**
     * Returns whether the provider is ready to accept operations.
     *
     * @return {@code true} if the provider is initialized and available, {@code false} otherwise
     */
    boolean isAvailable();
}
