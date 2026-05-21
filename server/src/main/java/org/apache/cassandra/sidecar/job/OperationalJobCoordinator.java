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

package org.apache.cassandra.sidecar.job;

import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.sidecar.common.data.OperationType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Coordinates cluster-wide operational jobs by managing active operation state.
 * Ensures mutual exclusion so that only one operation of a given type is active
 * at a time within a datacenter.
 */
public interface OperationalJobCoordinator
{
    /**
     * Attempt to set an operation as active. Implementations must ensure mutual exclusion
     * so that only one operation of the given type is active at a time.
     *
     * @param operationType the type of operation
     * @param operationId   the unique identifier for this operation
     * @return {@code true} if the operation was successfully set as active,
     *         {@code false} if an operation of the same type is already active
     */
    boolean trySetActive(OperationType operationType, UUID operationId);

    /**
     * Clear the active operation lock, but only if the provided operation ID matches
     * the currently active one.
     * <p>
     * This method is not called by {@code OperationalJobManager} during job submission.
     * For cluster-wide operations, the Sidecar that creates the job is not necessarily
     * the one that finishes it. Clearing is the responsibility of the orchestration layer
     * once all participating nodes have completed their work.
     *
     * @param operationType the type of operation
     * @param operationId   the operation ID to clear
     * @return {@code true} if the active operation was cleared,
     *         {@code false} if the provided operation ID did not match the active one
     */
    boolean clearActive(OperationType operationType, UUID operationId);

    /**
     * Get the active operation ID for a given operation type.
     *
     * @param operationType the type of operation
     * @return the active operation ID, or {@code null} if no operation of this type is active
     */
    @Nullable
    UUID getActiveOperation(OperationType operationType);

    /**
     * Get all active operations.
     *
     * @return a map of operation type to operation ID for all currently active operations,
     *         or an empty map if no operations are active
     */
    @NotNull
    Map<OperationType, UUID> getActiveOperations();
}
