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

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;

/**
 * Interface for coordinating local jobs that are part of cluster-wide operations.
 */
public interface LocalJobCoordinator
{
    /**
     * Determines whether all predecessors of the given node have completed,
     * allowing it to proceed with execution.
     *
     * @param nodeId             the node to check predecessors for
     * @param nodeExecutionOrder the ordered list of parallel groups
     * @param nodeStatuses               current statuses of all nodes in the operation
     * @param requiresPredecessorSuccess whether a failed predecessor should block execution
     * @return {@code true} if all predecessors are complete and the node can proceed
     */
    boolean arePredecessorsComplete(UUID nodeId,
                                    List<List<UUID>> nodeExecutionOrder,
                                    Map<UUID, OperationalJobStatus> nodeStatuses,
                                    boolean requiresPredecessorSuccess);

    /**
     * Reports the node's status 
     *
     * @param operationId the operation this node belongs to
     * @param nodeId      the node whose status is being reported
     * @param status      the new status
     */
    void reportNodeStatus(UUID operationId, UUID nodeId, OperationalJobStatus status);

    /**
     * Checks if the entire job is complete and finalizes if it is
     *
     * @param operationId   the operation identifier
     * @param operationType the type of operation
     * @param jobRecord     the job record
     * @param nodeStatuses  current statuses of all nodes
     */
    void checkAndFinalizeJobIfComplete(UUID operationId,
                                       OperationType operationType,
                                       OperationalJobRecord jobRecord,
                                       Map<UUID, OperationalJobStatus> nodeStatuses);
}
