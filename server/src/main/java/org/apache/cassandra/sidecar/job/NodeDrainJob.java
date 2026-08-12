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

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link OperationalJob} to perform node drain operation.
 */
public class NodeDrainJob extends OperationalJob
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeDrainJob.class);
    private static final String OPERATION = "drain";
    protected StorageOperations storageOperations;

    /**
     * Enum representing the various drain states of a Cassandra node.
     */
    public enum NodeDrainStateEnum
    {
        DRAINING(OperationalJobStatus.RUNNING),
        DRAINED(OperationalJobStatus.SUCCEEDED);

        final OperationalJobStatus jobStatus;

        NodeDrainStateEnum(OperationalJobStatus jobStatus)
        {
            this.jobStatus = jobStatus;
        }

        /**
         * Converts a string operation mode to a DrainState enum.
         *
         * @param operationMode the operation mode string
         * @return the corresponding DrainState, or null if not a drain state
         */
        public static NodeDrainStateEnum fromOperationMode(String operationMode)
        {
            try
            {
                return valueOf(operationMode);
            }
            catch (IllegalArgumentException | NullPointerException e)
            {
                LOGGER.error("No matching NodeDrainStateEnum found for Cassandra node status={}. Error={}",
                             operationMode, e);
                return null;
            }
        }
    }

    public NodeDrainJob(UUID jobId, StorageOperations storageOps)
    {
        this(jobId, null, storageOps);
    }

    public NodeDrainJob(UUID jobId, @Nullable UUID nodeId, StorageOperations storageOps)
    {
        super(jobId, nodeId);
        this.storageOperations = storageOps;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasConflict(@NotNull List<OperationalJob> sameOperationJobs)
    {
        String operationMode = storageOperations.operationMode();
        NodeDrainStateEnum nodeDrainStateEnum = NodeDrainStateEnum.fromOperationMode(operationMode);
        return nodeDrainStateEnum == NodeDrainStateEnum.DRAINING;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationalJobStatus status()
    {
        String operationMode = storageOperations.operationMode();
        NodeDrainStateEnum nodeDrainStateEnum = NodeDrainStateEnum.fromOperationMode(operationMode);

        if (nodeDrainStateEnum != null)
        {
            return nodeDrainStateEnum.jobStatus;
        }
        return super.status();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Future<Void> executeInternal()
    {
        LOGGER.info("Executing drain operation. jobId={}", this.jobId());
        try
        {
            storageOperations.drain();
        }
        catch (IOException | ExecutionException | InterruptedException ex)
        {
            LOGGER.error("Job execution failed. jobId={} reason='{}'", this.jobId(), ex.getMessage(), ex);
            throw OperationalJobException.wraps(ex);
        }

        return Future.succeededFuture();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationType operationType()
    {
        return OperationType.DRAIN;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String name()
    {
        return OPERATION;
    }
}
