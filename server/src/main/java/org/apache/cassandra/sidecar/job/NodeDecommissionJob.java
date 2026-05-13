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
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link OperationalJob} to perform node decommission operation.
 */
public class NodeDecommissionJob extends OperationalJob
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeDecommissionJob.class);
    private static final String OPERATION = "decommission";
    private final boolean isForce;
    protected StorageOperations storageOperations;

    public NodeDecommissionJob(UUID jobId, StorageOperations storageOps, boolean isForce)
    {
        this(jobId, null, storageOps, isForce);
    }

    public NodeDecommissionJob(UUID jobId, @Nullable UUID nodeId, StorageOperations storageOps, boolean isForce)
    {
        super(jobId, nodeId);
        this.storageOperations = storageOps;
        this.isForce = isForce;
    }

    /**
     * Node Decommission job determines conflict based on the current operation mode of the cluster.
     * If the cluster is in the middle of a decommission or leaving operation, the job will be rejected.
     * @param sameOperationJobs jobs tracked by the tracker are not relevant for this conflict check
     * @return true if the job has a conflict, false otherwise
     */
    @Override
    public boolean hasConflict(@NotNull List<OperationalJob> sameOperationJobs)
    {
        if (!sameOperationJobs.isEmpty())
        {
            return true;
        }

        String operationMode = storageOperations.operationMode();
        return "LEAVING".equals(operationMode) || "DECOMMISSIONED".equals(operationMode);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationalJobStatus status()
    {
        String operationMode = storageOperations.operationMode();

        if ("LEAVING".equals(operationMode))
        {
            return OperationalJobStatus.RUNNING;
        }
        else if ("DECOMMISSIONED".equals(operationMode))
        {
            return OperationalJobStatus.SUCCEEDED;
        }
        else
        {
            return super.status();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Future<Void> executeInternal()
    {
        LOGGER.info("Executing decommission operation. jobId={}", this.jobId());
        storageOperations.decommission(isForce);
        return Future.succeededFuture();
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

