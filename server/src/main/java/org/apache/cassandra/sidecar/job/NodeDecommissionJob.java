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

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.server.StorageOperations;

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
        super(jobId);
        this.storageOperations = storageOps;
        this.isForce = isForce;
    }

    @Override
    public boolean isRunningOnCassandra()
    {
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
    protected void executeInternal()
    {
        if (isRunningOnCassandra())
        {
            LOGGER.info("Not executing job as an ongoing or completed decommission operation was found jobId={}", this.jobId());
            return;
        }

        LOGGER.info("Executing decommission operation. jobId={}", this.jobId());
        storageOperations.decommission(isForce);
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

