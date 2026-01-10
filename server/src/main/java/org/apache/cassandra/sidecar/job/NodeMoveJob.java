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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException;
import org.jetbrains.annotations.NotNull;

/**
 * Implementation of {@link OperationalJob} to perform node move operation.
 */
public class NodeMoveJob extends OperationalJob
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeMoveJob.class);
    private static final String OPERATION = "move";
    private static final String OPERATION_MODE_MOVING = "MOVING";
    private final String newToken;
    protected StorageOperations storageOperations;

    public NodeMoveJob(UUID jobId, String newToken, StorageOperations storageOps)
    {
        super(jobId);
        this.newToken = newToken;
        this.storageOperations = storageOps;
    }

    @Override
    public boolean hasConflict(@NotNull List<OperationalJob> sameOperationJobs)
    {
        String operationMode = storageOperations.operationMode();
        return OPERATION_MODE_MOVING.equals(operationMode);
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    protected Future<Void> executeInternal() throws Exception
    {
        try
        {
            LOGGER.info("Executing move operation. jobId={} newToken={}", this.jobId(), newToken);
            storageOperations.move(newToken);
        }
        catch (IOException ex)
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
    public String name()
    {
        return OPERATION;
    }
}
