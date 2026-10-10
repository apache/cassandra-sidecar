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

import io.vertx.core.Future;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;

/**
 * Represents a job executed on a local Cassandra node as part of a cluster-wide operation.
 */
public abstract class LocalJob
{
    private final UUID operationId;
    private final UUID nodeId;
    private final String instanceHost;
    private final OperationType operationType;
    private volatile OperationalJobStatus status;
    private volatile boolean cancelled;

    /**
     * @param operationId   the cluster-wide operation this job belongs to
     * @param nodeId        the host ID of the Cassandra node
     * @param instanceHost  the hostname or IP address of the Cassandra instance
     * @param operationType the type of operation being performed
     */
    protected LocalJob(UUID operationId, UUID nodeId, String instanceHost, OperationType operationType)
    {
        this.operationId = operationId;
        this.nodeId = nodeId;
        this.instanceHost = instanceHost;
        this.operationType = operationType;
        this.status = OperationalJobStatus.CREATED;
    }

    /**
     * Performs the job's work. Called on a worker thread, so implementations may block.
     */
    protected abstract void executeInternal();

    /**
     * Requests cancellation of this job. The running task is not interrupted. Instead, a long-running
     * {@link #executeInternal()} is expected to check {@link #isCancelled()} at safe points and stop
     * and recover when cancelled.
     */
    public void cancel()
    {
        cancelled = true;
    }

    /**
     * @return whether cancellation has been requested for this job
     */
    protected boolean isCancelled()
    {
        return cancelled;
    }

    /**
     * Performs reconciliation when the overall cluster-wide job terminates with FAILED
     * status.
     *
     * @return a future that completes when reconciliation is done
     */
    public Future<Void> onJobFailed()
    {
        return Future.succeededFuture();
    }

    /**
     * Execute the job, tracking status transitions
     *
     * @return null (for compatibility with {@code Callable} in {@code executeBlocking})
     */
    public Void execute()
    {
        status = OperationalJobStatus.RUNNING;
        try
        {
            executeInternal();
            status = OperationalJobStatus.SUCCEEDED;
        }
        catch (Exception e)
        {
            status = OperationalJobStatus.FAILED;
            throw e;
        }
        return null;
    }

    public UUID operationId()
    {
        return operationId;
    }

    public UUID nodeId()
    {
        return nodeId;
    }

    public String instanceHost()
    {
        return instanceHost;
    }

    public OperationType operationType()
    {
        return operationType;
    }

    public OperationalJobStatus status()
    {
        return status;
    }
}
