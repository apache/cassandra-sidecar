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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.jetbrains.annotations.Nullable;

/**
 * Manages the lifecycle of {@link LocalJob} instances for this Sidecar.
 */
@Singleton
public class LocalJobManager
{
    private final Map<OperationNodeKey, LocalJob> activeJobs = new ConcurrentHashMap<>();
    private final TaskExecutorPool internalExecutorPool;

    @Inject
    public LocalJobManager(ExecutorPools executorPools)
    {
        this.internalExecutorPool = executorPools.internal();
    }

    /**
     * Submit a {@link LocalJob} for execution
     *
     * @param job the job to submit
     * @return a future that completes when the job finishes, or succeeds immediately if already tracked
     */
    public Future<Void> submitJob(LocalJob job)
    {
        LocalJob existing = activeJobs.putIfAbsent(new OperationNodeKey(job.operationId(), job.nodeId()), job);
        if (existing != null)
        {
            return Future.succeededFuture();
        }
        return internalExecutorPool.executeBlocking(job::execute).mapEmpty();
    }

    /**
     * Get the active job for a node within a specific operation, or {@code null} if none.
     *
     * @param operationId the cluster-wide operation identifier
     * @param nodeId      the node identifier
     * @return the active job, or {@code null}
     */
    @Nullable
    public LocalJob getJob(UUID operationId, UUID nodeId)
    {
        return activeJobs.get(new OperationNodeKey(operationId, nodeId));
    }

    /**
     * Get all active local jobs.
     *
     * @return an unmodifiable view of active jobs
     */
    public Collection<LocalJob> activeJobs()
    {
        return Collections.unmodifiableCollection(activeJobs.values());
    }

    /**
     * Remove a job from tracking, only if it is still the tracked one
     *
     * @param operationId the cluster-wide operation identifier
     * @param nodeId      the node whose job should be removed
     * @param expected    the job the caller is finished with
     * @return whether the job was removed
     */
    public boolean removeJob(UUID operationId, UUID nodeId, LocalJob expected)
    {
        return activeJobs.remove(new OperationNodeKey(operationId, nodeId), expected);
    }

    /**
     * Composite key identifying per-node state within a specific cluster-wide operation
     */
    static final class OperationNodeKey
    {
        private final UUID operationId;
        private final UUID nodeId;

        OperationNodeKey(UUID operationId, UUID nodeId)
        {
            this.operationId = operationId;
            this.nodeId = nodeId;
        }

        UUID operationId()
        {
            return operationId;
        }

        UUID nodeId()
        {
            return nodeId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (!(o instanceof OperationNodeKey))
            {
                return false;
            }
            OperationNodeKey that = (OperationNodeKey) o;
            return operationId.equals(that.operationId) && nodeId.equals(that.nodeId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(operationId, nodeId);
        }

        @Override
        public String toString()
        {
            return "OperationNodeKey{operationId=" + operationId + ", nodeId=" + nodeId + '}';
        }
    }
}
