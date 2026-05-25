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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.ActiveClusterOpsDatabaseAccessor;
import org.apache.cassandra.sidecar.db.ClusterOpsDatabaseAccessor;
import org.apache.cassandra.sidecar.db.ClusterOpsNodeStateDatabaseAccessor;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link StorageProvider} implementation backed by Cassandra system tables.
 * Delegates to three database accessors for cluster operations, node state, and active operation coordination.
 * Each instance is scoped to a single cluster identified by {@code clusterName}.
 */
public class CassandraStorageProvider implements StorageProvider
{
    private final CQLSessionProvider sessionProvider;
    private final ClusterOpsDatabaseAccessor clusterOpsAccessor;
    private final ClusterOpsNodeStateDatabaseAccessor nodeStateAccessor;
    private final ActiveClusterOpsDatabaseAccessor activeOpsAccessor;
    private volatile String clusterName;
    private volatile String datacenter;

    /**
     * Constructs a CassandraStorageProvider. When {@code clusterName} is non-null, it indicates that
     * operational state is stored in a separate Cassandra cluster from the one Sidecar manages. When null,
     * the cluster name is derived from the CQL session metadata during {@link #initialize()}, which is the
     * default for same-cluster storage.
     */
    public CassandraStorageProvider(CQLSessionProvider sessionProvider,
                                    ClusterOpsDatabaseAccessor clusterOpsAccessor,
                                    ClusterOpsNodeStateDatabaseAccessor nodeStateAccessor,
                                    ActiveClusterOpsDatabaseAccessor activeOpsAccessor,
                                    @Nullable String clusterName)
    {
        this.sessionProvider = sessionProvider;
        this.clusterOpsAccessor = clusterOpsAccessor;
        this.nodeStateAccessor = nodeStateAccessor;
        this.activeOpsAccessor = activeOpsAccessor;
        this.clusterName = clusterName;
    }

    @Override
    public void persistJob(OperationalJobRecord job)
    {
        execute("persistJob", () -> {
            clusterOpsAccessor.persistJob(clusterName, job);
            return null;
        });
    }

    @Override
    @Nullable
    public OperationalJobRecord findJob(UUID jobId)
    {
        return execute("findJob", () -> clusterOpsAccessor.findJob(clusterName, jobId));
    }

    @Override
    public void updateJobStatus(UUID jobId, OperationType operationType, OperationalJobStatus status,
                                @Nullable String failureReason)
    {
        execute("updateJobStatus", () -> {
            clusterOpsAccessor.updateJobStatus(clusterName, jobId, operationType, status, failureReason);
            return null;
        });
    }

    @Override
    @NotNull
    public List<OperationalJobRecord> findAllJobs(int limit)
    {
        return execute("findAllJobs", () -> clusterOpsAccessor.findAllJobs(clusterName, limit));
    }

    @Override
    public boolean trySetActiveOperation(OperationType operationType, UUID operationId)
    {
        return execute("trySetActiveOperation",
                       () -> activeOpsAccessor.trySetActiveOperation(clusterName, datacenter,
                                                                     operationType, operationId));
    }

    @Override
    @Nullable
    public UUID getActiveOperation(OperationType operationType)
    {
        return execute("getActiveOperation",
                       () -> activeOpsAccessor.getActiveOperation(clusterName, datacenter, operationType));
    }

    @Override
    @NotNull
    public Map<OperationType, UUID> getActiveOperations()
    {
        return execute("getActiveOperations",
                       () -> activeOpsAccessor.getActiveOperations(clusterName, datacenter));
    }

    @Override
    public boolean clearActiveOperation(OperationType operationType, UUID operationId)
    {
        return execute("clearActiveOperation",
                       () -> activeOpsAccessor.clearActiveOperation(clusterName, datacenter,
                                                                    operationType, operationId));
    }

    @Override
    public void updateNodeStatuses(UUID operationId, List<UUID> nodeIds, OperationalJobStatus nodeStatus)
    {
        execute("updateNodeStatuses", () -> {
            nodeStateAccessor.updateNodeStatuses(clusterName, operationId, nodeIds, nodeStatus);
            return null;
        });
    }

    @Override
    public void updateNodeStatus(UUID operationId, UUID nodeId, OperationalJobStatus nodeStatus)
    {
        execute("updateNodeStatus", () -> {
            nodeStateAccessor.updateNodeStatus(clusterName, operationId, nodeId, nodeStatus);
            return null;
        });
    }

    @Override
    @Nullable
    public OperationalJobStatus getNodeStatus(UUID operationId, UUID nodeId)
    {
        return execute("getNodeStatus",
                       () -> nodeStateAccessor.getNodeStatus(clusterName, operationId, nodeId));
    }

    @Override
    @NotNull
    public Map<UUID, OperationalJobStatus> getNodeStatusesForOperation(UUID operationId)
    {
        return execute("getNodeStatusesForOperation",
                       () -> nodeStateAccessor.getNodeStatusesForOperation(clusterName, operationId));
    }

    @Override
    public void initialize()
    {
        // Schema initialization is handled by SidecarSchemaInitializer.
        // TTL on all tables handles record pruning.
        try
        {
            Session session = sessionProvider.get();
            if (clusterName == null)
            {
                clusterName = session.getCluster().getMetadata().getClusterName();
            }
            if (datacenter == null)
            {
                Collection<Host> connectedHosts = session.getState().getConnectedHosts();
                if (connectedHosts.isEmpty())
                {
                    throw new StorageProviderException(
                    "Failed to resolve local datacenter: no connected hosts available");
                }
                datacenter = connectedHosts.iterator().next().getDatacenter();
            }
        }
        catch (CassandraUnavailableException e)
        {
            throw new StorageProviderException("Failed to initialize storage provider", e);
        }
    }

    @Override
    public boolean isAvailable()
    {
        return clusterOpsAccessor.isAvailable()
               && nodeStateAccessor.isAvailable()
               && activeOpsAccessor.isAvailable();
    }

    @Override
    public void close()
    {
        // Session lifecycle is managed by the DI container.
    }

    private <T> T execute(String operation, Supplier<T> action)
    {
        if (clusterName == null || datacenter == null)
        {
            throw new StorageProviderException("StorageProvider has not been initialized. Call initialize() first.");
        }
        try
        {
            return action.get();
        }
        catch (DriverException e)
        {
            throw new StorageProviderException("Failed to execute " + operation, e);
        }
    }
}
