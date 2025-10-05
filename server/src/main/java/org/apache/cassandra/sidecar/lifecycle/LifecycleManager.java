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

package org.apache.cassandra.sidecar.lifecycle;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import io.vertx.core.Future;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.Lifecycle.CassandraState;
import org.apache.cassandra.sidecar.common.data.Lifecycle.OperationStatus;
import org.apache.cassandra.sidecar.common.response.LifecycleInfoResponse;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.exceptions.LifecycleTaskConflictException;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Manages the lifecycle intents for local Cassandra hosts and submits tasks to the
 * LifecycleProvider, while ensuring that only one task can be active per host at a time.
 */
@Singleton
public class LifecycleManager
{
    protected static final Logger LOG = LoggerFactory.getLogger(LifecycleManager.class);

    private final InstanceMetadataFetcher metadataFetcher;
    private final LifecycleProvider lifecycleProvider;
    private final ExecutorPools executorPools;

    private final Set<String> convergingInstances = ConcurrentHashMap.newKeySet();
    private final Map<String, OperationStatus> lastCompletedStatus = new ConcurrentHashMap<>();
    private final Map<String, CassandraState> desiredStateByInstance = new ConcurrentHashMap<>();
    private final Map<String, String> lastUpdateMsgByInstance = new ConcurrentHashMap<>();

    @Inject
    public LifecycleManager(InstanceMetadataFetcher metadataFetcher,
                            LifecycleProvider lifecycleProvider,
                            ExecutorPools executorPools)
    {
        this.metadataFetcher = metadataFetcher;
        this.lifecycleProvider = lifecycleProvider;
        this.executorPools = executorPools;
    }

    public synchronized LifecycleInfoResponse updateDesiredState(String instanceId, CassandraState desiredState)
                                                        throws LifecycleTaskConflictException
    {
        // Nothing to do: already in successful desired state
        LifecycleInfoResponse current = getLifecycleInfo(instanceId);
        if  (current.desiredState() == desiredState && current.status() == OperationStatus.CONVERGED)
        {
            return current;
        }
        // Make sure there is a single lifecycle task running at a time per instance
        if (!convergingInstances.add(instanceId))
        {
            throw new LifecycleTaskConflictException(String.format("Cannot update lifecycle state of instance %s to %s. " +
                                                                   "Task already in progress for this host.",
                                                                   instanceId, desiredState));
        }
        // Converge state asynchronously and cleanup in-progress tasks after completion or error
        try
        {
            lastCompletedStatus.remove(instanceId);
            desiredStateByInstance.put(instanceId, desiredState);
            lastUpdateMsgByInstance.put(instanceId, "Submitting " +
                    (desiredState.isRunning() ? "start" : "stop") + " task for instance");
            LifecycleInfoResponse updatedDesireLifecycleInfo = getLifecycleInfo(instanceId);
            Future<Void> convergeTask = desiredState.isRunning()
                                          ? submitStartTask(instanceId)
                                          : submitStopTask(instanceId);
            convergeTask.onFailure(x -> LOG.warn("Unexpected failure while converging state.", x))
                        .onComplete(x -> convergingInstances.remove(instanceId));
            return updatedDesireLifecycleInfo;
        }
        catch (Exception e)
        {
            LOG.error("Unexpected error while submitting lifecycle task for instance {}: {}", instanceId, e.getMessage());
            convergingInstances.remove(instanceId);
            throw e;
        }
    }

    public synchronized LifecycleInfoResponse getLifecycleInfo(String instanceId)
    {
        CassandraState currentState = lifecycleProvider
            .isRunning(metadata(instanceId)) ? CassandraState.RUNNING : CassandraState.STOPPED;
        CassandraState desiredState = this.desiredStateByInstance.getOrDefault(instanceId, CassandraState.UNKNOWN);
        OperationStatus currentStatus = getStatus(instanceId, currentState, desiredState);
        maybeRefreshLastCompletedStatus(instanceId, currentStatus, currentState, desiredState);
        String lastUpdate = this.lastUpdateMsgByInstance.getOrDefault(instanceId, "No lifecycle task submitted for this instance yet.");
        return new LifecycleInfoResponse(currentState, desiredState, currentStatus, lastUpdate);
    }

    /**
     * This method keeps track of the last completed status of an instance and logs a warning if the operation status has changed unexpectedly.
     * This is useful for detecting unexpected state changes that may indicate issues with the lifecycle management.
     * See @{link LifecycleManagerTest#testStateChangesUnexpectedlyFlapping} for an example test case.
     */
    private void maybeRefreshLastCompletedStatus(String instanceId, OperationStatus currentStatus, CassandraState currentState,
                                                 CassandraState desiredState)
    {
        if (!currentStatus.isCompleted()) // this logic is only valid after a lifecycle task has completed
            return;
        OperationStatus lastStatus = lastCompletedStatus.put(instanceId, currentStatus);
        if (lastStatus != null && lastStatus != currentStatus)
        {
            String msg = currentStatus.isConverged() ?
                         String.format("Instance %s has converged back to the desired state %s.", instanceId, desiredState) :
                         String.format("Instance %s has unexpectedly diverged from the desired state %s to %s.", instanceId, desiredState, currentState);
            lastUpdateMsgByInstance.put(instanceId, msg);
            LOG.warn(msg);
        }
    }

    private Future<Void> submitStartTask(String instanceId)
    {
        return executorPools.internal().runBlocking(() ->
        {
            try
            {
                lastUpdateMsgByInstance.put(instanceId, "Starting instance");
                lifecycleProvider.start(metadata(instanceId));
                lastUpdateMsgByInstance.put(instanceId, "Instance has started");
            }
            catch (Exception e)
            {
                LOG.error("Failed to start instance {}", instanceId, e);
                lastUpdateMsgByInstance.put(instanceId, String.format("Failed to start instance %s: %s", instanceId, e.getMessage()));
            }
        });
    }

    private Future<Void> submitStopTask(String instanceId)
    {
        return executorPools.internal().runBlocking(() ->
        {
            try
            {
                lastUpdateMsgByInstance.put(instanceId, "Stopping instance");
                lifecycleProvider.stop(metadata(instanceId));
                lastUpdateMsgByInstance.put(instanceId, "Instance has stopped");
            }
            catch (Exception e)
            {
                LOG.error("Failed to stop instance {}", instanceId, e);
                lastUpdateMsgByInstance.put(instanceId, String.format("Failed to stop instance %s: %s", instanceId, e.getMessage()));
            }
        });
    }

    private OperationStatus getStatus(String instanceId, CassandraState currentState, CassandraState desiredState)
    {
        if (convergingInstances.contains(instanceId) && desiredState != currentState)
        {
            return OperationStatus.CONVERGING;
        }
        if (desiredState == CassandraState.UNKNOWN)
        {
            return OperationStatus.UNDEFINED;
        }
        if (desiredState != currentState)
        {
            return OperationStatus.DIVERGED;
        }
        return OperationStatus.CONVERGED;
    }

    private InstanceMetadata metadata(String host)
    {
        return metadataFetcher.instance(host);
    }
}
