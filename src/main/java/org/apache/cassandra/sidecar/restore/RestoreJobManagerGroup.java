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

package org.apache.cassandra.sidecar.restore;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;

/**
 * The group of managers of all instances
 */
@Singleton
public class RestoreJobManagerGroup
{
    private final RestoreJobConfiguration restoreJobConfig;
    // instance id --> RestoreJobManager
    private final Map<Integer, RestoreJobManager> managerGroup = new ConcurrentHashMap<>();
    private final RestoreProcessor restoreProcessor;
    private final RestoreJobDiscoverer jobDiscoverer;
    private final ExecutorPools executorPools;

    @Inject
    public RestoreJobManagerGroup(SidecarConfiguration configuration,
                                  InstancesConfig instancesConfig,
                                  ExecutorPools executorPools,
                                  PeriodicTaskExecutor periodicTaskExecutor,
                                  RestoreProcessor restoreProcessor,
                                  RestoreJobDiscoverer jobDiscoverer)
    {
        this.restoreJobConfig = configuration.restoreJobConfiguration();
        this.restoreProcessor = restoreProcessor;
        this.jobDiscoverer = jobDiscoverer;
        this.executorPools = executorPools;
        initializeManagers(instancesConfig);
        jobDiscoverer.signalRefresh();
        periodicTaskExecutor.schedule(jobDiscoverer);
        periodicTaskExecutor.schedule(restoreProcessor);
    }

    /**
     * Simply delegates to {@link RestoreJobManager#trySubmit(RestoreSlice, RestoreJob)}
     *
     * @param instance   the cassandra instance to submit the slice to
     * @param slice      restore slice
     * @param restoreJob the restore job instance
     * @return status of the submitted slice
     * @throws RestoreJobFatalException the job has failed
     */
    public RestoreSliceTracker.Status trySubmit(InstanceMetadata instance, RestoreSlice slice, RestoreJob restoreJob)
    throws RestoreJobFatalException
    {
        return getManager(instance).trySubmit(slice, restoreJob);
    }

    /**
     * Send a signal to the restore job discovery loop to refresh
     */
    public void signalRefreshRestoreJob()
    {
        jobDiscoverer.signalRefresh();
    }

    /**
     * Remove the tracker of the job when it is completed and delete its data on disk. The method internal.
     * It should only be called by the background task, when it discovers the job is
     * in the final {@link RestoreJobStatus}, i.e. SUCCEEDED or FAILED.
     * If the restore job is not cached, it is a no-op.
     *
     * @param jobId id of the restore job
     */
    void removeJobInternal(UUID jobId)
    {
        managerGroup.values().forEach(manager -> manager.removeJobInternal(jobId));
    }

    /**
     * Similar to {@link RestoreJobManager#updateRestoreJob(RestoreJob)}.
     * Update the restore job for each instance.
     * It should only be called by the background task, when it discovers the job is
     * in the CREATED job status.
     *
     * @param restoreJob restore job to update
     */
    void updateRestoreJob(RestoreJob restoreJob)
    {
        if (restoreJob.status != RestoreJobStatus.CREATED)
        {
            throw new IllegalStateException("Cannot update with a non-created restore job");
        }
        managerGroup.values().forEach(manager -> manager.updateRestoreJob(restoreJob));
    }

    /**
     * @return the {@link RestoreJobManager} of the instance
     */
    private RestoreJobManager getManager(InstanceMetadata instance)
    {
        return managerGroup.computeIfAbsent(instance.id(),
                                            id -> new RestoreJobManager(restoreJobConfig, instance,
                                                                        executorPools, restoreProcessor));
    }

    // Create RestoreJobManager instances eagerly
    private void initializeManagers(InstancesConfig instancesConfig)
    {
        // todo: allow register listener for instances list changes in the instancesConfig?
        instancesConfig.instances().forEach(this::getManager);
    }
}
