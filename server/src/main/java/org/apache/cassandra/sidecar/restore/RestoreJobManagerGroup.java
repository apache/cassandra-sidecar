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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;

/**
 * The group of managers of all instances
 */
@Singleton
public class RestoreJobManagerGroup
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreJobManagerGroup.class);

    private final RestoreJobConfiguration restoreJobConfig;
    // instance id --> RestoreJobManager
    private final Map<Integer, RestoreJobManager> managerGroup = new ConcurrentHashMap<>();
    private final RestoreProcessor restoreProcessor;
    private final ExecutorPools executorPools;

    @Inject
    public RestoreJobManagerGroup(SidecarConfiguration configuration,
                                  InstancesMetadata instancesMetadata,
                                  ExecutorPools executorPools,
                                  PeriodicTaskExecutor periodicTaskExecutor,
                                  RestoreProcessor restoreProcessor,
                                  RestoreJobDiscoverer jobDiscoverer,
                                  RingTopologyRefresher ringTopologyRefresher)
    {
        this.restoreJobConfig = configuration.restoreJobConfiguration();
        this.restoreProcessor = restoreProcessor;
        this.executorPools = executorPools;
        initializeManagers(instancesMetadata);
        periodicTaskExecutor.schedule(jobDiscoverer);
        periodicTaskExecutor.schedule(restoreProcessor);
        periodicTaskExecutor.schedule(ringTopologyRefresher);
    }

    /**
     * Simply delegates to {@link RestoreJobManager#trySubmit(RestoreRange, RestoreJob)}
     *
     * @param instance   the cassandra instance to submit the restore range to
     * @param range      restore range
     * @param restoreJob the restore job instance
     * @return status of the submitted restore range
     * @throws RestoreJobFatalException the job has failed
     */
    public RestoreJobProgressTracker.Status trySubmit(InstanceMetadata instance,
                                                      RestoreRange range, RestoreJob restoreJob)
    throws RestoreJobFatalException
    {
        return getManager(instance).trySubmit(range, restoreJob);
    }

    /**
     * Remove the tracker of the job when it is completed and delete its data on disk. The method internal.
     * It should only be called by the background task, when it discovers the job is
     * in the final {@link RestoreJobStatus}, i.e. SUCCEEDED or FAILED.
     * If the restore job is not cached, it is a no-op.
     *
     * @param restoreJob restore job
     */
    void removeJobInternal(RestoreJob restoreJob)
    {
        if (!restoreJob.status.isFinal())
        {
            throw new IllegalStateException("Cannot remove job that is not in final status");
        }
        managerGroup.values().forEach(manager -> manager.removeJobInternal(restoreJob.jobId));
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
        if (restoreJob.status.isFinal())
        {
            throw new IllegalStateException("Cannot update with a restore job in final status");
        }
        managerGroup.values().forEach(manager -> manager.updateRestoreJob(restoreJob));
    }

    /**
     * Discard the ranges that overlap with the given {@param otherRanges}
     * @param instanceMetadata cassandra instance to discard the restore range from
     * @param restoreJob restore job instance
     * @param otherRanges set of {@link TokenRange} to find the overlapping {@link RestoreRange} and discard
     * @return set of overlapping {@link RestoreRange}
     */
    Set<RestoreRange> discardOverlappingRanges(InstanceMetadata instanceMetadata, RestoreJob restoreJob, Set<TokenRange> otherRanges)
    {
        if (restoreJob.status.isFinal())
        {
            throw new IllegalStateException("Cannot remove ranges from a restore job in final status");
        }
        RestoreJobManager manager = managerGroup.get(instanceMetadata.id());
        if (manager == null)
        {
            LOGGER.debug("No RestoreJobManager found for Cassandra instance. No ranges to discard. instanceId={}",
                         instanceMetadata.id());
            return Set.of();
        }
        return manager.discardOverlappingRanges(restoreJob, otherRanges);
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
    private void initializeManagers(InstancesMetadata instancesMetadata)
    {
        // todo: allow register listener for instances list changes in the instancesMetadata?
        instancesMetadata.instances().forEach(this::getManager);
    }
}
