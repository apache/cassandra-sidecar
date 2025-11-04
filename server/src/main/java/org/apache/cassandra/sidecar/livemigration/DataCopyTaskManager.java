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

package org.apache.cassandra.sidecar.livemigration;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.request.LiveMigrationDataCopyRequest;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationDataCopyInProgressException;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationInvalidRequestException;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationTaskNotFoundException;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationMap;
import org.jetbrains.annotations.NotNull;

/**
 * Manages live migration data copy tasks. Handles task creation, tracking, and cancellation.
 * Data copy tasks should only be submitted to destination instances.
 */
@Singleton
public class DataCopyTaskManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DataCopyTaskManager.class);

    @VisibleForTesting
    final ConcurrentHashMap<Integer, LiveMigrationTask> currentTasks = new ConcurrentHashMap<>();
    private final InstancesMetadata instancesMetadata;
    private final SidecarConfiguration sidecarConfiguration;
    private final LiveMigrationMap liveMigrationMap;
    private final LiveMigrationTaskFactory liveMigrationTaskFactory;

    @Inject
    public DataCopyTaskManager(InstancesMetadata instancesMetadata,
                               SidecarConfiguration sidecarConfiguration,
                               LiveMigrationMap liveMigrationMap,
                               LiveMigrationTaskFactory liveMigrationTaskFactory)
    {
        this.instancesMetadata = instancesMetadata;
        this.sidecarConfiguration = sidecarConfiguration;
        this.liveMigrationMap = liveMigrationMap;
        this.liveMigrationTaskFactory = liveMigrationTaskFactory;
    }

    /**
     * Creates a live migration task for downloading files from source to destination.
     * Only one task can run per instance at a time.
     *
     * @param request     live migration request containing task parameters
     * @param currentHost the destination host where sidecar is running
     * @return Future containing the created LiveMigrationTask
     * @throws LiveMigrationDataCopyInProgressException if another task is already running
     * @throws LiveMigrationInvalidRequestException     if request validation fails
     */
    public Future<LiveMigrationTask> createTask(@NotNull LiveMigrationDataCopyRequest request,
                                                @NotNull String currentHost)
    throws LiveMigrationDataCopyInProgressException, LiveMigrationInvalidRequestException
    {
        int maxPossibleConcurrency = Objects.requireNonNull(sidecarConfiguration.liveMigrationConfiguration())
                                            .maxConcurrentDownloads();
        if (request.maxConcurrency > maxPossibleConcurrency)
        {
            return Future.failedFuture(
            new LiveMigrationInvalidRequestException("max concurrency can not be more than " + maxPossibleConcurrency));
        }

        InstanceMetadata localInstanceMetadata = instancesMetadata.instanceFromHost(currentHost);

        return liveMigrationMap.getSource(currentHost)
                               .compose(source -> createDataCopyTask(request, source, localInstanceMetadata));
    }

    Future<LiveMigrationTask> createDataCopyTask(LiveMigrationDataCopyRequest request,
                                                 String source,
                                                 InstanceMetadata localInstanceMetadata)
    {
        LiveMigrationTask newTask = createTask(request,
                                               source,
                                               sidecarConfiguration.serviceConfiguration().port(),
                                               localInstanceMetadata);

        // It is possible to serve only one live migration data copy request per instance at a time.
        // Checking if there is another migration is in progress before accepting new one.
        boolean accepted = currentTasks.compute(localInstanceMetadata.id(), (integer, taskInMap) -> {
            if (taskInMap == null)
            {
                return newTask;
            }

            if (!taskInMap.isCompleted())
            {
                // Accept new task if and only if the existing task has completed.
                return taskInMap;
            }
            else
            {
                return newTask;
            }
        }) == newTask;

        if (!accepted)
        {
            return Future.failedFuture(
            new LiveMigrationDataCopyInProgressException("Another task is already under progress. Cannot accept new task."));
        }
        LOGGER.info("Starting data copy task with id={}, source={}, destination={}",
                    newTask.id(), source, localInstanceMetadata.host());
        newTask.start();
        return Future.succeededFuture(newTask);
    }

    LiveMigrationTask createTask(LiveMigrationDataCopyRequest request,
                                 String source,
                                 int port,
                                 InstanceMetadata localInstanceMetadata)
    {

        return liveMigrationTaskFactory.create(UUIDs.timeBased().toString(), request, source, port, localInstanceMetadata);
    }

    /**
     * Returns all live migration tasks for the current host.
     *
     * @param currentHost the host where sidecar is running
     * @return list containing at most one task (empty if no active task)
     */
    public List<LiveMigrationTask> getAllTasks(@NotNull String currentHost)
    {
        InstanceMetadata localInstance = instancesMetadata.instanceFromHost(currentHost);
        if (currentTasks.isEmpty() || !currentTasks.containsKey(localInstance.id()))
        {
            return Collections.emptyList();
        }
        return Collections.singletonList(currentTasks.get(localInstance.id()));
    }

    /**
     * Returns the live migration task with the specified task ID.
     *
     * @param taskId      ID of the task to retrieve
     * @param currentHost the host where sidecar is running
     * @return the LiveMigrationTask matching the given taskId
     * @throws LiveMigrationTaskNotFoundException if no task found with the given ID
     */
    public LiveMigrationTask getTask(@NotNull String taskId,
                                     @NotNull String currentHost) throws LiveMigrationTaskNotFoundException
    {
        return getLiveMigrationTask(taskId, currentHost);
    }

    /**
     * Cancels the live migration task with the specified task ID.
     *
     * @param taskId      ID of the task to cancel
     * @param currentHost the host where sidecar is running
     * @return the cancelled LiveMigrationTask
     * @throws LiveMigrationTaskNotFoundException if no task found with the given ID
     */
    public LiveMigrationTask cancelTask(@NotNull String taskId,
                                        @NotNull String currentHost) throws LiveMigrationTaskNotFoundException
    {
        LiveMigrationTask taskInProgress = getLiveMigrationTask(taskId, currentHost);

        // Cancelling the task
        taskInProgress.cancel();

        return taskInProgress;
    }

    private LiveMigrationTask getLiveMigrationTask(@NotNull String taskId, @NotNull String currentHost)
    {
        InstanceMetadata localInstance = instancesMetadata.instanceFromHost(currentHost);
        LiveMigrationTask taskInProgress = currentTasks.get(localInstance.id());
        if (null == taskInProgress || !taskId.equals(taskInProgress.id()))
        {
            throw new LiveMigrationTaskNotFoundException("No data copy task found with given id " + taskId);
        }
        return taskInProgress;
    }
}
