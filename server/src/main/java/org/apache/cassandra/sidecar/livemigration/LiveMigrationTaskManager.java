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
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions;
import org.jetbrains.annotations.NotNull;

/**
 * Centralized singleton manager for all live migration tasks across Cassandra instances.
 * Enforces the constraint that only one {@link LiveMigrationTask} (of any type) can be active per instance at a time,
 * preventing concurrent data copy and files verification operations that could lead to resource conflicts or data integrity issues.
 * This singleton is shared across {@link DataCopyTaskManager} and {@link FilesVerificationTaskManager} to ensure
 * proper coordination and mutual exclusion of tasks per instance.
 */
@Singleton
public class LiveMigrationTaskManager
{
    @VisibleForTesting
    final ConcurrentHashMap<Integer, LiveMigrationTask<?>> currentTasks = new ConcurrentHashMap<>();

    private final InstancesMetadata instancesMetadata;

    @Inject
    public LiveMigrationTaskManager(InstancesMetadata instancesMetadata)
    {
        this.instancesMetadata = instancesMetadata;
    }

    /**
     * Attempts to submit a new task for the specified instance.
     * Only one task (of any type) can be active per instance at a time.
     *
     * @param instanceId the instance ID
     * @param newTask    the task to submit
     * @return true if the task was accepted, false if another task is already in progress
     */
    public boolean submitTask(int instanceId, LiveMigrationTask<?> newTask)
    {
        return newTask == currentTasks.compute(instanceId, (ignored, taskInMap) -> {
            if (taskInMap == null)
            {
                return newTask;
            }

            if (!taskInMap.isCompleted())
            {
                // Reject new task if existing task is still in progress
                return taskInMap;
            }
            else
            {
                // Accept new task if existing task has completed
                return newTask;
            }
        });
    }

    /**
     * Returns all live migration tasks for given currentHost.
     * This includes both active and completed tasks that haven't been replaced.
     *
     * @param currentHost the host where sidecar is running
     * @return list containing at most one task (empty if no task has ever been submitted for this host)
     */
    public List<LiveMigrationTask<?>> getAllTasks(@NotNull String currentHost)
    {
        InstanceMetadata localInstance = instancesMetadata.instanceFromHost(currentHost);
        LiveMigrationTask<?> task = currentTasks.get(localInstance.id());
        return task == null ? Collections.emptyList() : Collections.singletonList(task);
    }

    /**
     * Returns the live migration task with the specified task ID.
     *
     * @param taskId      ID of the task to retrieve
     * @param currentHost the host where sidecar is running
     * @return the LiveMigrationTask matching the given taskId
     * @throws LiveMigrationExceptions.LiveMigrationTaskNotFoundException if no task found with the given ID
     */
    public LiveMigrationTask<?> getTask(@NotNull String taskId,
                                        @NotNull String currentHost) throws LiveMigrationExceptions.LiveMigrationTaskNotFoundException
    {
        return getLiveMigrationTask(taskId, currentHost);
    }

    /**
     * Cancels the live migration task with the specified task ID.
     *
     * @param taskId      ID of the task to cancel
     * @param currentHost the host where sidecar is running
     * @return the cancelled LiveMigrationTask
     * @throws LiveMigrationExceptions.LiveMigrationTaskNotFoundException if no task found with the given ID
     */
    public LiveMigrationTask<?> cancelTask(@NotNull String taskId,
                                           @NotNull String currentHost) throws LiveMigrationExceptions.LiveMigrationTaskNotFoundException
    {
        LiveMigrationTask<?> taskInProgress = getLiveMigrationTask(taskId, currentHost);

        // Cancelling the task
        taskInProgress.cancel();

        return taskInProgress;
    }

    private LiveMigrationTask<?> getLiveMigrationTask(@NotNull String taskId, @NotNull String currentHost)
    {
        InstanceMetadata localInstance = instancesMetadata.instanceFromHost(currentHost);
        LiveMigrationTask<?> taskInProgress = currentTasks.get(localInstance.id());
        if (taskInProgress == null || !taskId.equals(taskInProgress.id()))
        {
            throw new LiveMigrationExceptions.LiveMigrationTaskNotFoundException("No task found with given id " + taskId);
        }
        return taskInProgress;
    }
}
