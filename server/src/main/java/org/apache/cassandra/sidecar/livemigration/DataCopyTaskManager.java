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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.request.LiveMigrationDataCopyRequest;
import org.apache.cassandra.sidecar.common.response.LiveMigrationDataCopyResponse;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationInvalidRequestException;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationTaskInProgressException;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationTaskNotFoundException;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationMap;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.livemigration.LiveMigrationDataCopyTask.DATA_COPY_TASK_TYPE;

/**
 * Manages live migration data copy tasks. Handles task creation, tracking, and cancellation.
 * Data copy tasks should only be submitted to destination instances.
 */
@Singleton
public class DataCopyTaskManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DataCopyTaskManager.class);

    private final LiveMigrationTaskManager taskManager;
    private final InstancesMetadata instancesMetadata;
    private final SidecarConfiguration sidecarConfiguration;
    private final LiveMigrationMap liveMigrationMap;
    private final LiveMigrationTaskFactory liveMigrationTaskFactory;

    @Inject
    public DataCopyTaskManager(LiveMigrationTaskManager taskManager,
                               InstancesMetadata instancesMetadata,
                               SidecarConfiguration sidecarConfiguration,
                               LiveMigrationMap liveMigrationMap,
                               LiveMigrationTaskFactory liveMigrationTaskFactory)
    {
        this.taskManager = taskManager;
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
     * @return Future containing the created LiveMigrationTask, or a failed Future with:
     * <ul>
     * <li>{@link LiveMigrationInvalidRequestException} if max concurrency exceeds the configured limit</li>
     * <li>{@link LiveMigrationTaskInProgressException} if another task is already running for this instance</li>
     * </ul>
     */
    public Future<LiveMigrationTask<LiveMigrationDataCopyResponse>> createTask(@NotNull LiveMigrationDataCopyRequest request,
                                                                               @NotNull String currentHost)
    {
        int maxPossibleConcurrency = Objects.requireNonNull(sidecarConfiguration.liveMigrationConfiguration())
                                            .maxConcurrentFileRequests();
        if (request.maxConcurrency > maxPossibleConcurrency)
        {
            return Future.failedFuture(
            new LiveMigrationInvalidRequestException("max concurrency can not be more than " + maxPossibleConcurrency));
        }

        InstanceMetadata localInstanceMetadata = instancesMetadata.instanceFromHost(currentHost);

        return liveMigrationMap.getSource(currentHost)
                               .compose(source -> createDataCopyTask(request, source, localInstanceMetadata));
    }

    private Future<LiveMigrationTask<LiveMigrationDataCopyResponse>> createDataCopyTask(LiveMigrationDataCopyRequest request,
                                                                                        String source,
                                                                                        InstanceMetadata localInstanceMetadata)
    {
        // Fast local JMX check before creating task - prevents task creation if Cassandra is running
        return verifyCassandraNotRunning(localInstanceMetadata)
               .compose(v -> {
                   LiveMigrationTask<LiveMigrationDataCopyResponse> newTask = createTask(request,
                                                                                         source,
                                                                                         sidecarConfiguration.serviceConfiguration().port(),
                                                                                         localInstanceMetadata);

                   // It is possible to serve only one live migration data copy request per instance at a time.
                   // Checking if there is another migration is in progress before accepting new one.
                   boolean accepted = taskManager.submitTask(localInstanceMetadata.id(), newTask);

                   if (!accepted)
                   {
                       return Future.failedFuture(
                       new LiveMigrationTaskInProgressException("Another task is already under progress. Cannot accept new task."));
                   }
                   LOGGER.info("Starting data copy task with id={}, source={}, destination={}",
                               newTask.id(), source, localInstanceMetadata.host());
                   newTask.start();
                   return Future.succeededFuture(newTask);
               });
    }

    /**
     * Initiating data copy once a Cassandra instance starts is not acceptable. This method checks whether
     * Cassandra is running or not at the moment on the destination instance by checking if Sidecar
     * is able to connect to the Cassandra instance's JMX port or native (CQL) port. It returns a failed
     * future if Sidecar is able to connect to either port of Cassandra.
     *
     * @param localInstance metadata for the local Cassandra instance
     * @return Future that succeeds if Cassandra is not running, fails if it is running
     */
    private Future<Void> verifyCassandraNotRunning(InstanceMetadata localInstance)
    {
        try
        {
            CassandraAdapterDelegate delegate = localInstance.delegate();

            if (delegate.isJmxUp() || delegate.isNativeUp())
            {
                return Future.failedFuture(new LiveMigrationInvalidRequestException(
                "Cannot start data copy: Cassandra is currently running on this instance " +
                "(JMX or native connectivity established). Data copy cannot proceed while Cassandra is active."));
            }

            // JMX and native are down - Cassandra is not running (or at least wasn't during last health check)
            LOGGER.debug("Local JMX and native check passed: Cassandra not detected as running on {}", localInstance.host());
            return Future.succeededFuture();
        }
        catch (CassandraUnavailableException e)
        {
            // No delegate available - Cassandra is not running
            LOGGER.debug("No Cassandra delegate available for {} (Cassandra not running)", localInstance.host());
            return Future.succeededFuture();
        }
        catch (Exception e)
        {
            // Unexpected error - be conservative and reject for safety
            LOGGER.warn("Unable to verify Cassandra status on {}, rejecting for safety", localInstance.host(), e);
            return Future.failedFuture(e);
        }
    }

    LiveMigrationTask<LiveMigrationDataCopyResponse> createTask(LiveMigrationDataCopyRequest request,
                                                                String source,
                                                                int port,
                                                                InstanceMetadata localInstanceMetadata)
    {
        return liveMigrationTaskFactory.create(UUIDs.timeBased().toString(), request, source, port, localInstanceMetadata);
    }

    /**
     * Returns all live migration data copy tasks for the current host.
     *
     * @param currentHost the host where sidecar is running
     * @return list containing at most one task (empty if no active task or if task is not a data copy task)
     */
    public List<LiveMigrationTask<LiveMigrationDataCopyResponse>> getAllTasks(@NotNull String currentHost)
    {
        List<LiveMigrationTask<?>> tasks = taskManager.getAllTasks(currentHost);
        if (tasks.isEmpty())
        {
            return Collections.emptyList();
        }

        LiveMigrationTask<?> task = tasks.get(0);
        if (isDataCopyTask(task))
        {
            return List.of(castToDataCopyTask(task));
        }

        return Collections.emptyList();
    }

    /**
     * Returns the live migration data copy task with the specified task ID.
     *
     * @param taskId      ID of the task to retrieve
     * @param currentHost the host where sidecar is running
     * @return the LiveMigrationTask matching the given taskId
     * @throws LiveMigrationTaskNotFoundException if no task found with the given ID or if task is not a data copy task
     */
    public LiveMigrationTask<LiveMigrationDataCopyResponse> getTask(@NotNull String taskId,
                                                                    @NotNull String currentHost) throws LiveMigrationTaskNotFoundException
    {
        LiveMigrationTask<?> task = taskManager.getTask(taskId, currentHost);

        if (isDataCopyTask(task))
        {
            return castToDataCopyTask(task);
        }

        throw new LiveMigrationTaskNotFoundException("Task " + taskId + " is not a data copy task");
    }

    /**
     * Cancels the live migration data copy task with the specified task ID.
     *
     * @param taskId      ID of the task to cancel
     * @param currentHost the host where sidecar is running
     * @return the cancelled LiveMigrationTask
     * @throws LiveMigrationTaskNotFoundException if no task found with the given ID or if task is not a data copy task
     */
    public LiveMigrationTask<LiveMigrationDataCopyResponse> cancelTask(@NotNull String taskId,
                                                                       @NotNull String currentHost) throws LiveMigrationTaskNotFoundException
    {
        LiveMigrationTask<?> task = taskManager.getTask(taskId, currentHost);

        if (isDataCopyTask(task))
        {
            return castToDataCopyTask(taskManager.cancelTask(taskId, currentHost));
        }

        throw new LiveMigrationTaskNotFoundException("Task " + taskId + " is not a data copy task");
    }

    private boolean isDataCopyTask(LiveMigrationTask<?> task)
    {
        return DATA_COPY_TASK_TYPE.equals(task.type());
    }

    @SuppressWarnings("unchecked")
    private LiveMigrationTask<LiveMigrationDataCopyResponse> castToDataCopyTask(LiveMigrationTask<?> task)
    {
        return (LiveMigrationTask<LiveMigrationDataCopyResponse>) task;
    }
}
