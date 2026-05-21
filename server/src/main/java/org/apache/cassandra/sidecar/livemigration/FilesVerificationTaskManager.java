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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.request.LiveMigrationFilesVerificationRequest;
import org.apache.cassandra.sidecar.common.response.LiveMigrationFilesVerificationResponse;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationInvalidRequestException;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationTaskInProgressException;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationTaskNotFoundException;
import org.apache.cassandra.sidecar.utils.DigestAlgorithmFactory;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.livemigration.LiveMigrationFilesVerificationTask.FILES_VERIFICATION_TASK_TYPE;

/**
 * Manages the lifecycle of file digest verification tasks during live migration operations.
 * This manager ensures that only one {@link LiveMigrationTask} can be active per instance at a time,
 * preventing concurrent migration operations that could impact system resources.
 * Tasks are created using the {@link LiveMigrationFilesVerificationTaskFactory} and
 * executed asynchronously to validate file integrity between source and destination nodes.
 */
@Singleton
public class FilesVerificationTaskManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FilesVerificationTaskManager.class);

    private final LiveMigrationTaskManager taskManager;
    private final LiveMigrationFilesVerificationTaskFactory taskFactory;
    private final SidecarConfiguration sidecarConfiguration;

    @Inject
    public FilesVerificationTaskManager(LiveMigrationTaskManager taskManager,
                                        SidecarConfiguration sidecarConfiguration,
                                        LiveMigrationFilesVerificationTaskFactory taskFactory)
    {
        this.taskManager = taskManager;
        this.sidecarConfiguration = sidecarConfiguration;
        this.taskFactory = taskFactory;
    }

    /**
     * Creates and submits a new file digest verification task for the specified instance.
     * Only one task (of any type) can be active per instance at a time. If a task is already running,
     * this method returns a failed future with {@link LiveMigrationTaskInProgressException}.
     *
     * @param request               the file verification request containing digest information
     * @param source                the source identifier for the verification request
     * @param localInstanceMetadata metadata of the local Cassandra instance
     * @return a future that succeeds if the task is created and started, or fails with
     * {@link LiveMigrationTaskInProgressException} if another task is already in progress
     */
    public Future<LiveMigrationTask<LiveMigrationFilesVerificationResponse>> createTask(LiveMigrationFilesVerificationRequest request,
                                                                                        String source,
                                                                                        InstanceMetadata localInstanceMetadata)
    {
        int maxPossibleConcurrency = sidecarConfiguration.liveMigrationConfiguration().maxConcurrentFileRequests();
        if (request.maxConcurrency() > maxPossibleConcurrency)
        {
            return Future.failedFuture(
            new LiveMigrationInvalidRequestException("max concurrency can not be more than " + maxPossibleConcurrency));
        }
        try
        {
            DigestAlgorithmFactory.validateAlgorithmName(request.digestAlgorithm());
        }
        catch (IllegalArgumentException iae)
        {
            return Future.failedFuture(new LiveMigrationInvalidRequestException(iae.getMessage(), iae));
        }

        String timeUuid = UUIDs.timeBased().toString();
        int sidecarPort = sidecarConfiguration.serviceConfiguration().port();
        LiveMigrationTask<LiveMigrationFilesVerificationResponse> newTask =
        taskFactory.create(timeUuid, source, sidecarPort, request, localInstanceMetadata);

        boolean accepted = taskManager.submitTask(localInstanceMetadata.id(), newTask);

        if (accepted)
        {
            newTask.start();
            LOGGER.info("Accepted new files digest verification task for instance={} taskId={}",
                        localInstanceMetadata.id(), newTask.id());
            return Future.succeededFuture(newTask);
        }
        else
        {
            return Future.failedFuture(new LiveMigrationTaskInProgressException(
            "Another files digest verification is in progress for instance=" + localInstanceMetadata.id()));
        }
    }

    /**
     * Returns all live migration files verification tasks for the current host.
     *
     * @param currentHost the host where sidecar is running
     * @return list containing at most one task (empty if no active task or if task is not a files verification task)
     */
    public List<LiveMigrationTask<LiveMigrationFilesVerificationResponse>> getAllTasks(@NotNull String currentHost)
    {
        List<LiveMigrationTask<?>> tasks = taskManager.getAllTasks(currentHost);
        if (tasks.isEmpty())
        {
            return List.of();
        }

        LiveMigrationTask<?> task = tasks.get(0);
        if (isFilesVerificationTask(task))
        {
            return List.of(castToFilesVerificationTask(task));
        }

        return List.of();
    }

    /**
     * Returns the files verification task with the specified task ID.
     *
     * @param taskId      ID of the task to retrieve
     * @param currentHost the host where sidecar is running
     * @return the LiveMigrationTask matching the given taskId
     * @throws LiveMigrationTaskNotFoundException if no task found with the given ID or if task is not a files verification task
     */
    public LiveMigrationTask<LiveMigrationFilesVerificationResponse> getTask(@NotNull String taskId,
                                                                             @NotNull String currentHost) throws LiveMigrationTaskNotFoundException
    {
        LiveMigrationTask<?> task = taskManager.getTask(taskId, currentHost);

        if (isFilesVerificationTask(task))
        {
            return castToFilesVerificationTask(task);
        }

        throw new LiveMigrationTaskNotFoundException("Task " + taskId + " is not a files verification task");
    }

    /**
     * Cancels the files verification task with the specified task ID.
     *
     * @param taskId      ID of the task to cancel
     * @param currentHost the host where sidecar is running
     * @return the cancelled LiveMigrationTask
     * @throws LiveMigrationTaskNotFoundException if no task found with the given ID or if task is not a files verification task
     */
    public LiveMigrationTask<LiveMigrationFilesVerificationResponse> cancelTask(@NotNull String taskId,
                                                                                @NotNull String currentHost) throws LiveMigrationTaskNotFoundException
    {
        LiveMigrationTask<?> task = taskManager.getTask(taskId, currentHost);

        if (isFilesVerificationTask(task))
        {
            return castToFilesVerificationTask(taskManager.cancelTask(taskId, currentHost));
        }

        throw new LiveMigrationTaskNotFoundException("Task " + taskId + " is not a files verification task");
    }

    private boolean isFilesVerificationTask(LiveMigrationTask<?> task)
    {
        return FILES_VERIFICATION_TASK_TYPE.equals(task.type());
    }

    @SuppressWarnings("unchecked")
    private LiveMigrationTask<LiveMigrationFilesVerificationResponse> castToFilesVerificationTask(LiveMigrationTask<?> task)
    {
        return (LiveMigrationTask<LiveMigrationFilesVerificationResponse>) task;
    }
}
