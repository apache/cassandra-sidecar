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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.request.LiveMigrationDataCopyRequest;
import org.apache.cassandra.sidecar.common.request.LiveMigrationFilesVerificationRequest;
import org.apache.cassandra.sidecar.common.response.LiveMigrationDataCopyResponse;
import org.apache.cassandra.sidecar.common.response.LiveMigrationFilesVerificationResponse;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationTaskInProgressException;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationTaskNotFoundException;
import org.apache.cassandra.sidecar.handlers.livemigration.FakeLiveMigrationTask;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.livemigration.LiveMigrationTaskManagerTestModule.DESTINATION_1;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationTaskManagerTestModule.DESTINATION_2;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationTaskManagerTestModule.DESTINATION_3;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationTaskManagerTestModule.DEST_1_ID;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationTaskManagerTestModule.DEST_2_ID;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationTaskManagerTestModule.SOURCE_1;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationTaskManagerTestModule.SOURCE_2;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Test class for {@link FilesVerificationTaskManager}.
 */
class FilesVerificationTaskManagerTest
{

    private static final int PORT = 9043;
    private final Vertx vertx = Vertx.vertx();

    private Injector getInjector()
    {
        return Guice.createInjector(new LiveMigrationTaskManagerTestModule(vertx));
    }


    @Test
    public void testGetAllTasks()
    {
        Injector injector = getInjector();

        LiveMigrationTaskManager liveMigrationTaskManager = injector.getInstance(LiveMigrationTaskManager.class);
        liveMigrationTaskManager.currentTasks.put(DEST_1_ID, getSucceededTask("task1", SOURCE_1));
        liveMigrationTaskManager.currentTasks.put(DEST_2_ID, getSucceededTask("task2", SOURCE_2));

        FilesVerificationTaskManager verificationTaskManager = injector.getInstance(FilesVerificationTaskManager.class);
        List<LiveMigrationTask<LiveMigrationFilesVerificationResponse>> tasks =
        verificationTaskManager.getAllTasks(DESTINATION_1);
        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).id()).isEqualTo("task1");

        tasks = verificationTaskManager.getAllTasks(DESTINATION_2);
        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).id()).isEqualTo("task2");

        tasks = verificationTaskManager.getAllTasks(DESTINATION_3);
        assertThat(tasks).isEmpty();
    }

    @Test
    public void testCreateTaskSuccess() throws InterruptedException
    {
        Injector injector = getInjector();
        FilesVerificationTaskManager verificationTaskManager = injector.getInstance(FilesVerificationTaskManager.class);
        InstanceMetadata mockDest1InstanceMeta = injector.getInstance(InstancesMetadata.class).instanceFromHost(DESTINATION_1);
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(2, "MD5");

        Future<LiveMigrationTask<LiveMigrationFilesVerificationResponse>> future =
        verificationTaskManager.createTask(request, SOURCE_1, mockDest1InstanceMeta);
        awaitForFuture(future);

        assertThat(future.succeeded()).isTrue();
        assertThat(future.result()).isNotNull();
        assertThat(future.result().id()).isNotNull();

        LiveMigrationTaskManager liveMigrationTaskManager = injector.getInstance(LiveMigrationTaskManager.class);
        assertThat(liveMigrationTaskManager.getAllTasks(mockDest1InstanceMeta.host())).hasSize(1);
    }

    @Test
    public void testUseFilesVerificationTaskWhenDataCopyTaskIsInProgress() throws InterruptedException
    {
        // Files verification task creation should not succeed when a data copy task is in progress.

        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = injector.getInstance(DataCopyTaskManager.class);
        InstanceMetadata mockDest1InstanceMeta = injector.getInstance(InstancesMetadata.class).instanceFromHost(DESTINATION_1);

        LiveMigrationTaskFactory liveMigrationTaskFactory = injector.getInstance(LiveMigrationTaskFactory.class);
        when(liveMigrationTaskFactory.create(anyString(), any(LiveMigrationDataCopyRequest.class),
                                             any(), anyInt(), any(InstanceMetadata.class)))
        .thenAnswer(invocationOnMock -> getInProgressDataCopyTask(invocationOnMock.getArgument(0)));

        // Create data copy task first
        awaitForFuture(dataCopyTaskManager.createTask(new LiveMigrationDataCopyRequest(1, 1, 1), DESTINATION_1));

        // Try to create verification task now
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(2, "MD5");
        FilesVerificationTaskManager verificationTaskManager = injector.getInstance(FilesVerificationTaskManager.class);
        Future<LiveMigrationTask<LiveMigrationFilesVerificationResponse>> future =
        verificationTaskManager.createTask(request, SOURCE_1, mockDest1InstanceMeta);
        awaitForFuture(future);

        assertThat(future.isComplete()).isTrue();
        assertThat(future.succeeded()).isFalse();
        assertThat(future.result()).isNull();
        assertThat(future.cause()).isNotNull().isInstanceOf(LiveMigrationTaskInProgressException.class);

        assertThat(verificationTaskManager.getAllTasks(mockDest1InstanceMeta.host())).isEmpty();
        assertThat(dataCopyTaskManager.getAllTasks(mockDest1InstanceMeta.host())).hasSize(1);

        // Data copy task ID should not be usable with files verification task manager.
        String dataCopyTaskId = dataCopyTaskManager.getAllTasks(DESTINATION_1).get(0).id();
        assertThatThrownBy(() -> verificationTaskManager.getTask(dataCopyTaskId, DESTINATION_1))
        .isInstanceOf(LiveMigrationTaskNotFoundException.class);
        assertThatThrownBy(() -> verificationTaskManager.cancelTask(dataCopyTaskId, DESTINATION_1))
        .isInstanceOf(LiveMigrationTaskNotFoundException.class);
    }

    @Test
    public void testCreateTaskWhenAnotherTaskInProgress() throws InterruptedException
    {
        Injector injector = getInjector();
        LiveMigrationTaskManager liveMigrationTaskManager = injector.getInstance(LiveMigrationTaskManager.class);
        FilesVerificationTaskManager verificationTaskManager = injector.getInstance(FilesVerificationTaskManager.class);
        InstanceMetadata mockDest1InstanceMeta = injector.getInstance(InstancesMetadata.class).instanceFromHost(DESTINATION_1);

        // Add an in-progress task
        LiveMigrationTask<LiveMigrationFilesVerificationResponse> inProgressTask = getInProgressTask("existing-task");
        liveMigrationTaskManager.currentTasks.put(DEST_1_ID, inProgressTask);

        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(2, "MD5");
        Future<LiveMigrationTask<LiveMigrationFilesVerificationResponse>> future = verificationTaskManager.createTask(request, SOURCE_1, mockDest1InstanceMeta);
        awaitForFuture(future);

        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(LiveMigrationTaskInProgressException.class);
        assertThat(future.cause().getMessage()).contains("Another files digest verification is in progress");
    }

    @Test
    public void testCreateTaskWhenPreviousTaskCompleted() throws InterruptedException
    {
        Injector injector = getInjector();
        LiveMigrationTaskManager liveMigrationTaskManager = injector.getInstance(LiveMigrationTaskManager.class);
        FilesVerificationTaskManager verificationTaskManager = injector.getInstance(FilesVerificationTaskManager.class);
        InstanceMetadata mockDest1InstanceMeta = injector.getInstance(InstancesMetadata.class).instanceFromHost(DESTINATION_1);

        // Add a completed task
        LiveMigrationTask<LiveMigrationFilesVerificationResponse> completedTask =
        getSucceededTask("completed-task", SOURCE_1);
        liveMigrationTaskManager.currentTasks.put(DEST_1_ID, completedTask);

        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(2, "MD5");
        Future<LiveMigrationTask<LiveMigrationFilesVerificationResponse>> future =
        verificationTaskManager.createTask(request, SOURCE_1, mockDest1InstanceMeta);
        awaitForFuture(future);

        assertThat(future.succeeded()).isTrue();
        assertThat(future.result()).isNotNull();
        assertThat(future.result().id()).isNotEqualTo("completed-task");
    }

    /**
     * Tests that multiple tasks can be created sequentially after each previous task completes.
     * This validates that the system properly allows new task creation once a task has finished,
     * and that each sequential task gets a unique ID and properly replaces the previous one.
     */
    @Test
    public void testMultipleSequentialTaskCreations() throws InterruptedException
    {
        Injector injector = getInjector();
        FilesVerificationTaskManager verificationTaskManager = injector.getInstance(FilesVerificationTaskManager.class);
        LiveMigrationTaskManager liveMigrationTaskManager = injector.getInstance(LiveMigrationTaskManager.class);
        InstanceMetadata mockDest1InstanceMeta = injector.getInstance(InstancesMetadata.class).instanceFromHost(DESTINATION_1);

        LiveMigrationFilesVerificationTaskFactory mockTaskFactory = injector.getInstance(LiveMigrationFilesVerificationTaskFactory.class);
        when(mockTaskFactory.create(anyString(), anyString(), anyInt(), any(LiveMigrationFilesVerificationRequest.class), any(InstanceMetadata.class)))
        .thenAnswer(invocation -> {
            String taskId = invocation.getArgument(0);
            String sourceHost = invocation.getArgument(1);
            return getSucceededTask(taskId, sourceHost);
        });

        final int numberOfSequentialTasks = 5;
        ConcurrentLinkedQueue<String> taskIds = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < numberOfSequentialTasks; i++)
        {
            LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(2, "MD5");
            Future<LiveMigrationTask<LiveMigrationFilesVerificationResponse>> future =
            verificationTaskManager.createTask(request, SOURCE_1, mockDest1InstanceMeta);
            awaitForFuture(future);

            assertThat(future.succeeded()).isTrue();
            assertThat(future.result()).isNotNull();
            assertThat(future.result().id()).isNotNull();

            String currentTaskId = future.result().id();
            taskIds.add(currentTaskId);

            // Verify the task is in the manager
            assertThat(liveMigrationTaskManager.getAllTasks(mockDest1InstanceMeta.host())).hasSize(1);
            assertThat(liveMigrationTaskManager.getAllTasks(mockDest1InstanceMeta.host()).get(0).id())
            .isEqualTo(currentTaskId);

            // Verify task is marked as completed
            LiveMigrationTask<LiveMigrationFilesVerificationResponse> currentTask =
            verificationTaskManager.getTask(currentTaskId, DESTINATION_1);
            assertThat(currentTask.isCompleted()).isTrue();
        }

        // Verify all task IDs are unique
        assertThat(taskIds).doesNotHaveDuplicates();
        assertThat(taskIds).hasSize(numberOfSequentialTasks);

        // Verify the last task is still in the manager
        assertThat(liveMigrationTaskManager.getAllTasks(mockDest1InstanceMeta.host())).hasSize(1);
    }

    @Test
    public void testGetTaskSuccess()
    {
        Injector injector = getInjector();
        LiveMigrationTaskManager liveMigrationTaskManager = injector.getInstance(LiveMigrationTaskManager.class);
        FilesVerificationTaskManager verificationTaskManager = injector.getInstance(FilesVerificationTaskManager.class);

        LiveMigrationTask<LiveMigrationFilesVerificationResponse> task = getSucceededTask("test-task", SOURCE_1);
        liveMigrationTaskManager.currentTasks.put(DEST_1_ID, task);

        LiveMigrationTask<LiveMigrationFilesVerificationResponse> retrievedTask =
        verificationTaskManager.getTask("test-task", DESTINATION_1);

        assertThat(retrievedTask).isNotNull();
        assertThat(retrievedTask.id()).isEqualTo("test-task");

        assertThat(retrievedTask.id()).isEqualTo(verificationTaskManager.getAllTasks(DESTINATION_1).get(0).id());
    }

    @Test
    public void testGetTaskNotFound()
    {
        Injector injector = getInjector();
        FilesVerificationTaskManager verificationTaskManager = injector.getInstance(FilesVerificationTaskManager.class);

        assertThatThrownBy(() -> verificationTaskManager.getTask("non-existent-task", DESTINATION_1))
        .isInstanceOf(LiveMigrationTaskNotFoundException.class);
    }

    @Test
    public void testGetTaskWrongTaskId()
    {
        Injector injector = getInjector();
        LiveMigrationTaskManager liveMigrationTaskManager = injector.getInstance(LiveMigrationTaskManager.class);
        FilesVerificationTaskManager verificationTaskManager = injector.getInstance(FilesVerificationTaskManager.class);

        LiveMigrationTask<LiveMigrationFilesVerificationResponse> task = getSucceededTask("actual-task", SOURCE_1);
        liveMigrationTaskManager.currentTasks.put(DEST_1_ID, task);

        assertThatThrownBy(() -> verificationTaskManager.getTask("wrong-task-id", DESTINATION_1))
        .isInstanceOf(LiveMigrationTaskNotFoundException.class);
    }

    @Test
    public void testCancelTaskSuccess()
    {
        Injector injector = getInjector();
        LiveMigrationTaskManager liveMigrationTaskManager = injector.getInstance(LiveMigrationTaskManager.class);
        FilesVerificationTaskManager verificationTaskManager = injector.getInstance(FilesVerificationTaskManager.class);

        LiveMigrationTask<LiveMigrationFilesVerificationResponse> task = getInProgressTask("cancelable-task");
        liveMigrationTaskManager.currentTasks.put(DEST_1_ID, task);

        LiveMigrationTask<LiveMigrationFilesVerificationResponse> cancelledTask =
        verificationTaskManager.cancelTask("cancelable-task", DESTINATION_1);

        assertThat(cancelledTask).isNotNull();
        assertThat(cancelledTask.id()).isEqualTo("cancelable-task");
        assertThat(cancelledTask.isCompleted()).isTrue(); // FakeLiveMigrationFilesVerificationTask returns true when cancelled
    }

    @Test
    public void testCancelTaskNotFound()
    {
        Injector injector = getInjector();
        FilesVerificationTaskManager verificationTaskManager = injector.getInstance(FilesVerificationTaskManager.class);

        assertThatThrownBy(() -> verificationTaskManager.cancelTask("non-existent-task", DESTINATION_1))
        .isInstanceOf(LiveMigrationTaskNotFoundException.class);
    }

    /**
     * Tests concurrent task creation to ensure only one task can be active at a time per instance.
     * This test specifically validates the atomic compute() operation in createTask() method.
     */
    @Test
    public void testConcurrentTaskCreation() throws InterruptedException
    {
        Injector injector = getInjector();
        LiveMigrationTaskManager liveMigrationTaskManager = injector.getInstance(LiveMigrationTaskManager.class);
        FilesVerificationTaskManager verificationTaskManager = injector.getInstance(FilesVerificationTaskManager.class);
        InstanceMetadata mockDest1InstanceMeta = injector.getInstance(InstancesMetadata.class).instanceFromHost(DESTINATION_1);

        LiveMigrationFilesVerificationTaskFactory mockTaskFactory = injector.getInstance(LiveMigrationFilesVerificationTaskFactory.class);
        when(mockTaskFactory.create(anyString(), anyString(), anyInt(), any(LiveMigrationFilesVerificationRequest.class), any(InstanceMetadata.class)))
        .thenAnswer(invocation -> {
            String taskId = invocation.getArgument(0);
            return getInProgressTask(taskId);
        });

        final int numberOfThreads = 20;
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch completionLatch = new CountDownLatch(numberOfThreads);

        final AtomicInteger successCount = new AtomicInteger(0);
        final AtomicInteger failureCount = new AtomicInteger(0);
        final ConcurrentLinkedQueue<Future<LiveMigrationTask<LiveMigrationFilesVerificationResponse>>> results = new ConcurrentLinkedQueue<>();

        try
        {
            // Launch multiple threads that all try to create tasks simultaneously
            for (int i = 0; i < numberOfThreads; i++)
            {
                executor.submit(() -> {
                    try
                    {
                        // Wait for all threads to be ready before starting
                        startLatch.await();

                        LiveMigrationFilesVerificationRequest request
                        = new LiveMigrationFilesVerificationRequest(2, "MD5");
                        Future<LiveMigrationTask<LiveMigrationFilesVerificationResponse>> future = verificationTaskManager.createTask(request, SOURCE_1, mockDest1InstanceMeta);
                        results.add(future);

                        // Use onComplete instead of awaitForFuture
                        future.onComplete(result -> {
                            if (result.succeeded())
                            {
                                successCount.incrementAndGet();
                            }
                            else
                            {
                                failureCount.incrementAndGet();
                            }
                            completionLatch.countDown();
                        });
                    }
                    catch (Exception e)
                    {
                        failureCount.incrementAndGet();
                        completionLatch.countDown();
                    }
                });
            }

            // Release all threads at once to maximize concurrency
            startLatch.countDown();

            // Wait for all tasks to complete
            assertThat(completionLatch.await(5, TimeUnit.SECONDS)).isTrue();

            // Verify concurrency behavior
            assertThat(successCount.get()).isEqualTo(1); // Only one task should succeed
            assertThat(failureCount.get()).isEqualTo(numberOfThreads - 1); // All others should fail
            assertThat(liveMigrationTaskManager.currentTasks).hasSize(1); // Only one task in the map

            // Verify the successful task
            LiveMigrationTask<?> successfulTask = liveMigrationTaskManager.currentTasks.values().iterator().next();
            assertThat(successfulTask).isNotNull();
            assertThat(successfulTask.id()).isNotNull();

            // Verify all failed tasks have the correct exception
            long inProgressExceptions = results.stream()
                                               .filter(Future::failed)
                                               .mapToLong(f -> f.cause() instanceof LiveMigrationTaskInProgressException ? 1 : 0)
                                               .sum();
            assertThat(inProgressExceptions).isEqualTo(numberOfThreads - 1);
        }
        finally
        {
            executor.shutdown();
            assertThat(executor.awaitTermination(1, TimeUnit.SECONDS)).isTrue();
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private <T> void awaitForFuture(Future<T> future) throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(1);
        future.onComplete(res -> latch.countDown());

        latch.await(5, TimeUnit.SECONDS);
    }

    private LiveMigrationTask<LiveMigrationFilesVerificationResponse> getInProgressTask(@NotNull String taskId)
    {
        LiveMigrationFilesVerificationResponse response = new LiveMigrationFilesVerificationResponse(
        taskId, "MD5", "IN_PROGRESS", SOURCE_1, PORT, 0, 0, 0, 0, 0, 0, 0
        );
        return new FakeFilesVerificationTask(response);
    }

    private LiveMigrationTask<LiveMigrationFilesVerificationResponse> getSucceededTask(@NotNull String taskId, @NotNull String sourceHost)
    {
        LiveMigrationFilesVerificationResponse response = new LiveMigrationFilesVerificationResponse(
        taskId, "MD5", "COMPLETED", sourceHost, PORT, 0, 0, 10, 0, 0, 0, 10
        );
        return new FakeFilesVerificationTask(response);
    }

    private LiveMigrationTask<LiveMigrationDataCopyResponse> getInProgressDataCopyTask(@NotNull String taskId)
    {
        List<LiveMigrationDataCopyResponse.Status> statusList =
        List.of(new LiveMigrationDataCopyResponse.Status(0, "PREPARING", 500L, 1, 1, 1, 0, 0, 500L));
        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 1);
        LiveMigrationDataCopyResponse response = new LiveMigrationDataCopyResponse(taskId, SOURCE_1, PORT, request, statusList);
        return new FakeLiveMigrationTask(response);
    }
}
