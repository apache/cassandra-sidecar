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
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationInvalidRequestException;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationTaskInProgressException;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationTaskNotFoundException;
import org.apache.cassandra.sidecar.handlers.livemigration.FakeLiveMigrationTask;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException.Service.CQL_AND_JMX;
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
 * Test class for {@link DataCopyTaskManager}.
 */
public class DataCopyTaskManagerTest
{
    private final Vertx vertx = Vertx.vertx();

    private Injector getInjector()
    {
        return Guice.createInjector(new LiveMigrationTaskManagerTestModule(vertx));
    }

    @Test
    public void getAllTasks()
    {
        Injector injector = getInjector();
        LiveMigrationTaskManager liveMigrationTaskManager = injector.getInstance(LiveMigrationTaskManager.class);
        DataCopyTaskManager dataCopyTaskManager = injector.getInstance(DataCopyTaskManager.class);

        liveMigrationTaskManager.currentTasks.put(DEST_1_ID, getSucceededTask("task1", SOURCE_1));
        liveMigrationTaskManager.currentTasks.put(DEST_2_ID, getSucceededTask("task2", SOURCE_2));

        List<LiveMigrationTask<LiveMigrationDataCopyResponse>> tasks = dataCopyTaskManager.getAllTasks(DESTINATION_1);
        assertThat(tasks.get(0).id()).isEqualTo("task1");

        tasks = dataCopyTaskManager.getAllTasks(DESTINATION_2);
        assertThat(tasks.get(0).id()).isEqualTo("task2");

        tasks = dataCopyTaskManager.getAllTasks(DESTINATION_3);
        assertThat(tasks).isEmpty();
    }

    @Test
    public void testCreateTaskSuccess() throws InterruptedException
    {
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = injector.getInstance(DataCopyTaskManager.class);
        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 2);

        Future<LiveMigrationTask<LiveMigrationDataCopyResponse>> future = dataCopyTaskManager.createTask(request, DESTINATION_1);
        awaitForFuture(future);

        assertThat(future.succeeded()).isTrue();
        assertThat(future.result()).isNotNull();
        assertThat(future.result().id()).isNotNull();
    }

    @Test
    void testUseeDataCopyTaskWhenFilesVerificationTaskIsInProgress() throws InterruptedException
    {
        // Trying to create a data copy task while a files verification task in progress should not succeed
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = injector.getInstance(DataCopyTaskManager.class);
        FilesVerificationTaskManager filesVerificationTaskManager = injector.getInstance(FilesVerificationTaskManager.class);

        LiveMigrationFilesVerificationTaskFactory verificationTaskFactory = injector.getInstance(LiveMigrationFilesVerificationTaskFactory.class);
        when(verificationTaskFactory.create(anyString(), anyString(), anyInt(),
                                            any(LiveMigrationFilesVerificationRequest.class), any(InstanceMetadata.class)))
        .thenAnswer(invocation -> {
            String id = invocation.getArgument(0);
            String source = invocation.getArgument(1);
            int port = invocation.getArgument(2);

            return new FakeFilesVerificationTask(
            new LiveMigrationFilesVerificationResponse(id, "MD5", "IN_PROGRESS", source, port, 0, 0, 0, 0, 0, 0, 0));
        });

        InstanceMetadata mockDest1InstanceMeta = injector.getInstance(InstancesMetadata.class).instanceFromHost(DESTINATION_1);
        filesVerificationTaskManager.createTask(new LiveMigrationFilesVerificationRequest(1, "md5"),
                                                SOURCE_1, mockDest1InstanceMeta);

        Future<LiveMigrationTask<LiveMigrationDataCopyResponse>> future = dataCopyTaskManager.createTask(
        new LiveMigrationDataCopyRequest(1, 0.4, 1), DESTINATION_1);
        awaitForFuture(future);

        assertThat(future.isComplete()).isTrue();
        assertThat(future.result()).isNull();
        assertThat(future.cause()).isNotNull();
        assertThat(future.cause()).isInstanceOf(LiveMigrationTaskInProgressException.class);

        assertThat(dataCopyTaskManager.getAllTasks(DESTINATION_1)).isEmpty();

        // Files verification task should not be usable with data copy task manager
        String verificationTaskId = filesVerificationTaskManager.getAllTasks(DESTINATION_1).get(0).id();
        assertThatThrownBy(() -> dataCopyTaskManager.getTask(verificationTaskId, DESTINATION_1))
        .isInstanceOf(LiveMigrationTaskNotFoundException.class);
        assertThatThrownBy(() -> dataCopyTaskManager.cancelTask(verificationTaskId, DESTINATION_1))
        .isInstanceOf(LiveMigrationTaskNotFoundException.class);
    }

    @Test
    public void testCreateTaskWithMaxConcurrencyExceeded() throws InterruptedException
    {
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = injector.getInstance(DataCopyTaskManager.class);
        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 10); // exceeds max concurrency of 5

        Future<LiveMigrationTask<LiveMigrationDataCopyResponse>> future = dataCopyTaskManager.createTask(request, DESTINATION_1);
        awaitForFuture(future);

        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(LiveMigrationInvalidRequestException.class);
        assertThat(future.cause().getMessage()).contains("max concurrency can not be more than 5");
    }

    @Test
    public void testCreateTaskWhenAnotherTaskInProgress() throws InterruptedException
    {
        Injector injector = getInjector();
        LiveMigrationTaskManager liveMigrationTaskManager = injector.getInstance(LiveMigrationTaskManager.class);
        DataCopyTaskManager dataCopyTaskManager = injector.getInstance(DataCopyTaskManager.class);

        // Add an in-progress task
        LiveMigrationTask<LiveMigrationDataCopyResponse> inProgressTask = getInProgressTask("existing-task");
        liveMigrationTaskManager.currentTasks.put(DEST_1_ID, inProgressTask);

        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 2);
        Future<LiveMigrationTask<LiveMigrationDataCopyResponse>> future = dataCopyTaskManager.createTask(request, DESTINATION_1);
        awaitForFuture(future);

        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(LiveMigrationTaskInProgressException.class);
        assertThat(future.cause().getMessage()).contains("Another task is already under progress");
    }

    @Test
    public void testCreateTaskWhenPreviousTaskCompleted() throws InterruptedException
    {
        Injector injector = getInjector();
        LiveMigrationTaskManager liveMigrationTaskManager = injector.getInstance(LiveMigrationTaskManager.class);
        DataCopyTaskManager dataCopyTaskManager = injector.getInstance(DataCopyTaskManager.class);

        // Add a completed task
        LiveMigrationTask<LiveMigrationDataCopyResponse> completedTask = getSucceededTask("completed-task", SOURCE_1);
        liveMigrationTaskManager.currentTasks.put(DEST_1_ID, completedTask);

        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 2);
        Future<LiveMigrationTask<LiveMigrationDataCopyResponse>> future = dataCopyTaskManager.createTask(request, DESTINATION_1);
        awaitForFuture(future);

        assertThat(future.succeeded()).isTrue();
        assertThat(future.result()).isNotNull();
        assertThat(future.result().id()).isNotEqualTo("completed-task");
    }

    @Test
    public void testCreateTaskShouldFailWhenCassandraInstanceJMXIsUp() throws InterruptedException
    {
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = injector.getInstance(DataCopyTaskManager.class);
        InstancesMetadata instancesMetadata = injector.getInstance(InstancesMetadata.class);
        InstanceMetadata destinationMetadata = instancesMetadata.instanceFromHost(DESTINATION_1);

        // Mocking JMX as up
        when(destinationMetadata.delegate().isJmxUp()).thenReturn(true);

        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 2);
        Future<LiveMigrationTask<LiveMigrationDataCopyResponse>> future = dataCopyTaskManager.createTask(request, DESTINATION_1);
        awaitForFuture(future);

        assertThat(future.succeeded()).isFalse();
        assertThat(future.failed()).isTrue();
        assertThat(future.result()).isNull();
        assertThat(future.cause()).isNotNull();
        assertThat(future.cause()).isInstanceOf(LiveMigrationInvalidRequestException.class);
    }

    @Test
    public void testCreateTaskShouldFailWhenCassandraInstanceNativeIsUp() throws InterruptedException
    {
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = injector.getInstance(DataCopyTaskManager.class);
        InstancesMetadata instancesMetadata = injector.getInstance(InstancesMetadata.class);
        InstanceMetadata destinationMetadata = instancesMetadata.instanceFromHost(DESTINATION_1);

        // Mocking native (CQL) as up but JMX as down
        when(destinationMetadata.delegate().isJmxUp()).thenReturn(false);
        when(destinationMetadata.delegate().isNativeUp()).thenReturn(true);

        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 2);
        Future<LiveMigrationTask<LiveMigrationDataCopyResponse>> future = dataCopyTaskManager.createTask(request, DESTINATION_1);
        awaitForFuture(future);

        assertThat(future.succeeded()).isFalse();
        assertThat(future.failed()).isTrue();
        assertThat(future.result()).isNull();
        assertThat(future.cause()).isNotNull();
        assertThat(future.cause()).isInstanceOf(LiveMigrationInvalidRequestException.class);
    }

    @Test
    public void testCreateTaskShouldSucceedWhenCassandraAdapterIsNotAvailable() throws InterruptedException
    {
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = injector.getInstance(DataCopyTaskManager.class);
        InstancesMetadata instancesMetadata = injector.getInstance(InstancesMetadata.class);
        InstanceMetadata destinationMetadata = instancesMetadata.instanceFromHost(DESTINATION_1);
        when(destinationMetadata.delegate())
        .thenThrow(new CassandraUnavailableException(CQL_AND_JMX, "CassandraAdapterDelegate is not available"));

        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 2);
        Future<LiveMigrationTask<LiveMigrationDataCopyResponse>> future = dataCopyTaskManager.createTask(request, DESTINATION_1);
        awaitForFuture(future);

        assertThat(future.succeeded()).isTrue();
        assertThat(future.failed()).isFalse();
        assertThat(future.result()).isNotNull();
        assertThat(future.result().id()).isNotNull();
        assertThat(future.cause()).isNull();
    }

    @Test
    public void testGetTaskSuccess()
    {
        Injector injector = getInjector();
        LiveMigrationTaskManager liveMigrationTaskManager = injector.getInstance(LiveMigrationTaskManager.class);
        DataCopyTaskManager dataCopyTaskManager = injector.getInstance(DataCopyTaskManager.class);

        LiveMigrationTask<LiveMigrationDataCopyResponse> task = getSucceededTask("test-task", SOURCE_1);
        liveMigrationTaskManager.currentTasks.put(DEST_1_ID, task);

        LiveMigrationTask<LiveMigrationDataCopyResponse> retrievedTask = dataCopyTaskManager.getTask("test-task", DESTINATION_1);

        assertThat(retrievedTask).isNotNull();
        assertThat(retrievedTask.id()).isEqualTo("test-task");
    }

    @Test
    public void testGetTaskNotFound()
    {
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = injector.getInstance(DataCopyTaskManager.class);

        assertThatThrownBy(() -> dataCopyTaskManager.getTask("non-existent-task", DESTINATION_1))
        .isInstanceOf(LiveMigrationTaskNotFoundException.class);
    }

    @Test
    public void testGetTaskWrongTaskId()
    {
        Injector injector = getInjector();
        LiveMigrationTaskManager liveMigrationTaskManager = injector.getInstance(LiveMigrationTaskManager.class);
        DataCopyTaskManager dataCopyTaskManager = injector.getInstance(DataCopyTaskManager.class);

        LiveMigrationTask<LiveMigrationDataCopyResponse> task = getSucceededTask("actual-task", SOURCE_1);
        liveMigrationTaskManager.currentTasks.put(DEST_1_ID, task);

        assertThatThrownBy(() -> dataCopyTaskManager.getTask("wrong-task-id", DESTINATION_1))
        .isInstanceOf(LiveMigrationTaskNotFoundException.class);
    }

    @Test
    public void testCancelTaskSuccess()
    {
        Injector injector = getInjector();
        LiveMigrationTaskManager liveMigrationTaskManager = injector.getInstance(LiveMigrationTaskManager.class);
        DataCopyTaskManager dataCopyTaskManager = injector.getInstance(DataCopyTaskManager.class);

        LiveMigrationTask<LiveMigrationDataCopyResponse> task = getInProgressTask("cancelable-task");
        liveMigrationTaskManager.currentTasks.put(DEST_1_ID, task);

        LiveMigrationTask<LiveMigrationDataCopyResponse> cancelledTask = dataCopyTaskManager.cancelTask("cancelable-task", DESTINATION_1);

        assertThat(cancelledTask).isNotNull();
        assertThat(cancelledTask.id()).isEqualTo("cancelable-task");
        assertThat(cancelledTask.isCompleted()).isTrue(); // FakeLiveMigrationTask returns true when cancelled
    }

    @Test
    public void testCancelTaskNotFound()
    {
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = injector.getInstance(DataCopyTaskManager.class);

        assertThatThrownBy(() -> dataCopyTaskManager.cancelTask("non-existent-task", DESTINATION_1))
        .isInstanceOf(LiveMigrationTaskNotFoundException.class);
    }

    /**
     * Tests concurrent task creation to ensure only one task can be active at a time per instance.
     * This test specifically validates the atomic compute() operation in createDataCopyTask() method.
     */
    @Test
    public void testConcurrentTaskCreation() throws InterruptedException
    {
        Injector injector = getInjector();
        LiveMigrationTaskManager liveMigrationTaskManager = injector.getInstance(LiveMigrationTaskManager.class);
        DataCopyTaskManager dataCopyTaskManager = injector.getInstance(DataCopyTaskManager.class);

        LiveMigrationTaskFactory mockTaskFactory = injector.getInstance(LiveMigrationTaskFactory.class);
        when(mockTaskFactory.create(anyString(), any(LiveMigrationDataCopyRequest.class), anyString(), anyInt(), any(InstanceMetadata.class)))
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
        final ConcurrentLinkedQueue<Future<LiveMigrationTask<LiveMigrationDataCopyResponse>>> results = new ConcurrentLinkedQueue<>();

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

                        LiveMigrationDataCopyRequest request
                        = new LiveMigrationDataCopyRequest(1, 1.0, 2);
                        Future<LiveMigrationTask<LiveMigrationDataCopyResponse>> future = dataCopyTaskManager.createTask(request, DESTINATION_1);
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

        latch.await(2, TimeUnit.SECONDS);
    }

    private LiveMigrationTask<LiveMigrationDataCopyResponse> getInProgressTask(@NotNull String taskId)
    {
        List<LiveMigrationDataCopyResponse.Status> statusList =
        List.of(new LiveMigrationDataCopyResponse.Status(0, "PREPARING", 500L, 1, 1, 1, 0, 0, 500L));
        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 1);
        LiveMigrationDataCopyResponse response = new LiveMigrationDataCopyResponse(taskId, SOURCE_1, 9043, request, statusList);
        return new FakeLiveMigrationTask(response);
    }

    private LiveMigrationTask<LiveMigrationDataCopyResponse> getSucceededTask(@NotNull String taskId, @NotNull String sourceHost)
    {
        List<LiveMigrationDataCopyResponse.Status> statusList =
        List.of(new LiveMigrationDataCopyResponse.Status(0, "SUCCESS", 1000L, 1, 1, 1, 1, 0, 1000L));
        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 1);
        LiveMigrationDataCopyResponse response = new LiveMigrationDataCopyResponse(taskId, sourceHost, 9043, request, statusList);
        return new FakeLiveMigrationTask(response);
    }
}
