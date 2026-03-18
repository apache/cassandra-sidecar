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

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.request.LiveMigrationDataCopyRequest;
import org.apache.cassandra.sidecar.common.response.LiveMigrationTaskResponse;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationDataCopyInProgressException;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationInvalidRequestException;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationTaskNotFoundException;
import org.apache.cassandra.sidecar.handlers.livemigration.FakeLiveMigrationTask;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationMap;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException.Service.CQL_AND_JMX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test class for {@link DataCopyTaskManager}.
 */
public class DataCopyTaskManagerTest
{
    private static final String source1Name = "source1";
    private static final String source2Name = "source2";
    private static final int dest1Id = 200001;
    private static final int dest2Id = 200002;
    private static final int dest3Id = 200003;
    private static final String dest1Name = "destination1";
    private static final String dest2Name = "destination2";
    private static final String dest3Name = "destination3";

    private Injector getInjector()
    {
        return Guice.createInjector(new DataCopyTaskManagerTestModule());
    }

    @Test
    public void getAllTasks()
    {
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = getDataCopyTaskManager(injector);

        dataCopyTaskManager.currentTasks.put(dest1Id, getSucceededTask("task1", source1Name));
        dataCopyTaskManager.currentTasks.put(dest2Id, getSucceededTask("task2", source2Name));

        List<LiveMigrationTask> tasks = dataCopyTaskManager.getAllTasks(dest1Name);
        assertThat(tasks.get(0).id()).isEqualTo("task1");

        tasks = dataCopyTaskManager.getAllTasks(dest2Name);
        assertThat(tasks.get(0).id()).isEqualTo("task2");

        tasks = dataCopyTaskManager.getAllTasks(dest3Name);
        assertThat(tasks).isEmpty();
    }

    @Test
    public void testCreateTaskSuccess() throws InterruptedException
    {
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = getDataCopyTaskManager(injector);
        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 2);

        Future<LiveMigrationTask> future = dataCopyTaskManager.createTask(request, dest1Name);
        awaitForFuture(future);

        assertThat(future.succeeded()).isTrue();
        assertThat(future.result()).isNotNull();
        assertThat(future.result().id()).isNotNull();
    }

    @Test
    public void testCreateTaskWithMaxConcurrencyExceeded() throws InterruptedException
    {
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = getDataCopyTaskManager(injector);
        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 10); // exceeds max concurrency of 5

        Future<LiveMigrationTask> future = dataCopyTaskManager.createTask(request, dest1Name);
        awaitForFuture(future);

        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(LiveMigrationInvalidRequestException.class);
        assertThat(future.cause().getMessage()).contains("max concurrency can not be more than 5");
    }

    @Test
    public void testCreateTaskWhenAnotherTaskInProgress() throws InterruptedException
    {
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = getDataCopyTaskManager(injector);

        // Add an in-progress task
        LiveMigrationTask inProgressTask = getInProgressTask("existing-task");
        dataCopyTaskManager.currentTasks.put(dest1Id, inProgressTask);

        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 2);
        Future<LiveMigrationTask> future = dataCopyTaskManager.createTask(request, dest1Name);
        awaitForFuture(future);

        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(LiveMigrationDataCopyInProgressException.class);
        assertThat(future.cause().getMessage()).contains("Another task is already under progress");
    }

    @Test
    public void testCreateTaskWhenPreviousTaskCompleted() throws InterruptedException
    {
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = getDataCopyTaskManager(injector);

        // Add a completed task
        LiveMigrationTask completedTask = getSucceededTask("completed-task", source1Name);
        dataCopyTaskManager.currentTasks.put(dest1Id, completedTask);

        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 2);
        Future<LiveMigrationTask> future = dataCopyTaskManager.createTask(request, dest1Name);
        awaitForFuture(future);

        assertThat(future.succeeded()).isTrue();
        assertThat(future.result()).isNotNull();
        assertThat(future.result().id()).isNotEqualTo("completed-task");
    }

    @Test
    public void testCreateTaskShouldFailWhenCassandraInstanceJMXIsUp() throws InterruptedException
    {
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = getDataCopyTaskManager(injector);
        InstancesMetadata instancesMetadata = injector.getInstance(InstancesMetadata.class);
        InstanceMetadata destinationMetadata = instancesMetadata.instanceFromHost(dest1Name);

        // Mocking JMX as up
        when(destinationMetadata.delegate().isJmxUp()).thenReturn(true);

        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 2);
        Future<LiveMigrationTask> future = dataCopyTaskManager.createTask(request, dest1Name);
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
        DataCopyTaskManager dataCopyTaskManager = getDataCopyTaskManager(injector);
        InstancesMetadata instancesMetadata = injector.getInstance(InstancesMetadata.class);
        InstanceMetadata destinationMetadata = instancesMetadata.instanceFromHost(dest1Name);

        // Mocking native (CQL) as up but JMX as down
        when(destinationMetadata.delegate().isJmxUp()).thenReturn(false);
        when(destinationMetadata.delegate().isNativeUp()).thenReturn(true);

        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 2);
        Future<LiveMigrationTask> future = dataCopyTaskManager.createTask(request, dest1Name);
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
        DataCopyTaskManager dataCopyTaskManager = getDataCopyTaskManager(injector);
        InstancesMetadata instancesMetadata = injector.getInstance(InstancesMetadata.class);
        InstanceMetadata destinationMetadata = instancesMetadata.instanceFromHost(dest1Name);
        when(destinationMetadata.delegate())
        .thenThrow(new CassandraUnavailableException(CQL_AND_JMX, "CassandraAdapterDelegate is not available"));

        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 2);
        Future<LiveMigrationTask> future = dataCopyTaskManager.createTask(request, dest1Name);
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
        DataCopyTaskManager dataCopyTaskManager = getDataCopyTaskManager(injector);

        LiveMigrationTask task = getSucceededTask("test-task", source1Name);
        dataCopyTaskManager.currentTasks.put(dest1Id, task);

        LiveMigrationTask retrievedTask = dataCopyTaskManager.getTask("test-task", dest1Name);

        assertThat(retrievedTask).isNotNull();
        assertThat(retrievedTask.id()).isEqualTo("test-task");
    }

    @Test
    public void testGetTaskNotFound()
    {
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = getDataCopyTaskManager(injector);

        assertThatThrownBy(() -> dataCopyTaskManager.getTask("non-existent-task", dest1Name))
        .isInstanceOf(LiveMigrationTaskNotFoundException.class);
    }

    @Test
    public void testGetTaskWrongTaskId()
    {
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = getDataCopyTaskManager(injector);

        LiveMigrationTask task = getSucceededTask("actual-task", source1Name);
        dataCopyTaskManager.currentTasks.put(dest1Id, task);

        assertThatThrownBy(() -> dataCopyTaskManager.getTask("wrong-task-id", dest1Name))
        .isInstanceOf(LiveMigrationTaskNotFoundException.class);
    }

    @Test
    public void testCancelTaskSuccess()
    {
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = getDataCopyTaskManager(injector);

        LiveMigrationTask task = getInProgressTask("cancelable-task");
        dataCopyTaskManager.currentTasks.put(dest1Id, task);

        LiveMigrationTask cancelledTask = dataCopyTaskManager.cancelTask("cancelable-task", dest1Name);

        assertThat(cancelledTask).isNotNull();
        assertThat(cancelledTask.id()).isEqualTo("cancelable-task");
        assertThat(cancelledTask.isCompleted()).isTrue(); // FakeLiveMigrationTask returns true when cancelled
    }

    @Test
    public void testCancelTaskNotFound()
    {
        Injector injector = getInjector();
        DataCopyTaskManager dataCopyTaskManager = getDataCopyTaskManager(injector);

        assertThatThrownBy(() -> dataCopyTaskManager.cancelTask("non-existent-task", dest1Name))
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
        DataCopyTaskManager dataCopyTaskManager = getDataCopyTaskManager(injector);

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
        final ConcurrentLinkedQueue<Future<LiveMigrationTask>> results = new ConcurrentLinkedQueue<>();

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
                        Future<LiveMigrationTask> future = dataCopyTaskManager.createTask(request, dest1Name);
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
            assertThat(dataCopyTaskManager.currentTasks).hasSize(1); // Only one task in the map

            // Verify the successful task
            LiveMigrationTask successfulTask = dataCopyTaskManager.currentTasks.values().iterator().next();
            assertThat(successfulTask).isNotNull();
            assertThat(successfulTask.id()).isNotNull();

            // Verify all failed tasks have the correct exception
            long inProgressExceptions = results.stream()
                                               .filter(Future::failed)
                                               .mapToLong(f -> f.cause() instanceof LiveMigrationDataCopyInProgressException ? 1 : 0)
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

    private DataCopyTaskManager getDataCopyTaskManager(Injector injector)
    {
        InstancesMetadata instancesMetadata = injector.getInstance(InstancesMetadata.class);
        SidecarConfiguration sidecarConfiguration = injector.getInstance(SidecarConfiguration.class);
        LiveMigrationMap liveMigrationMap = injector.getInstance(LiveMigrationMap.class);
        LiveMigrationTaskFactory liveMigrationTaskFactory = injector.getInstance(LiveMigrationTaskFactory.class);

        return new DataCopyTaskManager(instancesMetadata, sidecarConfiguration, liveMigrationMap,
                                       liveMigrationTaskFactory);
    }

    private LiveMigrationTask getInProgressTask(@NotNull String taskId)
    {
        List<LiveMigrationTaskResponse.Status> statusList =
        List.of(new LiveMigrationTaskResponse.Status(0, "PREPARING", 500L, 1, 1, 1, 0, 0, 500L));
        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 1);
        LiveMigrationTaskResponse response = new LiveMigrationTaskResponse(taskId, source1Name, 9043, request, statusList);
        return new FakeLiveMigrationTask(response);
    }

    private LiveMigrationTask getSucceededTask(@NotNull String taskId, @NotNull String sourceHost)
    {
        List<LiveMigrationTaskResponse.Status> statusList =
        List.of(new LiveMigrationTaskResponse.Status(0, "SUCCESS", 1000L, 1, 1, 1, 1, 0, 1000L));
        LiveMigrationDataCopyRequest request = new LiveMigrationDataCopyRequest(1, 1.0, 1);
        LiveMigrationTaskResponse response = new LiveMigrationTaskResponse(taskId, sourceHost, 9043, request, statusList);
        return new FakeLiveMigrationTask(response);
    }

    private static class DataCopyTaskManagerTestModule extends AbstractModule
    {
        private final LiveMigrationTaskFactory mockLiveMigrationTaskFactory = mock(LiveMigrationTaskFactory.class);
        private final SidecarConfiguration mockSidecarConfiguration = mock(SidecarConfiguration.class);
        private final ServiceConfiguration mockServiceConfiguration = mock(ServiceConfiguration.class);
        private final LiveMigrationConfiguration mockLiveMigrationConfiguration = mock(LiveMigrationConfiguration.class);
        private final LiveMigrationMap mockLiveMigrationmap = mock(LiveMigrationMap.class);
        private final InstanceMetadata mockDest1InstanceMeta = mock(InstanceMetadata.class);
        private final InstanceMetadata mockDest2InstanceMeta = mock(InstanceMetadata.class);
        private final InstanceMetadata mockDest3InstanceMeta = mock(InstanceMetadata.class);
        private final InstanceMetadata mockSourceInstanceMeta = mock(InstanceMetadata.class);
        private final InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);

        @Override
        protected void configure()
        {
            bind(Vertx.class).toInstance(Vertx.vertx());
            bind(LiveMigrationTaskFactory.class).toInstance(mockLiveMigrationTaskFactory);
            bind(SidecarConfiguration.class).toInstance(mockSidecarConfiguration);
            bind(LiveMigrationMap.class).toInstance(mockLiveMigrationmap);
            bind(InstancesMetadata.class).toInstance(mockInstancesMetadata);

            // Configure SidecarConfiguration mocks
            when(mockSidecarConfiguration.serviceConfiguration()).thenReturn(mockServiceConfiguration);
            when(mockServiceConfiguration.port()).thenReturn(9043);
            when(mockSidecarConfiguration.liveMigrationConfiguration()).thenReturn(mockLiveMigrationConfiguration);
            when(mockLiveMigrationConfiguration.maxConcurrentDownloads()).thenReturn(5);

            // Configure InstanceMetadata mocks
            when(mockInstancesMetadata.instanceFromHost(dest1Name)).thenReturn(mockDest1InstanceMeta);
            when(mockDest1InstanceMeta.id()).thenReturn(dest1Id);
            when(mockDest1InstanceMeta.dataDirs()).thenReturn(List.of("/data1", "/data2"));
            when(mockDest1InstanceMeta.delegate()).thenReturn(mock(CassandraAdapterDelegate.class));

            when(mockInstancesMetadata.instanceFromHost(dest2Name)).thenReturn(mockDest2InstanceMeta);
            when(mockDest2InstanceMeta.id()).thenReturn(dest2Id);
            when(mockDest2InstanceMeta.dataDirs()).thenReturn(List.of("/data1", "/data2"));
            when(mockDest2InstanceMeta.delegate()).thenReturn(mock(CassandraAdapterDelegate.class));

            when(mockInstancesMetadata.instanceFromHost(dest3Name)).thenReturn(mockDest3InstanceMeta);
            when(mockDest3InstanceMeta.id()).thenReturn(dest3Id);
            when(mockDest3InstanceMeta.dataDirs()).thenReturn(List.of("/data1", "/data2"));
            when(mockDest3InstanceMeta.delegate()).thenReturn(mock(CassandraAdapterDelegate.class));

            when(mockInstancesMetadata.instanceFromHost(source1Name)).thenReturn(mockSourceInstanceMeta);
            when(mockSourceInstanceMeta.dataDirs()).thenReturn(List.of("/data1"));
            when(mockSourceInstanceMeta.delegate()).thenReturn(mock(CassandraAdapterDelegate.class));

            // Configure LiveMigrationTaskFactory to return fake tasks
            when(mockLiveMigrationTaskFactory.create(anyString(), any(LiveMigrationDataCopyRequest.class), anyString(), anyInt(), any(InstanceMetadata.class))).thenAnswer(invocation -> {
                String id = invocation.getArgument(0);
                LiveMigrationDataCopyRequest request = invocation.getArgument(1);
                String source = invocation.getArgument(2);
                int port = invocation.getArgument(3);

                final List<LiveMigrationTaskResponse.Status> statusList =
                List.of(new LiveMigrationTaskResponse.Status(0, "SUCCESS", 1000L, 1, 1, 1, 1, 0, 1000L));
                final LiveMigrationTaskResponse taskResponse = new LiveMigrationTaskResponse(id, source, port, request, statusList);
                return new FakeLiveMigrationTask(taskResponse);
            });

            try
            {
                when(mockLiveMigrationmap.getSource(anyString())).thenReturn(Future.succeededFuture(source1Name));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
