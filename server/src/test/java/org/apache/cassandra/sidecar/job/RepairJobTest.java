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

package org.apache.cassandra.sidecar.job;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.request.data.RepairPayload;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.RepairJobsConfiguration;
import org.apache.cassandra.sidecar.config.yaml.RepairJobsConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.exceptions.OperationalJobConflictException;
import org.apache.cassandra.sidecar.handlers.data.RepairRequestParam;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests to validate job to run repairs
 */
class RepairJobTest
{
    private static final int MAX_ATTEMPTS = 5;
    protected Vertx vertx;
    protected ExecutorPools executorPool;
    protected PeriodicTaskExecutor periodicTaskExecutor;
    protected ClusterLease clusterLease;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        executorPool = new ExecutorPools(vertx, new ServiceConfigurationImpl());
        clusterLease = new ClusterLease(ClusterLease.Ownership.CLAIMED);
        periodicTaskExecutor = new PeriodicTaskExecutor(executorPool, clusterLease);
    }

    @AfterEach
    void cleanup()
    {
        if (periodicTaskExecutor != null)
        {
            periodicTaskExecutor.close();
        }
        TestResourceReaper.create().with(executorPool).with(vertx).close();
    }

    @Test
    void testRepairJob() throws Exception
    {
        StorageOperations storageOperations = mock(StorageOperations.class);
        when(storageOperations.getParentRepairStatus(anyInt())).thenReturn(List.of(RepairJob.ParentRepairStatus.COMPLETED.name()));
        when(storageOperations.repairAsync(any(), any())).thenReturn(1);

        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of("testtable"))
                                             .build();
        RepairRequestParam repairParams = RepairRequestParam.from(new Name("testkeyspace"), payload);

        RepairJobsConfiguration config = new RepairJobsConfigurationImpl(MAX_ATTEMPTS, MillisecondBoundConfiguration.parse("100ms")); // Use a shorter poll interval for testing
        RepairJob testJob = new RepairJob(periodicTaskExecutor, config, UUIDs.timeBased(), storageOperations, repairParams);

        Promise<Void> promise = Promise.promise();
        testJob.execute(promise);

        // Wait for the job to complete (with timeout)
        if (!promise.future().isComplete())
        {
            promise.future().toCompletionStage().toCompletableFuture().get(1, TimeUnit.SECONDS);
        }

        assertThat(testJob.asyncResult().isComplete()).isTrue();
        assertThat(testJob.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
    }

    @Test
    void testRunningRepairJob()
    {
        StorageOperations storageOperations = mock(StorageOperations.class);
        when(storageOperations.getParentRepairStatus(anyInt()))
        .thenReturn(Collections.singletonList(RepairJob.ParentRepairStatus.IN_PROGRESS.name()));
        when(storageOperations.repairAsync(any(), any())).thenReturn(1);
        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of("testtable"))
                                             .build();
        RepairRequestParam repairParams = RepairRequestParam.from(new Name("testkeyspace"), payload);

        RepairJobsConfiguration config = new RepairJobsConfigurationImpl(MAX_ATTEMPTS, MillisecondBoundConfiguration.parse("100ms")); // Shorter poll interval for testing
        RepairJob testJob = new RepairJob(periodicTaskExecutor, config, UUIDs.timeBased(), storageOperations, repairParams);

        Promise<Void> promise = Promise.promise();
        testJob.execute(promise);

        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

        assertThat(testJob.asyncResult().isComplete()).isTrue();
        assertThat(testJob.status()).isEqualTo(OperationalJobStatus.RUNNING);
        Mockito.verify(storageOperations, atLeast(2)).getParentRepairStatus(anyInt());
    }

    @Test
    void testLongRunningRepairJobTimeout() throws Exception
    {
        StorageOperations storageOperations = mock(StorageOperations.class);
        // Return null for MAX_ATTEMPTS times to simulate timeout
        for (int i = 0; i < MAX_ATTEMPTS; i++)
        {
            when(storageOperations.getParentRepairStatus(anyInt()))
            .thenReturn(null);
        }
        when(storageOperations.repairAsync(any(), any())).thenReturn(1);

        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of("testtable"))
                                             .build();
        RepairRequestParam repairParams = RepairRequestParam.from(new Name("testkeyspace"), payload);

        RepairJobsConfiguration config = new RepairJobsConfigurationImpl(MAX_ATTEMPTS, MillisecondBoundConfiguration.parse("100ms")); // Shorter poll interval for testing
        RepairJob testJob = new RepairJob(periodicTaskExecutor, config, UUIDs.timeBased(), storageOperations, repairParams);

        Promise<Void> promise = Promise.promise();
        testJob.execute(promise);

        promise.future().toCompletionStage().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertThat(testJob.asyncResult().isComplete()).isTrue();
        assertThat(testJob.status()).isEqualTo(OperationalJobStatus.RUNNING);
    }

    @Test
    void testTimersOnCompletion() throws Exception
    {
        StorageOperations storageOperations = mock(StorageOperations.class);
        when(storageOperations.getParentRepairStatus(anyInt()))
        .thenReturn(List.of(RepairJob.ParentRepairStatus.COMPLETED.name(), "Repair completed successfully"));
        when(storageOperations.repairAsync(any(), any())).thenReturn(1);

        RepairJobsConfiguration config = new RepairJobsConfigurationImpl(MAX_ATTEMPTS, MillisecondBoundConfiguration.parse("100ms")); // Short poll interval for quick test
        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of("testtable"))
                                             .build();
        RepairRequestParam repairParams = RepairRequestParam.from(new Name("testkeyspace"), payload);

        RepairJob testJob = new RepairJob(periodicTaskExecutor, config, UUIDs.timeBased(), storageOperations, repairParams);

        Promise<Void> promise = Promise.promise();
        testJob.execute(promise);

        if (!promise.future().isComplete())
        {
            promise.future().toCompletionStage().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
        }

        assertThat(testJob.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
    }

    @Test
    void testTimersOnFailure() throws Exception
    {
        StorageOperations storageOperations = mock(StorageOperations.class);
        when(storageOperations.getParentRepairStatus(anyInt()))
        .thenReturn(List.of(RepairJob.ParentRepairStatus.FAILED.name(), "Repair failed with error"));
        when(storageOperations.repairAsync(any(), any())).thenReturn(1);

        RepairJobsConfiguration config = new RepairJobsConfigurationImpl(MAX_ATTEMPTS, MillisecondBoundConfiguration.parse("100ms")); // Short poll interval for quick test
        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of("testtable"))
                                             .build();
        RepairRequestParam repairParams = RepairRequestParam.from(new Name("testkeyspace"), payload);
        RepairJob testJob = new RepairJob(periodicTaskExecutor, config, UUIDs.timeBased(), storageOperations, repairParams);

        Promise<Void> promise = Promise.promise();
        testJob.execute(promise);

        try
        {
            promise.future().toCompletionStage().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
        }
        catch (java.util.concurrent.ExecutionException e)
        {
            // Expected since the job should fail
        }

        assertThat(testJob.status()).isEqualTo(OperationalJobStatus.FAILED);
    }

    @Test
    void testMultipleRepairJobsRunningInParallel() throws Exception
    {
        // Create a job tracker and manager
        OperationalJobTracker tracker = new OperationalJobTracker(10);
        OperationalJobManager manager = new OperationalJobManager(tracker, executorPool);

        // Mock the storage operations
        StorageOperations storageOperations = mock(StorageOperations.class);
        when(storageOperations.getParentRepairStatus(anyInt())).thenReturn(List.of(RepairJob.ParentRepairStatus.COMPLETED.name()));
        when(storageOperations.repairAsync(any(), any())).thenReturn(1);

        // Create configuration for repair jobs with shorter poll interval for testing
        RepairJobsConfiguration config = new RepairJobsConfigurationImpl(MAX_ATTEMPTS, MillisecondBoundConfiguration.parse("100ms"));

        // Create multiple repair jobs with different parameters
        RepairJob job1 = createRepairJob(config, storageOperations, "keyspace1", "table1");
        RepairJob job2 = createRepairJob(config, storageOperations, "keyspace1", "table2");
        RepairJob job3 = createRepairJob(config, storageOperations, "keyspace2", "table1");

        CountDownLatch latch = new CountDownLatch(1);
        BiConsumer<OperationalJob, OperationalJobConflictException> onComplete = (job, exception) -> {
            assertThat(exception).isNull();
            latch.countDown();
        };

        // Submit all jobs to the manager
        Promise<Void> promise1 = Promise.promise();
        Promise<Void> promise2 = Promise.promise();
        Promise<Void> promise3 = Promise.promise();

        job1.execute(promise1);
        job2.execute(promise2);
        job3.execute(promise3);

        manager.trySubmitJob(job1, onComplete, executorPool.service(), SecondBoundConfiguration.parse("5s"));
        manager.trySubmitJob(job2, onComplete, executorPool.service(), SecondBoundConfiguration.parse("5s"));
        manager.trySubmitJob(job3, onComplete, executorPool.service(), SecondBoundConfiguration.parse("5s"));

        // Wait for all jobs to complete (with timeout)
        if (!promise1.future().isComplete())
        {
            promise1.future().toCompletionStage().toCompletableFuture().get(2, TimeUnit.SECONDS);
        }
        if (!promise2.future().isComplete())
        {
            promise2.future().toCompletionStage().toCompletableFuture().get(2, TimeUnit.SECONDS);
        }
        if (!promise3.future().isComplete())
        {
            promise3.future().toCompletionStage().toCompletableFuture().get(2, TimeUnit.SECONDS);
        }

        // Verify all jobs completed successfully
        assertThat(job1.asyncResult().isComplete()).isTrue();
        assertThat(job1.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertThat(tracker.get(job1.jobId())).isNotNull();

        assertThat(job2.asyncResult().isComplete()).isTrue();
        assertThat(job2.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertThat(tracker.get(job2.jobId())).isNotNull();

        assertThat(job3.asyncResult().isComplete()).isTrue();
        assertThat(job3.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertThat(tracker.get(job3.jobId())).isNotNull();

        // Verify that all jobs were tracked
        assertThat(tracker.jobsView().size()).isEqualTo(3);
    }

    @Test
    void testEmptyRepairStatusHandling() throws Exception
    {
        StorageOperations storageOperations = mock(StorageOperations.class);

        // Return empty status (repair not started), then return IN_PROGRESS, then COMPLETED
        when(storageOperations.getParentRepairStatus(anyInt()))
        .thenReturn(Collections.emptyList())
        .thenReturn(Collections.emptyList())
        .thenReturn(List.of(RepairJob.ParentRepairStatus.COMPLETED.name()));

        when(storageOperations.repairAsync(any(), any())).thenReturn(1);

        RepairJobsConfiguration config = new RepairJobsConfigurationImpl(MAX_ATTEMPTS, MillisecondBoundConfiguration.parse("100ms"));
        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of("testtable"))
                                             .build();
        RepairRequestParam repairParams = RepairRequestParam.from(new Name("testkeyspace"), payload);

        RepairJob testJob = new RepairJob(periodicTaskExecutor, config, UUIDs.timeBased(), storageOperations, repairParams);
        Promise<Void> promise = Promise.promise();
        testJob.execute(promise);

        if (!promise.future().isComplete())
        {
            promise.future().toCompletionStage().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
        }

        assertThat(testJob.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        // Verify the repair status was queried at least 4 times
        Mockito.verify(storageOperations, Mockito.atLeast(4)).getParentRepairStatus(anyInt());
    }

    /**
     * Helper method to create a repair job with specific parameters
     */
    private RepairJob createRepairJob(RepairJobsConfiguration config, StorageOperations storageOperations,
                                      String keyspace, String table)
    {
        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of(table))
                                             .build();
        RepairRequestParam repairParams = RepairRequestParam.from(new Name(keyspace), payload);
        return new RepairJob(periodicTaskExecutor, config, UUIDs.timeBased(), storageOperations, repairParams);
    }
}
