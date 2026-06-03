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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;
import org.apache.cassandra.sidecar.job.storage.StorageProvider;
import org.apache.cassandra.sidecar.job.storage.StorageProviderException;

import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.CREATED;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.FAILED;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.RUNNING;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.SUCCEEDED;
import static org.apache.cassandra.sidecar.job.OperationalJobTest.createOperationalJob;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DurableOperationalJobTracker}
 */
class DurableOperationalJobTrackerTest
{
    private StorageProvider storageProvider;
    private DurableOperationalJobTracker tracker;
    private Vertx vertx;
    private ExecutorPools executorPools;
    private TaskExecutorPool executorPool;

    @BeforeEach
    void setUp()
    {
        storageProvider = mock(StorageProvider.class);
        vertx = Vertx.vertx();
        executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());
        executorPool = executorPools.internal();
        tracker = new DurableOperationalJobTracker(new ServiceConfigurationImpl(), storageProvider, executorPool);
    }

    @AfterEach
    void cleanup()
    {
        TestResourceReaper.create().with(vertx).with(executorPools).close();
    }

    @Test
    void testComputeIfAbsentPersistsNewJob()
    {
        OperationalJob job = createOperationalJob(CREATED);

        OperationalJob result = tracker.computeIfAbsent(job.jobId(), id -> job);

        assertThat(result).isSameAs(job);
        verify(storageProvider).persistJob(argThat(record ->
            record.jobId().equals(job.jobId()) &&
            record.operationType() == job.operationType() &&
            record.status() == CREATED
        ));
    }

    @Test
    void testComputeIfAbsentDoesNotPersistExistingJob()
    {
        OperationalJob job = createOperationalJob(CREATED);

        tracker.computeIfAbsent(job.jobId(), id -> job);
        OperationalJob secondCall = tracker.computeIfAbsent(job.jobId(), id -> createOperationalJob(CREATED));

        assertThat(secondCall).isSameAs(job);
        verify(storageProvider, times(1)).persistJob(any());
    }

    @Test
    void testComputeIfAbsentUpdatesStatusOnCompletion() throws InterruptedException
    {
        UUID jobId = UUIDs.timeBased();
        OperationalJob job = OperationalJobTest.createOperationalJob(jobId, MillisecondBoundConfiguration.parse("50ms"));
        CountDownLatch latch = new CountDownLatch(1);

        tracker.computeIfAbsent(jobId, id -> job);
        executorPool.executeBlocking(job::execute);

        job.asyncResult().onComplete(ar -> latch.countDown());
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        loopAssert(2, () -> {
            verify(storageProvider).updateJobStatus(eq(jobId), eq(OperationType.DRAIN), eq(SUCCEEDED), isNull());
        });
    }

    @Test
    void testComputeIfAbsentUpdatesStatusOnFailure() throws InterruptedException
    {
        UUID jobId = UUIDs.timeBased();
        OperationalJob job = OperationalJobTest.createOperationalJob(jobId,
            MillisecondBoundConfiguration.parse("50ms"),
            new org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException("test failure"));
        CountDownLatch latch = new CountDownLatch(1);

        tracker.computeIfAbsent(jobId, id -> job);
        executorPool.executeBlocking(job::execute);

        job.asyncResult().onComplete(ar -> latch.countDown());
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        loopAssert(2, () -> {
            verify(storageProvider).updateJobStatus(eq(jobId), eq(OperationType.DRAIN), eq(FAILED), eq("test failure"));
        });
    }

    @Test
    void testComputeIfAbsentRemovesJobFromLocalMapOnCompletion() throws InterruptedException
    {
        UUID jobId = UUIDs.timeBased();
        OperationalJob job = OperationalJobTest.createOperationalJob(jobId, MillisecondBoundConfiguration.parse("50ms"));
        CountDownLatch latch = new CountDownLatch(1);

        tracker.computeIfAbsent(jobId, id -> job);
        executorPool.executeBlocking(job::execute);

        assertThat(tracker.jobsView()).containsKey(jobId);

        job.asyncResult().onComplete(ar -> latch.countDown());
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        loopAssert(2, () -> {
            assertThat(tracker.jobsView()).doesNotContainKey(jobId);
        });
    }

    @Test
    void testGetReturnsLiveJobFromLocalMap()
    {
        OperationalJob job = createOperationalJob(CREATED);
        tracker.computeIfAbsent(job.jobId(), id -> job);

        OperationalJobInfo result = tracker.get(job.jobId());

        assertThat(result).isSameAs(job);
        verify(storageProvider, never()).findJob(any());
    }

    @Test
    void testGetFallsBackToStorage()
    {
        UUID jobId = UUIDs.timeBased();
        OperationalJobRecord record = new OperationalJobRecord(jobId, OperationType.DECOMMISSION, SUCCEEDED);
        when(storageProvider.findJob(jobId)).thenReturn(record);

        OperationalJobInfo result = tracker.get(jobId);

        assertThat(result).isInstanceOf(OperationalJobRecord.class);
        assertThat(result.jobId()).isEqualTo(jobId);
        assertThat(result.status()).isEqualTo(SUCCEEDED);
        assertThat(result.name()).isEqualTo(OperationType.DECOMMISSION.name());
        assertThat(result.operationType()).isEqualTo(OperationType.DECOMMISSION);
        verify(storageProvider).findJob(jobId);
    }

    @Test
    void testGetReturnsNullWhenNotFoundAnywhere()
    {
        UUID jobId = UUIDs.timeBased();
        when(storageProvider.findJob(jobId)).thenReturn(null);

        OperationalJobInfo result = tracker.get(jobId);

        assertThat(result).isNull();
        verify(storageProvider).findJob(jobId);
    }

    @Test
    void testJobsViewReturnsOnlyLocalMapContents()
    {
        OperationalJob job1 = createOperationalJob(CREATED);
        OperationalJob job2 = createOperationalJob(CREATED);
        tracker.computeIfAbsent(job1.jobId(), id -> job1);
        tracker.computeIfAbsent(job2.jobId(), id -> job2);

        Map<UUID, OperationalJob> view = tracker.jobsView();

        assertThat(view).hasSize(2);
        assertThat(view).containsEntry(job1.jobId(), job1);
        assertThat(view).containsEntry(job2.jobId(), job2);
        verify(storageProvider, never()).findJob(any());
        verify(storageProvider, never()).findAllJobs(any(int.class));
    }

    @Test
    void testJobsViewIsImmutable()
    {
        OperationalJob job = createOperationalJob(CREATED);
        tracker.computeIfAbsent(job.jobId(), id -> job);

        Map<UUID, OperationalJob> view = tracker.jobsView();

        assertThatThrownBy(() -> view.put(UUIDs.timeBased(), job))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testInflightJobsByOperationFiltersLocalMap()
    {
        OperationalJob createdJob = createOperationalJob("decommission", CREATED);
        OperationalJob runningJob = createOperationalJob("decommission", RUNNING);
        OperationalJob succeededJob = createOperationalJob("decommission", SUCCEEDED);
        OperationalJob drainJob = createOperationalJob("drain", RUNNING);

        tracker.computeIfAbsent(createdJob.jobId(), id -> createdJob);
        tracker.computeIfAbsent(runningJob.jobId(), id -> runningJob);
        tracker.computeIfAbsent(succeededJob.jobId(), id -> succeededJob);
        tracker.computeIfAbsent(drainJob.jobId(), id -> drainJob);

        List<OperationalJob> inflightDecommission = tracker.inflightJobsByOperation("decommission");

        assertThat(inflightDecommission)
            .hasSize(2)
            .containsExactlyInAnyOrder(createdJob, runningJob);
        verify(storageProvider, never()).findJob(any());
    }

    @Test
    void testComputeIfAbsentPropagatesPersistenceFailure()
    {
        doThrow(new StorageProviderException("Storage unavailable"))
            .when(storageProvider).persistJob(any());
        OperationalJob job = createOperationalJob(CREATED);

        assertThatThrownBy(() -> tracker.computeIfAbsent(job.jobId(), id -> job))
            .isInstanceOf(StorageProviderException.class)
            .hasMessage("Storage unavailable");
    }

    @Test
    void testGetPropagatesStorageFallbackFailure()
    {
        UUID jobId = UUIDs.timeBased();
        when(storageProvider.findJob(jobId)).thenThrow(new StorageProviderException("Storage unavailable"));

        assertThatThrownBy(() -> tracker.get(jobId))
            .isInstanceOf(StorageProviderException.class)
            .hasMessage("Storage unavailable");
    }

    @Test
    void testOnCompleteRetrySucceedsAfterTransientFailure() throws InterruptedException
    {
        UUID jobId = UUIDs.timeBased();
        OperationalJob job = OperationalJobTest.createOperationalJob(jobId, MillisecondBoundConfiguration.parse("50ms"));

        doThrow(new StorageProviderException("Transient failure"))
            .doNothing()
            .when(storageProvider).updateJobStatus(eq(jobId), eq(OperationType.DRAIN), eq(SUCCEEDED), isNull());

        CountDownLatch latch = new CountDownLatch(1);

        tracker.computeIfAbsent(jobId, id -> job);
        executorPool.executeBlocking(job::execute);

        job.asyncResult().onComplete(ar -> latch.countDown());
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        loopAssert(2, () -> {
            verify(storageProvider, times(2)).updateJobStatus(eq(jobId), eq(OperationType.DRAIN), eq(SUCCEEDED), isNull());
        });
    }

    @Test
    void testOnCompleteRemovesJobFromLocalMapWhenAllRetriesFail() throws InterruptedException
    {
        UUID jobId = UUIDs.timeBased();
        OperationalJob job = OperationalJobTest.createOperationalJob(jobId, MillisecondBoundConfiguration.parse("50ms"));

        doThrow(new StorageProviderException("Persistent failure"))
            .when(storageProvider).updateJobStatus(eq(jobId), eq(OperationType.DRAIN), eq(SUCCEEDED), isNull());

        CountDownLatch latch = new CountDownLatch(1);
        tracker.computeIfAbsent(jobId, id -> job);
        executorPool.executeBlocking(job::execute);

        job.asyncResult().onComplete(ar -> latch.countDown());
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        loopAssert(2, () -> {
            verify(storageProvider, times(2)).updateJobStatus(eq(jobId), eq(OperationType.DRAIN), eq(SUCCEEDED), isNull());
            assertThat(tracker.jobsView()).doesNotContainKey(jobId);
        });
    }
}
