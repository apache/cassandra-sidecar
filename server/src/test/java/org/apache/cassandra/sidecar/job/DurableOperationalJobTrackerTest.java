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
import java.util.HashMap;
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
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
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
import static org.mockito.Mockito.atLeastOnce;
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
        when(storageProvider.isAvailable()).thenReturn(true);
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
        loopAssert(2, () -> {
            verify(storageProvider).persistJob(argThat(record ->
                record.jobId().equals(job.jobId()) &&
                record.operationType() == job.operationType() &&
                record.status() == CREATED
            ));
        });
    }

    @Test
    void testComputeIfAbsentDoesNotPersistExistingJob()
    {
        OperationalJob job = createOperationalJob(CREATED);

        tracker.computeIfAbsent(job.jobId(), id -> job);
        OperationalJob secondCall = tracker.computeIfAbsent(job.jobId(), id -> createOperationalJob(CREATED));

        assertThat(secondCall).isSameAs(job);
        loopAssert(2, () -> {
            verify(storageProvider, times(1)).persistJob(any());
        });
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
    void testTerminalStatusNotUpdatedWhenPersistFails() throws InterruptedException
    {
        UUID jobId = UUIDs.timeBased();
        OperationalJob job = OperationalJobTest.createOperationalJob(jobId, MillisecondBoundConfiguration.parse("50ms"));

        doThrow(new StorageProviderException("persist failed")).when(storageProvider).persistJob(any());

        CountDownLatch completed = new CountDownLatch(1);
        tracker.computeIfAbsent(jobId, id -> job);
        executorPool.executeBlocking(job::execute);
        job.asyncResult().onComplete(ar -> completed.countDown());

        assertThat(completed.await(5, TimeUnit.SECONDS)).isTrue();
        // The tracker only registers its completion handler after a successful persist. Since persist
        // failed here, the handler is never registered, so updateJobStatus should not run.
        verify(storageProvider, never()).updateJobStatus(any(), any(), any(), any());
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
        OperationalJobRecord record = OperationalJobRecord.builder()
                                                          .jobId(jobId)
                                                          .operationType(OperationType.DECOMMISSION)
                                                          .status(SUCCEEDED)
                                                          .build();
        when(storageProvider.findJob(jobId)).thenReturn(record);
        when(storageProvider.getNodeStatusesForOperation(jobId)).thenReturn(Collections.emptyMap());

        OperationalJobInfo result = tracker.get(jobId);

        assertThat(result).isInstanceOf(OperationalJobRecord.class);
        assertThat(result.jobId()).isEqualTo(jobId);
        assertThat(result.status()).isEqualTo(SUCCEEDED);
        assertThat(result.name()).isEqualTo(OperationType.DECOMMISSION.name().toLowerCase());
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
    void testComputeIfAbsentKeepsJobInMapOnPersistenceFailureUntilCompletion() throws InterruptedException
    {
        UUID jobId = UUIDs.timeBased();
        OperationalJob job = OperationalJobTest.createOperationalJob(jobId, MillisecondBoundConfiguration.parse("200ms"));

        doThrow(new StorageProviderException("Storage unavailable"))
            .when(storageProvider).persistJob(any());

        CountDownLatch completed = new CountDownLatch(1);
        tracker.computeIfAbsent(jobId, id -> job);
        executorPool.executeBlocking(job::execute);
        job.asyncResult().onComplete(ar -> completed.countDown());

        // The persist failed, but the job is already executing, so it must remain in the live map
        // (durability-degraded) rather than being dropped mid-flight.
        loopAssert(2, () -> {
            verify(storageProvider, atLeastOnce()).persistJob(any());
            assertThat(tracker.jobsView()).containsKey(jobId);
        });

        // Once the job completes, the completion handler removes it from the live map to avoid leaking.
        assertThat(completed.await(5, TimeUnit.SECONDS)).isTrue();
        loopAssert(2, () -> {
            assertThat(tracker.jobsView()).doesNotContainKey(jobId);
        });
    }

    @Test
    void testComputeIfAbsentFailsWhenStorageUnavailable()
    {
        when(storageProvider.isAvailable()).thenReturn(false);
        OperationalJob job = createOperationalJob(CREATED);

        assertThatThrownBy(() -> tracker.computeIfAbsent(job.jobId(), id -> job))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Storage provider is not available");
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
    void testGetEnrichesRecordWithNodeStatuses()
    {
        UUID jobId = UUIDs.timeBased();
        UUID pendingNode = UUIDs.timeBased();
        UUID runningNode = UUIDs.timeBased();
        UUID succeededNode = UUIDs.timeBased();
        UUID failedNode = UUIDs.timeBased();

        OperationalJobRecord record = OperationalJobRecord.builder()
                                                          .jobId(jobId)
                                                          .operationType(OperationType.DECOMMISSION)
                                                          .status(RUNNING)
                                                          .build();
        when(storageProvider.findJob(jobId)).thenReturn(record);

        Map<UUID, OperationalJobStatus> nodeStatuses = new HashMap<>();
        nodeStatuses.put(pendingNode, CREATED);
        nodeStatuses.put(runningNode, RUNNING);
        nodeStatuses.put(succeededNode, SUCCEEDED);
        nodeStatuses.put(failedNode, FAILED);
        when(storageProvider.getNodeStatusesForOperation(jobId)).thenReturn(nodeStatuses);

        OperationalJobInfo result = tracker.get(jobId);

        assertThat(result).isInstanceOf(OperationalJobRecord.class);
        assertThat(result.nodesPending()).containsExactly(pendingNode);
        assertThat(result.nodesExecuting()).containsExactly(runningNode);
        assertThat(result.nodesSucceeded()).containsExactly(succeededNode);
        assertThat(result.nodesFailed()).containsExactly(failedNode);
        verify(storageProvider).getNodeStatusesForOperation(jobId);
    }

    @Test
    void testGetSkipsEnrichmentWhenNodeListsAlreadyPopulated()
    {
        UUID jobId = UUIDs.timeBased();
        UUID succeededNode = UUIDs.timeBased();

        OperationalJobRecord record = OperationalJobRecord.builder()
                                                          .jobId(jobId)
                                                          .operationType(OperationType.DECOMMISSION)
                                                          .status(SUCCEEDED)
                                                          .nodesSucceeded(Collections.singletonList(succeededNode))
                                                          .build();
        when(storageProvider.findJob(jobId)).thenReturn(record);

        OperationalJobInfo result = tracker.get(jobId);

        assertThat(result).isSameAs(record);
        assertThat(result.nodesSucceeded()).containsExactly(succeededNode);
        verify(storageProvider, never()).getNodeStatusesForOperation(any());
    }

    @Test
    void testGetReturnsUnenrichedRecordWhenNoNodeStatuses()
    {
        UUID jobId = UUIDs.timeBased();
        OperationalJobRecord record = OperationalJobRecord.builder()
                                                          .jobId(jobId)
                                                          .operationType(OperationType.DECOMMISSION)
                                                          .status(SUCCEEDED)
                                                          .build();
        when(storageProvider.findJob(jobId)).thenReturn(record);
        when(storageProvider.getNodeStatusesForOperation(jobId)).thenReturn(Collections.emptyMap());

        OperationalJobInfo result = tracker.get(jobId);

        assertThat(result).isSameAs(record);
        assertThat(result.nodesPending()).isEmpty();
        assertThat(result.nodesExecuting()).isEmpty();
        assertThat(result.nodesSucceeded()).isEmpty();
        assertThat(result.nodesFailed()).isEmpty();
        verify(storageProvider).getNodeStatusesForOperation(jobId);
    }

    @Test
    void testGetPropagatesNodeStatusQueryFailure()
    {
        UUID jobId = UUIDs.timeBased();
        OperationalJobRecord record = OperationalJobRecord.builder()
                                                          .jobId(jobId)
                                                          .operationType(OperationType.DECOMMISSION)
                                                          .status(SUCCEEDED)
                                                          .build();
        when(storageProvider.findJob(jobId)).thenReturn(record);
        when(storageProvider.getNodeStatusesForOperation(jobId))
            .thenThrow(new StorageProviderException("Node state unavailable"));

        assertThatThrownBy(() -> tracker.get(jobId))
            .isInstanceOf(StorageProviderException.class)
            .hasMessage("Node state unavailable");
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
            verify(storageProvider, times(3)).updateJobStatus(eq(jobId), eq(OperationType.DRAIN), eq(SUCCEEDED), isNull());
            assertThat(tracker.jobsView()).doesNotContainKey(jobId);
        });
    }

    @Test
    void testInitialPersistRetrySucceedsAfterTransientFailure() throws InterruptedException
    {
        UUID jobId = UUIDs.timeBased();
        OperationalJob job = OperationalJobTest.createOperationalJob(jobId, MillisecondBoundConfiguration.parse("50ms"));

        doThrow(new StorageProviderException("Transient failure"))
            .doNothing()
            .when(storageProvider).persistJob(any());

        tracker.computeIfAbsent(jobId, id -> job);
        executorPool.executeBlocking(job::execute);

        loopAssert(2, () -> {
            verify(storageProvider, times(2)).persistJob(any());
            verify(storageProvider).updateJobStatus(eq(jobId), eq(OperationType.DRAIN), eq(SUCCEEDED), isNull());
        });
    }

    @Test
    void testInitialPersistExhaustsRetriesAndTracksInMemoryOnly() throws InterruptedException
    {
        UUID jobId = UUIDs.timeBased();
        OperationalJob job = OperationalJobTest.createOperationalJob(jobId, MillisecondBoundConfiguration.parse("50ms"));

        doThrow(new StorageProviderException("Persistent failure")).when(storageProvider).persistJob(any());

        CountDownLatch completed = new CountDownLatch(1);
        tracker.computeIfAbsent(jobId, id -> job);
        executorPool.executeBlocking(job::execute);
        job.asyncResult().onComplete(ar -> completed.countDown());

        assertThat(completed.await(5, TimeUnit.SECONDS)).isTrue();

        loopAssert(2, () -> {
            verify(storageProvider, times(3)).persistJob(any());
            verify(storageProvider, never()).updateJobStatus(any(), any(), any(), any());
            assertThat(tracker.jobsView()).doesNotContainKey(jobId);
        });
    }
}
