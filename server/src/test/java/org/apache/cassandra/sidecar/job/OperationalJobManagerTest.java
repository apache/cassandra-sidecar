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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.exceptions.OperationalJobConflictException;
import org.apache.cassandra.sidecar.job.storage.StorageProvider;
import org.apache.cassandra.sidecar.job.storage.StorageProviderException;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.FAILED;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.RUNNING;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.SUCCEEDED;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests to validate the Job submission behavior for scenarios which are a combination of values for
 *
 * <ul>
 * <ol> 1) Downstream job existence,</ol>
 * <ol> 2) Cached job (null (not in cache), Completed/Failed job, Running job), and</ol>
 * <ol> 3) Request UUID (null (no header), UUID)</ol>
 * </ul>
 */
class OperationalJobManagerTest
{
    protected Vertx vertx;
    protected ExecutorPools executorPool;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        executorPool = new ExecutorPools(vertx, new ServiceConfigurationImpl());
    }

    @AfterEach
    void cleanup()
    {
        TestResourceReaper.create().with(vertx).with(executorPool).close();
    }

    @Test
    void testWithNoDownstreamJob() throws InterruptedException
    {
        OperationalJobTracker tracker = new InMemoryOperationalJobTracker(4);
        OperationalJobManager manager = new OperationalJobManager(tracker, new DisabledOperationalJobCoordinator(), executorPool);
        CountDownLatch latch = new CountDownLatch(1);

        OperationalJob testJob = OperationalJobTest.createOperationalJob(SUCCEEDED);
        BiConsumer<OperationalJob, OperationalJobConflictException> onComplete = (job, exception) -> {
            assertThat(exception).isNull();
            latch.countDown();
        };

        manager.trySubmitJob(testJob, onComplete, executorPool.service(), SecondBoundConfiguration.parse("5s"));
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(testJob.asyncResult().isComplete()).isTrue();
        assertThat(testJob.status()).isEqualTo(SUCCEEDED);
        assertThat(tracker.get(testJob.jobId())).isNotNull();
    }

    @Test
    void testWithRunningDownstreamJob() throws InterruptedException
    {
        OperationalJob runningJob = OperationalJobTest.createOperationalJob(RUNNING);
        OperationalJobTracker tracker = new InMemoryOperationalJobTracker(4);
        OperationalJobManager manager = new OperationalJobManager(tracker, new DisabledOperationalJobCoordinator(), executorPool);
        CountDownLatch latch = new CountDownLatch(1);

        BiConsumer<OperationalJob, OperationalJobConflictException> onComplete = (job, exception) -> {
            assertThat(exception).isInstanceOf(OperationalJobConflictException.class);
            assertThat(exception.getMessage()).isEqualTo("The same operational job is already running on Cassandra. operationName='Operation X'");
            latch.countDown();
        };

        manager.trySubmitJob(runningJob, onComplete, executorPool.service(), SecondBoundConfiguration.parse("5s"));
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void testWithLongRunningJob() throws InterruptedException
    {
        UUID jobId = UUIDs.timeBased();
        OperationalJobTracker tracker = new InMemoryOperationalJobTracker(4);
        OperationalJobManager manager = new OperationalJobManager(tracker, new DisabledOperationalJobCoordinator(), executorPool);
        CountDownLatch latch = new CountDownLatch(1);

        OperationalJob testJob = OperationalJobTest.createOperationalJob(jobId, SecondBoundConfiguration.parse("2s"));
        BiConsumer<OperationalJob, OperationalJobConflictException> onComplete = (job, exception) -> {
            assertThat(exception).isNull();
            latch.countDown();
        };

        manager.trySubmitJob(testJob, onComplete, executorPool.service(), SecondBoundConfiguration.parse("5s"));
        // Job should be running initially
        assertThat(testJob.asyncResult().isComplete()).isFalse();
        assertThat(tracker.get(jobId)).isNotNull();

        // Wait for completion
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(testJob.asyncResult().isComplete()).isTrue();
    }

    @Test
    void testWithFailingJob() throws InterruptedException
    {
        UUID jobId = UUIDs.timeBased();
        OperationalJobTracker tracker = new InMemoryOperationalJobTracker(4);
        OperationalJobManager manager = new OperationalJobManager(tracker, new DisabledOperationalJobCoordinator(), executorPool);
        CountDownLatch latch = new CountDownLatch(1);

        String msg = "Test Job failed";
        OperationalJob failingJob = new OperationalJob(jobId)
        {
            @Override
            public boolean hasConflict(List<OperationalJob> jobs)
            {
                return false;
            }

            @Override
            public OperationType operationType()
            {
                return OperationType.DRAIN;
            }

            @Override
            protected Future<Void> executeInternal() throws OperationalJobException
            {
                throw new OperationalJobException(msg);
            }
        };

        BiConsumer<OperationalJob, OperationalJobConflictException> onComplete = (job, exception) -> {
            assertThat(exception).isNull(); // Exception handled internally by job
            assertThat(job.asyncResult().isComplete()).isTrue();
            assertThat(job.asyncResult().failed()).isTrue();
            latch.countDown();
        };

        manager.trySubmitJob(failingJob, onComplete, executorPool.service(), SecondBoundConfiguration.parse("5s"));
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(tracker.get(jobId)).isNotNull();
    }

    @Test
    void testJobRemovedFromTrackerWhenPersistenceFails()
    {
        StorageProvider storageProvider = mock(StorageProvider.class);
        when(storageProvider.isAvailable()).thenReturn(true);
        doThrow(new StorageProviderException("Storage unavailable"))
            .when(storageProvider).persistJob(any());

        DurableOperationalJobTracker durableTracker = new DurableOperationalJobTracker(new ServiceConfigurationImpl(),
                                                                                       storageProvider,
                                                                                       executorPool.service());
        OperationalJobManager manager = new OperationalJobManager(durableTracker, new DisabledOperationalJobCoordinator(), executorPool);

        UUID jobId = UUIDs.timeBased();
        OperationalJob job = OperationalJobTest.createOperationalJob(jobId, MillisecondBoundConfiguration.parse("50ms"));

        manager.trySubmitJob(job,
                             (j, ex) -> {},
                             executorPool.service(),
                             SecondBoundConfiguration.parse("5s"));

        loopAssert(2, () -> {
            assertThat(durableTracker.jobsView()).doesNotContainKey(jobId);
        });
    }

    void testCoordinatorCalledWhenJobRequiresCoordination() throws InterruptedException
    {
        OperationalJobTracker tracker = new InMemoryOperationalJobTracker(4);
        OperationalJobCoordinator coordinator = mock(OperationalJobCoordinator.class);
        when(coordinator.trySetActive(any(), any())).thenReturn(true);
        OperationalJobManager manager = new OperationalJobManager(tracker, coordinator, executorPool);
        CountDownLatch latch = new CountDownLatch(1);

        OperationalJob job = createCoordinatedJob(UUIDs.timeBased());
        BiConsumer<OperationalJob, OperationalJobConflictException> onComplete = (j, ex) -> {
            assertThat(ex).isNull();
            latch.countDown();
        };

        manager.trySubmitJob(job, onComplete, executorPool.service(), SecondBoundConfiguration.parse("5s"));
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        verify(coordinator).trySetActive(OperationType.MOVE, job.jobId());
        verify(coordinator, timeout(5000)).clearActive(OperationType.MOVE, job.jobId());
    }

    @Test
    void testConflictWhenCoordinatorReturnsFalse() throws InterruptedException
    {
        OperationalJobTracker tracker = new InMemoryOperationalJobTracker(4);
        OperationalJobCoordinator coordinator = mock(OperationalJobCoordinator.class);
        when(coordinator.trySetActive(any(), any())).thenReturn(false);
        OperationalJobManager manager = new OperationalJobManager(tracker, coordinator, executorPool);
        CountDownLatch latch = new CountDownLatch(1);

        OperationalJob job = createCoordinatedJob(UUIDs.timeBased());
        BiConsumer<OperationalJob, OperationalJobConflictException> onComplete = (j, ex) -> {
            assertThat(ex).isInstanceOf(OperationalJobConflictException.class);
            assertThat(ex.getMessage()).contains("An active operation already exists");
            latch.countDown();
        };

        manager.trySubmitJob(job, onComplete, executorPool.service(), SecondBoundConfiguration.parse("5s"));
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        OperationalJobInfo tracked = tracker.get(job.jobId());
        assertThat(tracked).isNotNull();
        assertThat(tracked.status()).isEqualTo(FAILED);
        assertThat(tracked.failureReason()).contains("An active operation already exists");
        assertThat(tracker.inflightJobsByOperation(job.name())).doesNotContain(job);
        verify(coordinator, never()).clearActive(any(), any());
    }

    @Test
    void testCoordinationFailsWhenCoordinationDisabled() throws InterruptedException
    {
        OperationalJobTracker tracker = new InMemoryOperationalJobTracker(4);
        // Coordination is disabled on this instance, yet the job requires coordination.
        OperationalJobManager manager = new OperationalJobManager(tracker, new DisabledOperationalJobCoordinator(), executorPool);
        CountDownLatch latch = new CountDownLatch(1);

        OperationalJob job = createCoordinatedJob(UUIDs.timeBased());
        BiConsumer<OperationalJob, OperationalJobConflictException> onComplete = (j, ex) -> {
            assertThat(ex).isInstanceOf(OperationalJobConflictException.class);
            assertThat(ex.getMessage()).contains("coordination is not supported by this Sidecar instance");
            latch.countDown();
        };

        manager.trySubmitJob(job, onComplete, executorPool.service(), SecondBoundConfiguration.parse("5s"));
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        OperationalJobInfo tracked = tracker.get(job.jobId());
        assertThat(tracked).isNotNull();
        assertThat(tracked.status()).isEqualTo(FAILED);
        assertThat(tracked.failureReason()).contains("coordination is not supported by this Sidecar instance");
        assertThat(tracker.inflightJobsByOperation(job.name())).doesNotContain(job);
    }

    @Test
    void testLockNotReleasedWhenJobOptsOut() throws InterruptedException
    {
        OperationalJobTracker tracker = new InMemoryOperationalJobTracker(4);
        OperationalJobCoordinator coordinator = mock(OperationalJobCoordinator.class);
        when(coordinator.trySetActive(any(), any())).thenReturn(true);
        OperationalJobManager manager = new OperationalJobManager(tracker, coordinator, executorPool);
        CountDownLatch latch = new CountDownLatch(1);

        // A distributed cluster-wide job that acquires the lock locally but relies on the orchestration
        // layer to clear it once all nodes finish, so the manager must not auto-release on local completion.
        OperationalJob job = createNonReleasingCoordinatedJob(UUIDs.timeBased());
        BiConsumer<OperationalJob, OperationalJobConflictException> onComplete = (j, ex) -> {
            assertThat(ex).isNull();
            latch.countDown();
        };

        manager.trySubmitJob(job, onComplete, executorPool.service(), SecondBoundConfiguration.parse("5s"));
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        verify(coordinator).trySetActive(OperationType.MOVE, job.jobId());
        verify(coordinator, after(1000).never()).clearActive(any(), any());
    }

    @Test
    void testCoordinatorNotCalledWhenJobDoesNotRequireCoordination() throws InterruptedException
    {
        OperationalJobTracker tracker = new InMemoryOperationalJobTracker(4);
        OperationalJobCoordinator coordinator = mock(OperationalJobCoordinator.class);
        OperationalJobManager manager = new OperationalJobManager(tracker, coordinator, executorPool);
        CountDownLatch latch = new CountDownLatch(1);

        OperationalJob job = OperationalJobTest.createOperationalJob(SUCCEEDED);
        BiConsumer<OperationalJob, OperationalJobConflictException> onComplete = (j, ex) -> {
            assertThat(ex).isNull();
            latch.countDown();
        };

        manager.trySubmitJob(job, onComplete, executorPool.service(), SecondBoundConfiguration.parse("5s"));
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        verify(coordinator, never()).trySetActive(any(), any());
    }

    private static OperationalJob createCoordinatedJob(UUID jobId)
    {
        return new OperationalJob(jobId)
        {
            @Override
            public boolean hasConflict(@NotNull List<OperationalJob> sameOperationJobs)
            {
                return false;
            }

            @Override
            public OperationType operationType()
            {
                return OperationType.MOVE;
            }

            @Override
            public boolean requiresCoordination()
            {
                return true;
            }

            @Override
            protected Future<Void> executeInternal()
            {
                return Future.succeededFuture();
            }
        };
    }

    private static OperationalJob createNonReleasingCoordinatedJob(UUID jobId)
    {
        return new OperationalJob(jobId)
        {
            @Override
            public boolean hasConflict(@NotNull List<OperationalJob> sameOperationJobs)
            {
                return false;
            }

            @Override
            public OperationType operationType()
            {
                return OperationType.MOVE;
            }

            @Override
            public boolean requiresCoordination()
            {
                return true;
            }

            @Override
            public boolean releasesOnCompletion()
            {
                return false;
            }

            @Override
            protected Future<Void> executeInternal()
            {
                return Future.succeededFuture();
            }
        };
    }
}
