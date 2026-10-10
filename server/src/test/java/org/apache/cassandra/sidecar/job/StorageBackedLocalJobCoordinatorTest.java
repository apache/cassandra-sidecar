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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.OperationalJobConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.OperationalJobConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;
import org.apache.cassandra.sidecar.job.storage.StorageProvider;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.TimeProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link StorageBackedLocalJobCoordinator}
 */
class StorageBackedLocalJobCoordinatorTest
{
    private static final UUID OPERATION_ID = UUIDs.timeBased();
    private static final UUID NODE_1 = UUID.randomUUID();
    private static final UUID NODE_2 = UUID.randomUUID();

    private Vertx vertx;
    private ExecutorPools executorPools;
    private OperationalJobCoordinator operationalJobCoordinator;
    private StorageProvider storageProvider;
    private InstanceMetadataFetcher instanceMetadataFetcher;
    private LocalJobManager localJobManager;
    private StorageBackedLocalJobCoordinator coordinator;
    private LocalJobFactory localJobFactory;
    private final FakeTimeProvider timeProvider = new FakeTimeProvider();

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());
        operationalJobCoordinator = mock(OperationalJobCoordinator.class);
        storageProvider = mock(StorageProvider.class);
        instanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        localJobManager = new LocalJobManager(executorPools);
    }

    @AfterEach
    void cleanup()
    {
        TestResourceReaper.create().with(vertx).with(executorPools).close();
    }

    @Test
    void testIdleWhenNoActiveOperations()
    {
        buildCoordinator();
        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations()).thenReturn(Collections.emptyMap());

        executeAndWait();

        verify(storageProvider, never()).findJob(any());
    }

    @Test
    void testIdleWhenStorageProviderUnavailable()
    {
        buildCoordinator();
        when(storageProvider.isAvailable()).thenReturn(false);

        executeAndWait();

        verify(operationalJobCoordinator, never()).getActiveOperations();
    }

    @Test
    void testLocalNodeNotInExecutionOrder()
    {
        buildCoordinator();
        setupSingleNodeInstance(NODE_1, "127.0.0.1");
        stubActiveOperation(createJobRecord(group(UUID.randomUUID())), new HashMap<>());

        executeAndWait();

        verify(storageProvider, never()).updateNodeStatus(any(), any(), any());
    }

    @Test
    void testWaitForPredecessorsWhenEarlierGroupsNotDone()
    {
        buildCoordinator();
        setupSingleNodeInstance(NODE_2, "127.0.0.2");
        stubActiveOperation(createJobRecord(group(NODE_1), group(NODE_2)),
                            statuses(NODE_1, OperationalJobStatus.RUNNING));

        executeAndWait();

        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testWaitForPredecessorsWhenEarlierGroupsNotStarted()
    {
        buildCoordinator();
        setupSingleNodeInstance(NODE_2, "127.0.0.2");
        stubActiveOperation(createJobRecord(group(NODE_1), group(NODE_2)),
                            statuses(NODE_1, OperationalJobStatus.CREATED));

        executeAndWait();

        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testPredecessorsCompleteAllSucceeded()
    {
        buildCoordinator();
        setupSingleNodeInstance(NODE_2, "127.0.0.2");
        stubActiveOperation(createJobRecord(group(NODE_1), group(NODE_2)),
                            statuses(NODE_1, OperationalJobStatus.SUCCEEDED));

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testFailedPredecessorDoesNotBlockWhenRequiresPredecessorSuccessFalse()
    {
        buildCoordinator();
        setupSingleNodeInstance(NODE_2, "127.0.0.2");
        stubActiveOperation(createJobRecord(group(NODE_1), group(NODE_2)),
                            statuses(NODE_1, OperationalJobStatus.FAILED));

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testZeroWaitBetweenExecutionGroupsSubmitsImmediately()
    {
        localJobFactory = new TestLocalJobFactory().withWaitBetweenExecutionGroups("0s");
        buildCoordinator();
        setupSingleNodeInstance(NODE_2, "127.0.0.2");
        stubActiveOperation(createJobRecord(group(NODE_1), group(NODE_2)),
                            statuses(NODE_1, OperationalJobStatus.SUCCEEDED));

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testWaitBetweenExecutionGroupsBlocksSubmissionUntilElapsed()
    {
        localJobFactory = new TestLocalJobFactory().withWaitBetweenExecutionGroups("60s");
        buildCoordinator();
        setupSingleNodeInstance(NODE_2, "127.0.0.2");
        stubActiveOperation(createJobRecord(group(NODE_1), group(NODE_2)),
                            statuses(NODE_1, OperationalJobStatus.SUCCEEDED));

        executeAndWait();
        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);

        timeProvider.advance(59, TimeUnit.SECONDS);
        executeAndWait();
        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);

        timeProvider.advance(2, TimeUnit.SECONDS);
        executeAndWait();
        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testWaitBetweenExecutionGroupsRunsFromFirstObservationNotOperationStart()
    {
        localJobFactory = new TestLocalJobFactory().withWaitBetweenExecutionGroups("60s");
        buildCoordinator();
        setupSingleNodeInstance(NODE_2, "127.0.0.2");
        Map<UUID, OperationalJobStatus> statuses = statuses(NODE_1, OperationalJobStatus.RUNNING);
        stubActiveOperation(createJobRecord(group(NODE_1), group(NODE_2)), statuses);

        executeAndWait();
        timeProvider.advance(10, TimeUnit.MINUTES);
        statuses.put(NODE_1, OperationalJobStatus.SUCCEEDED);

        executeAndWait();
        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);

        timeProvider.advance(61, TimeUnit.SECONDS);
        executeAndWait();
        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testWaitBetweenExecutionGroupsDoesNotDelayTheFirstGroup()
    {
        localJobFactory = new TestLocalJobFactory().withWaitBetweenExecutionGroups("60s");
        buildCoordinator();
        setupSingleNodeInstance(NODE_1, "127.0.0.1");
        stubActiveOperation(createJobRecord(group(NODE_1), group(NODE_2)), new HashMap<>());

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
    }

    @Test
    void testCrashRecoveryIsNotBlockedByTheInterGroupWait()
    {
        localJobFactory = new TestLocalJobFactory().withCanRecover().withWaitBetweenExecutionGroups("60s");
        buildCoordinator();
        setupSingleNodeInstance(NODE_2, "127.0.0.2");
        stubActiveOperation(createJobRecord(group(NODE_1), group(NODE_2)),
                            statuses(NODE_1, OperationalJobStatus.SUCCEEDED, NODE_2, OperationalJobStatus.RUNNING));

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.CREATED);
        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testLocalNodeAlreadySucceeded()
    {
        buildCoordinator();
        setupSingleNodeInstance(NODE_1, "127.0.0.1");
        stubActiveOperation(createJobRecord(group(NODE_1)), statuses(NODE_1, OperationalJobStatus.SUCCEEDED));

        executeAndWait();

        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
    }

    @Test
    void testLocalNodeAlreadyFailed()
    {
        buildCoordinator();
        setupSingleNodeInstance(NODE_1, "127.0.0.1");
        stubActiveOperation(createJobRecord(group(NODE_1)), statuses(NODE_1, OperationalJobStatus.FAILED));

        executeAndWait();

        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
    }

    @Test
    void testMultipleLocalInstances()
    {
        buildCoordinator();
        setupMultipleNodeInstances(new UUID[]{ NODE_1, NODE_2 }, new String[]{ "127.0.0.1", "127.0.0.2" });
        stubActiveOperation(createJobRecord(group(NODE_1, NODE_2)), new HashMap<>());

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testArePredecessorsCompleteBlocksWhenRequiresPredecessorSuccessTrue()
    {
        buildCoordinator();
        List<List<UUID>> executionOrder = Arrays.asList(group(NODE_1), group(NODE_2));

        assertThat(coordinator.arePredecessorsComplete(NODE_2, executionOrder,
                                                       statuses(NODE_1, OperationalJobStatus.FAILED), true))
        .isFalse();
    }

    @Test
    void testArePredecessorsCompleteAllowsProgressWhenRequiresPredecessorSuccessFalse()
    {
        buildCoordinator();
        List<List<UUID>> executionOrder = Arrays.asList(group(NODE_1), group(NODE_2));

        assertThat(coordinator.arePredecessorsComplete(NODE_2, executionOrder,
                                                       statuses(NODE_1, OperationalJobStatus.FAILED), false))
        .isTrue();
    }

    @Test
    void testArePredecessorsCompleteReturnsFalseForUnknownNode()
    {
        buildCoordinator();
        List<List<UUID>> executionOrder = Collections.singletonList(group(NODE_1));

        assertThat(coordinator.arePredecessorsComplete(UUID.randomUUID(), executionOrder, new HashMap<>(), false))
        .isFalse();
    }

    @Test
    void testFailedPredecessorReportsSuccessorFailedWhenRequiresPredecessorSuccessTrue()
    {
        localJobFactory = new TestLocalJobFactory().withRequiresPredecessorSuccess();
        buildCoordinator();
        setupSingleNodeInstance(NODE_2, "127.0.0.2");
        stubActiveOperation(createJobRecord(group(NODE_1), group(NODE_2)),
                            statuses(NODE_1, OperationalJobStatus.FAILED));

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.FAILED);
        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testJobSubmittedAndStatusReported()
    {
        buildCoordinator();
        setupSingleNodeInstance(NODE_1, "127.0.0.1");
        stubActiveOperation(createJobRecord(group(NODE_1)), new HashMap<>());

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
        assertThat(localJobManager.getJob(OPERATION_ID, NODE_1)).isNotNull();
    }

    @Test
    void testClearActiveOperationWhenAllNodesSucceeded()
    {
        buildCoordinator();
        setupSingleNodeInstance(NODE_1, "127.0.0.1");
        stubActiveOperation(createJobRecord(group(NODE_1, NODE_2)),
                            statuses(NODE_1, OperationalJobStatus.SUCCEEDED, NODE_2, OperationalJobStatus.SUCCEEDED));
        when(operationalJobCoordinator.clearActive(OperationType.DRAIN, OPERATION_ID)).thenReturn(true);

        executeAndWait();

        verify(storageProvider).updateJobStatus(eq(OPERATION_ID), eq(OperationType.DRAIN),
                                                eq(OperationalJobStatus.SUCCEEDED), any());
        verify(operationalJobCoordinator).clearActive(OperationType.DRAIN, OPERATION_ID);
    }

    @Test
    void testClearActiveOperationWhenSomeNodesFailed()
    {
        buildCoordinator();
        setupSingleNodeInstance(NODE_1, "127.0.0.1");
        stubActiveOperation(createJobRecord(group(NODE_1, NODE_2)),
                            statuses(NODE_1, OperationalJobStatus.SUCCEEDED, NODE_2, OperationalJobStatus.FAILED));
        when(operationalJobCoordinator.clearActive(OperationType.DRAIN, OPERATION_ID)).thenReturn(true);

        executeAndWait();

        verify(storageProvider).updateJobStatus(eq(OPERATION_ID), eq(OperationType.DRAIN),
                                                eq(OperationalJobStatus.FAILED), any());
        verify(operationalJobCoordinator).clearActive(OperationType.DRAIN, OPERATION_ID);
    }

    @Test
    void testRecoveryRunsWhenLocalJobFails() throws InterruptedException
    {
        CountDownLatch recovered = new CountDownLatch(1);
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
                throw new RuntimeException("Exception");
            }

            @Override
            public Future<Void> onJobFailed()
            {
                recovered.countDown();
                return Future.succeededFuture();
            }
        };
        buildCoordinator();
        setupSingleNodeInstance(NODE_1, "127.0.0.1");
        stubActiveOperation(createJobRecord(group(NODE_1)), new HashMap<>());

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
        verify(storageProvider, timeout(5000)).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.FAILED);
        assertThat(recovered.await(5, TimeUnit.SECONDS))
            .describedAs("Recovery must run on the node's own FAILED transition")
            .isTrue();
    }

    @Test
    void testTimeoutCancelsLocalJobAndRunsRecovery() throws InterruptedException
    {
        CountDownLatch recovered = new CountDownLatch(1);
        AtomicBoolean cancelObserved = new AtomicBoolean(false);
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
                while (!isCancelled())
                {
                    try
                    {
                        Thread.sleep(20);
                    }
                    catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                cancelObserved.set(true);
                throw new RuntimeException("cancelled");
            }

            @Override
            public Future<Void> onJobFailed()
            {
                recovered.countDown();
                return Future.succeededFuture();
            }
        };
        OperationalJobConfiguration config = OperationalJobConfigurationImpl.builder()
                                                                            .nodeExecutionTimeout(SecondBoundConfiguration.parse("1s"))
                                                                            .build();
        buildCoordinator(config);
        setupSingleNodeInstance(NODE_1, "127.0.0.1");
        stubActiveOperation(createJobRecord(group(NODE_1)), new HashMap<>());

        executeAndWait();

        verify(storageProvider, timeout(5000)).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.FAILED);
        assertThat(recovered.await(5, TimeUnit.SECONDS))
            .describedAs("Recovery must run after the timed-out job unwinds")
            .isTrue();
        assertThat(cancelObserved.get())
            .describedAs("The running job must observe cancellation on timeout")
            .isTrue();
    }

    @Test
    void testRecoveryNotRunWhenLocalJobSucceeds()
    {
        AtomicBoolean recovered = new AtomicBoolean(false);
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
            }

            @Override
            public Future<Void> onJobFailed()
            {
                recovered.set(true);
                return Future.succeededFuture();
            }
        };
        buildCoordinator();
        setupSingleNodeInstance(NODE_1, "127.0.0.1");
        stubActiveOperation(createJobRecord(group(NODE_1)), new HashMap<>());

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
        verify(storageProvider, timeout(5000)).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.SUCCEEDED);
        assertThat(recovered.get())
            .describedAs("Recovery must not run for a successful job")
            .isFalse();
    }

    @Test
    void testRecoveryFailureDoesNotPropagate() throws InterruptedException
    {
        CountDownLatch recoveryAttempted = new CountDownLatch(1);
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
                throw new RuntimeException("Exception");
            }

            @Override
            public Future<Void> onJobFailed()
            {
                recoveryAttempted.countDown();
                return Future.failedFuture(new RuntimeException("Reconciliation error"));
            }
        };
        buildCoordinator();
        setupSingleNodeInstance(NODE_1, "127.0.0.1");
        stubActiveOperation(createJobRecord(group(NODE_1)), new HashMap<>());

        executeAndWait();

        verify(storageProvider, timeout(5000)).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.FAILED);
        assertThat(recoveryAttempted.await(5, TimeUnit.SECONDS))
            .describedAs("A failing recovery future is swallowed, not propagated")
            .isTrue();
    }

    @Test
    void testDoNotClearWhenNodesStillRunning()
    {
        buildCoordinator();
        setupSingleNodeInstance(NODE_1, "127.0.0.1");
        stubActiveOperation(createJobRecord(group(NODE_1, NODE_2)),
                            statuses(NODE_1, OperationalJobStatus.SUCCEEDED, NODE_2, OperationalJobStatus.RUNNING));

        executeAndWait();

        verify(operationalJobCoordinator, never()).clearActive(any(), any());
    }

    @Test
    void testCrashRecoveryCanRecover()
    {
        localJobFactory = new TestLocalJobFactory().withCanRecover();
        buildCoordinator();
        setupSingleNodeInstance(NODE_1, "127.0.0.1");
        stubActiveOperation(createJobRecord(group(NODE_1)), statuses(NODE_1, OperationalJobStatus.RUNNING));

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.CREATED);
        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
    }

    @Test
    void testCrashRecoveryCannotRecoverStillRunsOnJobFailed()
    {
        AtomicBoolean onJobFailedInvoked = new AtomicBoolean(false);
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
            }

            @Override
            public Future<Void> onJobFailed()
            {
                onJobFailedInvoked.set(true);
                return Future.succeededFuture();
            }
        };
        buildCoordinator();
        setupSingleNodeInstance(NODE_1, "127.0.0.1");
        stubActiveOperation(createJobRecord(group(NODE_1)), statuses(NODE_1, OperationalJobStatus.RUNNING));

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.FAILED);
        assertThat(onJobFailedInvoked).isTrue();
    }

    @Test
    void testCreatedNodeIsSubmitted()
    {
        buildCoordinator();
        setupSingleNodeInstance(NODE_1, "127.0.0.1");
        stubActiveOperation(createJobRecord(group(NODE_1)), statuses(NODE_1, OperationalJobStatus.CREATED));

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
    }

    @Test
    void testMultipleConcurrentActiveOperations()
    {
        buildCoordinator();

        UUID drainOperationId = OPERATION_ID;
        UUID repairOperationId = UUIDs.timeBased();

        OperationalJobRecord drainJob = createJobRecord(group(NODE_1));
        OperationalJobRecord repairJob = OperationalJobRecord.builder()
                                                             .jobId(repairOperationId)
                                                             .operationType(OperationType.REPAIR)
                                                             .status(OperationalJobStatus.RUNNING)
                                                             .nodeExecutionOrder(Collections.singletonList(group(NODE_2)))
                                                             .build();

        setupMultipleNodeInstances(new UUID[]{ NODE_1, NODE_2 }, new String[]{ "127.0.0.1", "127.0.0.2" });

        Map<OperationType, UUID> activeOps = new HashMap<>();
        activeOps.put(OperationType.DRAIN, drainOperationId);
        activeOps.put(OperationType.REPAIR, repairOperationId);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations()).thenReturn(activeOps);
        when(storageProvider.findJob(drainOperationId)).thenReturn(drainJob);
        when(storageProvider.findJob(repairOperationId)).thenReturn(repairJob);
        when(storageProvider.getNodeStatusesForOperation(drainOperationId)).thenReturn(new HashMap<>());
        when(storageProvider.getNodeStatusesForOperation(repairOperationId)).thenReturn(new HashMap<>());

        executeAndWait();

        verify(storageProvider).updateNodeStatus(drainOperationId, NODE_1, OperationalJobStatus.RUNNING);
        verify(storageProvider).updateNodeStatus(repairOperationId, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testStorageProviderExceptionHandledGracefully()
    {
        buildCoordinator();
        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenThrow(new RuntimeException("storage error"));

        executeAndWait();
    }

    @Test
    void testScheduleDecisionSkipsWhenDisabled()
    {
        OperationalJobConfiguration config = OperationalJobConfigurationImpl.builder()
                                                                            .coordinationEnabled(false)
                                                                            .build();
        buildCoordinator(config);
        assertThat(coordinator.scheduleDecision()).isEqualTo(ScheduleDecision.SKIP);
    }

    @Test
    void testRandomizedInitialDelay()
    {
        buildCoordinator();
        long delayMs = coordinator.delay().toMillis();
        long initialDelayMs = coordinator.initialDelay().toMillis();
        assertThat(initialDelayMs).isBetween(0L, delayMs);
    }

    private void buildCoordinator()
    {
        buildCoordinator(new OperationalJobConfigurationImpl());
    }

    private void buildCoordinator(OperationalJobConfiguration config)
    {
        SidecarConfiguration sidecarConfig = mock(SidecarConfiguration.class);
        when(sidecarConfig.operationalJobConfiguration()).thenReturn(config);
        if (localJobFactory == null)
        {
            localJobFactory = new TestLocalJobFactory();
        }
        coordinator = new StorageBackedLocalJobCoordinator(executorPools,
                                                           operationalJobCoordinator,
                                                           storageProvider,
                                                           instanceMetadataFetcher,
                                                           localJobManager,
                                                           sidecarConfig,
                                                           localJobFactory,
                                                           timeProvider);
    }

    static class FakeTimeProvider implements TimeProvider
    {
        private final AtomicLong nanos = new AtomicLong(0);

        @Override
        public long currentTimeMillis()
        {
            return TimeUnit.NANOSECONDS.toMillis(nanos.get());
        }

        @Override
        public long nanoTime()
        {
            return nanos.get();
        }

        void advance(long value, TimeUnit unit)
        {
            nanos.addAndGet(unit.toNanos(value));
        }
    }

    static class TestLocalJobFactory implements LocalJobFactory
    {
        private boolean canRecover;
        private boolean requiresPredecessorSuccess;
        private SecondBoundConfiguration waitBetweenExecutionGroups;

        TestLocalJobFactory withCanRecover()
        {
            canRecover = true;
            return this;
        }

        TestLocalJobFactory withRequiresPredecessorSuccess()
        {
            requiresPredecessorSuccess = true;
            return this;
        }

        TestLocalJobFactory withWaitBetweenExecutionGroups(String wait)
        {
            waitBetweenExecutionGroups = SecondBoundConfiguration.parse(wait);
            return this;
        }

        @Override
        public LocalJob createJob(UUID opId, UUID nodeId, String host, OperationType opType)
        {
            return new LocalJob(opId, nodeId, host, opType)
            {
                @Override
                protected void executeInternal()
                {
                }
            };
        }

        @Override
        public boolean canRecover()
        {
            return canRecover;
        }

        @Override
        public boolean requiresPredecessorSuccess()
        {
            return requiresPredecessorSuccess;
        }

        @Override
        public SecondBoundConfiguration waitBetweenExecutionGroups(Map<String, String> operationMetadata)
        {
            return waitBetweenExecutionGroups;
        }
    }

    private static List<UUID> group(UUID... nodeIds)
    {
        return Arrays.asList(nodeIds);
    }

    @SafeVarargs
    private static OperationalJobRecord createJobRecord(List<UUID>... groups)
    {
        return OperationalJobRecord.builder()
                                   .jobId(OPERATION_ID)
                                   .operationType(OperationType.DRAIN)
                                   .status(OperationalJobStatus.RUNNING)
                                   .nodeExecutionOrder(Arrays.asList(groups))
                                   .build();
    }

    private static Map<UUID, OperationalJobStatus> statuses(UUID nodeId, OperationalJobStatus status)
    {
        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(nodeId, status);
        return statuses;
    }

    private static Map<UUID, OperationalJobStatus> statuses(UUID nodeId1, OperationalJobStatus status1,
                                                            UUID nodeId2, OperationalJobStatus status2)
    {
        Map<UUID, OperationalJobStatus> statuses = statuses(nodeId1, status1);
        statuses.put(nodeId2, status2);
        return statuses;
    }

    private void stubActiveOperation(OperationalJobRecord jobRecord, Map<UUID, OperationalJobStatus> nodeStatuses)
    {
        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(jobRecord.operationType(), jobRecord.jobId()));
        when(storageProvider.findJob(jobRecord.jobId())).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(jobRecord.jobId())).thenReturn(nodeStatuses);
    }

    private void setupSingleNodeInstance(UUID nodeId, String host)
    {
        setupMultipleNodeInstances(new UUID[]{ nodeId }, new String[]{ host });
    }

    private void setupMultipleNodeInstances(UUID[] nodeIds, String[] hosts)
    {
        List<InstanceMetadata> instances = new ArrayList<>();
        for (int i = 0; i < nodeIds.length; i++)
        {
            InstanceMetadata instance = mock(InstanceMetadata.class);
            CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
            NodeSettings nodeSettings = NodeSettings.builder().hostId(nodeIds[i]).build();

            when(instance.host()).thenReturn(hosts[i]);
            when(instance.delegate()).thenReturn(delegate);
            when(delegate.nodeSettings()).thenReturn(nodeSettings);
            instances.add(instance);
        }
        when(instanceMetadataFetcher.allLocalInstances()).thenReturn(instances);
    }

    private void executeAndWait()
    {
        CountDownLatch latch = new CountDownLatch(1);
        Promise<Void> promise = Promise.promise();
        promise.future().onComplete(ar -> latch.countDown());
        coordinator.execute(promise);
        try
        {
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}
