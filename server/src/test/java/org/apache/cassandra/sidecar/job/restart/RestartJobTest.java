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

package org.apache.cassandra.sidecar.job.restart;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.data.Lifecycle.CassandraState;
import org.apache.cassandra.sidecar.common.data.Lifecycle.OperationStatus;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.response.LifecycleInfoResponse;
import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.response.data.RingEntry;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.exceptions.LifecycleTaskConflictException;
import org.apache.cassandra.sidecar.lifecycle.LifecycleManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RestartJob}.
 */
class RestartJobTest
{
    private static final int RETRY_ATTEMPTS = 3;
    private static final SecondBoundConfiguration HEALTH_TIMEOUT = SecondBoundConfiguration.parse("2s");
    private static final SecondBoundConfiguration TRANSITION_TIMEOUT = SecondBoundConfiguration.parse("2s");
    private static final String INSTANCE_HOST = "127.0.0.1";

    private final AtomicReference<CassandraState> currentState = new AtomicReference<>();
    private final AtomicReference<CassandraState> desiredState = new AtomicReference<>();
    private final AtomicReference<String> localRingStatus = new AtomicReference<>();
    private Supplier<RingResponse> ringSupplier;
    private LifecycleManager lifecycleManager;
    private RingHealthChecker healthChecker;
    private UUID nodeId;

    @BeforeEach
    void setup()
    {
        lifecycleManager = mock(LifecycleManager.class);
        healthChecker = mock(RingHealthChecker.class);
        nodeId = UUID.randomUUID();
        localRingStatus.set("Up");
        ringSupplier = () -> ring(localRingStatus.get());
    }

    @Test
    void testRunningNodeIsStoppedThenStarted() throws Exception
    {
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenReturn(true);
        stubLifecycle(CassandraState.RUNNING);

        RestartJob job = createJob();
        job.execute();

        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        verify(lifecycleManager).updateDesiredState(INSTANCE_HOST, CassandraState.STOPPED);
        verify(lifecycleManager).updateDesiredState(INSTANCE_HOST, CassandraState.RUNNING);
    }

    @Test
    void testAlreadyDownStartsWithoutHealthCheckOrStop() throws Exception
    {
        stubLifecycle(CassandraState.STOPPED);

        RestartJob job = createJob();
        job.execute();

        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        verify(healthChecker, never()).isHealthy(any(), any());
        verify(lifecycleManager, never()).updateDesiredState(any(), eq(CassandraState.STOPPED));
        verify(lifecycleManager, times(1)).updateDesiredState(INSTANCE_HOST, CassandraState.RUNNING);
    }

    @Test
    void testUnhealthyRingFailsWithoutTouchingTheNode() throws Exception
    {
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenReturn(false);
        stubLifecycle(CassandraState.RUNNING);

        RestartJob job = createJob(0);

        assertThatThrownBy(job::execute).isInstanceOf(RestartFailedException.class);
        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
        verify(lifecycleManager, never()).updateDesiredState(any(), any());
    }

    @Test
    void testStopConflictIsRetriedUntilItSucceeds() throws Exception
    {
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenReturn(true);
        stubLifecycle(CassandraState.RUNNING);
        AtomicInteger stopCallCount = new AtomicInteger(0);
        when(lifecycleManager.updateDesiredState(eq(INSTANCE_HOST), eq(CassandraState.STOPPED)))
        .thenAnswer(inv -> {
            if (stopCallCount.incrementAndGet() == 1)
            {
                throw new LifecycleTaskConflictException("Task already in progress");
            }
            converge(CassandraState.STOPPED);
            return info();
        });

        RestartJob job = createJob();
        job.execute();

        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertThat(stopCallCount.get()).isEqualTo(2);
    }

    @Test
    void testEachAttemptChecksRingHealthAndFailsAfterAllRetries() throws Exception
    {
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenReturn(true);
        stubLifecycle(CassandraState.RUNNING);
        when(lifecycleManager.updateDesiredState(eq(INSTANCE_HOST), eq(CassandraState.STOPPED)))
        .thenThrow(new RuntimeException("Stop failed"));
        int retryAttempts = 1;

        RestartJob job = createJob(retryAttempts);

        assertThatThrownBy(job::execute).isInstanceOf(RestartFailedException.class);
        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
        int totalAttempts = retryAttempts + 1;
        verify(healthChecker, times(totalAttempts)).isHealthy(any(), eq(nodeId));
        verify(lifecycleManager, times(totalAttempts)).updateDesiredState(INSTANCE_HOST, CassandraState.STOPPED);
    }

    @Test
    void testDivergedStartIsRetriedAsAStartOfAStoppedNode() throws Exception
    {
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenReturn(true);
        stubLifecycle(CassandraState.RUNNING);
        AtomicInteger startCallCount = new AtomicInteger(0);
        when(lifecycleManager.updateDesiredState(eq(INSTANCE_HOST), eq(CassandraState.RUNNING)))
        .thenAnswer(inv -> {
            desiredState.set(CassandraState.RUNNING);
            if (startCallCount.incrementAndGet() == 1)
            {
                currentState.set(CassandraState.STOPPED);
                return new LifecycleInfoResponse(CassandraState.STOPPED, CassandraState.RUNNING,
                                                 OperationStatus.DIVERGED, "test");
            }
            converge(CassandraState.RUNNING);
            return info();
        });
        when(lifecycleManager.getLifecycleInfo(INSTANCE_HOST)).thenAnswer(inv -> {
            if (startCallCount.get() == 1 && currentState.get() == CassandraState.STOPPED)
            {
                return new LifecycleInfoResponse(CassandraState.STOPPED, CassandraState.RUNNING,
                                                 OperationStatus.DIVERGED, "test");
            }
            return info();
        });

        RestartJob job = createJob();
        job.execute();

        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertThat(startCallCount.get()).isEqualTo(2);
        verify(lifecycleManager, times(1)).updateDesiredState(INSTANCE_HOST, CassandraState.STOPPED);
    }

    @Test
    void testStartFailsWhenTheLocalNodeNeverBecomesNormal() throws Exception
    {
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenReturn(true);
        stubLifecycle(CassandraState.RUNNING);
        localRingStatus.set("Down");

        RestartJob job = createJob(0);

        assertThatThrownBy(job::execute).isInstanceOf(RestartFailedException.class);
        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
    }

    @Test
    void testStartWaitsUntilTheLocalNodeIsNormal() throws Exception
    {
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenReturn(true);
        stubLifecycle(CassandraState.RUNNING);
        AtomicBoolean startRequested = new AtomicBoolean();
        when(lifecycleManager.updateDesiredState(eq(INSTANCE_HOST), eq(CassandraState.RUNNING)))
        .thenAnswer(inv -> {
            converge(CassandraState.RUNNING);
            startRequested.set(true);
            return info();
        });
        AtomicInteger failedReads = new AtomicInteger();
        ringSupplier = () -> {
            if (startRequested.get() && failedReads.get() < 3)
            {
                failedReads.incrementAndGet();
                throw new RuntimeException("JMX not available yet");
            }
            return ring(localRingStatus.get());
        };

        RestartJob job = createJob();
        job.execute();

        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertThat(failedReads.get()).isEqualTo(3);
        verify(lifecycleManager, times(1)).updateDesiredState(INSTANCE_HOST, CassandraState.RUNNING);
    }

    @Test
    void testOnJobFailedStartsStoppedNode() throws Exception
    {
        when(lifecycleManager.getLifecycleInfo(INSTANCE_HOST))
        .thenReturn(new LifecycleInfoResponse(CassandraState.STOPPED, CassandraState.STOPPED,
                                              OperationStatus.CONVERGED, "test"));

        createJob().onJobFailed();

        verify(lifecycleManager).updateDesiredState(INSTANCE_HOST, CassandraState.RUNNING);
    }

    @Test
    void testOnJobFailedLeavesRunningNodeAlone() throws Exception
    {
        when(lifecycleManager.getLifecycleInfo(INSTANCE_HOST))
        .thenReturn(new LifecycleInfoResponse(CassandraState.RUNNING, CassandraState.RUNNING,
                                              OperationStatus.CONVERGED, "test"));

        createJob().onJobFailed();

        verify(lifecycleManager, never()).updateDesiredState(any(), any());
    }

    @Test
    void testOnJobFailedStartsNodeWhoseStopHasNotConvergedYet() throws Exception
    {
        when(lifecycleManager.getLifecycleInfo(INSTANCE_HOST))
        .thenReturn(new LifecycleInfoResponse(CassandraState.RUNNING, CassandraState.STOPPED,
                                              OperationStatus.CONVERGING, "test"));

        createJob().onJobFailed();

        verify(lifecycleManager).updateDesiredState(INSTANCE_HOST, CassandraState.RUNNING);
    }

    @Test
    void testOnJobFailedRetriesWhileTheStopItIsRecoveringFromIsStillInFlight() throws Exception
    {
        when(lifecycleManager.getLifecycleInfo(INSTANCE_HOST))
        .thenReturn(new LifecycleInfoResponse(CassandraState.STOPPED, CassandraState.STOPPED,
                                              OperationStatus.CONVERGED, "test"));
        AtomicInteger attempts = new AtomicInteger();
        when(lifecycleManager.updateDesiredState(eq(INSTANCE_HOST), eq(CassandraState.RUNNING)))
        .thenAnswer(inv -> {
            if (attempts.incrementAndGet() < 3)
            {
                throw new LifecycleTaskConflictException("Task already in progress for this host.");
            }
            return new LifecycleInfoResponse(CassandraState.RUNNING, CassandraState.RUNNING,
                                             OperationStatus.CONVERGING, "test");
        });

        createJob().onJobFailed();

        verify(lifecycleManager, times(3)).updateDesiredState(INSTANCE_HOST, CassandraState.RUNNING);
    }

    @Test
    void testCancelBeforeExecuteSkipsAllWork() throws Exception
    {
        stubLifecycle(CassandraState.RUNNING);

        RestartJob job = createJob();
        job.cancel();

        assertThatThrownBy(job::execute).isInstanceOf(RestartFailedException.class);
        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
        verify(lifecycleManager, never()).updateDesiredState(any(), any());
    }

    @Test
    void testCancelDuringRingHealthCheckFailsAsCancelled() throws Exception
    {
        stubLifecycle(CassandraState.RUNNING);
        RestartJob job = createJob(0);
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenAnswer(inv -> {
            job.cancel();
            return false;
        });

        assertThatThrownBy(job::execute).isInstanceOf(RestartFailedException.class)
                                        .hasMessageStartingWith("Restart cancelled");
        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
        verify(lifecycleManager, never()).updateDesiredState(any(), eq(CassandraState.STOPPED));
        verify(healthChecker, times(1)).isHealthy(any(), eq(nodeId));
    }

    @Test
    void testCancelBeforeTheStopIsRequestedFailsAsCancelledAndLeavesTheNodeRunning() throws Exception
    {
        stubLifecycle(CassandraState.RUNNING);
        RestartJob job = createJob(0);
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenAnswer(inv -> {
            job.cancel();
            return true;
        });

        assertThatThrownBy(job::execute).isInstanceOf(RestartFailedException.class)
                                        .hasMessageStartingWith("Restart cancelled");
        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
        verify(lifecycleManager, never()).updateDesiredState(any(), eq(CassandraState.STOPPED));
        assertThat(currentState.get()).isEqualTo(CassandraState.RUNNING);
    }

    @Test
    void testCancelWhileStoppingStillBringsTheNodeBackUp() throws Exception
    {
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenReturn(true);
        stubLifecycle(CassandraState.RUNNING);
        RestartJob job = createJob();
        when(lifecycleManager.updateDesiredState(eq(INSTANCE_HOST), eq(CassandraState.STOPPED)))
        .thenAnswer(inv -> {
            converge(CassandraState.STOPPED);
            job.cancel();
            return info();
        });

        job.execute();

        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        verify(lifecycleManager).updateDesiredState(INSTANCE_HOST, CassandraState.RUNNING);
        assertThat(currentState.get()).isEqualTo(CassandraState.RUNNING);
    }

    @Test
    void testCancelWhileStartingStillBringsTheNodeBackUp() throws Exception
    {
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenReturn(true);
        stubLifecycle(CassandraState.RUNNING);
        RestartJob job = createJob();
        AtomicInteger pollsSinceStartRequested = new AtomicInteger(-1);
        when(lifecycleManager.getLifecycleInfo(INSTANCE_HOST)).thenAnswer(inv -> {
            if (pollsSinceStartRequested.get() >= 0 && pollsSinceStartRequested.incrementAndGet() >= 2)
            {
                currentState.set(CassandraState.RUNNING);
            }
            return info();
        });
        when(lifecycleManager.updateDesiredState(eq(INSTANCE_HOST), eq(CassandraState.RUNNING)))
        .thenAnswer(inv -> {
            desiredState.set(CassandraState.RUNNING);
            pollsSinceStartRequested.set(0);
            job.cancel();
            return info();
        });

        job.execute();

        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertThat(currentState.get()).isEqualTo(CassandraState.RUNNING);
    }

    private RestartJob createJob()
    {
        return createJob(RETRY_ATTEMPTS);
    }

    private RestartJob createJob(int retryAttempts)
    {
        RestartJob job = new RestartJob(UUID.randomUUID(), nodeId, INSTANCE_HOST, OperationType.RESTART,
                                        lifecycleManager, healthChecker,
                                        retryAttempts, HEALTH_TIMEOUT, TRANSITION_TIMEOUT,
                                        () -> ringSupplier.get());
        job.setPollIntervalsForTesting(10, 10);
        return job;
    }

    private RingResponse ring(String localStatus)
    {
        RingResponse ring = new RingResponse();
        ring.add(new RingEntry.Builder()
                 .datacenter("dc1")
                 .rack("rack1")
                 .status(localStatus)
                 .state("Normal")
                 .address(INSTANCE_HOST)
                 .port(9042)
                 .token("0")
                 .load("100 KiB")
                 .owns("")
                 .fqdn("localhost")
                 .hostId(nodeId.toString())
                 .build());
        return ring;
    }

    private void stubLifecycle(CassandraState initial) throws Exception
    {
        currentState.set(initial);
        desiredState.set(initial);
        when(lifecycleManager.getLifecycleInfo(INSTANCE_HOST)).thenAnswer(inv -> info());
        when(lifecycleManager.updateDesiredState(eq(INSTANCE_HOST), any())).thenAnswer(inv -> {
            converge(inv.getArgument(1));
            return info();
        });
    }

    private void converge(CassandraState state)
    {
        desiredState.set(state);
        currentState.set(state);
    }

    private LifecycleInfoResponse info()
    {
        OperationStatus status = currentState.get() == desiredState.get()
                                 ? OperationStatus.CONVERGED : OperationStatus.CONVERGING;
        return new LifecycleInfoResponse(currentState.get(), desiredState.get(), status, "test");
    }
}
