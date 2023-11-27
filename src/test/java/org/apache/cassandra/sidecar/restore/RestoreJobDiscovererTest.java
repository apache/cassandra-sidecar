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

package org.apache.cassandra.sidecar.restore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobDatabaseAccessor;
import org.apache.cassandra.sidecar.db.RestoreJobTest;
import org.apache.cassandra.sidecar.db.RestoreSliceDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.stats.TestRestoreJobStats;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RestoreJobDiscovererTest
{
    private static final long activeLoopDelay = 1000;
    private static final long idleLoopDelay = 2000;
    private static final int recencyDays = 10;
    private final TestRestoreJobStats stats = new TestRestoreJobStats();
    private RestoreJobDatabaseAccessor mockJobAccessor = mock(RestoreJobDatabaseAccessor.class);
    private RestoreSliceDatabaseAccessor mockSliceAccessor = mock(RestoreSliceDatabaseAccessor.class);
    private RestoreJobManagerGroup mockManagers = mock(RestoreJobManagerGroup.class);
    private PeriodicTaskExecutor executor = mock(PeriodicTaskExecutor.class);
    private SidecarSchema sidecarSchema = mock(SidecarSchema.class);
    private RestoreJobDiscoverer loop = new RestoreJobDiscoverer(testConfig(),
                                                                 sidecarSchema,
                                                                 mockJobAccessor,
                                                                 mockSliceAccessor,
                                                                 () -> mockManagers,
                                                                 null,
                                                                 stats);

    @Test
    void testGetDelay()
    {
        // when there is no active restore job. The delay is idle loop delay
        assertThat(loop.delay()).isEqualTo(idleLoopDelay);
        // when there is active restore job (status: CREATED)
        UUID jobId = UUIDs.timeBased();
        when(mockJobAccessor.findAllRecent(anyInt()))
        .thenReturn(Collections.singletonList(RestoreJob.forUpdates(jobId, "agent",
                                                                    RestoreJobStatus.CREATED, null,
                                                                    new Date(System.currentTimeMillis() + 10000L))));
        loop.registerPeriodicTaskExecutor(executor);
        executeBlocking();
        assertThat(stats.activeJobCount).describedAs("active jobs count is updated")
                                        .isOne();
        assertThat(loop.delay()).isEqualTo(activeLoopDelay);
        // when no more jobs are active, the delay is reset back to idle loop delay accordingly.
        when(mockJobAccessor.findAllRecent(anyInt()))
        .thenReturn(Collections.singletonList(RestoreJob.forUpdates(jobId, "agent",
                                                                    RestoreJobStatus.SUCCEEDED, null,
                                                                    new Date(System.currentTimeMillis() + 10000L))));
        executeBlocking();
        assertThat(stats.activeJobCount).describedAs("active jobs count is updated")
                                        .isZero();
        assertThat(loop.delay()).isEqualTo(idleLoopDelay);
    }

    // A carefully curated test script for several executions (i.e. loop) that should cover all cases
    // In the first execution, it finds 3 restore jobs, a new, a failed and a succeeded.
    // In the second execution, it finds 1 restore job, and it succeeds
    // In the third execution, it is no-op, as there is no inflight job
    // Signal refresh and in the fourth execution, it finds a new job
    // In the fifth execution, it finds the job failed and there is no more inflight jobs
    @Test
    void testExecute() throws JsonProcessingException
    {
        when(sidecarSchema.isInitialized()).thenReturn(true);
        // setup, cassandra should return 3 jobs, a new job, a failed and a succeeded
        List<RestoreJob> mockResult = new ArrayList<>(3);
        UUID newJobId = UUIDs.timeBased();
        UUID failedJobId = UUIDs.timeBased();
        UUID succeededJobId = UUIDs.timeBased();
        mockResult.add(RestoreJobTest.createNewTestingJob(newJobId));
        mockResult.add(RestoreJob.forUpdates(failedJobId, "agent", RestoreJobStatus.ABORTED, null,
                                             new Date(System.currentTimeMillis() + 10000L)));
        mockResult.add(RestoreJob.forUpdates(succeededJobId, "agent", RestoreJobStatus.SUCCEEDED, null,
                                             new Date(System.currentTimeMillis() + 10000L)));
        ArgumentCaptor<UUID> jobIdCapture = ArgumentCaptor.forClass(UUID.class);
        doNothing().when(mockManagers).removeJobInternal(jobIdCapture.capture());
        when(mockJobAccessor.findAllRecent(anyInt())).thenReturn(mockResult);
        loop.registerPeriodicTaskExecutor(executor);

        assertThat(loop.hasInflightJobs())
        .describedAs("No inflight jobs are discovery when loop has not started")
        .isFalse();
        assertThat(loop.jobDiscoveryRecencyDays())
        .describedAs("Initial recency days should be " + recencyDays)
        .isEqualTo(recencyDays);
        assertThat(stats.activeJobCount).isZero();

        // Execution 1
        executeBlocking();

        assertThat(stats.activeJobCount).isOne();
        assertThat(jobIdCapture.getAllValues()).containsExactlyInAnyOrder(failedJobId, succeededJobId);
        assertThat(loop.hasInflightJobs())
        .describedAs("An inflight job should be found")
        .isTrue();
        assertThat(loop.jobDiscoveryRecencyDays())
        .describedAs("The recency days should be adjusted to 0")
        .isZero();

        // Execution 2
        when(mockJobAccessor.findAllRecent(anyInt()))
        .thenReturn(Collections.singletonList(RestoreJob.forUpdates(newJobId, "agent",
                                                  RestoreJobStatus.SUCCEEDED, null, new Date())));
        executeBlocking();

        assertThat(stats.activeJobCount).isZero();
        assertThat(loop.hasInflightJobs())
        .describedAs("There should be no more inflight jobs")
        .isFalse();
        assertThat(loop.isRefreshSignaled())
        .describedAs("There should be no refresh signal")
        .isFalse();

        // Execution 3
        // shouldSkip always returns false
        assertThat(loop.shouldSkip()).isFalse();

        // Execution 4
        loop.signalRefresh();
        UUID newJobId2 = UUIDs.timeBased();
        when(mockJobAccessor.findAllRecent(anyInt()))
        .thenReturn(Collections.singletonList(RestoreJobTest.createNewTestingJob(newJobId2)));
        assertThat(loop.isRefreshSignaled()).isTrue();

        executeBlocking();

        assertThat(stats.activeJobCount).isOne();
        assertThat(loop.hasInflightJobs())
        .describedAs("It should find a new inflight job")
        .isTrue();
        assertThat(loop.isRefreshSignaled())
        .describedAs("The signal should reset after execution")
        .isFalse();

        // Execution 5
        when(mockJobAccessor.findAllRecent(anyInt()))
        .thenReturn(Collections.singletonList(RestoreJob.forUpdates(newJobId2, "agent",
                                                                    RestoreJobStatus.ABORTED, null, new Date())));
        executeBlocking();

        assertThat(stats.activeJobCount).isZero();
        assertThat(loop.hasInflightJobs())
        .describedAs("Last job failed, no more inflight jobs")
        .isFalse();
        assertThat(jobIdCapture.getValue())
        .describedAs("Failed job should be removed")
        .isEqualTo(newJobId2);
    }

    @Test
    void testSkipExecuteWhenSidecarSchemaIsNotInitialized()
    {
        when(sidecarSchema.isInitialized()).thenReturn(false);
        assertThat(loop.shouldSkip()).isTrue();
    }

    @Test
    void testExecuteWithExpiredJobs() throws Exception
    {
        // setup: all 3 jobs are expired. All of them should be aborted via mockJobAccessor
        when(sidecarSchema.isInitialized()).thenReturn(true);
        List<RestoreJob> mockResult
        = IntStream.range(0, 3)
                   .boxed()
                   .map(x -> RestoreJob.forUpdates(UUIDs.timeBased(), "agent", RestoreJobStatus.CREATED, null,
                                                   new Date(System.currentTimeMillis() - 1000L)))
                   .collect(Collectors.toList());
        ArgumentCaptor<UUID> abortedJobs = ArgumentCaptor.forClass(UUID.class);
        doNothing().when(mockJobAccessor).abort(abortedJobs.capture());
        when(mockJobAccessor.findAllRecent(anyInt())).thenReturn(mockResult);
        loop.registerPeriodicTaskExecutor(executor);
        executeBlocking();

        List<UUID> expectedAbortedJobs = mockResult.stream().map(s -> s.jobId).collect(Collectors.toList());
        assertThat(abortedJobs.getAllValues()).isEqualTo(expectedAbortedJobs);
    }

    private RestoreJobConfiguration testConfig()
    {
        RestoreJobConfiguration restoreJobConfiguration = mock(RestoreJobConfiguration.class);
        when(restoreJobConfiguration.jobDiscoveryActiveLoopDelayMillis()).thenReturn(activeLoopDelay);
        when(restoreJobConfiguration.jobDiscoveryIdleLoopDelayMillis()).thenReturn(idleLoopDelay);
        when(restoreJobConfiguration.jobDiscoveryRecencyDays()).thenReturn(recencyDays);
        when(restoreJobConfiguration.processMaxConcurrency()).thenReturn(TestModule.RESTORE_MAX_CONCURRENCY);
        when(restoreJobConfiguration.restoreJobTablesTtlSeconds()).thenReturn(TimeUnit.DAYS.toSeconds(14) + 1);

        return restoreJobConfiguration;
    }

    private void executeBlocking()
    {
        Promise<Void> promise = Promise.promise();
        loop.execute(promise);
        CountDownLatch latch = new CountDownLatch(1);
        promise.future().onComplete(v -> latch.countDown());
    }
}
