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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.locator.LocalTokenRangesProvider;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobDatabaseAccessor;
import org.apache.cassandra.sidecar.db.RestoreRangeDatabaseAccessor;
import org.apache.cassandra.sidecar.db.RestoreSliceDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetricsImpl;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.mockito.ArgumentCaptor;

import static org.apache.cassandra.sidecar.db.RestoreJobTest.createNewTestingJob;
import static org.apache.cassandra.sidecar.db.RestoreJobTest.createUpdatedJob;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RestoreJobDiscovererTest
{
    private static final long activeLoopDelay = 1000;
    private static final long idleLoopDelay = 2000;
    private static final int recencyDays = 10;
    private final RestoreJobDatabaseAccessor mockJobAccessor = mock(RestoreJobDatabaseAccessor.class);
    private final RestoreSliceDatabaseAccessor mockSliceAccessor = mock(RestoreSliceDatabaseAccessor.class);
    private final RestoreRangeDatabaseAccessor mockRangeAccessor = mock(RestoreRangeDatabaseAccessor.class);
    private final RestoreJobManagerGroup mockManagers = mock(RestoreJobManagerGroup.class);
    private final LocalTokenRangesProvider rangesProvider = mock(LocalTokenRangesProvider.class);
    private final PeriodicTaskExecutor executor = mock(PeriodicTaskExecutor.class);
    private final SidecarSchema sidecarSchema = mock(SidecarSchema.class);
    private SidecarMetrics metrics;
    private RestoreJobDiscoverer loop;

    @BeforeEach
    void setup()
    {
        MetricRegistryFactory mockRegistryFactory = mock(MetricRegistryFactory.class);
        when(mockRegistryFactory.getOrCreate()).thenReturn(registry());
        InstanceMetadataFetcher mockMetadataFetcher = mock(InstanceMetadataFetcher.class);
        metrics = new SidecarMetricsImpl(mockRegistryFactory, mockMetadataFetcher);
        loop = new RestoreJobDiscoverer(testConfig(),
                                        sidecarSchema,
                                        mockJobAccessor,
                                        mockSliceAccessor,
                                        mockRangeAccessor,
                                        () -> mockManagers,
                                        rangesProvider,
                                        null,
                                        null,
                                        metrics);
    }

    @AfterEach
    void clear()
    {
        registry().removeMatching((name, metric) -> true);
        registry(1).removeMatching((name, metric) -> true);
    }

    @Test
    void testGetDelay()
    {
        // when there is no active restore job. The delay is idle loop delay
        assertThat(loop.delay()).isEqualTo(idleLoopDelay);
        // when there is active restore job (status: CREATED)
        UUID jobId = UUIDs.timeBased();
        when(mockJobAccessor.findAllRecent(anyInt()))
        .thenReturn(Collections.singletonList(RestoreJob.builder()
                                                        .createdAt(RestoreJob.toLocalDate(jobId))
                                                        .jobId(jobId)
                                                        .jobAgent("agent")
                                                        .jobStatus(RestoreJobStatus.CREATED)
                                                        .expireAt(new Date(System.currentTimeMillis() + 10000L))
                                                        .build()));
        loop.registerPeriodicTaskExecutor(executor);
        executeBlocking();
        assertThat(metrics.server().restore().activeJobs.metric.getValue()).describedAs("active jobs count is updated")
                                                               .isOne();
        assertThat(loop.delay()).isEqualTo(activeLoopDelay);
        // when no more jobs are active, the delay is reset back to idle loop delay accordingly.
        when(mockJobAccessor.findAllRecent(anyInt()))
        .thenReturn(Collections.singletonList(RestoreJob.builder()
                                                        .createdAt(RestoreJob.toLocalDate(jobId))
                                                        .jobId(jobId)
                                                        .jobAgent("agent")
                                                        .jobStatus(RestoreJobStatus.SUCCEEDED)
                                                        .expireAt(new Date(System.currentTimeMillis() + 10000L))
                                                        .build()));
        executeBlocking();
        assertThat(metrics.server().restore().activeJobs.metric.getValue()).describedAs("active jobs count is updated")
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
    void testExecute()
    {
        when(sidecarSchema.isInitialized()).thenReturn(true);
        // setup, cassandra should return 3 jobs, a new job, a failed and a succeeded
        List<RestoreJob> mockResult = new ArrayList<>(3);
        UUID newJobId = UUIDs.timeBased();
        UUID failedJobId = UUIDs.timeBased();
        UUID succeededJobId = UUIDs.timeBased();
        mockResult.add(createNewTestingJob(newJobId));
        mockResult.add(createUpdatedJob(failedJobId, "agent", RestoreJobStatus.ABORTED, null,
                                        new Date(System.currentTimeMillis() + 10000L)));
        mockResult.add(createUpdatedJob(succeededJobId, "agent", RestoreJobStatus.SUCCEEDED, null,
                                        new Date(System.currentTimeMillis() + 10000L)));
        ArgumentCaptor<RestoreJob> jobCapture = ArgumentCaptor.forClass(RestoreJob.class);
        doNothing().when(mockManagers).removeJobInternal(jobCapture.capture());
        when(mockJobAccessor.findAllRecent(anyInt())).thenReturn(mockResult);
        loop.registerPeriodicTaskExecutor(executor);

        assertThat(loop.hasInflightJobs())
        .describedAs("No inflight jobs are discovery when loop has not started")
        .isFalse();
        assertThat(loop.jobDiscoveryRecencyDays())
        .describedAs("Initial recency days should be " + recencyDays)
        .isEqualTo(recencyDays);
        assertThat(metrics.server().restore().activeJobs.metric.getValue()).isZero();

        // Execution 1
        executeBlocking();

        assertThat(metrics.server().restore().activeJobs.metric.getValue()).isOne();
        assertThat(jobCapture.getAllValues().stream().map(job -> job.jobId))
        .containsExactlyInAnyOrder(failedJobId, succeededJobId);
        assertThat(loop.hasInflightJobs())
        .describedAs("An inflight job should be found")
        .isTrue();
        assertThat(loop.jobDiscoveryRecencyDays())
        .describedAs("The recency days should be adjusted to 0")
        .isZero();

        // Execution 2
        when(mockJobAccessor.findAllRecent(anyInt()))
        .thenReturn(Collections.singletonList(createUpdatedJob(newJobId, "agent",
                                                               RestoreJobStatus.SUCCEEDED, null, new Date())));
        executeBlocking();

        assertThat(metrics.server().restore().activeJobs.metric.getValue()).isZero();
        assertThat(loop.hasInflightJobs())
        .describedAs("There should be no more inflight jobs")
        .isFalse();

        // Execution 3
        // shouldSkip always returns false
        assertThat(loop.shouldSkip()).isFalse();

        // Execution 4
        UUID newJobId2 = UUIDs.timeBased();
        when(mockJobAccessor.findAllRecent(anyInt()))
        .thenReturn(Collections.singletonList(createNewTestingJob(newJobId2)));

        executeBlocking();

        assertThat(metrics.server().restore().activeJobs.metric.getValue()).isOne();
        assertThat(loop.hasInflightJobs())
        .describedAs("It should find a new inflight job")
        .isTrue();

        // Execution 5
        when(mockJobAccessor.findAllRecent(anyInt()))
        .thenReturn(Collections.singletonList(createUpdatedJob(newJobId2, "agent",
                                                               RestoreJobStatus.ABORTED, null, new Date())));
        executeBlocking();

        assertThat(metrics.server().restore().activeJobs.metric.getValue()).isZero();
        assertThat(loop.hasInflightJobs())
        .describedAs("Last job failed, no more inflight jobs")
        .isFalse();
        assertThat(jobCapture.getValue().jobId)
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
    void testExecuteWithExpiredJobs()
    {
        // setup: all 3 jobs are expired. All of them should be aborted via mockJobAccessor
        when(sidecarSchema.isInitialized()).thenReturn(true);
        List<RestoreJob> mockResult = IntStream.range(0, 3)
                                               .boxed()
                                               .map(x -> createUpdatedJob(UUIDs.timeBased(), "agent",
                                                                          RestoreJobStatus.CREATED, null,
                                                                          new Date(System.currentTimeMillis() - 1000L)))
                                               .collect(Collectors.toList());
        ArgumentCaptor<UUID> abortedJobs = ArgumentCaptor.forClass(UUID.class);
        doNothing().when(mockJobAccessor).abort(abortedJobs.capture(), eq("Expired"));
        when(mockJobAccessor.findAllRecent(anyInt())).thenReturn(mockResult);
        loop.registerPeriodicTaskExecutor(executor);
        executeBlocking();

        List<UUID> expectedAbortedJobs = mockResult.stream().map(s -> s.jobId).collect(Collectors.toList());
        assertThat(abortedJobs.getAllValues()).isEqualTo(expectedAbortedJobs);
    }

    @Test
    void testWhenJobShouldBeLogged()
    {
        RestoreJobDiscoverer.JobIdsByDay jobIdsByDay = new RestoreJobDiscoverer.JobIdsByDay();
        RestoreJob job = createNewTestingJob(UUIDs.timeBased());
        assertThat(jobIdsByDay.shouldLogJob(job))
        .describedAs("should return true for the new job")
        .isTrue();
        assertThat(jobIdsByDay.shouldLogJob(job))
        .describedAs("should return true for the same job in CREATED status")
        .isTrue();
        RestoreJob statusUpdated = job.unbuild().jobStatus(RestoreJobStatus.SUCCEEDED).build();
        assertThat(jobIdsByDay.shouldLogJob(statusUpdated))
        .describedAs("should return true for the status-updated job")
        .isTrue();
        assertThat(jobIdsByDay.shouldLogJob(statusUpdated))
        .describedAs("should return false for the same SUCCEEDED job")
        .isFalse();
    }

    @Test
    void testCleanupJobIdsByDay()
    {
        RestoreJobDiscoverer.JobIdsByDay jobIdsByDay = new RestoreJobDiscoverer.JobIdsByDay();
        RestoreJob job = createNewTestingJob(UUIDs.timeBased());
        jobIdsByDay.shouldLogJob(job); // insert the job
        jobIdsByDay.cleanupMaybe(); // issue a cleanup. but it should not remove anything
        assertThat(jobIdsByDay.jobsByDay()).hasSize(1)
                                           .containsKey(job.createdAt.getDaysSinceEpoch());
        RestoreJob jobOfNextDay = job.unbuild().createdAt(LocalDate.fromDaysSinceEpoch(job.createdAt.getDaysSinceEpoch() + 1)).build();
        jobIdsByDay.shouldLogJob(jobOfNextDay);
        jobIdsByDay.cleanupMaybe(); // issue a new cleanup. it should remove the job that is not reported in the new round
        assertThat(jobIdsByDay.jobsByDay()).hasSize(1)
                                           .containsKey(jobOfNextDay.createdAt.getDaysSinceEpoch())
                                           .doesNotContainKey(job.createdAt.getDaysSinceEpoch());
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
