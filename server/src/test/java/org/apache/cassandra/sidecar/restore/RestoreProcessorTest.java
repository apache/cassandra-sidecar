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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.codahale.metrics.SharedMetricRegistries;
import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.ExecutorPoolsHelper;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.cluster.locator.LocalTokenRangesProvider;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.RestoreJobConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobTest;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreRangeDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetricsImpl;
import org.apache.cassandra.sidecar.metrics.instance.InstanceRestoreMetrics;
import org.apache.cassandra.sidecar.metrics.server.ServerMetricsImpl;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;
import org.apache.cassandra.sidecar.utils.SSTableImporter;

import static org.apache.cassandra.sidecar.restore.RestoreRangeTask.failOnCancelled;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class RestoreProcessorTest
{
    private static final Vertx vertx = Vertx.vertx();
    private static final ExecutorPools executorPools = ExecutorPoolsHelper.createdSharedTestPool(vertx);

    private final InstanceMetrics instanceMetrics = instanceMetrics();
    private RestoreProcessor processor;
    private SidecarSchema sidecarSchema;
    private PeriodicTaskExecutor periodicTaskExecutor;

    @BeforeEach
    void setup()
    {
        sidecarSchema = mock(SidecarSchema.class);
        SidecarMetrics sidecarMetrics = mock(SidecarMetrics.class);
        when(sidecarMetrics.instance(1)).thenReturn(instanceMetrics);
        when(sidecarMetrics.server()).thenReturn(new ServerMetricsImpl(registry()));
        SidecarConfiguration sidecarConfig = SidecarConfigurationImpl
                                             .builder()
                                             .restoreJobConfiguration(RestoreJobConfigurationImpl
                                                                      .builder()
                                                                      .processMaxConcurrency(TestModule.RESTORE_MAX_CONCURRENCY)
                                                                      .slowTaskThreshold(SecondBoundConfiguration.parse("10s"))
                                                                      .slowTaskReportDelay(SecondBoundConfiguration.parse("2m"))
                                                                      .build())
                                             .build();
        RestoreProcessor delegate = new RestoreProcessor(executorPools,
                                                         sidecarConfig,
                                                         sidecarSchema,
                                                         mock(StorageClientPool.class),
                                                         mock(SSTableImporter.class),
                                                         mock(RestoreRangeDatabaseAccessor.class),
                                                         mock(RestoreJobUtil.class),
                                                         mock(LocalTokenRangesProvider.class),
                                                         sidecarMetrics);
        periodicTaskExecutor = new PeriodicTaskExecutor(executorPools, new ClusterLease());
        processor = spy(delegate);
        when(processor.delay()).thenReturn(MillisecondBoundConfiguration.parse("100ms"));
        when(processor.sidecarSchema()).thenReturn(sidecarSchema);
    }

    @AfterEach
    void clear()
    {
        SharedMetricRegistries.clear();
        periodicTaskExecutor.close();
    }

    @AfterAll
    static void afterAll()
    {
        TestResourceReaper.create().with(vertx).with(executorPools).close();
    }

    @Test
    void testMaxProcessConcurrency()
    {
        // SidecarSchema is initialized
        when(sidecarSchema.isInitialized()).thenReturn(true);

        int concurrency = TestModule.RESTORE_MAX_CONCURRENCY;
        periodicTaskExecutor.schedule(processor);

        assertThat(processor.activeRanges()).isZero();

        CountDownLatch latch = new CountDownLatch(1);

        int total = concurrency * 3;
        long sliceAge = TimeUnit.SECONDS.toNanos(5);
        for (int i = 0; i < total; i++)
        {
            RestoreRange mockRange = mockRestoreRange();
            when(mockRange.sliceCreationTimeNanos()).thenReturn(System.nanoTime() - sliceAge);
            RestoreRange range = mockSlowRestoreRange(latch, mockRange);
            range.completeImportPhase(); // complete prematurely for testing purpose
            processor.submit(range);
        }

        InstanceRestoreMetrics instanceRestoreMetrics = instanceMetrics.restore();

        // assert before any slice can be completed
        loopAssert(3, () -> {
            // expect slice import queue has the size of concurrency
            assertThat(instanceRestoreMetrics.sliceImportQueueLength.metric.getValue())
            .isLessThanOrEqualTo(concurrency);

            // expect the pending slices count equals to "total - concurrency"
            assertThat(instanceRestoreMetrics.pendingSliceCount.metric.getValue())
            .isLessThanOrEqualTo(total - concurrency);

            assertThat(processor.activeRanges()).isEqualTo(concurrency);
        });

        // slices start to succeed
        latch.countDown();

        // it never grows beyond `concurrency`
        loopAssert(3, () -> {
            assertThat(processor.activeRanges())
            .describedAs("Active slice count should be in the range of (0, concurrency]")
            .isLessThanOrEqualTo(concurrency)
            .isPositive();
        });

        // the active slices should be back to 0
        // and the pending slices should be back to 0
        loopAssert(3, () -> {
            assertThat(processor.activeRanges()).isZero();
            assertThat(instanceRestoreMetrics.sliceImportQueueLength.metric.getValue()).isZero();
            assertThat(instanceRestoreMetrics.pendingSliceCount.metric.getValue()).isZero();
        });

        // all slices complete successfully
        assertThat(instanceRestoreMetrics.sliceCompletionTime.metric.getSnapshot().getValues()).hasSize(total);
        for (long sliceCompleteDuration : instanceRestoreMetrics.sliceCompletionTime.metric.getSnapshot().getValues())
        {
            assertThat(sliceCompleteDuration).isGreaterThan(sliceAge);
        }
    }

    @Test
    void testSkipExecuteWhenSidecarSchemaIsNotInitialized()
    {
        when(sidecarSchema.isInitialized()).thenReturn(false);

        assertThat(processor.scheduleDecision()).isEqualTo(ScheduleDecision.SKIP);
        assertThat(processor.activeRanges()).isZero();

        CountDownLatch latch = new CountDownLatch(1);
        processor.submit(mockSlowRestoreRange(latch));
        assertThat(processor.activeRanges())
        .describedAs("No slice should be active because executions are skipped")
        .isZero();

        // Make slice completable. But since all executions are skipped, the active slice should remain as 1
        latch.countDown();
        loopAssert(3, () -> {
            assertThat(processor.pendingStartRanges()).isOne();
            assertThat(processor.activeRanges()).isZero();
        });
    }

    @Test
    void testLongRunningHandlerDetection()
    {
        when(sidecarSchema.isInitialized()).thenReturn(true);
        periodicTaskExecutor.schedule(processor);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong currentTime = new AtomicLong(0);
        RestoreRange range = mockSlowRestoreRange(latch, currentTime::get); // Sets the start time
        long oneMinutesInNanos = TimeUnit.NANOSECONDS.convert(1, TimeUnit.MINUTES);
        currentTime.set(oneMinutesInNanos);
        processor.submit(range);
        loopAssert(3, () -> {
            long[] slowRestoreTaskTimes = instanceMetrics
                                          .restore()
                                          .slowRestoreTaskTime.metric.getSnapshot().getValues();
            assertThat(slowRestoreTaskTimes)
            .describedAs("The task takes 1 minute. " +
                         "The slow task threshold is 10 seconds and report delay is 2 minutes (see TestModule). " +
                         "It should only report once")
            .hasSize(1);
            long handlerTimeInNanos = slowRestoreTaskTimes[0];
            assertThat(handlerTimeInNanos).isEqualTo(oneMinutesInNanos);
            assertThat(processor.activeTasks()).isOne();
        });

        // Make range completable.
        latch.countDown();

        // Make sure when the range completes the active handler is removed
        loopAssert(3, () -> {
            assertThat(processor.activeTasks()).isZero();
        });
    }

    @Test
    void testSubmitFailedTask()
    {
        when(sidecarSchema.isInitialized()).thenReturn(true);
        periodicTaskExecutor.schedule(processor);

        RestoreRange range = RestoreRangeTest.createTestRange();
        // the range is already cancelled, it should produce a failed task directly.
        range.cancel();
        processor.submit(range);

        // the canceled range should fail the job
        loopAssert(3,
                   () -> assertThat(range.trackerUnsafe().isFailed()).isTrue());

        // trying to submit the range again, it throws fatal exception that the range has been cancelled
        assertThatThrownBy(() -> range.trackerUnsafe().trySubmit(range))
        .isExactlyInstanceOf(RestoreJobFatalException.class)
        .hasMessageContaining("Restore range is cancelled.");
    }

    @Test
    void testDiscardPendingRange()
    {
        RestoreRange range = RestoreRangeTest.createTestRange();
        processor.submit(range);
        assertThat(processor.pendingStartRanges()).isOne();
        processor.discardAndRemove(range);
        assertThat(processor.pendingStartRanges()).isZero();
        assertThat(range.isDiscarded()).isTrue();
    }

    @Test
    void testDiscardStartedRange()
    {
        CountDownLatch readyToFinish = new CountDownLatch(1);
        RestoreRange range = mockSlowRestoreRange(readyToFinish);
        processor.submit(range);
        processor.execute(Promise.promise());
        assertThat(processor.activeRanges()).isOne();
        assertThat(processor.activeTasks()).isOne();
        processor.discardAndRemove(range);
        assertThat(processor.activeRanges())
        .describedAs("The range is being processed already, we wait for it to fail " +
                     "and discard in org.apache.cassandra.sidecar.restore.RestoreProcessor.taskFailureHandler")
        .isOne();
        assertThat(range.isDiscarded())
        .describedAs("The range should be marked as discarded")
        .isTrue();
        readyToFinish.countDown();
        loopAssert(3, () -> {
            assertThat(processor.activeRanges()).isZero();
            assertThat(processor.activeTasks()).isZero();
        });
    }

    @Test
    void testStagedRangeWaitsForImportSignal()
    {
        CountDownLatch readyToFinish = new CountDownLatch(1);
        RestoreRange range = stagedRange(readyToFinish, RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.STAGED));
        processor.submit(range);
        processor.execute(Promise.promise());
        assertThat(processor.activeTasks())
        .describedAs("A staged range of a regular job should wait for the IMPORT_READY signal, " +
                     "even when the job has entered STAGED status")
        .isZero();
        readyToFinish.countDown();
    }

    @Test
    void testStagedRangeOfFastForwardJobImportsOnceStaged()
    {
        CountDownLatch readyToFinish = new CountDownLatch(1);
        RestoreRange range = stagedRange(readyToFinish, RestoreJobTest.createFastForwardTestingJob(UUIDs.timeBased(), RestoreJobStatus.STAGED));
        processor.submit(range);
        processor.execute(Promise.promise());
        assertThat(processor.activeTasks())
        .describedAs("A staged range of a fast forward job should import as soon as the job enters STAGED status")
        .isOne();
        readyToFinish.countDown();
        loopAssert(3, () -> assertThat(processor.activeTasks()).isZero());
    }

    private InstanceMetrics instanceMetrics()
    {
        return new InstanceMetricsImpl(registry(1));
    }

    private RestoreRange mockSlowRestoreRange(CountDownLatch latch)
    {
        return mockSlowRestoreRange(latch, System::nanoTime);
    }

    // a range that has completed its staging phase and belongs to the given restore job
    private RestoreRange stagedRange(CountDownLatch latch, RestoreJob job)
    {
        RestoreRange range = mockSlowRestoreRange(latch);
        doReturn(true).when(range).hasStaged();
        doReturn(job).when(range).job();
        return range;
    }

    private RestoreRange mockSlowRestoreRange(CountDownLatch latch, RestoreRange mockRange)
    {
        return mockSlowRestoreRange(latch, System::nanoTime, mockRange);
    }

    private RestoreRange mockSlowRestoreRange(CountDownLatch latch, Supplier<Long> timeInNanosSupplier)
    {
        RestoreRange range = mockRestoreRange();
        return mockSlowRestoreRange(latch, timeInNanosSupplier, range);
    }

    private RestoreRange mockSlowRestoreRange(CountDownLatch latch, Supplier<Long> timeInNanosSupplier, RestoreRange range)
    {
        when(range.toAsyncTask(any(), any(), any(), anyDouble(), any(), any(), any(), any())).thenReturn(
        new RestoreRangeHandler()
        {
            private final Long startTime = timeInNanosSupplier.get();

            @Override
            public void handle(Promise<RestoreRange> promise)
            {
                Uninterruptibles.awaitUninterruptibly(latch);
                failOnCancelled(range, range)
                .onComplete(promise);
            }

            @Override
            public long elapsedInNanos()
            {
                return timeInNanosSupplier.get() - startTime;
            }

            @Override
            public RestoreRange range()
            {
                return range;
            }
        });
        return range;
    }

    private RestoreRange mockRestoreRange()
    {
        RestoreRange mockRange = RestoreRangeTest.createTestRange();
        RestoreRange range = spy(mockRange);
        when(range.owner().metrics()).thenReturn(instanceMetrics);
        return range;
    }
}
