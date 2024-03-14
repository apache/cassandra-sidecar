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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.stats.RestoreJobStats;
import org.apache.cassandra.sidecar.stats.TestRestoreJobStats;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.mockito.Mockito;

import static org.apache.cassandra.sidecar.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class RestoreProcessorTest
{
    private RestoreProcessor processor;
    private SidecarSchema sidecarSchema;
    private PeriodicTaskExecutor periodicTaskExecutor;
    private TestRestoreJobStats stats;

    @BeforeEach
    void setup()
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(new TestModule()));
        sidecarSchema = mock(SidecarSchema.class);
        RestoreProcessor delegate = injector.getInstance(RestoreProcessor.class);
        processor = spy(delegate);
        when(processor.delay()).thenReturn(100L);
        when(processor.sidecarSchema()).thenReturn(sidecarSchema);
        periodicTaskExecutor = injector.getInstance(PeriodicTaskExecutor.class);
        stats = (TestRestoreJobStats) injector.getInstance(RestoreJobStats.class);
    }

    @Test
    void testMaxProcessConcurrency()
    {
        // SidecarSchema is initialized
        when(sidecarSchema.isInitialized()).thenReturn(true);

        int concurrency = TestModule.RESTORE_MAX_CONCURRENCY;
        periodicTaskExecutor.schedule(processor);

        assertThat(stats.sliceImportQueueLengths).isEmpty();
        assertThat(processor.activeSlices()).isZero();

        CountDownLatch latch = new CountDownLatch(1);

        int total = concurrency * 3;
        for (int i = 0; i < total; i++)
        {
            processor.submit(mockSlowSlice(latch));
        }

        // assert before any slice can be completed
        loopAssert(3, () -> {
            // expect slice import queue has the size of concurrency
            assertThat(stats.sliceImportQueueLengths).isNotEmpty();
            int lastValueIndex = stats.sliceImportQueueLengths.size() - 1;
            assertThat(stats.sliceImportQueueLengths.get(lastValueIndex))
            .isLessThanOrEqualTo(concurrency);

            // expect the pending slices count equals to "total - concurrency"
            assertThat(stats.pendingSliceCounts).isNotEmpty();
            lastValueIndex = stats.pendingSliceCounts.size() - 1;
            assertThat(stats.pendingSliceCounts.get(lastValueIndex))
            .isLessThanOrEqualTo(total - concurrency);

            assertThat(processor.activeSlices()).isEqualTo(concurrency);
        });

        // slices start to succeed
        latch.countDown();

        // it never grows beyond `concurrency`
        loopAssert(3, () -> {
            assertThat(processor.activeSlices())
            .describedAs("Active slice count should be in the range of (0, concurrency]")
            .isLessThanOrEqualTo(concurrency)
            .isPositive();
        });

        // the active slices should be back to 0
        // and the pending slices should be back to 0
        loopAssert(3, () -> {
            assertThat(processor.activeSlices()).isZero();
            int lastValueIndex = stats.sliceImportQueueLengths.size() - 1;
            assertThat(stats.sliceImportQueueLengths.get(lastValueIndex)).isZero();
            lastValueIndex = stats.pendingSliceCounts.size() - 1;
            assertThat(stats.pendingSliceCounts.get(lastValueIndex)).isZero();
        });

        // assert on the historic captured values
        for (long historicQueueSize : stats.sliceImportQueueLengths)
        {
            assertThat(historicQueueSize)
            .describedAs("All captured queue size should be in the range of [0, concurrency]")
            .isNotNegative()
            .isLessThanOrEqualTo(concurrency);
        }

        for (long historicPendingCount : stats.pendingSliceCounts)
        {
            assertThat(historicPendingCount)
            .describedAs("All captured counts should be in the range of [0, total - concurrency]")
            .isNotNegative()
            .isLessThanOrEqualTo(total - concurrency);
        }

        // all slices complete successfully
        assertThat(stats.sliceCompletionTimes).hasSize(total);
        for (long sliceCompleteDuration : stats.sliceCompletionTimes)
        {
            assertThat(sliceCompleteDuration).isPositive();
        }
    }

    @Test
    void testSkipExecuteWhenSidecarSchemaIsNotInitialized()
    {
        when(sidecarSchema.isInitialized()).thenReturn(false);

        assertThat(processor.shouldSkip()).isTrue();
        assertThat(processor.activeSlices()).isZero();

        CountDownLatch latch = new CountDownLatch(1);
        processor.submit(mockSlowSlice(latch));
        assertThat(processor.activeSlices())
        .describedAs("No slice should be active because executions are skipped")
        .isZero();

        // Make slice completable. But since all executions are skipped, the active slice should remain as 1
        latch.countDown();
        loopAssert(3, () -> {
            assertThat(processor.pendingStartSlices()).isOne();
            assertThat(processor.activeSlices()).isZero();
        });
    }

    @Test
    public void testLongRunningHandlerDetection()
    {

        when(sidecarSchema.isInitialized()).thenReturn(true);
        periodicTaskExecutor.schedule(processor);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong currentTime = new AtomicLong(0);
        RestoreSlice slice = mockSlowSlice(latch, currentTime::get); // Sets the start time
        long fiveMinutesInNanos = TimeUnit.NANOSECONDS.convert(5, TimeUnit.MINUTES);
        currentTime.set(fiveMinutesInNanos);
        processor.submit(slice);
        loopAssert(3, () -> {
            assertThat(stats.longRunningRestoreHandlers.size()).isEqualTo(1);
            Long handlerTimeInNanos = stats.longRunningRestoreHandlers.get(slice.owner().id());
            assertThat(handlerTimeInNanos).isNotNull();
            assertThat(handlerTimeInNanos).isEqualTo(fiveMinutesInNanos);
            assertThat(processor.activeTasks()).isOne();
        });

        // Make slice completable.
        latch.countDown();

        // Make sure when the slice completes the active handler is removed
        loopAssert(3, () -> {
            assertThat(processor.activeTasks()).isZero();
        });
    }

    private RestoreSlice mockSlowSlice(CountDownLatch latch)
    {
        return mockSlowSlice(latch, System::nanoTime);
    }

    private RestoreSlice mockSlowSlice(CountDownLatch latch, Supplier<Long> timeInNanosSupplier)
    {
        RestoreSlice slice = mock(RestoreSlice.class, Mockito.RETURNS_DEEP_STUBS);
        when(slice.jobId()).thenReturn(UUIDs.timeBased());
        when(slice.owner().id()).thenReturn(1);
        when(slice.key()).thenReturn("SliceKey");
        RestoreJob job = RestoreJob.builder()
                                   .jobStatus(RestoreJobStatus.CREATED)
                                   .build();
        when(slice.job()).thenReturn(job);
        when(slice.toAsyncTask(any(), any(), any(), anyDouble(), any(), any(), any())).thenReturn(
        new RestoreSliceHandler()
        {
            private Long startTime = timeInNanosSupplier.get();

            public void handle(Promise<RestoreSlice> promise)
            {
                Uninterruptibles.awaitUninterruptibly(latch);
                promise.complete(slice);
            }

            public long elapsedInNanos()
            {
                return timeInNanosSupplier.get() - startTime;
            }

            public RestoreSlice slice()
            {
                return slice;
            }
        });
        when(slice.hasImported()).thenReturn(true);
        return slice;
    }
}
