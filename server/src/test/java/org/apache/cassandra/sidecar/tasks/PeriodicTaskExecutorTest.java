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

package org.apache.cassandra.sidecar.tasks;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.coordination.ExecuteOnClusterLeaseholderOnly;

import static org.apache.cassandra.sidecar.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link PeriodicTaskExecutor} class
 */
class PeriodicTaskExecutorTest
{
    private final Vertx vertx = Vertx.vertx();
    private final ExecutorPools executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());
    private final ClusterLease clusterLease = new ClusterLease();
    private PeriodicTaskExecutor taskExecutor;

    @BeforeEach
    void beforeEach()
    {
        taskExecutor = new PeriodicTaskExecutor(executorPools, clusterLease);
    }

    @AfterEach
    void afterEach()
    {
        clusterLease.setOwnershipTesting(ClusterLease.Ownership.INDETERMINATE);
        if (taskExecutor != null)
        {
            taskExecutor.close(Promise.promise());
        }
    }

    @Test
    void testLoopFailure()
    {
        int totalFailures = 5;
        AtomicInteger failuresCount = new AtomicInteger(0);
        CountDownLatch closeLatch = new CountDownLatch(1);
        AtomicBoolean isClosed = new AtomicBoolean(false);
        taskExecutor.schedule(new PeriodicTask()
        {
            @Override
            public long delay()
            {
                return 1;
            }

            @Override
            public void execute(Promise<Void> promise)
            {
                if (failuresCount.incrementAndGet() == totalFailures)
                {
                    taskExecutor.unschedule(this);
                }
                throw new RuntimeException("ah, it failed");
            }

            @Override
            public void close()
            {
                isClosed.set(true);
                closeLatch.countDown();
            }
        });
        Uninterruptibles.awaitUninterruptibly(closeLatch);
        assertThat(isClosed.get()).isTrue();
        assertThat(failuresCount.get()).isEqualTo(totalFailures);
    }

    @Test
    void testPeriodicTaskExecutionShouldEnsureMemoryConsistency()
    {
        // starts 10 concurrent incremental tasks, and
        // assert that for each task, the non-thread-safe value update has
        // the same effect of the thread-safe value update
        List<IncrementPeriodicTask> tasks = new ArrayList<>(10);
        for (int i = 0; i < 10; i++)
        {
            IncrementPeriodicTask incTask = new IncrementPeriodicTask("task" + i);
            tasks.add(incTask);
            taskExecutor.schedule(incTask);
        }
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        tasks.forEach(taskExecutor::unschedule);
        // wait until unschedule is complete
        loopAssert(1, () -> assertThat(taskExecutor.timerIds()).isEmpty());
        loopAssert(1, () -> assertThat(taskExecutor.poisonPilledTasks()).isEmpty());
        tasks.forEach(incTask -> assertThat(incTask.atomicValue.get())
                                 .describedAs(incTask.name() + " should have same value")
                                 .isEqualTo(incTask.value)
                                 .isPositive());
    }

    @Test
    void testScheduleSameTaskMultipleTimes()
    {
        AtomicInteger counter = new AtomicInteger(0);
        // each time, the task is executed, the counter is incremented by 1.
        // The task has the initial delay of 0, but the periodic interval is 1 day to ensure it is executed only once during the test runtime
        PeriodicTask task = createSimplePeriodicTask("task", 0, TimeUnit.DAYS.toMillis(1), counter::incrementAndGet);
        // schedule the same task for 10 times, only the first schedule goes through
        for (int i = 0; i < 10; i++)
        {
            taskExecutor.schedule(task);
        }
        // in case that the test environment gets slow and the counter update is delayed
        loopAssert(1, () -> assertThat(counter.get()).isEqualTo(1));
    }

    @Test
    void testUnscheduleShouldStopExecution()
    {
        // the first run starts immediately, and the subsequent delay is 1 millis
        testUnscheduleShouldStopExecution("task1", 0, 1);
        // the first run starts immediately, and the subsequent runs also starts immediately
        testUnscheduleShouldStopExecution("task2", 0, 0);
        // tasks are scheduled with delay 1 millis
        testUnscheduleShouldStopExecution("task3", 1, 1);
    }

    @Test
    void testUnscheduleNonExistTaskHasNoEffect()
    {
        PeriodicTask notScheduled = createSimplePeriodicTask("simple task", 1, () -> {});
        taskExecutor.unschedule(notScheduled);
        assertThat(taskExecutor.poisonPilledTasks()).isEmpty();
        assertThat(taskExecutor.timerIds()).isEmpty();
    }

    @Test
    void testRescheduleNonExistTaskShouldNotClose()
    {
        AtomicBoolean isCloseCalled = new AtomicBoolean(false);
        CountDownLatch taskScheduled = new CountDownLatch(1);
        PeriodicTask task = new PeriodicTask()
        {
            @Override
            public long delay()
            {
                return 1;
            }

            @Override
            public void execute(Promise<Void> promise)
            {
                taskScheduled.countDown();
                promise.complete();
            }

            @Override
            public void close()
            {
                isCloseCalled.set(true);
            }
        };

        // for such unscheduled task, reschedule has the same effect as schedule.
        taskExecutor.reschedule(task);
        Uninterruptibles.awaitUninterruptibly(taskScheduled);
        assertThat(isCloseCalled.get())
        .describedAs("When rescheduling an unscheduled task, the close method of the task should not be called")
        .isFalse();
    }

    @Test
    void testReschedule()
    {
        AtomicInteger counter1 = new AtomicInteger(0);
        AtomicInteger counter2 = new AtomicInteger(0);
        int totalSamples = 10;
        Set<Integer> counterValues = ConcurrentHashMap.newKeySet(totalSamples);

        CountDownLatch latch1 = new CountDownLatch(1);
        // The periodic task increment counter1 initially, then switches to increment counter 2 once it is rescheduled
        PeriodicTask task = createSimplePeriodicTask("simple periodic task", 1, () -> {
            if (latch1.getCount() > 0)
            {
                counter1.incrementAndGet();
            }
            else
            {
                counterValues.add(counter2.incrementAndGet());
            }
            Uninterruptibles.awaitUninterruptibly(latch1);
        });
        taskExecutor.schedule(task);
        loopAssert(1, () -> assertThat(counter1.get()).isEqualTo(1));
        assertThat(counterValues).isEmpty();
        taskExecutor.reschedule(task);
        assertThat(counter1.get()).isEqualTo(1);
        assertThat(counterValues).isEmpty();
        latch1.countDown();
        // After getting rescheduled, it should produce unique values from counter2 and store in counterValues
        assertThat(counter1.get()).isEqualTo(1);
        loopAssert(2, () -> assertThat(counterValues.size()).isGreaterThanOrEqualTo(totalSamples));
        taskExecutor.unschedule(task);
    }

    @Test
    void testLeaseholderOnlyTaskOnNonClusterLeaseholder()
    {
        CountDownLatch skipLatch = new CountDownLatch(1);
        SimulatedTask task = new SimulatedTask(skipLatch, null);
        clusterLease.setOwnershipTesting(ClusterLease.Ownership.LOST);

        taskExecutor.schedule(task);
        assertThat(Uninterruptibles.awaitUninterruptibly(skipLatch, 30, TimeUnit.SECONDS)).isTrue();
        assertThat(task.shouldSkipCount.get()).isEqualTo(5);
        assertThat(task.executionCount.get()).isEqualTo(0);
        assertThat(task.initialDelayCount.get()).isEqualTo(1);
    }

    @Test
    void testLeaseholderOnlyTaskOnClusterLeaseholder()
    {
        CountDownLatch latch = new CountDownLatch(1);
        SimulatedTask task = new SimulatedTask(latch);
        clusterLease.setOwnershipTesting(ClusterLease.Ownership.CLAIMED);

        taskExecutor.schedule(task);
        assertThat(Uninterruptibles.awaitUninterruptibly(latch, 30, TimeUnit.SECONDS)).isTrue();
        assertThat(task.executionCount.get()).isEqualTo(5);
        assertThat(task.initialDelayCount.get()).isEqualTo(1);
    }

    @Test
    void testLeaseholderOnlyTaskIsRescheduledWhenIndeterminate()
    {
        CountDownLatch latch = new CountDownLatch(1);
        SimulatedTask taskThatRunsOnExecutor = new SimulatedTask(latch);

        ClusterLease clusterLease = new TestClusterLease(new ClusterLease());
        PeriodicTaskExecutor taskExecutor = new PeriodicTaskExecutor(executorPools, clusterLease);
        taskExecutor.schedule(taskThatRunsOnExecutor);
        assertThat(Uninterruptibles.awaitUninterruptibly(latch, 30, TimeUnit.SECONDS)).isTrue();
        // Task gets rescheduled on the INDETERMINATE state, so we expect the initial delay
        // to be called twice, on the first scheduling, and when rescheduling with the indeterminate state
        assertThat(taskThatRunsOnExecutor.initialDelayCount.get()).isEqualTo(2);
        // and we only actually execute 4 times, since the first time was rescheduled
        assertThat(taskThatRunsOnExecutor.executionCount.get()).isEqualTo(4);
        taskExecutor.close(Promise.promise());
    }

    private void testUnscheduleShouldStopExecution(String taskName, long taskInitialDelay, long taskDelayMillis)
    {
        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch testFinish = new CountDownLatch(1);
        PeriodicTask task = createSimplePeriodicTask(taskName, taskInitialDelay, taskDelayMillis, () -> {
            counter.incrementAndGet();
            testFinish.countDown();
        });
        taskExecutor.schedule(task);
        Uninterruptibles.awaitUninterruptibly(testFinish);
        taskExecutor.unschedule(task);
        loopAssert(1,
                   () -> assertThat(taskExecutor.poisonPilledTasks()).isEmpty());
        loopAssert(1,
                   () -> assertThat(taskExecutor.timerIds())
                         .describedAs("Execution should stop after unschedule is called")
                         .isEmpty());
    }

    static class TestClusterLease extends ClusterLease
    {
        private final AtomicReference<ClusterLease> delegate;

        TestClusterLease(ClusterLease delegate)
        {
            this.delegate = new AtomicReference<>(delegate);
        }

        @Override
        public ScheduleDecision toScheduleDecision()
        {
            ScheduleDecision scheduleDecision = delegate.get().toScheduleDecision();
            if (scheduleDecision == ScheduleDecision.RESCHEDULE)
            {
                delegate.set(new ClusterLease(Ownership.CLAIMED));
            }
            return scheduleDecision;
        }

        @Override
        public boolean isClaimedByLocalSidecar()
        {
            return delegate.get().isClaimedByLocalSidecar();
        }
    }

    static class SimulatedTask implements PeriodicTask, ExecuteOnClusterLeaseholderOnly
    {
        final AtomicInteger executionCount = new AtomicInteger(0);
        final AtomicInteger shouldSkipCount = new AtomicInteger(0);
        final AtomicInteger initialDelayCount = new AtomicInteger(0);
        private final CountDownLatch shouldSkipLatch;
        private final CountDownLatch executeLatch;
        private PeriodicTaskExecutor executor;

        SimulatedTask(CountDownLatch executeLatch)
        {
            this(new CountDownLatch(1), executeLatch);
        }

        SimulatedTask(CountDownLatch shouldSkipLatch, CountDownLatch executeLatch)
        {
            this.shouldSkipLatch = shouldSkipLatch;
            this.executeLatch = executeLatch;
        }

        @Override
        public void registerPeriodicTaskExecutor(PeriodicTaskExecutor executor)
        {
            this.executor = executor;
        }

        @Override
        public long initialDelay()
        {
            initialDelayCount.incrementAndGet();
            return 0;
        }

        @Override
        public long delay()
        {
            return 1;
        }

        @Override
        public ScheduleDecision scheduleDecision()
        {
            if (shouldSkipCount.incrementAndGet() == 5)
            {
                shouldSkipLatch.countDown();
                if (executeLatch == null)
                {
                    // unschedule to avoid flakiness
                    executor.unschedule(this);
                }
            }
            return ScheduleDecision.EXECUTE;
        }

        @Override
        public void execute(Promise<Void> promise)
        {
            executionCount.incrementAndGet();
            if (shouldSkipCount.get() == 5)
            {
                executeLatch.countDown();
                // unschedule to avoid flakiness
                executor.unschedule(this);
            }
            promise.complete();
        }
    }

    // a stateful periodic task that increments its value every run
    private static class IncrementPeriodicTask implements PeriodicTask
    {
        private final String name;
        int value = 0;
        AtomicInteger atomicValue = new AtomicInteger(0);

        IncrementPeriodicTask(String name)
        {
            this.name = name;
        }

        @Override
        public String name()
        {
            return name;
        }

        @Override
        public long delay()
        {
            return 1;
        }

        @Override
        public void execute(Promise<Void> promise)
        {
            value += 1;
            atomicValue.incrementAndGet();
            promise.complete();
        }
    }

    static PeriodicTask createSimplePeriodicTask(String name, long delayMillis, Runnable taskBody)
    {
        return createSimplePeriodicTask(name, delayMillis, delayMillis, taskBody);
    }

    static PeriodicTask createSimplePeriodicTask(String name, long initialDelayMillis, long delayMillis, Runnable taskBody)
    {
        return new PeriodicTask()
        {
            @Override
            public String name()
            {
                return name;
            }

            @Override
            public long initialDelay()
            {
                return initialDelayMillis;
            }

            @Override
            public long delay()
            {
                return delayMillis;
            }

            @Override
            public void execute(Promise<Void> promise)
            {
                taskBody.run();
                promise.complete();
            }
        };
    }
}
