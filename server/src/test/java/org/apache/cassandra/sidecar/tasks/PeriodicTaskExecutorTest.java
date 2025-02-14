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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.coordination.ExecuteOnClusterLeaseholderOnly;

import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link PeriodicTaskExecutor} class
 */
class PeriodicTaskExecutorTest
{
    private static final Vertx vertx = Vertx.vertx();
    private static final ExecutorPools executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());
    private static final ClusterLease clusterLease = new ClusterLease();
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

    @AfterAll
    static void teardown()
    {
        executorPools.close();
        vertx.close();
    }

    @Test
    void testLoopFailure()
    {
        int totalFailures = 5;
        AtomicInteger failuresCount = new AtomicInteger(0);
        CountDownLatch closeLatch = new CountDownLatch(1);
        AtomicBoolean isClosed = new AtomicBoolean(false);
        PeriodicTask task = createSimplePeriodicTask("SimpleTask", 1, 1,
                                                     () -> failuresCount.incrementAndGet() >= totalFailures
                                                           ? ScheduleDecision.SKIP
                                                           : ScheduleDecision.EXECUTE,
                                                     () -> {
                                                         throw new RuntimeException("ah, it failed");
                                                     },
                                                     () -> {
                                                         isClosed.set(true);
                                                         closeLatch.countDown();
                                                     });
        taskExecutor.schedule(task);
        loopAssert(1, 1, () -> {
            assertThat(failuresCount.get()).isGreaterThanOrEqualTo(totalFailures);
            taskExecutor.unschedule(task);
        });
        Uninterruptibles.awaitUninterruptibly(closeLatch);
        assertThat(isClosed.get()).isTrue();
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
        PeriodicTask notScheduled = createSimplePeriodicTask("simple task", 1, 1, () -> {});
        Future<Void> unscheduleFuture = taskExecutor.unschedule(notScheduled);
        assertThat(unscheduleFuture.failed()).isTrue();
        assertThat(unscheduleFuture.cause()).hasMessage("No such task to unschedule");
        assertThat(taskExecutor.timerIds()).isEmpty();
    }

    @Test
    void testUnscheduleNonExistTaskShouldNotClose()
    {
        AtomicBoolean isCloseCalled = new AtomicBoolean(false);
        PeriodicTask task = createSimplePeriodicTask("SimpleTask", 1, 1,
                                                     () -> ScheduleDecision.EXECUTE,
                                                     () -> {},
                                                     () -> isCloseCalled.set(true));

        taskExecutor.unschedule(task);
        assertThat(isCloseCalled.get())
        .describedAs("When rescheduling an unscheduled task, the close method of the task should not be called")
        .isFalse();
    }

    @Test
    void testRescheduleDecision()
    {
        testRescheduleDecision(1, 0);
        testRescheduleDecision(0, 0);
        testRescheduleDecision(1, 1);
        testRescheduleDecision(0, 1);
    }

    void testRescheduleDecision(long initialDelay, long delay)
    {
        AtomicInteger totalScheduleDecisionCalls = new AtomicInteger(0);
        AtomicBoolean isExecuteCalled = new AtomicBoolean(false);
        AtomicBoolean isCloseCalled = new AtomicBoolean(false);
        // the task that keeps on rescheduling
        PeriodicTask task = createSimplePeriodicTask("SimpleTask", initialDelay, delay,
                                                     () -> {
                                                         totalScheduleDecisionCalls.incrementAndGet();
                                                         return ScheduleDecision.RESCHEDULE;
                                                     },
                                                     () -> {
                                                         isExecuteCalled.set(true);
                                                         Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
                                                     },
                                                     () -> isCloseCalled.set(true));
        taskExecutor.schedule(task);
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        taskExecutor.unschedule(task);
        Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
        assertThat(isCloseCalled.get()).describedAs("Task close should be called").isTrue();
        assertThat(isExecuteCalled.get()).describedAs("Task should never be executed").isFalse();
        int lastVal = totalScheduleDecisionCalls.get();
        Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
        assertThat(totalScheduleDecisionCalls.get()).isEqualTo(lastVal);
        assertThat(taskExecutor.timerIds()).isEmpty();
    }

    @Test
    void testCloseIsCalledAfterExecutionWhenUnscheduling()
    {
        CountDownLatch readyToExecute = new CountDownLatch(1);
        CountDownLatch unscheduleCalled = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        AtomicBoolean executionFinishes = new AtomicBoolean(false);
        AtomicBoolean closeAfterLastExecution = new AtomicBoolean(false);
        PeriodicTask task = createSimplePeriodicTask("SimpleTask", 1, 1,
                                                     () -> ScheduleDecision.EXECUTE,
                                                     () -> {
                                                         readyToExecute.countDown();
                                                         Uninterruptibles.awaitUninterruptibly(unscheduleCalled);
                                                         executionFinishes.set(true);
                                                     },
                                                     () -> {
                                                         if (executionFinishes.get())
                                                         {
                                                             closeAfterLastExecution.set(true);
                                                         }
                                                         closed.countDown();
                                                     });
        assertThat(taskExecutor.activeRuns()).isEmpty();
        taskExecutor.schedule(task);
        Uninterruptibles.awaitUninterruptibly(readyToExecute);
        // unschedule the task. There should be an active run at the moment
        taskExecutor.unschedule(task);
        assertThat(taskExecutor.activeRuns()).hasSize(1);
        // signal unschedule is called and wait for the task to be closed
        unscheduleCalled.countDown();
        Uninterruptibles.awaitUninterruptibly(closed);
        assertThat(executionFinishes.get()).isTrue();
        assertThat(closeAfterLastExecution.get())
        .describedAs("Close should be called after the active run when unscheduling")
        .isTrue();
    }

    @Test
    void testUnscheduleWhenAlreadyUnscheduled()
    {
        CountDownLatch readyToExecute = new CountDownLatch(1);
        CountDownLatch executeFinishes = new CountDownLatch(1);
        PeriodicTask task = createSimplePeriodicTask("SimpleTask", 1, 1,
                                                     () -> ScheduleDecision.EXECUTE,
                                                     () -> {
                                                         readyToExecute.countDown();
                                                         Uninterruptibles.awaitUninterruptibly(executeFinishes);
                                                     },
                                                     () -> {});
        assertThat(taskExecutor.timerIds()).isEmpty();
        taskExecutor.schedule(task);
        assertThat(taskExecutor.timerIds()).hasSize(1);
        Uninterruptibles.awaitUninterruptibly(readyToExecute);
        Future<Void> unscheduleFuture = taskExecutor.unschedule(task);
        assertThat(unscheduleFuture.isComplete())
        .describedAs("Unscheduled future should be pending")
        .isFalse();
        assertThat(taskExecutor.timerIds())
        .describedAs("task should be unscheduled")
        .hasSize(1).containsValue(-2L);
        // now call unschedule for the second time
        Future<Void> alreadyUnscheduled = taskExecutor.unschedule(task);
        assertThat(alreadyUnscheduled.failed()).isTrue();
        assertThat(alreadyUnscheduled.cause()).hasMessage("Task is already unscheduled");
        assertThat(taskExecutor.timerIds())
        .describedAs("task should continue to be unscheduled")
        .hasSize(1).containsValue(-2L);
        executeFinishes.countDown();
        loopAssert(1, 10, () -> assertThat(taskExecutor.timerIds()).isEmpty());
        assertThat(unscheduleFuture.isComplete())
        .describedAs("Unscheduled future should not be completed")
        .isTrue();
    }

    @Test
    void testRejectSubsequentExecutionWhenClosing()
    {
        PeriodicTaskExecutor testTaskExecutor = new PeriodicTaskExecutor(executorPools, clusterLease);
        AtomicBoolean executorClosed = new AtomicBoolean(false);
        AtomicBoolean taskExecutedAfterClosure = new AtomicBoolean(false);
        CountDownLatch taskExecuted = new CountDownLatch(5);
        PeriodicTask task = createSimplePeriodicTask("SimpleTask", 1, 1, () -> {
            if (executorClosed.get())
            {
                taskExecutedAfterClosure.set(true);
            }
            taskExecuted.countDown();
        });
        testTaskExecutor.schedule(task);
        Uninterruptibles.awaitUninterruptibly(taskExecuted);
        assertThat(testTaskExecutor.timerIds()).hasSize(1);
        Promise<Void> closePromise = Promise.promise();
        testTaskExecutor.close(closePromise);
        getBlocking(closePromise.future().andThen(ignored -> executorClosed.set(true))); // wait for the close to complete
        assertThat(executorClosed.get()).isTrue();
        assertThat(taskExecutedAfterClosure.get()).isFalse();
        assertThat(testTaskExecutor.timerIds()).isEmpty();
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
        public DurationSpec initialDelay()
        {
            initialDelayCount.incrementAndGet();
            return MillisecondBoundConfiguration.ZERO;
        }

        @Override
        public DurationSpec delay()
        {
            return MillisecondBoundConfiguration.ONE;
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
        public DurationSpec delay()
        {
            return MillisecondBoundConfiguration.ONE;
        }

        @Override
        public void execute(Promise<Void> promise)
        {
            value += 1;
            atomicValue.incrementAndGet();
            promise.complete();
        }
    }

    static PeriodicTask createSimplePeriodicTask(String name, long initialDelayMillis, long delayMillis, Runnable taskBody)
    {
        return createSimplePeriodicTask(name, initialDelayMillis, delayMillis, () -> ScheduleDecision.EXECUTE, taskBody, () -> {});
    }

    static PeriodicTask createSimplePeriodicTask(String name,
                                                 long initialDelayMillis,
                                                 long delayMillis,
                                                 Supplier<ScheduleDecision> scheduleDecision,
                                                 Runnable taskBody,
                                                 Runnable closeBody)
    {
        return new PeriodicTask()
        {
            @Override
            public ScheduleDecision scheduleDecision()
            {
                return scheduleDecision.get();
            }

            @Override
            public String name()
            {
                return name;
            }

            @Override
            public DurationSpec initialDelay()
            {
                return new MillisecondBoundConfiguration(initialDelayMillis, TimeUnit.MILLISECONDS);
            }

            @Override
            public DurationSpec delay()
            {
                return new MillisecondBoundConfiguration(delayMillis, TimeUnit.MILLISECONDS);
            }

            @Override
            public void execute(Promise<Void> promise)
            {
                taskBody.run();
                promise.complete();
            }

            @Override
            public void close()
            {
                closeBody.run();
            }
        };
    }
}
