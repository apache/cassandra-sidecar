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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Test;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.coordination.ExecuteOnClusterLeaseholderOnly;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link PeriodicTaskExecutor} class
 */
class PeriodicTaskExecutorTest
{
    Vertx vertx = Vertx.vertx();
    ExecutorPools executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());

    @Test
    void testLoopFailure()
    {
        PeriodicTaskExecutor taskExecutor = new PeriodicTaskExecutor(executorPools);

        int totalFailures = 5;
        AtomicInteger failuresCount = new AtomicInteger(0);
        CountDownLatch closeLatch = new CountDownLatch(1);
        AtomicBoolean isClosed = new AtomicBoolean(false);
        taskExecutor.schedule(new PeriodicTask()
        {
            @Override
            public long delay()
            {
                return 20;
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
    void testPeriodicTaskOnNonExecutor()
    {
        CountDownLatch latch = new CountDownLatch(1);
        SimulatedTask taskThatRunsOnNonExecutor = new SimulatedTask(latch);
        PeriodicTaskExecutor taskExecutorNeverExecute = new PeriodicTaskExecutor(executorPools, new ClusterLease(ExecutionDetermination.SKIP_EXECUTION));

        taskExecutorNeverExecute.schedule(taskThatRunsOnNonExecutor);
        assertThat(Uninterruptibles.awaitUninterruptibly(latch, 30, TimeUnit.SECONDS)).isTrue();
        assertThat(taskThatRunsOnNonExecutor.executionCount.get()).isEqualTo(0);
        assertThat(taskThatRunsOnNonExecutor.initialDelayCount.get()).isEqualTo(1);
    }

    @Test
    void testPeriodicTaskOnExecutor()
    {
        CountDownLatch latch = new CountDownLatch(1);
        SimulatedTask taskThatRunsOnExecutor = new SimulatedTask(latch);
        PeriodicTaskExecutor taskExecutorAlwaysExecute = new PeriodicTaskExecutor(executorPools, new ClusterLease(ExecutionDetermination.EXECUTE));

        taskExecutorAlwaysExecute.schedule(taskThatRunsOnExecutor);
        assertThat(Uninterruptibles.awaitUninterruptibly(latch, 30, TimeUnit.SECONDS)).isTrue();
        assertThat(taskThatRunsOnExecutor.executionCount.get()).isEqualTo(5);
        assertThat(taskThatRunsOnExecutor.initialDelayCount.get()).isEqualTo(1);
    }

    @Test
    void testPeriodicTaskIsRescheduledWhenIndeterminate()
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
    }

    static class TestClusterLease extends ClusterLease
    {
        private final AtomicReference<ClusterLease> delegate;

        TestClusterLease(ClusterLease delegate)
        {
            this.delegate = new AtomicReference<>(delegate);
        }

        @Override
        public ExecutionDetermination executionDetermination()
        {
            ExecutionDetermination executionDetermination = delegate.get().executionDetermination();
            if (executionDetermination == ExecutionDetermination.INDETERMINATE)
            {
                delegate.set(new ClusterLease(ExecutionDetermination.EXECUTE));
            }
            return executionDetermination;
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
        private final CountDownLatch latch;

        SimulatedTask(CountDownLatch latch)
        {
            this.latch = latch;
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
        public boolean shouldSkip()
        {
            if (shouldSkipCount.incrementAndGet() == 5)
            {
                latch.countDown();
            }
            return false;
        }

        @Override
        public void execute(Promise<Void> promise)
        {
            executionCount.incrementAndGet();
            promise.complete();
        }
    }
}
