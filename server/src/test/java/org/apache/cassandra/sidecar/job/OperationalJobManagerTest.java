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
import org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.exceptions.OperationalJobConflictException;

import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.RUNNING;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.SUCCEEDED;
import static org.assertj.core.api.Assertions.assertThat;

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
        OperationalJobTracker tracker = new OperationalJobTracker(4);
        OperationalJobManager manager = new OperationalJobManager(tracker, executorPool);
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
        OperationalJobTracker tracker = new OperationalJobTracker(4);
        OperationalJobManager manager = new OperationalJobManager(tracker, executorPool);
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
        OperationalJobTracker tracker = new OperationalJobTracker(4);
        OperationalJobManager manager = new OperationalJobManager(tracker, executorPool);
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
        OperationalJobTracker tracker = new OperationalJobTracker(4);
        OperationalJobManager manager = new OperationalJobManager(tracker, executorPool);
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
}
