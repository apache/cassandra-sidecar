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

import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.exceptions.OperationalJobConflictException;

import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.RUNNING;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.SUCCEEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    void testWithNoDownstreamJob()
    {
        OperationalJobTracker tracker = new OperationalJobTracker(4);
        OperationalJobManager manager = new OperationalJobManager(tracker, executorPool);

        OperationalJob testJob = OperationalJobTest.createOperationalJob(SUCCEEDED);
        manager.trySubmitJob(testJob);
        testJob.execute(Promise.promise());
        assertThat(testJob.asyncResult().isComplete()).isTrue();
        assertThat(testJob.status()).isEqualTo(SUCCEEDED);
        assertThat(tracker.get(testJob.jobId())).isNotNull();
    }

    @Test
    void testWithRunningDownstreamJob()
    {
        OperationalJob runningJob = OperationalJobTest.createOperationalJob(RUNNING);
        OperationalJobTracker tracker = new OperationalJobTracker(4);
        ExecutorPools mockPools = mock(ExecutorPools.class);
        TaskExecutorPool mockExecPool = mock(TaskExecutorPool.class);
        when(mockPools.internal()).thenReturn(mockExecPool);
        when(mockExecPool.runBlocking(any())).thenReturn(null);
        OperationalJobManager manager = new OperationalJobManager(tracker, executorPool);
        assertThatThrownBy(() -> manager.trySubmitJob(runningJob))
        .isExactlyInstanceOf(OperationalJobConflictException.class)
        .hasMessage("The same operational job is already running on Cassandra. operationName='Operation X'");
    }

    @Test
    void testWithLongRunningJob()
    {
        UUID jobId = UUIDs.timeBased();

        OperationalJobTracker tracker = new OperationalJobTracker(4);
        OperationalJobManager manager = new OperationalJobManager(tracker, executorPool);

        OperationalJob testJob = OperationalJobTest.createOperationalJob(jobId, SecondBoundConfiguration.parse("10s"));

        manager.trySubmitJob(testJob);
        // execute the job async.
        vertx.executeBlocking(testJob::execute);
        // by the time of checking, the job should still be running. It runs for 10 seconds.
        assertThat(testJob.asyncResult().isComplete()).isFalse();
        assertThat(tracker.get(jobId)).isNotNull();
    }

    @Test
    void testWithFailingJob()
    {
        UUID jobId = UUIDs.timeBased();

        OperationalJobTracker tracker = new OperationalJobTracker(4);
        OperationalJobManager manager = new OperationalJobManager(tracker, executorPool);

        String msg = "Test Job failed";
        OperationalJob failingJob = new OperationalJob(jobId)
        {
            @Override
            public boolean isRunningOnCassandra()
            {
                return false;
            }

            @Override
            protected void executeInternal() throws OperationalJobException
            {
                throw new OperationalJobException(msg);
            }
        };

        manager.trySubmitJob(failingJob);
        failingJob.execute(Promise.promise());
        assertThat(failingJob.asyncResult().isComplete()).isTrue();
        assertThat(failingJob.asyncResult().failed()).isTrue();
        assertThat(tracker.get(jobId)).isNotNull();
    }
}
