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

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;

import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests to validate the Job APIs
 */
class OperationalJobTest
{
    private final Vertx vertx = Vertx.vertx();
    private final ExecutorPools executorPools = new ExecutorPools(Vertx.vertx(), new ServiceConfigurationImpl());
    private final TaskExecutorPool executorPool = executorPools.internal();

    public static OperationalJob createOperationalJob(OperationalJobStatus jobStatus)
    {
        return createOperationalJob(UUIDs.timeBased(), jobStatus);
    }

    public static OperationalJob createOperationalJob(UUID jobId, OperationalJobStatus jobStatus)
    {
        return new OperationalJob(jobId)
        {
            @Override
            protected void executeInternal() throws OperationalJobException
            {
            }

            @Override
            public boolean isRunningOnCassandra()
            {
                return jobStatus == OperationalJobStatus.RUNNING;
            }

            @Override
            public OperationalJobStatus status()
            {
                return jobStatus;
            }

            @Override
            public String name()
            {
                return "Operation X";
            }
        };
    }

    public static OperationalJob createOperationalJob(UUID jobId, DurationSpec jobDuration)
    {
        return createOperationalJob(jobId, jobDuration, null);
    }

    public static OperationalJob createOperationalJob(UUID jobId, DurationSpec jobDuration, OperationalJobException jobFailure)
    {
        return new OperationalJob(jobId)
        {
            @Override
            public boolean isRunningOnCassandra()
            {
                return false;
            }

            @Override
            protected void executeInternal() throws OperationalJobException
            {
                if (jobDuration != null)
                {
                    Uninterruptibles.sleepUninterruptibly(jobDuration.quantity(), jobDuration.unit());
                }

                if (jobFailure != null)
                {
                    throw jobFailure;
                }
            }

            @Override
            public String name()
            {
                return "Operation X";
            }
        };
    }

    @AfterEach
    void cleanup()
    {
        TestResourceReaper.create().with(vertx).with(executorPools).close();
    }

    @Test
    void testJobCompletion()
    {
        OperationalJob job = createOperationalJob(OperationalJobStatus.SUCCEEDED);
        Promise<Void> p = Promise.promise();
        job.execute(p);
        Future<Void> future = p.future();
        assertThat(future.succeeded()).isTrue();
        assertThat(job.asyncResult().succeeded()).isTrue();
        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
    }

    @Test
    void testJobFailed()
    {
        String msg = "Test Job failed";
        OperationalJob failingJob = new OperationalJob(UUIDs.timeBased())
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

        Promise<Void> p = Promise.promise();
        failingJob.execute(p);

        Future<Void> future = p.future();
        assertThat(future.failed()).isTrue();
        assertThat(future.cause())
        .isExactlyInstanceOf(OperationalJobException.class)
        .hasMessage(msg);
        assertThat(failingJob.status()).isEqualTo(OperationalJobStatus.FAILED);
        assertThat(failingJob.asyncResult().failed()).isTrue();
        assertThat(failingJob.asyncResult().cause())
        .isExactlyInstanceOf(OperationalJobException.class)
        .hasMessage(msg);
    }

    @Test
    void testGetAsyncResultInWaitTime()
    {
        OperationalJob longRunning = createOperationalJob(UUIDs.timeBased(), MillisecondBoundConfiguration.parse("500ms"));
        executorPool.executeBlocking(longRunning::execute);
        DurationSpec waitTime = SecondBoundConfiguration.parse("2s");
        Future<Void> result = longRunning.asyncResult(executorPool, waitTime);
        // it should finish in around 500 ms.
        loopAssert(1, () -> assertThat(result.succeeded()).isTrue());
    }

    @Test
    void testGetFailedAsyncResultInWaitTime()
    {
        OperationalJobException jobFailure = new OperationalJobException("Job fails");
        OperationalJob longButFailedJob = createOperationalJob(UUIDs.timeBased(), MillisecondBoundConfiguration.parse("500ms"), jobFailure);
        executorPool.executeBlocking(longButFailedJob::execute);
        DurationSpec waitTime = SecondBoundConfiguration.parse("2s");
        Future<Void> result = longButFailedJob.asyncResult(executorPool, waitTime);
        // it should finish in around 500 ms.
        loopAssert(1, () -> {
            assertThat(result.failed()).isTrue();
            assertThat(result.cause()).isEqualTo(jobFailure);
        });
    }

    @Test
    void testGetAsyncResultExceedsWaitTime()
    {
        OperationalJob longRunning = createOperationalJob(UUIDs.timeBased(), SecondBoundConfiguration.parse("5s"));
        executorPool.executeBlocking(longRunning::execute);
        DurationSpec waitTime = MillisecondBoundConfiguration.parse("200ms");
        Future<Void> result = longRunning.asyncResult(executorPool, waitTime);
        loopAssert(1, () -> {
            // the composite future is completed in 200ms. The operational job is still running, so the isExecuting should return true too.
            assertThat(result.succeeded()).isTrue();
            assertThat(longRunning.isExecuting()).isTrue();
        });
    }
}
