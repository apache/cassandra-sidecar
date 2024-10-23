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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.utils.JobResult;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests to validate the Job APIs
 */
public class JobTest
{

    public static Job createJobWithSupplier(Supplier<JobResult> supplier)
    {
        return new Job(UUID.randomUUID())
        {
            @Override
            public Supplier<JobResult> jobOperationSupplier()
            {
                return supplier;
            }

            public String operation()
            {
                return "test";
            }

            public boolean jobInProgress()
            {
                return false;
            }
        };
    }

    @Test
    void testJobCompletion()
    {
        Job job = createJobWithSupplier(() -> new JobResult(JobResult.JobStatus.Completed));
        Executors.newSingleThreadExecutor()
                 .submit(() -> job.execute());
        assertThat(job.isResultAvailable(5)).isTrue();
        assertThat(job.status()).isEqualTo(JobResult.JobStatus.Completed);
        assertThat(job.failureReason()).isEmpty();
    }

    @Test
    void testJobFailed()
    {
        Job failingJob = new Job(UUID.randomUUID())
        {
            public Supplier<JobResult> jobOperationSupplier() throws Exception
            {
                throw new Exception("Job failed");
            }

            public String operation()
            {
                return "test";
            }

            public boolean jobInProgress()
            {
                return false;
            }
        };
        Executors.newSingleThreadExecutor().submit(() -> failingJob.execute());

        assertThat(failingJob.isResultAvailable(5)).isTrue();
        assertThat(failingJob.status()).isEqualTo(JobResult.JobStatus.Failed);
        assertThat(failingJob.failureReason()).isEqualTo("Job failed");
    }

    @Test
    void testLongRunningJob()
    {
        Job delayedJob = new Job(UUID.randomUUID())
        {
            public Supplier<JobResult> jobOperationSupplier()
            {
                return () -> {
                    Uninterruptibles.sleepUninterruptibly(6, TimeUnit.SECONDS);
                    return new JobResult(JobResult.JobStatus.Completed);
                };
            }

            public String operation()
            {
                return "test";
            }

            public boolean jobInProgress()
            {
                return true;
            }
        };

        Executors.newSingleThreadExecutor().submit(() -> delayedJob.execute());
        assertThat(delayedJob.isResultAvailable(5)).isFalse();
        assertThat(delayedJob.status()).isEqualTo(JobResult.JobStatus.Pending);
        assertThat(delayedJob.failureReason()).isEmpty();
    }
}
