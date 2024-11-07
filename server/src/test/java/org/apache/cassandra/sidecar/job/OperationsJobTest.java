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

import org.apache.cassandra.sidecar.common.utils.OperationsJobResult;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests to validate the Job APIs
 */
public class OperationsJobTest
{

    public static OperationsJob createJobWithSupplier(Supplier<OperationsJobResult> supplier)
    {
        return new OperationsJob(UUID.randomUUID())
        {
            @Override
            public Supplier<OperationsJobResult> jobOperationSupplier()
            {
                return supplier;
            }

            public String operation()
            {
                return "test";
            }

            public boolean checkInflightJob()
            {
                return false;
            }
        };
    }

    @Test
    void testJobCompletion()
    {
        OperationsJob job = createJobWithSupplier(() -> new OperationsJobResult(OperationsJobResult.OperationsJobStatus.Completed));
        Executors.newSingleThreadExecutor()
                 .submit(() -> job.execute());
        assertThat(job.isResultAvailable(5)).isTrue();
        assertThat(job.status()).isEqualTo(OperationsJobResult.OperationsJobStatus.Completed);
        assertThat(job.failureReason()).isEmpty();
    }

    @Test
    void testJobFailed()
    {
        OperationsJob failingJob = new OperationsJob(UUID.randomUUID())
        {
            public Supplier<OperationsJobResult> jobOperationSupplier() throws Exception
            {
                throw new Exception("Test Job failed");
            }

            public String operation()
            {
                return "test";
            }

            public boolean checkInflightJob()
            {
                return false;
            }
        };
        Executors.newSingleThreadExecutor().submit(() -> failingJob.execute());

        assertThat(failingJob.isResultAvailable(5)).isTrue();
        assertThat(failingJob.status()).isEqualTo(OperationsJobResult.OperationsJobStatus.Failed);
        assertThat(failingJob.failureReason()).contains("Test Job failed");
    }

    @Test
    void testLongRunningJob()
    {
        OperationsJob delayedJob = new OperationsJob(UUID.randomUUID())
        {
            public Supplier<OperationsJobResult> jobOperationSupplier()
            {
                return () -> {
                    Uninterruptibles.sleepUninterruptibly(6, TimeUnit.SECONDS);
                    return new OperationsJobResult(OperationsJobResult.OperationsJobStatus.Completed);
                };
            }

            public String operation()
            {
                return "test";
            }

            public boolean checkInflightJob()
            {
                return true;
            }
        };

        Executors.newSingleThreadExecutor().submit(() -> delayedJob.execute());
        assertThat(delayedJob.isResultAvailable(5)).isFalse();
        assertThat(delayedJob.status()).isEqualTo(OperationsJobResult.OperationsJobStatus.Pending);
        assertThat(delayedJob.failureReason()).isEmpty();
    }
}
