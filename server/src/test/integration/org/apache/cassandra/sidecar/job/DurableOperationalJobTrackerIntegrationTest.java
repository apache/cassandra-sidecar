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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;
import org.apache.cassandra.sidecar.job.storage.StorageProvider;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link DurableOperationalJobTracker} with a Cassandra-backed {@link StorageProvider}.
 */
class DurableOperationalJobTrackerIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void testDurableOperationalJobTrackerOperations() throws InterruptedException
    {
        waitForSchemaReady(10, TimeUnit.SECONDS);

        StorageProvider storageProvider = injector.getInstance(StorageProvider.class);
        storageProvider.initialize();

        ExecutorPools executorPools = injector.getInstance(ExecutorPools.class);
        TaskExecutorPool executorPool = executorPools.internal();
        DurableOperationalJobTracker tracker = new DurableOperationalJobTracker(new ServiceConfigurationImpl(),
                                                                                storageProvider,
                                                                                executorPool);

        UUID jobId1 = UUIDs.timeBased();
        OperationalJob job1 = OperationalJobTest.createOperationalJob(jobId1, OperationalJobStatus.CREATED);

        tracker.computeIfAbsent(jobId1, id -> job1);

        loopAssert(2, () -> {
            OperationalJobRecord record1 = storageProvider.findJob(jobId1);
            assertThat(record1)
                .withFailMessage("Job should be persisted to Cassandra on creation")
                .isNotNull()
                .satisfies(r -> {
                    assertThat(r.jobId()).isEqualTo(jobId1);
                    assertThat(r.operationType()).isEqualTo(job1.operationType());
                    assertThat(r.status()).isEqualTo(OperationalJobStatus.CREATED);
                });
        });

        UUID jobId2 = UUIDs.timeBased();
        OperationalJob job2 = OperationalJobTest.createOperationalJob(jobId2, MillisecondBoundConfiguration.parse("50ms"));
        CountDownLatch latch = new CountDownLatch(1);

        tracker.computeIfAbsent(jobId2, id -> job2);
        executorPool.executeBlocking(job2::execute);

        job2.asyncResult().onComplete(ar -> latch.countDown());
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        loopAssert(2, () -> {
            OperationalJobRecord record2 = storageProvider.findJob(jobId2);
            assertThat(record2)
                .withFailMessage("Job status should be updated to SUCCEEDED in Cassandra")
                .isNotNull()
                .satisfies(r -> assertThat(r.status()).isEqualTo(OperationalJobStatus.SUCCEEDED));
        });

        loopAssert(2, () -> {
            assertThat(tracker.jobsView())
                .withFailMessage("Job should be removed from local map after completion")
                .doesNotContainKey(jobId2);
        });

        OperationalJobInfo retrieved = tracker.get(jobId2);
        assertThat(retrieved)
            .withFailMessage("get() should return an OperationalJobRecord from storage after completion")
            .isNotNull()
            .isInstanceOf(OperationalJobRecord.class);
        assertThat(retrieved.jobId()).isEqualTo(jobId2);
        assertThat(retrieved.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertThat(retrieved.operationType()).isEqualTo(job2.operationType());
    }
}
