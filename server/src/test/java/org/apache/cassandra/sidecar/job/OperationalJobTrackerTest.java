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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException;

import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.SUCCEEDED;
import static org.apache.cassandra.sidecar.job.OperationalJobTest.createOperationalJob;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests to validate job tracking
 */
class OperationalJobTrackerTest
{
    private OperationalJobTracker jobTracker;
    private static final int trackerSize = 3;

    OperationalJob job1 = createOperationalJob(SUCCEEDED);
    OperationalJob job2 = createOperationalJob(SUCCEEDED);
    OperationalJob job3 = createOperationalJob(SUCCEEDED);
    OperationalJob job4 = createOperationalJob(SUCCEEDED);

    long twoDaysAgo = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2);
    OperationalJob jobWithStaleCreationTime = new OperationalJob(UUIDs.startOf(twoDaysAgo))
    {
        @Override
        protected void executeInternal() throws OperationalJobException
        {
        }

        @Override
        public OperationalJobStatus status()
        {
            return SUCCEEDED;
        }
    };

    @BeforeEach
    void setUp()
    {
        jobTracker = new OperationalJobTracker(trackerSize);
    }

    @Test
    void testPutAndGet()
    {
        jobTracker.put(job1);
        jobTracker.put(job2);
        assertThat(jobTracker.get(job1.jobId)).isSameAs(job1);
        assertThat(jobTracker.get(job2.jobId)).isSameAs(job2);
    }

    @Test
    void testComputeIfAbsent()
    {
        jobTracker.put(job1);
        OperationalJob job = jobTracker.computeIfAbsent(job1.jobId, v -> job3);
        assertThat(job).isNotSameAs(job3);
        assertThat(job).isSameAs(job1);
        assertThat(jobTracker.get(job1.jobId)).isSameAs(job1);
    }

    @Test
    void testNoEviction()
    {
        jobTracker.put(job1);
        jobTracker.put(job2);
        jobTracker.put(job3);
        jobTracker.put(job4);

        assertThat(jobTracker.size())
        .describedAs("Although the tracker initial size is 3, no job is evicted since all jobs are still running")
        .isEqualTo(4);
        assertThat(jobTracker.get(job1.jobId)).isNotNull();
        assertThat(jobTracker.get(job2.jobId)).isNotNull();
        assertThat(jobTracker.get(job3.jobId)).isNotNull();
        assertThat(jobTracker.get(job4.jobId)).isNotNull();
    }

    @Test
    void testRemoveEldestEntryEvictionOnExpiry()
    {
        jobTracker.put(jobWithStaleCreationTime);
        jobTracker.put(job1);
        jobTracker.put(job2);
        jobTracker.put(job3);

        assertThat(jobTracker.size()).isEqualTo(3);
        assertThat(jobTracker.get(job1.jobId)).isNotNull();
        assertThat(jobTracker.get(job2.jobId)).isNotNull();
        assertThat(jobTracker.get(job3.jobId)).isNotNull();
        assertThat(jobTracker.get(jobWithStaleCreationTime.jobId)).isNull();
    }

    @Test
    void testGetViewImmutable()
    {
        // Test the immutable view returned by getView
        jobTracker.put(job1);
        jobTracker.put(job2);

        Map<UUID, OperationalJob> view = jobTracker.getJobsView();
        assertThat(view.size()).isEqualTo(2);
        assertThatThrownBy(() -> view.put(job3.jobId, job3))
        .isExactlyInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testConcurrentAccess() throws Exception
    {
        int one = 1;
        long pastTimestamp = System.currentTimeMillis() - OperationalJobTracker.ONE_DAY_TTL - 1000L;
        OperationalJobTracker tracker = new OperationalJobTracker(one);
        ExecutorService executorService = Executors.newFixedThreadPool(trackerSize);
        List<OperationalJob> sortedJobs = IntStream.range(0, trackerSize + 10)
                                                   .boxed()
                                                   .map(i -> createOperationalJob(UUIDs.startOf(pastTimestamp + i), SUCCEEDED))
                                                   .collect(Collectors.toList());
        sortedJobs.forEach(tracker::put);
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);
        assertThat(tracker.size()).isEqualTo(one);
        assertThat(tracker.getJobsView().values().iterator().next())
        .describedAs("Only the last job is kept")
        .isSameAs(sortedJobs.get(sortedJobs.size() - 1));
    }
}
