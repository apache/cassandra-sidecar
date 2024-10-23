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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.utils.JobResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.cassandra.sidecar.job.JobTest.createJobWithSupplier;

/**
 * Tests to validate job tracking
 */
public class JobTrackerTest
{
    private JobTracker jobTracker;
    private static final int trackerSize = 3;

    Job job1 = createJobWithSupplier(() -> new JobResult(JobResult.JobStatus.Completed));
    Job job2 = createJobWithSupplier(() -> new JobResult(JobResult.JobStatus.Completed));
    Job job3 = createJobWithSupplier(() -> new JobResult(JobResult.JobStatus.Completed));
    Job job4 = createJobWithSupplier(() -> new JobResult(JobResult.JobStatus.Completed));

    @BeforeEach
    void setUp()
    {
        jobTracker = new JobTracker(trackerSize);
    }

    @Test
    void testPutAndGet()
    {
        UUID key1 = UUID.randomUUID();
        UUID key2 = UUID.randomUUID();
        jobTracker.put(key1, job1);
        jobTracker.put(key2, job2);
        assertEquals(job1, jobTracker.get(key1));
        assertEquals(job2, jobTracker.get(key2));
    }

    @Test
    void testPutIfAbsent()
    {
        UUID key1 = UUID.randomUUID();
        jobTracker.put(key1, job1);
        jobTracker.putIfAbsent(key1, job3);
        assertEquals(job1, jobTracker.get(key1));
    }

    @Test
    void testRemove()
    {
        UUID key1 = UUID.randomUUID();
        jobTracker.put(key1, job1);
        jobTracker.remove(key1);
        assertNull(jobTracker.get(key1));
    }

    @Test
    void testClear()
    {
        UUID key1 = UUID.randomUUID();
        UUID key2 = UUID.randomUUID();
        jobTracker.put(key1, job1);
        jobTracker.put(key2, job2);
        jobTracker.clear();
        assertTrue(jobTracker.isEmpty());
    }

    @Test
    void testContainsKey()
    {
        UUID key1 = UUID.randomUUID();
        jobTracker.put(key1, job1);
        assertTrue(jobTracker.containsKey(key1));
        assertFalse(jobTracker.containsKey(UUID.randomUUID()));
    }

    @Test
    void testSizeAndIsEmpty()
    {
        assertTrue(jobTracker.isEmpty());
        jobTracker.put(UUID.randomUUID(), job1);
        assertEquals(1, jobTracker.size());
        assertFalse(jobTracker.isEmpty());
    }

    @Test
    void testRemoveEldestEntryEviction()
    {
        UUID key1 = UUID.randomUUID();
        UUID key2 = UUID.randomUUID();
        UUID key3 = UUID.randomUUID();
        UUID key4 = UUID.randomUUID();

        jobTracker.put(key1, job1);
        jobTracker.put(key2, job2);
        jobTracker.put(key3, job3);
        jobTracker.put(key4, job4);

        assertNull(jobTracker.get(key1));
        assertNotNull(jobTracker.get(key2));
    }

    @Test
    void testGetViewImmutable()
    {
        UUID key1 = UUID.randomUUID();
        UUID key2 = UUID.randomUUID();
        UUID key3 = UUID.randomUUID();

        // Test the immutable view returned by getView
        jobTracker.put(key1, job1);
        jobTracker.put(key2, job2);

        ImmutableMap<UUID, Job> view = jobTracker.getJobsView();
        assertEquals(2, view.size());
        assertThrows(UnsupportedOperationException.class, () -> view.put(key3, job3));
    }

    @Test
    void testConcurrentAccess() throws InterruptedException
    {
        ExecutorService executorService = Executors.newFixedThreadPool(trackerSize);
        for (int i = 0; i < trackerSize; i++)
        {
            executorService.submit(() -> {
                jobTracker.put(UUID.randomUUID(), createJobWithSupplier(() -> new JobResult(JobResult.JobStatus.Completed)));
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);
        assertEquals(trackerSize, jobTracker.size());
    }


    @Test
    void testRemoveEldestEntryChecksMaxSize()
    {
        UUID key1 = UUID.randomUUID();
        UUID key2 = UUID.randomUUID();
        UUID key3 = UUID.randomUUID();
        UUID key4 = UUID.randomUUID();

        JobTracker jobTracker = new JobTracker(3);

        jobTracker.putIfAbsent(key1, job1);
        jobTracker.putIfAbsent(key2, job2);
        jobTracker.putIfAbsent(key3, job3);
        jobTracker.putIfAbsent(key4, job4);

        assertFalse(jobTracker.getJobsView().containsKey(key1));
    }
}
