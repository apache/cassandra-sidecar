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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationsJobException;
import org.apache.cassandra.sidecar.common.utils.OperationsJobResult.OperationsJobStatus;

import static org.apache.cassandra.sidecar.common.utils.OperationsJobResult.OperationsJobStatus.COMPLETED;
import static org.apache.cassandra.sidecar.job.OperationsJobTest.createJobWithStatus;


/**
 * Tests to validate job tracking
 */
public class OperationsJobTrackerTest
{
    private OperationsJobTracker jobTracker;
    private static final int trackerSize = 3;
    protected Vertx vertx = Vertx.vertx();

    OperationsJob job1 = createJobWithStatus(COMPLETED);
    OperationsJob job2 = createJobWithStatus(COMPLETED);
    OperationsJob job3 = createJobWithStatus(COMPLETED);
    OperationsJob job4 = createJobWithStatus(COMPLETED);

    OperationsJob jobWithStaleCreationTime = new OperationsJob(vertx, UUID.randomUUID())
    {
        public String operation()
        {
            return "test";
        }

        public boolean isRunningDownstream()
        {
            return false;
        }

        public long creationTime()
        {
            return System.nanoTime() - TimeUnit.DAYS.toNanos(2);
        }

        protected OperationsJobStatus executeInternal() throws OperationsJobException
        {
            return COMPLETED;
        }
    };

    @BeforeEach
    void setUp()
    {
        vertx = Vertx.vertx();
        jobTracker = new OperationsJobTracker(trackerSize);
    }

    @Test
    void testPutAndGet()
    {
        UUID key1 = UUID.randomUUID();
        UUID key2 = UUID.randomUUID();
        jobTracker.put(key1, job1);
        jobTracker.put(key2, job2);
        Assertions.assertEquals(job1, jobTracker.get(key1));
        Assertions.assertEquals(job2, jobTracker.get(key2));
    }

    @Test
    void testComputeIfAbsent()
    {
        UUID key1 = UUID.randomUUID();
        jobTracker.put(key1, job1);
        jobTracker.computeIfAbsent(key1, v -> job3);
        Assertions.assertEquals(job1, jobTracker.get(key1));
    }

    @Test
    void testNoEviction()
    {
        UUID key1 = UUID.randomUUID();
        UUID key2 = UUID.randomUUID();
        UUID key3 = UUID.randomUUID();
        UUID key4 = UUID.randomUUID();

        jobTracker.put(key1, job1);
        jobTracker.put(key2, job2);
        jobTracker.put(key3, job3);
        jobTracker.put(key4, job4);

        Assertions.assertNotNull(jobTracker.get(key1));
        Assertions.assertNotNull(jobTracker.get(key4));
    }

    @Test
    void testRemoveEldestEntryEvictionOnExpiry()
    {
        UUID key1 = UUID.randomUUID();
        UUID key2 = UUID.randomUUID();
        UUID key3 = UUID.randomUUID();
        UUID key6 = UUID.randomUUID();

        jobWithStaleCreationTime.setStatus(Future.succeededFuture(COMPLETED));
        jobTracker.put(key6, jobWithStaleCreationTime);
        jobTracker.put(key1, job1);
        jobTracker.put(key2, job2);
        jobTracker.put(key3, job3);

        Assertions.assertNotNull(jobTracker.get(key3));
        Assertions.assertNull(jobTracker.get(key6));
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

        Map<UUID, OperationsJob> view = jobTracker.getJobsView();
        Assertions.assertEquals(2, view.size());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> view.put(key3, job3));
    }

    @Test
    void testConcurrentAccess() throws InterruptedException
    {
        ExecutorService executorService = Executors.newFixedThreadPool(trackerSize);
        for (int i = 0; i < trackerSize; i++)
        {
            executorService.submit(() -> {
                jobTracker.put(UUID.randomUUID(), createJobWithStatus(COMPLETED));
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);
        Assertions.assertEquals(trackerSize, jobTracker.size());
    }
}
