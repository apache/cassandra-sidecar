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

package org.apache.cassandra.sidecar.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests to make sure locking the {@link org.apache.cassandra.sidecar.utils.SSTableImporter.ImportQueue}
 * class works as expected
 */
class SSTableImporterImportQueueTest
{
    @Test
    void testLocking()
    {
        SSTableImporter.ImportQueue queue = new SSTableImporter.ImportQueue();

        assertThat(queue.tryLock()).isTrue();
        // queue is already locked
        assertThat(queue.tryLock()).isFalse();

        // unlock the queue
        queue.unlock();

        // we should be able to lock the queue again
        assertThat(queue.tryLock()).isTrue();
    }

    @Test
    void testMultiThreadedLocking() throws InterruptedException
    {
        SSTableImporter.ImportQueue queue = new SSTableImporter.ImportQueue();
        AtomicInteger lockCount = new AtomicInteger(0);
        AtomicInteger executionCount = new AtomicInteger(0);
        int nThreads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        CountDownLatch latch = new CountDownLatch(nThreads);

        for (int i = 0; i < nThreads; i++)
        {
            pool.submit(() -> {
                try
                {
                    // Invoke tryLock roughly at the same time
                    latch.countDown();
                    latch.await();

                    if (queue.tryLock())
                    {
                        // Only a single thread should be able to lock
                        lockCount.incrementAndGet();
                    }
                    executionCount.incrementAndGet();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }

        pool.shutdown();
        assertThat(pool.awaitTermination(1, TimeUnit.MINUTES)).isTrue();
        queue.unlock();
        // ensure that only a single thread locked the queue during the execution
        assertThat(lockCount.get()).isEqualTo(1);
        assertThat(executionCount.get()).isEqualTo(nThreads);
    }
}
