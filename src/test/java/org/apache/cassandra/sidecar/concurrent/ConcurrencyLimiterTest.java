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

package org.apache.cassandra.sidecar.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test {@link ConcurrencyLimiter} limits as expected.
 */
public class ConcurrencyLimiterTest
{
    @Test
    void testLimiter()
    {
        ConcurrencyLimiter concurrencyLimiter = new ConcurrencyLimiter(() -> 2);

        assertThat(concurrencyLimiter.tryAcquire()).isTrue();
        assertThat(concurrencyLimiter.tryAcquire()).isTrue();
        assertThat(concurrencyLimiter.tryAcquire()).isFalse();

        concurrencyLimiter.releasePermit();
        assertThat(concurrencyLimiter.tryAcquire()).isTrue();
        assertThat(concurrencyLimiter.tryAcquire()).isFalse();
    }

    @Test
    void testConcurrentPermitsAcquired() throws InterruptedException
    {
        ConcurrencyLimiter concurrencyLimiter = new ConcurrencyLimiter(() -> 20);

        AtomicInteger successfulAcquires = new AtomicInteger(0);
        AtomicInteger attemptedAcquires = new AtomicInteger(0);
        int nThreads = 30;
        ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        CountDownLatch latch = new CountDownLatch(nThreads);

        for (int i = 0; i < nThreads; i++)
        {
            pool.submit(() -> {
                try
                {
                    // Invoke tryAcquire roughly at the same time
                    latch.countDown();
                    latch.await();

                    if (concurrencyLimiter.tryAcquire())
                    {
                        // Only a single thread should be able to lock
                        successfulAcquires.incrementAndGet();
                    }
                    attemptedAcquires.incrementAndGet();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }

        pool.shutdown();
        assertThat(pool.awaitTermination(1, TimeUnit.MINUTES)).isTrue();
        assertThat(attemptedAcquires.get()).isEqualTo(nThreads);
        assertThat(successfulAcquires.get()).isEqualTo(20);
    }

    @Test
    void testReleaseShouldMaintainLimit()
    {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(() -> 2);

        limiter.releasePermit();
        limiter.releasePermit();
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isFalse();
    }

    @Test
    void testLimitIncreasedAfterReachedLimit()
    {
        AtomicInteger limit = new AtomicInteger(2);
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(limit::get);

        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isFalse();

        // now increase the limit and make sure we are able to acquire until we reach
        // the limit
        limit.set(4);

        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isFalse();
    }

    @Test
    void testConcurrentPermitRelease() throws InterruptedException
    {
        int permits = 20;
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(() -> permits);

        for (int i = 0; i < permits; i++)
        {
            assertThat(limiter.tryAcquire()).isTrue();
        }
        assertThat(limiter.tryAcquire()).isFalse();

        int nThreads = 30;
        ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        CountDownLatch latch = new CountDownLatch(nThreads);

        for (int i = 0; i < nThreads; i++)
        {
            pool.submit(() -> {
                try
                {
                    // Invoke releasePermit roughly at the same time
                    latch.countDown();
                    latch.await();

                    limiter.releasePermit();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }

        pool.shutdown();
        assertThat(pool.awaitTermination(1, TimeUnit.MINUTES)).isTrue();
        assertThat(limiter.permits.get()).isEqualTo(0);
    }
}
