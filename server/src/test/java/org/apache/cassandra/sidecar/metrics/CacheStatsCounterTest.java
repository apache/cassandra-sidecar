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

package org.apache.cassandra.sidecar.metrics;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.testing.FakeTicker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.apache.cassandra.sidecar.AssertionUtils;
import org.assertj.core.data.Offset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Unit tests for {@link CacheStatsCounter}
 */
class CacheStatsCounterTest
{
    private Cache<String, String> cache;
    private FakeTicker fakeTicker;
    private CacheStatsCounter instance;

    @BeforeEach
    void setup()
    {
        MetricRegistry registry = SharedMetricRegistries.getOrCreate("stats_counter_test");
        fakeTicker = new FakeTicker();
        instance = new CacheStatsCounter(registry, "test");
        cache = Caffeine.newBuilder()
                        .maximumSize(5)
                        .expireAfterAccess(1, TimeUnit.HOURS)
                        .ticker(fakeTicker::read)
                        .recordStats(() -> instance)
                        .build();
    }

    @AfterEach
    void cleanup()
    {
        SharedMetricRegistries.clear();
    }

    @Test
    void testStatsCounter()
    {
        // all metrics should start cleared
        assertThat(instance.hits.metric.getValue()).isZero();
        assertThat(instance.misses.metric.getValue()).isZero();
        assertThat(instance.loadSuccess.metric.getCount()).isZero();
        assertThat(instance.loadFailure.metric.getCount()).isZero();
        assertThat(instance.evictions.metric.getValue()).isZero();
        assertThat(instance.totalLoadTimeNanos.longValue()).isZero();

        // let's start populating the cache
        assertThat(cache.get("foo", k -> "bar")).isEqualTo("bar");
        assertThat(instance.hits.metric.getValue()).isZero();
        assertThat(instance.misses.metric.getValue()).isOne();
        assertThat(instance.loadSuccess.metric.getCount()).isOne();
        assertThat(instance.loadFailure.metric.getCount()).isZero();
        assertThat(instance.evictions.metric.getValue()).isZero();
        assertThat(instance.totalLoadTimeNanos.longValue()).isGreaterThan(0);

        // let's get some hits in a loop
        for (int i = 0; i < 10; i++)
        {
            assertThat(cache.get("foo", k -> "bar")).isEqualTo("bar");
        }
        assertThat(instance.hits.metric.getValue()).isEqualTo(10);

        // now let's fail a load to the cache
        assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> cache.get("failMe", k -> {
            throw new RuntimeException("failed you");
        }));

        // load success is still the same as before
        assertThat(instance.loadSuccess.metric.getCount()).isOne();
        // and load failure is now tracking one
        assertThat(instance.loadFailure.metric.getCount()).isOne();
        // and misses increases to 1, but hits is not affected
        assertThat(instance.misses.metric.getValue()).isOne();
        assertThat(instance.hits.metric.getValue()).isZero();

        // let's evict the entry
        fakeTicker.advance(1, TimeUnit.HOURS);

        // make the cache perform any pending operations
        cache.cleanUp();
        // ensure the cache is now empty
        assertThat(cache.estimatedSize()).isZero();
        // and make sure the evictions are tracked
        assertThat(instance.evictions.metric.getValue()).isOne();

        // Now let's get a snapshot of the stats
        CacheStats snapshot = instance.snapshot();
        assertThat(snapshot.averageLoadPenalty()).isGreaterThan(0);
        assertThat(snapshot.evictionCount()).isZero(); // consumed by pulling the metric directly
        assertThat(snapshot.evictionWeight()).isZero();
        assertThat(snapshot.hitCount()).isZero();
        assertThat(snapshot.hitRate()).isOne();
        assertThat(snapshot.loadCount()).isEqualTo(2);
        assertThat(snapshot.loadFailureCount()).isOne();
        assertThat(snapshot.loadFailureRate()).isEqualTo(0.5, Offset.offset(0.01));
        assertThat(snapshot.loadSuccessCount()).isOne();
        assertThat(snapshot.missCount()).isZero();
        assertThat(snapshot.missRate()).isZero();
        assertThat(snapshot.requestCount()).isZero();
        assertThat(snapshot.totalLoadTime()).isGreaterThan(0);
    }

    @Test
    void testEvictionsWhenExceedingCacheCapacity()
    {
        for (int i = 0; i < 100; i++)
        {
            int finalI = i;
            assertThat(cache.get("foo" + i, k -> ("bar" + finalI))).isEqualTo("bar" + i);
        }

        // no hits, all are new entries
        assertThat(instance.hits.metric.getValue()).isZero();
        // misses and load successes should equal the number of loops
        assertThat(instance.misses.metric.getValue()).isEqualTo(100);
        assertThat(instance.loadSuccess.metric.getCount()).isEqualTo(100);
        // no failures
        assertThat(instance.loadFailure.metric.getCount()).isZero();
        // make sure the cache performs all necessary cleanups
        cache.cleanUp();
        LongAdder evictions = new LongAdder();
        // we only have capacity for 5, so 95 should be evicted
        // It might take some time for the cache maintainence tasks to finish. Use loop assert to check that it eventally evicts 95
        AssertionUtils.loopAssert(1, () -> {
            evictions.add(instance.evictions.metric.getValue());
            assertThat(evictions.sum()).isEqualTo(95);
        });
        assertThat(instance.totalLoadTimeNanos.longValue()).isGreaterThan(0);
    }

    @Test
    void testConcurrentStatsCounterUpdates() throws InterruptedException
    {
        // Let's get some concurrent updates to the cache
        int nThreads = 30;
        ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        CountDownLatch latch = new CountDownLatch(nThreads);
        for (int i = 0; i < nThreads; i++)
        {
            pool.submit(() -> {
                try
                {
                    // Invoke get roughly at the same time
                    latch.countDown();
                    latch.await();

                    assertThat(cache.get("foo", k -> "bar")).isEqualTo("bar");
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }

        pool.shutdown();
        assertThat(pool.awaitTermination(1, TimeUnit.MINUTES)).isTrue();

        // Let's make sure only a single cache entry was updated
        assertThat(cache.estimatedSize()).isEqualTo(1);

        assertThat(instance.hits.metric.getValue()).isEqualTo(nThreads - 1);
        assertThat(instance.misses.metric.getValue()).isOne();
        assertThat(instance.loadSuccess.metric.getCount()).isOne();
        assertThat(instance.loadFailure.metric.getCount()).isZero();
        assertThat(instance.evictions.metric.getValue()).isZero();
        assertThat(instance.totalLoadTimeNanos.longValue()).isGreaterThan(0);
    }
}
