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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.testing.FakeTicker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.Cache;
import io.vertx.core.Future;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.SSTableImportConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.yaml.CacheConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SSTableImportConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.TestServiceConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for the {@link CacheFactory} class
 */
class CacheFactoryTest
{
    static final long SSTABLE_IMPORT_EXPIRE_AFTER_ACCESS_MILLIS = TimeUnit.HOURS.toMillis(2);
    static final long SSTABLE_IMPORT_CACHE_MAX_SIZE = 10L;
    private CacheFactory cacheFactory;
    private FakeTicker fakeTicker;

    @BeforeEach
    void setup()
    {
        fakeTicker = new FakeTicker();

        CacheConfiguration ssTableImportCacheConfiguration =
        new CacheConfigurationImpl(SSTABLE_IMPORT_EXPIRE_AFTER_ACCESS_MILLIS, // 2 hours
                                   SSTABLE_IMPORT_CACHE_MAX_SIZE);

        SSTableImportConfiguration ssTableImportConfiguration =
        new SSTableImportConfigurationImpl(ssTableImportCacheConfiguration);
        ServiceConfiguration serviceConfiguration =
        TestServiceConfiguration.builder()
                                .ssTableImportConfiguration(ssTableImportConfiguration)
                                .build();
        SSTableImporter mockSSTableImporter = mock(SSTableImporter.class);
        cacheFactory = new CacheFactory(serviceConfiguration, mockSSTableImporter, fakeTicker::read);
    }

    @Test
    void testSSTableImportCacheExpiration() throws ExecutionException, InterruptedException
    {
        Cache<SSTableImporter.ImportOptions, Future<Void>> cache = cacheFactory.ssTableImportCache();
        SSTableImporter.ImportOptions type1Options1 = buildImportOptions("ks", "tbl", "uuid");
        SSTableImporter.ImportOptions type1Options2 = buildImportOptions("ks", "tbl", "uuid");
        SSTableImporter.ImportOptions type1Options3 = buildImportOptions("ks", "tbl", "uuid");
        SSTableImporter.ImportOptions type1Options4 = buildImportOptions("ks", "tbl", "uuid");
        SSTableImporter.ImportOptions type1Options5 = buildImportOptions("ks", "tbl", "uuid");
        SSTableImporter.ImportOptions type2Options1 = buildImportOptions("ks2", "tbl2", "uuid");

        Void result1 = ssTableImportCacheEntry(cache, type1Options1, mock(Void.class));
        Void type2Result1 = ssTableImportCacheEntry(cache, type2Options1, mock(Void.class));

        assertThat(result1).isNotNull()
                           .isNotSameAs(type2Result1);

        // advance ticker 1 minute
        fakeTicker.advance(1, TimeUnit.MINUTES);

        // should get the same instance, type1Options2.equals(type1Options1)
        Void result2 = ssTableImportCacheEntry(cache, type1Options2, mock(Void.class));
        assertThat(result2).isSameAs(result1); // same instance

        // advance ticker 1 hour
        fakeTicker.advance(1, TimeUnit.HOURS);

        // should still get the same instance
        Void result3 = ssTableImportCacheEntry(cache, type1Options3, mock(Void.class));
        assertThat(result3).isSameAs(result1);

        // advance ticker 1 hour and 59 minutes and 59 seconds
        fakeTicker.advance(1, TimeUnit.HOURS);
        fakeTicker.advance(59, TimeUnit.MINUTES);
        fakeTicker.advance(59, TimeUnit.SECONDS);

        // should still get the same instance
        Void result4 = ssTableImportCacheEntry(cache, type1Options4, mock(Void.class));
        assertThat(result4).isSameAs(result1);

        // advance ticker for 2 hours
        fakeTicker.advance(2, TimeUnit.HOURS);

        // should get a different instance
        Void result5 = ssTableImportCacheEntry(cache, type1Options5, mock(Void.class));
        assertThat(result5).isNotSameAs(result1);
    }

    @Test
    void testSSTableImportCacheLimit() throws ExecutionException, InterruptedException
    {
        Cache<SSTableImporter.ImportOptions, Future<Void>> cache = cacheFactory.ssTableImportCache();
        cache.invalidateAll(); // make sure our cache is emptied out before testing
        int n = (int) SSTABLE_IMPORT_CACHE_MAX_SIZE * 2;
        for (int i = 0; i < n; i++)
        {
            Void mockVoid = mock(Void.class);
            SSTableImporter.ImportOptions importOptions = buildImportOptions("ks" + i, "tbl" + i, "uuid" + i);
            Void result = ssTableImportCacheEntry(cache, importOptions, mockVoid);
            assertThat(result).isNotNull();
        }
        assertThat(cache.estimatedSize()).isLessThanOrEqualTo(SSTABLE_IMPORT_CACHE_MAX_SIZE);
    }

    @Test
    void testConcurrentThreadsAccessingSameKey() throws InterruptedException, ExecutionException
    {
        Cache<SSTableImporter.ImportOptions, Future<Void>> cache = cacheFactory.ssTableImportCache();
        final int nThreads = 20;
        final ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        final Void[] voidArray = new Void[nThreads];
        final CountDownLatch latch = new CountDownLatch(nThreads);

        for (int i = 0; i < nThreads; i++)
        {
            final int finalI = i;
            pool.submit(() -> {
                try
                {
                    // Invoke getDirectory roughly at the same time
                    latch.countDown();
                    latch.await();
                    SSTableImporter.ImportOptions importOptions = buildImportOptions("ks", "tbl", "uuid");
                    // The first thread to win creates the object, the rest should get the same instance
                    voidArray[finalI] = ssTableImportCacheEntry(cache, importOptions, mock(Void.class));
                    fakeTicker.advance(1, TimeUnit.MINUTES);
                }
                catch (InterruptedException | ExecutionException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }

        pool.shutdown();
        assertThat(pool.awaitTermination(1, TimeUnit.MINUTES)).isTrue();

        for (int i = 1; i < nThreads; i++)
        {
            assertThat(voidArray[i]).isSameAs(voidArray[0]);
        }

        // advance ticker for 4 hours
        fakeTicker.advance(4, TimeUnit.HOURS);
        SSTableImporter.ImportOptions importOptions = buildImportOptions("ks", "tbl", "uuid");
        assertThat(ssTableImportCacheEntry(cache, importOptions, mock(Void.class))).isNotSameAs(voidArray[0]);
    }

    private Void ssTableImportCacheEntry(Cache<SSTableImporter.ImportOptions, Future<Void>> cache,
                                         SSTableImporter.ImportOptions key, Void value)
    throws ExecutionException, InterruptedException
    {
        Future<Void> voidFuture = cache.get(key, k -> Future.succeededFuture(value));
        assertThat(voidFuture).isNotNull();
        return voidFuture.toCompletionStage().toCompletableFuture().get();
    }

    private static SSTableImporter.ImportOptions buildImportOptions(String keyspace, String tableName, String uuid)
    {
        return new SSTableImporter.ImportOptions.Builder()
               .keyspace(keyspace)
               .tableName(tableName)
               .directory("/tmp/" + uuid)
               .uploadId(uuid)
               .host("localhost")
               .build();
    }
}
