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

package org.apache.cassandra.sidecar.configmanagement;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link InMemoryConfigurationProvider}
 */
class InMemoryConfigurationProviderTest
{
    private InMemoryConfigurationProvider provider;
    private InstanceMetadata instance1;
    private InstanceMetadata instance2;

    @BeforeEach
    void setUp()
    {
        provider = new InMemoryConfigurationProvider();
        instance1 = mockInstance(1);
        instance2 = mockInstance(2);
    }

    @Test
    void testGetReturnsNullForUnknownInstance()
    {
        assertThat(provider.getOverlay(instance1)).isNull();
    }

    @Test
    void testStoreAndGet()
    {
        ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", 64);

        boolean stored = provider.storeOverlay(instance1, null, snapshot);

        assertThat(stored).isTrue();
        ConfigurationOverlaySnapshot fetched = provider.getOverlay(instance1);
        assertThat(fetched).isSameAs(snapshot);
    }

    @Test
    void testStoreReturnsTrueOnSuccess()
    {
        ConfigurationOverlaySnapshot snapshot = createSnapshot("memtable_flush_writers", 8);

        assertThat(provider.storeOverlay(instance1, null, snapshot)).isTrue();
    }

    @Test
    void testStoreReturnsFalseOnHashMismatch()
    {
        ConfigurationOverlaySnapshot initial = createSnapshot("concurrent_reads", 32);
        provider.storeOverlay(instance1, null, initial);

        ConfigurationOverlaySnapshot update = createSnapshot("concurrent_reads", 64);
        assertThat(provider.storeOverlay(instance1, "sha256:stale", update)).isFalse();

        // Original is preserved
        assertThat(provider.getOverlay(instance1)).isSameAs(initial);
    }

    @Test
    void testStoreReturnsFalseWhenNoOverlayButHashProvided()
    {
        ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", 32);
        assertThat(provider.storeOverlay(instance1, "sha256:unexpected", snapshot)).isFalse();
        assertThat(provider.getOverlay(instance1)).isNull();
    }

    @Test
    void testStoreReturnsFalseWhenOverlayExistsButNullHashProvided()
    {
        ConfigurationOverlaySnapshot initial = createSnapshot("concurrent_reads", 32);
        provider.storeOverlay(instance1, null, initial);

        ConfigurationOverlaySnapshot update = createSnapshot("concurrent_reads", 64);
        assertThat(provider.storeOverlay(instance1, null, update)).isFalse();

        // Original is preserved
        assertThat(provider.getOverlay(instance1)).isSameAs(initial);
    }

    @Test
    void testInstanceIsolation()
    {
        ConfigurationOverlaySnapshot snap1 = createSnapshot("concurrent_reads", 32);
        ConfigurationOverlaySnapshot snap2 = createSnapshot("concurrent_reads", 64);

        provider.storeOverlay(instance1, null, snap1);
        provider.storeOverlay(instance2, null, snap2);

        assertThat(provider.getOverlay(instance1)).isSameAs(snap1);
        assertThat(provider.getOverlay(instance2)).isSameAs(snap2);
    }

    @Test
    void testConcurrentStoresDifferentInstances() throws Exception
    {
        int instanceCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(instanceCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<>();

        for (int i = 0; i < instanceCount; i++)
        {
            int instanceId = i;
            futures.add(executor.submit(() ->
            {
                startLatch.await();
                InstanceMetadata instance = mockInstance(instanceId);
                ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", instanceId * 10);
                return provider.storeOverlay(instance, null, snapshot);
            }));
        }

        startLatch.countDown();
        for (Future<Boolean> future : futures)
        {
            assertThat(future.get(5, TimeUnit.SECONDS)).isTrue();
        }

        for (int i = 0; i < instanceCount; i++)
        {
            assertThat(provider.getOverlay(mockInstance(i))).isNotNull();
        }

        executor.shutdown();
    }

    @Test
    void testConcurrentStoresSameInstance() throws Exception
    {
        ConfigurationOverlaySnapshot initial = createSnapshot("concurrent_reads", 32);
        provider.storeOverlay(instance1, null, initial);
        String hashBeforeRace = initial.hash();

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger successes = new AtomicInteger(0);
        AtomicInteger conflicts = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++)
        {
            int value = (i + 1) * 100;
            futures.add(executor.submit(() ->
            {
                try
                {
                    startLatch.await();
                    ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", value);
                    boolean stored = provider.storeOverlay(instance1, hashBeforeRace, snapshot);
                    if (stored)
                    {
                        successes.incrementAndGet();
                    }
                    else
                    {
                        conflicts.incrementAndGet();
                    }
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        startLatch.countDown();
        for (Future<?> future : futures)
        {
            future.get(5, TimeUnit.SECONDS);
        }

        assertThat(successes.get()).isEqualTo(1);
        assertThat(conflicts.get()).isEqualTo(threadCount - 1);

        executor.shutdown();
    }

    private static ConfigurationOverlaySnapshot createSnapshot(String field, int value)
    {
        JsonObject yaml = new JsonObject().put(field, value);
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yaml, null);
        return new ConfigurationOverlaySnapshot(Instant.now(), overlay);
    }

    private static InstanceMetadata mockInstance(int id)
    {
        InstanceMetadata instance = mock(InstanceMetadata.class);
        when(instance.id()).thenReturn(id);
        return instance;
    }
}
