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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.vertx.core.json.JsonObject;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link FileBasedConfigurationProvider}
 */
class FileBasedConfigurationProviderTest
{
    @TempDir
    Path tempDir;

    private FileBasedConfigurationProvider provider;
    private InstanceMetadata instance1;
    private InstanceMetadata instance2;

    @BeforeEach
    void setUp()
    {
        provider = new FileBasedConfigurationProvider(tempDir);
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
        assertThat(fetched).isNotNull();
        assertThat(fetched.configuration()).isEqualTo(snapshot.configuration());
        assertThat(fetched.lastModified()).isEqualTo(snapshot.lastModified());
        assertThat(fetched.hash()).isEqualTo(snapshot.hash());
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
        ConfigurationOverlaySnapshot fetched = provider.getOverlay(instance1);
        assertThat(fetched.configuration()).isEqualTo(initial.configuration());
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
        ConfigurationOverlaySnapshot fetched = provider.getOverlay(instance1);
        assertThat(fetched.configuration()).isEqualTo(initial.configuration());
    }

    @Test
    void testInstanceIsolation()
    {
        ConfigurationOverlaySnapshot snap1 = createSnapshot("concurrent_reads", 32);
        ConfigurationOverlaySnapshot snap2 = createSnapshot("concurrent_reads", 64);

        provider.storeOverlay(instance1, null, snap1);
        provider.storeOverlay(instance2, null, snap2);

        ConfigurationOverlaySnapshot fetched1 = provider.getOverlay(instance1);
        ConfigurationOverlaySnapshot fetched2 = provider.getOverlay(instance2);
        assertThat(fetched1.configuration()).isEqualTo(snap1.configuration());
        assertThat(fetched2.configuration()).isEqualTo(snap2.configuration());
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

    @Test
    void testPersistenceAcrossProviderInstances()
    {
        ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", 64);
        provider.storeOverlay(instance1, null, snapshot);

        FileBasedConfigurationProvider newProvider = new FileBasedConfigurationProvider(tempDir);
        ConfigurationOverlaySnapshot fetched = newProvider.getOverlay(instance1);

        assertThat(fetched).isNotNull();
        assertThat(fetched.configuration()).isEqualTo(snapshot.configuration());
        assertThat(fetched.lastModified()).isEqualTo(snapshot.lastModified());
        assertThat(fetched.hash()).isEqualTo(snapshot.hash());
    }

    @Test
    void testCreatesDirectoryStructure()
    {
        ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", 64);
        provider.storeOverlay(instance1, null, snapshot);

        Path instanceDir = tempDir.resolve("1");
        assertThat(instanceDir).isDirectory();
        assertThat(instanceDir.resolve("overlay.json")).isRegularFile();
    }

    @Test
    void testAtomicWriteNoPartialFile() throws IOException
    {
        ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", 64);
        provider.storeOverlay(instance1, null, snapshot);

        Path instanceDir = tempDir.resolve("1");
        try (Stream<Path> files = Files.list(instanceDir))
        {
            List<String> fileNames = files.map(p -> p.getFileName().toString()).sorted().collect(Collectors.toList());
            assertThat(fileNames).containsExactly("overlay.json");
        }
    }

    @Test
    void testOverwriteExistingOverlay()
    {
        ConfigurationOverlaySnapshot initial = createSnapshot("concurrent_reads", 32);
        provider.storeOverlay(instance1, null, initial);
        String hash = initial.hash();

        ConfigurationOverlaySnapshot updated = createSnapshot("concurrent_reads", 128);
        assertThat(provider.storeOverlay(instance1, hash, updated)).isTrue();

        ConfigurationOverlaySnapshot fetched = provider.getOverlay(instance1);
        assertThat(fetched.configuration()).isEqualTo(updated.configuration());
        assertThat(fetched.hash()).isEqualTo(updated.hash());
    }

    @Test
    void testStoreWithExtraJvmOpts()
    {
        JsonObject yaml = new JsonObject().put("concurrent_reads", 64);
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yaml,
                                                                                  Collections.singletonMap("-Xmx", "4G"));
        ConfigurationOverlaySnapshot snapshot = new ConfigurationOverlaySnapshot(Instant.now(), overlay);

        provider.storeOverlay(instance1, null, snapshot);

        ConfigurationOverlaySnapshot fetched = provider.getOverlay(instance1);
        assertThat(fetched).isNotNull();
        assertThat(fetched.configuration().extraJvmOpts()).containsEntry("-Xmx", "4G");
        assertThat(fetched.configuration().cassandraYaml().getInteger("concurrent_reads")).isEqualTo(64);
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
