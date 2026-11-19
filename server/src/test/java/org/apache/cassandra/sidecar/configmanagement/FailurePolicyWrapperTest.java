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
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.vertx.core.json.JsonObject;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link FailurePolicyWrapper}
 */
class FailurePolicyWrapperTest
{
    @TempDir
    Path tempDir;

    private InstanceMetadata instance;

    @BeforeEach
    void setUp()
    {
        instance = mockInstance(1);
    }

    // -- Factory method tests --

    @Test
    void testWrapReturnsUnwrappedFileBasedProvider()
    {
        FileBasedConfigurationProvider fileBased = new FileBasedConfigurationProvider(tempDir);
        ConfigurationProvider result = FailurePolicyWrapper.wrap(fileBased, tempDir, FailurePolicy.CACHED_READ_ONLY);
        assertThat(result).isSameAs(fileBased);
    }

    @Test
    void testWrapReturnsWrapperForNonFileBased()
    {
        InMemoryConfigurationProvider inMemory = new InMemoryConfigurationProvider();
        ConfigurationProvider result = FailurePolicyWrapper.wrap(inMemory, tempDir, FailurePolicy.CACHED_READ_ONLY);
        assertThat(result).isInstanceOf(FailurePolicyWrapper.class);
    }

    // -- Happy path tests (delegate works) --

    @Test
    void testGetOverlayDelegatesSuccessfully()
    {
        InMemoryConfigurationProvider delegate = new InMemoryConfigurationProvider();
        ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", 64);
        delegate.storeOverlay(instance, null, snapshot);

        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_ONLY);
        ConfigurationOverlaySnapshot result = wrapper.getOverlay(instance);

        assertThat(result).isNotNull();
        assertThat(result.configuration().cassandraYaml().getInteger("concurrent_reads")).isEqualTo(64);
    }

    @Test
    void testGetOverlayPopulatesCache()
    {
        InMemoryConfigurationProvider delegate = new InMemoryConfigurationProvider();
        ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", 64);
        delegate.storeOverlay(instance, null, snapshot);

        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_ONLY);
        wrapper.getOverlay(instance);

        Path cachedFile = tempDir.resolve("1").resolve(FailurePolicyWrapper.CACHED_OVERLAY_FILE_NAME);
        assertThat(cachedFile).isRegularFile();
    }

    @Test
    void testGetOverlaySkipsCacheWriteWhenContentUnchanged()
    {
        InMemoryConfigurationProvider delegate = new InMemoryConfigurationProvider();
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_ONLY);

        // First fetch populates the cache with a snapshot stamped at t1
        Instant t1 = Instant.parse("2020-01-01T00:00:00Z");
        ConfigurationOverlaySnapshot first = snapshotAt(t1, "concurrent_reads", 64);
        delegate.storeOverlay(instance, null, first);
        wrapper.getOverlay(instance);

        // Second fetch returns identical content (same hash) but a newer lastModified (t2)
        Instant t2 = Instant.parse("2021-06-15T12:00:00Z");
        ConfigurationOverlaySnapshot second = snapshotAt(t2, "concurrent_reads", 64);
        delegate.storeOverlay(instance, first.hash(), second);
        assertThat(second.hash()).isEqualTo(first.hash()); // hash is content-only, ignores lastModified
        wrapper.getOverlay(instance);

        // Because the content hash was unchanged, the cache write was skipped: the persisted snapshot
        // still carries t1 rather than being rewritten with t2.
        ConfigurationOverlaySnapshot cached =
                new FileBasedConfigurationProvider(tempDir, FailurePolicyWrapper.CACHED_OVERLAY_FILE_NAME)
                        .getOverlay(instance);
        assertThat(cached).isNotNull();
        assertThat(cached.lastModified()).isEqualTo(t1);
    }

    @Test
    void testGetOverlayReturnsNullWhenDelegateReturnsNull()
    {
        InMemoryConfigurationProvider delegate = new InMemoryConfigurationProvider();
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_ONLY);

        assertThat(wrapper.getOverlay(instance)).isNull();
    }

    @Test
    void testStoreOverlayDelegatesSuccessfully()
    {
        InMemoryConfigurationProvider delegate = new InMemoryConfigurationProvider();
        ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", 64);

        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_ONLY);
        boolean stored = wrapper.storeOverlay(instance, null, snapshot);

        assertThat(stored).isTrue();
        assertThat(delegate.getOverlay(instance)).isNotNull();
    }

    @Test
    void testStoreOverlayUpdatesCacheOnSuccess()
    {
        InMemoryConfigurationProvider delegate = new InMemoryConfigurationProvider();
        ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", 64);

        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_ONLY);
        wrapper.storeOverlay(instance, null, snapshot);

        Path cachedFile = tempDir.resolve("1").resolve(FailurePolicyWrapper.CACHED_OVERLAY_FILE_NAME);
        assertThat(cachedFile).isRegularFile();
    }

    @Test
    void testStoreOverlayDoesNotUpdateCacheOnConflict()
    {
        InMemoryConfigurationProvider delegate = new InMemoryConfigurationProvider();
        ConfigurationOverlaySnapshot initial = createSnapshot("concurrent_reads", 32);
        delegate.storeOverlay(instance, null, initial);

        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_ONLY);
        ConfigurationOverlaySnapshot update = createSnapshot("concurrent_reads", 64);

        boolean stored = wrapper.storeOverlay(instance, "sha256:stale", update);

        assertThat(stored).isFalse();
        assertThat(tempDir.resolve("1").resolve(FailurePolicyWrapper.CACHED_OVERLAY_FILE_NAME)).doesNotExist();
    }

    // -- FAIL policy tests --

    @Test
    void testGetOverlayFailPolicyThrowsProviderUnavailable()
    {
        ConfigurationProvider failing = failingProvider();
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(failing, tempDir, FailurePolicy.FAIL);

        assertThatThrownBy(() -> wrapper.getOverlay(instance))
                .isInstanceOf(ConfigurationProviderUnavailableException.class)
                .hasMessageContaining("FAIL policy")
                .hasCauseInstanceOf(UncheckedIOException.class);
    }

    @Test
    void testStoreOverlayFailPolicyThrowsProviderUnavailable()
    {
        ConfigurationProvider failing = failingProvider();
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(failing, tempDir, FailurePolicy.FAIL);
        ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", 64);

        assertThatThrownBy(() -> wrapper.storeOverlay(instance, null, snapshot))
                .isInstanceOf(ConfigurationProviderUnavailableException.class)
                .hasMessageContaining("FAIL policy")
                .hasCauseInstanceOf(UncheckedIOException.class);
    }

    // -- CACHED_READ_ONLY policy tests --

    @Test
    void testGetOverlayReadOnlyReturnsCachedWhenDelegateThrows()
    {
        ToggleableProvider delegate = new ToggleableProvider();
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_ONLY);

        ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", 64);
        delegate.storeOverlay(instance, null, snapshot);
        wrapper.getOverlay(instance);

        delegate.setFailing(true);
        ConfigurationOverlaySnapshot cached = wrapper.getOverlay(instance);

        assertThat(cached).isNotNull();
        assertThat(cached.configuration().cassandraYaml().getInteger("concurrent_reads")).isEqualTo(64);
    }

    @Test
    void testGetOverlayReadOnlyThrowsWhenNoCacheAndDelegateThrows()
    {
        ConfigurationProvider failing = failingProvider();
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(failing, tempDir, FailurePolicy.CACHED_READ_ONLY);

        assertThatThrownBy(() -> wrapper.getOverlay(instance))
                .isInstanceOf(ConfigurationProviderUnavailableException.class)
                .hasMessageContaining("no overlay is cached")
                .hasCauseInstanceOf(UncheckedIOException.class);
    }

    @Test
    void testStoreOverlayReadOnlyRejectsWriteWhenDelegateThrows()
    {
        ToggleableProvider delegate = new ToggleableProvider();
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_ONLY);

        delegate.setFailing(true);
        ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", 64);

        assertThatThrownBy(() -> wrapper.storeOverlay(instance, null, snapshot))
                .isInstanceOf(ConfigurationProviderUnavailableException.class)
                .hasMessageContaining("CACHED_READ_ONLY")
                .hasCauseInstanceOf(UncheckedIOException.class);
    }

    // -- CACHED_READ_WRITE policy tests --

    @Test
    void testGetOverlayReadWriteReturnsCachedWhenDelegateThrows()
    {
        ToggleableProvider delegate = new ToggleableProvider();
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_WRITE);

        ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", 64);
        delegate.storeOverlay(instance, null, snapshot);
        wrapper.getOverlay(instance);

        delegate.setFailing(true);
        ConfigurationOverlaySnapshot cached = wrapper.getOverlay(instance);

        assertThat(cached).isNotNull();
        assertThat(cached.configuration().cassandraYaml().getInteger("concurrent_reads")).isEqualTo(64);
    }

    @Test
    void testStoreOverlayReadWriteWritesToCacheWhenDelegateThrows()
    {
        ToggleableProvider delegate = new ToggleableProvider();
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_WRITE);

        delegate.setFailing(true);
        ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", 64);

        boolean stored = wrapper.storeOverlay(instance, null, snapshot);

        assertThat(stored).isTrue();
        Path cachedFile = tempDir.resolve("1").resolve(FailurePolicyWrapper.CACHED_OVERLAY_FILE_NAME);
        assertThat(cachedFile).isRegularFile();
    }

    @Test
    void testStoreOverlayReadWriteReturnsFalseOnCacheHashMismatch()
    {
        ToggleableProvider delegate = new ToggleableProvider();
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_WRITE);

        // Seed the cache with an initial value via a successful call
        ConfigurationOverlaySnapshot initial = createSnapshot("concurrent_reads", 32);
        delegate.storeOverlay(instance, null, initial);
        wrapper.getOverlay(instance);

        delegate.setFailing(true);
        ConfigurationOverlaySnapshot update = createSnapshot("concurrent_reads", 64);

        // originalHash doesn't match the cached overlay's hash
        boolean stored = wrapper.storeOverlay(instance, "sha256:stale", update);
        assertThat(stored).isFalse();
    }

    // -- Cache behavior across transitions --

    @Test
    void testCacheUpdatedAfterSuccessfulGetThenServesOnFailure()
    {
        ToggleableProvider delegate = new ToggleableProvider();
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_ONLY);

        // Store initial, fetch to populate cache
        ConfigurationOverlaySnapshot v1 = createSnapshot("concurrent_reads", 32);
        delegate.storeOverlay(instance, null, v1);
        wrapper.getOverlay(instance);

        // Update to v2, fetch to update cache
        ConfigurationOverlaySnapshot v2 = createSnapshot("concurrent_reads", 64);
        delegate.storeOverlay(instance, v1.hash(), v2);
        wrapper.getOverlay(instance);

        // Fail delegate, should get v2 from cache
        delegate.setFailing(true);
        ConfigurationOverlaySnapshot cached = wrapper.getOverlay(instance);

        assertThat(cached).isNotNull();
        assertThat(cached.configuration().cassandraYaml().getInteger("concurrent_reads")).isEqualTo(64);
    }

    @Test
    void testCacheUpdatedAfterSuccessfulStoreThenServesOnFailure()
    {
        ToggleableProvider delegate = new ToggleableProvider();
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_ONLY);

        ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", 128);
        wrapper.storeOverlay(instance, null, snapshot);

        delegate.setFailing(true);
        ConfigurationOverlaySnapshot cached = wrapper.getOverlay(instance);

        assertThat(cached).isNotNull();
        assertThat(cached.configuration().cassandraYaml().getInteger("concurrent_reads")).isEqualTo(128);
    }

    @Test
    void testCacheStoresEmptyOverlayWhenDelegateReturnsNull()
    {
        // A null delegate result means no overlay exists. The wrapper caches an empty overlay so a
        // deleted overlay is not resurrected from cache during a later outage.
        ControllableProvider delegate = new ControllableProvider();
        delegate.set(null);
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_ONLY);

        assertThat(wrapper.getOverlay(instance)).isNull();

        // An empty overlay is written to the cache
        Path cachedFile = tempDir.resolve("1").resolve(FailurePolicyWrapper.CACHED_OVERLAY_FILE_NAME);
        assertThat(cachedFile).isRegularFile();
        ConfigurationOverlaySnapshot cached =
                new FileBasedConfigurationProvider(tempDir, FailurePolicyWrapper.CACHED_OVERLAY_FILE_NAME)
                        .getOverlay(instance);
        assertThat(cached).isNotNull();
        assertThat(cached.configuration().cassandraYaml()).isEqualTo(new JsonObject());
        assertThat(cached.configuration().extraJvmOpts()).isEmpty();
    }

    @Test
    void testDeletedUpstreamOverlayNotResurrectedFromCacheDuringOutage()
    {
        ControllableProvider delegate = new ControllableProvider();
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_ONLY);

        // Seed the cache with a real overlay via a successful read
        delegate.set(createSnapshot("concurrent_reads", 64));
        assertThat(wrapper.getOverlay(instance)).isNotNull();

        // Overlay is deleted upstream: a successful read now returns null and overwrites the cache
        delegate.set(null);
        assertThat(wrapper.getOverlay(instance)).isNull();

        // During a later outage, the cache serves the empty overlay, not the stale deleted value
        delegate.setFailing(true);
        ConfigurationOverlaySnapshot cached = wrapper.getOverlay(instance);
        assertThat(cached).isNotNull();
        assertThat(cached.configuration().cassandraYaml().getInteger("concurrent_reads")).isNull();
    }

    @Test
    void testStoreOverlayReadWriteWithExtraJvmOpts()
    {
        ToggleableProvider delegate = new ToggleableProvider();
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_WRITE);

        delegate.setFailing(true);

        JsonObject yaml = new JsonObject().put("concurrent_reads", 64);
        Map<String, String> jvmOpts = new LinkedHashMap<>();
        jvmOpts.put("-Xmx", "4G");
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yaml, jvmOpts);
        ConfigurationOverlaySnapshot snapshot = new ConfigurationOverlaySnapshot(Instant.now(), overlay);

        boolean stored = wrapper.storeOverlay(instance, null, snapshot);
        assertThat(stored).isTrue();

        ConfigurationOverlaySnapshot cached = wrapper.getOverlay(instance);
        assertThat(cached).isNotNull();
        assertThat(cached.configuration().extraJvmOpts()).containsEntry("-Xmx", "4G");
        assertThat(cached.configuration().cassandraYaml().getInteger("concurrent_reads")).isEqualTo(64);
    }

    // -- Failure classification tests --

    @Test
    void testGetOverlayPropagatesRejectionInsteadOfFallingBackToCache()
    {
        ControllableProvider delegate = new ControllableProvider();
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_ONLY);

        // Seed the cache with a successful read so a fallback would have something to return
        delegate.set(createSnapshot("concurrent_reads", 64));
        assertThat(wrapper.getOverlay(instance)).isNotNull();

        delegate.setFailure(new IllegalArgumentException("unsupported instance"));

        assertThatThrownBy(() -> wrapper.getOverlay(instance))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("unsupported instance");
    }

    @Test
    void testGetOverlayFailPolicyPropagatesRejectionUnwrapped()
    {
        // A rejection must not be relabeled as an outage, which would surface as a 503
        ControllableProvider delegate = new ControllableProvider();
        delegate.setFailure(new IllegalArgumentException("unsupported instance"));
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.FAIL);

        assertThatThrownBy(() -> wrapper.getOverlay(instance))
                .isInstanceOf(IllegalArgumentException.class)
                .isNotInstanceOf(ConfigurationProviderUnavailableException.class);
    }

    @Test
    void testStoreOverlayReadWriteDoesNotCacheRejectedWrite()
    {
        ControllableProvider delegate = new ControllableProvider();
        FailurePolicyWrapper wrapper = new FailurePolicyWrapper(delegate, tempDir, FailurePolicy.CACHED_READ_WRITE);

        delegate.setFailure(new IllegalArgumentException("invalid overlay"));
        ConfigurationOverlaySnapshot snapshot = createSnapshot("concurrent_reads", 64);

        assertThatThrownBy(() -> wrapper.storeOverlay(instance, null, snapshot))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid overlay");

        // The rejected snapshot was neither persisted nor reported as stored
        assertThat(tempDir.resolve("1").resolve(FailurePolicyWrapper.CACHED_OVERLAY_FILE_NAME)).doesNotExist();
    }

    @Test
    void testIsUnavailableClassification()
    {
        assertThat(FailurePolicyWrapper.isUnavailable(new UncheckedIOException(new IOException("down")))).isTrue();
        assertThat(FailurePolicyWrapper.isUnavailable(new ProviderClientException("down"))).isTrue();
        assertThat(FailurePolicyWrapper.isUnavailable(
                new ConfigurationProviderUnavailableException("down", null))).isTrue();

        assertThat(FailurePolicyWrapper.isUnavailable(new IllegalArgumentException("bad"))).isFalse();
        assertThat(FailurePolicyWrapper.isUnavailable(new NullPointerException())).isFalse();
        assertThat(FailurePolicyWrapper.isUnavailable(new IllegalStateException("bug"))).isFalse();
        assertThat(FailurePolicyWrapper.isUnavailable(new UnsupportedOperationException("nope"))).isFalse();
        assertThat(FailurePolicyWrapper.isUnavailable(
                new ConfigurationConflictException("sha256:a", "sha256:b"))).isFalse();
        assertThat(FailurePolicyWrapper.isUnavailable(
                new ConfigurationPatchException("bad patch", null))).isFalse();
    }

    // -- Helpers --

    private static ConfigurationOverlaySnapshot createSnapshot(String field, int value)
    {
        JsonObject yaml = new JsonObject().put(field, value);
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yaml, null);
        return new ConfigurationOverlaySnapshot(Instant.now(), overlay);
    }

    private static ConfigurationOverlaySnapshot snapshotAt(Instant lastModified, String field, int value)
    {
        JsonObject yaml = new JsonObject().put(field, value);
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yaml, null);
        return new ConfigurationOverlaySnapshot(lastModified, overlay);
    }

    private static InstanceMetadata mockInstance(int id)
    {
        InstanceMetadata inst = mock(InstanceMetadata.class);
        when(inst.id()).thenReturn(id);
        return inst;
    }

    private static ConfigurationProvider failingProvider()
    {
        return new ConfigurationProvider()
        {
            @Override
            public ConfigurationOverlaySnapshot getOverlay(InstanceMetadata instance)
            {
                throw new UncheckedIOException(new IOException("provider unavailable"));
            }

            @Override
            public boolean storeOverlay(InstanceMetadata instance, String originalHash,
                                        ConfigurationOverlaySnapshot newSnapshot)
            {
                throw new UncheckedIOException(new IOException("provider unavailable"));
            }
        };
    }

    /**
     * A provider backed by {@link InMemoryConfigurationProvider} that can be toggled to fail.
     */
    private static class ToggleableProvider implements ConfigurationProvider
    {
        private final InMemoryConfigurationProvider inner = new InMemoryConfigurationProvider();
        private final AtomicBoolean failing = new AtomicBoolean(false);

        void setFailing(boolean fail)
        {
            failing.set(fail);
        }

        @Override
        public ConfigurationOverlaySnapshot getOverlay(InstanceMetadata instance)
        {
            if (failing.get())
            {
                throw new UncheckedIOException(new IOException("provider unavailable"));
            }
            return inner.getOverlay(instance);
        }

        @Override
        public boolean storeOverlay(InstanceMetadata instance, String originalHash,
                                    ConfigurationOverlaySnapshot newSnapshot)
        {
            if (failing.get())
            {
                throw new UncheckedIOException(new IOException("provider unavailable"));
            }
            return inner.storeOverlay(instance, originalHash, newSnapshot);
        }
    }

    /**
     * A provider whose current overlay can be set directly (including to {@code null} to simulate an
     * upstream deletion) and which can be toggled to fail, for testing cache invalidation behavior.
     */
    private static class ControllableProvider implements ConfigurationProvider
    {
        private volatile ConfigurationOverlaySnapshot current;
        private volatile RuntimeException failure;

        void set(ConfigurationOverlaySnapshot snapshot)
        {
            this.current = snapshot;
        }

        void setFailing(boolean fail)
        {
            this.failure = fail ? new UncheckedIOException(new IOException("provider unavailable")) : null;
        }

        void setFailure(RuntimeException failure)
        {
            this.failure = failure;
        }

        @Override
        public ConfigurationOverlaySnapshot getOverlay(InstanceMetadata instance)
        {
            if (failure != null)
            {
                throw failure;
            }
            return current;
        }

        @Override
        public boolean storeOverlay(InstanceMetadata instance, String originalHash,
                                    ConfigurationOverlaySnapshot newSnapshot)
        {
            if (failure != null)
            {
                throw failure;
            }
            this.current = newSnapshot;
            return true;
        }
    }

    /**
     * Stands in for a remote provider's own client exception type, which the wrapper cannot enumerate.
     */
    private static class ProviderClientException extends RuntimeException
    {
        ProviderClientException(String message)
        {
            super(message);
        }
    }
}
