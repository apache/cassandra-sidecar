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

package org.apache.cassandra.sidecar.cache;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link ToggleableCache}
 */
class ToggleableCacheTest
{
    @SuppressWarnings("unchecked")
    @ParameterizedTest(name = "cacheEnabled={0}")
    @ValueSource(booleans = { true, false })
    void testDelegation(boolean cacheEnabled)
    {
        Cache<String, String> delegate = mock(Cache.class);
        ToggleableCache<String, String> cache = new ToggleableCache<>(delegate, () -> cacheEnabled);

        List<String> keys = Collections.singletonList("key");
        Map<String, String> map = Collections.singletonMap("key", "value");
        Function<String, String> mappingFunction = key -> key;
        Function<Iterable<? extends String>, Map<String, String>> iterableMapFunction = strings ->
                                                                                        Collections.emptyMap();

        // methods where the cache toggle takes effect
        cache.get("key", mappingFunction);
        cache.getAll(keys, iterableMapFunction);

        if (cacheEnabled)
        {
            verify(delegate, times(1)).get("key", mappingFunction);
            verify(delegate, times(1)).getAll(keys, iterableMapFunction);
        }
        else
        {
            verify(delegate, never()).get("key", mappingFunction);
            verify(delegate, never()).getAll(keys, iterableMapFunction);
        }

        // methods where the cache toggle does not take any effect

        cache.getIfPresent("key");
        verify(delegate, times(1)).getIfPresent("key");

        cache.getAllPresent(keys);
        verify(delegate, times(1)).getAllPresent(keys);

        cache.put("key", "value");
        verify(delegate, times(1)).put("key", "value");

        cache.putAll(map);
        verify(delegate, times(1)).putAll(map);

        cache.invalidate("key");
        verify(delegate, times(1)).invalidate("key");

        cache.invalidateAll(keys);
        verify(delegate, times(1)).invalidateAll(keys);

        cache.invalidateAll();
        verify(delegate, times(1)).invalidateAll();

        cache.estimatedSize();
        verify(delegate, times(1)).estimatedSize();

        cache.stats();
        verify(delegate, times(1)).stats();

        cache.asMap();
        verify(delegate, times(1)).asMap();

        cache.cleanUp();
        verify(delegate, times(1)).cleanUp();

        cache.policy();
        verify(delegate, times(1)).policy();
    }

    @Test
    void testGet()
    {
        AtomicInteger value = new AtomicInteger(0);
        AtomicBoolean cacheEnabled = new AtomicBoolean(true);
        Function<String, String> mappingFunction = key -> String.valueOf(value.incrementAndGet());

        ToggleableCache<String, String> cache = new ToggleableCache<>(Caffeine.newBuilder().build(), cacheEnabled::get);

        // Start with the cache enabled
        assertThat(cache.get("key", mappingFunction)).isEqualTo("1");
        assertThat(value.get()).isEqualTo(1);
        // retrieving the value from the cache should not call the mapping function
        assertThat(cache.get("key", mappingFunction)).isEqualTo("1");
        assertThat(value.get()).isEqualTo(1);
        // now let's disable the cache mapping function should not be called
        cacheEnabled.set(false);
        assertThat(cache.get("key", mappingFunction)).isEqualTo("2");
        assertThat(cache.get("key", mappingFunction)).isEqualTo("3");
        assertThat(cache.get("key-is-irrelevant-when-cache-is-disabled", mappingFunction)).isEqualTo("4");

        // if we re-enable the cache, we should see data associated with the key in the past
        cacheEnabled.set(true);
        assertThat(cache.get("key", mappingFunction)).isEqualTo("1");

        // let's use a different key, we should see the value change
        assertThat(cache.get("other-key", mappingFunction)).isEqualTo("5");
        assertThat(cache.get("other-key", mappingFunction)).isEqualTo("5");

        // the key "key-is-irrelevant-when-cache-is-disabled" was never associated to the cache
        // because the cache was disabled while being accessed, so the effect when the cache is
        // enabled is to associate it for the first time in the cache
        assertThat(cache.get("key-is-irrelevant-when-cache-is-disabled", mappingFunction)).isEqualTo("6");
        assertThat(cache.get("key-is-irrelevant-when-cache-is-disabled", mappingFunction)).isEqualTo("6");
    }

    @Test
    void testGetAll()
    {
        AtomicInteger value = new AtomicInteger(0);
        AtomicBoolean cacheEnabled = new AtomicBoolean(true);

        List<String> keys1 = Arrays.asList("key1", "key2", "key3");

        Function<Iterable<? extends String>, Map<String, String>> mappingFunction = k -> {
            Map<String, String> map = new HashMap<>();
            int i = value.incrementAndGet();
            k.forEach(key -> map.put(key, key + "-value" + i));
            return map;
        };

        ToggleableCache<String, String> cache = new ToggleableCache<>(Caffeine.newBuilder().build(), cacheEnabled::get);

        // Start with the cache enabled
        assertThat(cache.getAll(keys1, mappingFunction)).hasSize(3)
                                                        .containsValues("key1-value1", "key2-value1", "key3-value1");
        assertThat(value.get()).isEqualTo(1);
        // retrieve the values for the same keys should not call the mapping function
        assertThat(cache.getAll(keys1, mappingFunction)).hasSize(3)
                                                        .containsValues("key1-value1", "key2-value1", "key3-value1");
        assertThat(value.get()).isEqualTo(1);

        // disabling the cache should change the values in the returned map
        cacheEnabled.set(false);
        assertThat(cache.getAll(keys1, mappingFunction)).hasSize(3)
                                                        .containsValues("key1-value2", "key2-value2", "key3-value2");
        assertThat(cache.getAll(keys1, mappingFunction)).hasSize(3)
                                                        .containsValues("key1-value3", "key2-value3", "key3-value3");
        assertThat(cache.getAll(keys1, mappingFunction)).hasSize(3)
                                                        .containsValues("key1-value4", "key2-value4", "key3-value4");
        assertThat(value.get()).isEqualTo(4);
    }
}
