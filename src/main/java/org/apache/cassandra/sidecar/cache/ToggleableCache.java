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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link Cache} where methods that take a mapping {@link Function} as a parameter can be toggled
 * (enabled / disabled) such that when the cache is disabled, the {@link Function} will be called
 * directly without retrieving cached values for any {@code key}, and when the cache is enabled the
 * behavior remains the same as the provided behavior by the {@code delegate}.
 *
 * <p>When the cache is toggled to disabled, existing entries in the cache will <b>NOT</b> be
 * removed, and if the cache is re-enabled and entries have not expired or have not been explicitly
 * removed; the cache will serve the existing data from the {@code delegate} cache.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values
 */
public class ToggleableCache<K, V> implements Cache<K, V>
{
    private final Cache<K, V> delegate;
    private final BooleanSupplier cacheEnabledSupplier;

    public ToggleableCache(Cache<K, V> delegate, BooleanSupplier cacheEnabledSupplier)
    {
        this.delegate = Objects.requireNonNull(delegate, "delegate is required");
        this.cacheEnabledSupplier = Objects.requireNonNull(cacheEnabledSupplier, "toggleSupplier is required");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nullable
    public V getIfPresent(@NonNull Object key)
    {
        return delegate.getIfPresent(key);
    }

    /**
     * When the cache is enabled, returns the value associated with the {@code key} in this cache,
     * obtaining that value from the {@code mappingFunction} if necessary. When the cache is disabled,
     * it will always obtain the value from the {@code mappingFunction}.
     *
     * @param key             the key with which the specified value is to be associated
     * @param mappingFunction the function to compute a value
     * @return when the cache is enabled the current (existing or computed) value associated with the
     * specified key, or null if the computed value is null. If the cache is disabled, the computed
     * value from the mapping function.
     * @throws NullPointerException  if the specified key or mappingFunction is null
     * @throws IllegalStateException if the computation detectably attempts a recursive update to this
     *                               cache that would otherwise never complete (when the cache is enabled)
     * @throws RuntimeException      or Error if the mappingFunction does so, in which case if the cache
     *                               is enabled the mapping is left unestablished
     */
    @Override
    @Nullable
    public V get(@NonNull K key, @NonNull Function<? super K, ? extends V> mappingFunction)
    {
        if (cacheEnabledSupplier.getAsBoolean())
        {
            return delegate.get(key, mappingFunction);
        }
        return Objects.requireNonNull(mappingFunction, "mappingFunction is required").apply(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public Map<@NonNull K, @NonNull V> getAllPresent(@NonNull Iterable<@NonNull ?> keys)
    {
        return delegate.getAllPresent(keys);
    }

    /**
     * When the cache is enabled, returns a map of the values associated with the {@code keys},
     * creating or retrieving those values if necessary. The returned map contains entries that were
     * already cached, combined with the newly loaded entries; it will never contain null keys or
     * values.
     * <p>
     * If the cache is disabled, it will always obtain the value from the {@code mappingFunction}.
     *
     * @param keys            the keys whose associated values are to be returned
     * @param mappingFunction the function to compute the values
     * @return an unmodifiable mapping of keys to values for the specified keys in this cache, or
     * the computed value of the mappingFunction when the cache is disabled
     * @throws NullPointerException if the specified collection is null or contains a null element, or
     *                              if the map returned by the mappingFunction is null
     * @throws RuntimeException     or Error if the mappingFunction does so, in which case the mapping is
     *                              left unestablished
     */
    @Override
    @NonNull
    public Map<K, V> getAll(@NonNull Iterable<? extends @NonNull K> keys,
                            @NonNull Function<Iterable<? extends @NonNull K>, @NonNull Map<K, V>> mappingFunction)
    {
        if (cacheEnabledSupplier.getAsBoolean())
        {
            return delegate.getAll(keys, mappingFunction);
        }
        return Objects.requireNonNull(mappingFunction, "mappingFunction is required").apply(keys);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(@NonNull K key, @NonNull V value)
    {
        delegate.put(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(@NonNull Map<? extends @NonNull K, ? extends @NonNull V> map)
    {
        delegate.putAll(map);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void invalidate(@NonNull Object key)
    {
        delegate.invalidate(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void invalidateAll(@NonNull Iterable<@NonNull ?> keys)
    {
        delegate.invalidateAll(keys);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void invalidateAll()
    {
        delegate.invalidateAll();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNegative
    public long estimatedSize()
    {
        return delegate.estimatedSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public CacheStats stats()
    {
        return delegate.stats();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public ConcurrentMap<@NonNull K, @NonNull V> asMap()
    {
        return delegate.asMap();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanUp()
    {
        delegate.cleanUp();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public Policy<K, V> policy()
    {
        return delegate.policy();
    }
}
