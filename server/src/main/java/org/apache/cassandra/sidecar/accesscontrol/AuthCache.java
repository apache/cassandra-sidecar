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

package org.apache.cassandra.sidecar.accesscontrol;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

/**
 * {@link AuthCache} caches information needed for authenticating sidecar users.
 *
 * @param <K> key stored in cache for retrieving value
 * @param <V> value cached
 *
 * for {@link IdentityRoleCache} key will be String representing identity extracted from certificate and value will
 * be String representing Cassandra role associated with the identity
 */
public abstract class AuthCache<K, V>
{
    protected static final Logger LOGGER = LoggerFactory.getLogger(AuthCache.class);
    protected final String name;
    protected final boolean enabled;
    protected final Function<K, V> loadFunction;
    protected final Supplier<Map<K, V>> bulkLoadFunction;
    protected final int warmingRetries;
    protected final long warmingRetryIntervalInMillis;

    protected long expireAfterMillis;
    protected long maxEntries;

    protected volatile LoadingCache<K, V> cache;

    protected AuthCache(String name,
                        boolean enabled,
                        Function<K, V> loadFunction,
                        Supplier<Map<K, V>> bulkLoadFunction,
                        int warmingRetries,
                        long warmingRetryIntervalInMillis,
                        long expireAfterMillis,
                        long maxEntries)
    {
        this.name = name;
        this.enabled = enabled;
        this.loadFunction = loadFunction;
        this.bulkLoadFunction = bulkLoadFunction;
        this.warmingRetries = warmingRetries;
        this.warmingRetryIntervalInMillis = warmingRetryIntervalInMillis;
        this.expireAfterMillis = expireAfterMillis;
        this.maxEntries = maxEntries;
        this.cache = initCache(null);
    }

    public void setExpireAfterMillis(long expireAfterMillis)
    {
        if (enabled && this.expireAfterMillis != expireAfterMillis)
        {
            synchronized (this)
            {
                if (this.expireAfterMillis == expireAfterMillis)
                {
                    return;
                }
                this.expireAfterMillis = expireAfterMillis;
                cache = initCache(cache);
            }
        }
    }

    public long getExpireAfterMillis()
    {
        return this.expireAfterMillis;
    }

    public void setMaxEntries(long maxEntries)
    {
        if (enabled && this.maxEntries != maxEntries)
        {
            synchronized (this)
            {
                if (this.maxEntries == maxEntries)
                {
                    return;
                }
                this.maxEntries = maxEntries;
                cache = initCache(cache);
            }
        }
    }

    public long getMaxEntries()
    {
        return this.maxEntries;
    }

    /**
     * Retrieve a value from the cache. Will call {@link LoadingCache#get(Object)} which will
     * "load" the value if it's not present, thus populating the key.
     * @param k key
     * @return The current value of {@code K} if cached or loaded.
     *
     * See {@link LoadingCache#get(Object)} for possible exceptions.
     */
    public V get(K k)
    {
        if (cache == null)
        {
            return loadFunction.apply(k);
        }
        return cache.get(k);
    }

    /**
     * Retrieve all cached entries. Will call {@link LoadingCache#asMap()} which does not trigger "load".
     * @return a map of cached key-value pairs
     */
    public Map<K, V> getAll()
    {
        if (cache == null)
        {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(cache.asMap());
    }

    // We might need to add support for invalidating cache entries when we support removing auth entries from
    // Cassandra auth tables through sidecar
    public void invalidate(K k)
    {
        throw new UnsupportedOperationException("Invalidate functionality is not yet supported for AuthCache");
    }

    private LoadingCache<K, V> initCache(LoadingCache<K, V> existing)
    {
        if (!enabled)
            return null;

        LoadingCache<K, V> updatedCache;
        if (existing == null)
        {
            updatedCache = Caffeine.newBuilder()
                                   // setting refreshAfterWrite and expireAfterWrite to same value makes sure no stale
                                   // data is fetched after expire time
                                   .refreshAfterWrite(expireAfterMillis, TimeUnit.MILLISECONDS)
                                   .expireAfterWrite(expireAfterMillis, TimeUnit.MILLISECONDS)
                                   .maximumSize(getMaxEntries())
                                   .build(loadFunction::apply);
        }
        else
        {
            updatedCache = cache;
            // Always set as mandatory
            cache.policy().expireAfterWrite().ifPresent(policy -> policy.setExpiresAfter(expireAfterMillis, TimeUnit.MILLISECONDS));
            cache.policy().eviction().ifPresent(policy -> policy.setMaximum(getMaxEntries()));
        }
        return updatedCache;
    }

    public void warm()
    {
        if (cache == null)
        {
            LOGGER.warn("Cache {} not enabled, skipping pre-warming", name);
            return;
        }

        warm(warmingRetries);
    }

    private void warm(int retry)
    {
        if (retry < 1)
        {
            LOGGER.warn("Retries exhausted, unexpected error pre warming cache {}", name);
            return;
        }
        try
        {
            Map<K, V> entries = bulkLoadFunction.get();
            cache.putAll(entries);
        }
        catch (Exception e)
        {
            LOGGER.warn("Unexpected error encountered during cache {} pre-warming, ", name, e);
            Uninterruptibles.sleepUninterruptibly(warmingRetryIntervalInMillis, TimeUnit.MILLISECONDS);
            warm(retry - 1);
        }
    }
}
