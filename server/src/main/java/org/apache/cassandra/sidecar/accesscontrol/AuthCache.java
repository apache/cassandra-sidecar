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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.jetbrains.annotations.VisibleForTesting;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_CQL_READY;

/**
 * Caches information needed for authenticating sidecar users.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public abstract class AuthCache<K, V>
{
    protected static final Logger LOGGER = LoggerFactory.getLogger(AuthCache.class);
    protected final String name;
    protected final Vertx vertx;
    protected final Function<K, V> loadFunction;
    protected final Supplier<Map<K, V>> bulkLoadFunction;
    protected final CacheConfiguration config;

    // cache is null when AuthCache is disabled
    protected volatile LoadingCache<K, V> cache;

    protected AuthCache(String name,
                        Vertx vertx,
                        Function<K, V> loadFunction,
                        Supplier<Map<K, V>> bulkLoadFunction,
                        CacheConfiguration cacheConfiguration)
    {
        this.name = name;
        this.vertx = vertx;
        this.loadFunction = loadFunction;
        this.bulkLoadFunction = bulkLoadFunction;
        this.config = cacheConfiguration;
        this.cache = initCache();

        if (this.config.enabled())
        {
            configureSidecarServerEventListener();
        }
    }

    /**
     * Retrieves a value from the cache. Will call {@link LoadingCache#get(Object)} which will
     * "load" the value if it's not present, thus populating the key. When the cache is disabled, data is fetched
     * with loadFunction.
     *
     * @param k key
     * @return The current value of {@code K} if cached or loaded.
     *
     * See {@link LoadingCache#get(Object)} for possible exceptions.
     */
    public V get(K k)
    {
        if (!config.enabled())
        {
            return loadFunction.apply(k);
        }
        return cache.get(k);
    }

    /**
     * Retrieves all cached entries. Will call {@link LoadingCache#asMap()} which does not trigger "load". When cache
     * is disabled, data is fetched with bulkLoadFunction.
     *
     * @return a map of cached key-value pairs
     */
    public Map<K, V> getAll()
    {
        if (!config.enabled())
        {
            return bulkLoadFunction.get();
        }
        return Collections.unmodifiableMap(cache.asMap());
    }

    private LoadingCache<K, V> initCache()
    {
        if (!config.enabled())
            return null;

        return Caffeine.newBuilder()
                       // setting refreshAfterWrite and expireAfterWrite to same value makes sure no stale
                       // data is fetched after expire time
                       .refreshAfterWrite(config.expireAfterAccessMillis(), TimeUnit.MILLISECONDS)
                       .expireAfterWrite(config.expireAfterAccessMillis(), TimeUnit.MILLISECONDS)
                       .maximumSize(config.maximumSize())
                       .build(loadFunction::apply);
    }

    private void configureSidecarServerEventListener()
    {
        EventBus eventBus = vertx.eventBus();
        eventBus.localConsumer(ON_CASSANDRA_CQL_READY.address(), message -> warm());
    }

    @VisibleForTesting
    protected void warm()
    {
        if (!config.enabled())
        {
            LOGGER.info("Cache {} not enabled, skipping pre-warming", name);
            return;
        }

        int retries = config.warmupRetries();
        while (retries-- >= 1)
        {
            try
            {
                Map<K, V> entries = bulkLoadFunction.get();
                cache.putAll(entries);
                return;
            }
            catch (Exception e)
            {
                LOGGER.warn("Unexpected error encountered during cache {} pre-warming, ", name, e);
                sleepUninterruptibly(config.warmupRetryIntervalMillis(), TimeUnit.MILLISECONDS);
            }
        }
        LOGGER.warn("Retries exhausted, unexpected error pre warming cache {}", name);
    }
}
