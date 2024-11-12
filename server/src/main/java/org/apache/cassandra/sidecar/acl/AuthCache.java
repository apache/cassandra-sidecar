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

package org.apache.cassandra.sidecar.acl;

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
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.jetbrains.annotations.VisibleForTesting;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_ALL_CASSANDRA_CQL_READY;

/**
 * Caches information needed for authenticating sidecar users.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public abstract class AuthCache<K, V>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AuthCache.class);
    private final String name;
    private final Function<K, V> loadFunction;
    private final Supplier<Map<K, V>> bulkLoadFunction;
    private final CacheConfiguration config;
    private final TaskExecutorPool internalPool;
    // cache is null when AuthCache is disabled
    private volatile LoadingCache<K, V> cache;
    protected final Vertx vertx;

    protected AuthCache(String name,
                        Vertx vertx,
                        ExecutorPools executorPools,
                        Function<K, V> loadFunction,
                        Supplier<Map<K, V>> bulkLoadFunction,
                        CacheConfiguration cacheConfiguration)
    {
        this.name = name;
        this.vertx = vertx;
        this.internalPool = executorPools.internal();
        this.loadFunction = loadFunction;
        this.bulkLoadFunction = bulkLoadFunction;
        this.config = cacheConfiguration;

        if (this.config.enabled())
        {
            this.cache = initCache();
            configureSidecarServerEventListener();
        }
    }

    /**
     * @return Cache maintained by this AuthCache.
     */
    protected LoadingCache<K, V> cache()
    {
        return cache;
    }

    /**
     * Retrieves a value from the cache. Will call {@link LoadingCache#get(Object)} which will
     * "load" the value if it's not present, thus populating the key. When the cache is disabled, data is fetched
     * with loadFunction.
     *
     * @param k key
     * @return The current value of {@code K} if cached or loaded.
     * <p>
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
        eventBus.localConsumer(ON_ALL_CASSANDRA_CQL_READY.address(), message -> warmUpAsync(config.warmupRetries()));
    }

    private void warmUpAsync(int availableRetries)
    {
        internalPool.runBlocking(() -> warmUp(availableRetries));
    }

    @VisibleForTesting
    protected void warmUp(int availableRetries)
    {
        if (!config.enabled())
        {
            LOGGER.info("Cache={} not enabled, skipping pre-warming", name);
            return;
        }

        if (availableRetries < 1)
        {
            LOGGER.warn("Retries exhausted, unexpected error pre-warming cache={}", name);
            return;
        }

        try
        {
            cache.putAll(bulkLoadFunction.get());
        }
        catch (Exception e)
        {
            LOGGER.warn("Unexpected error encountered during pre-warming of cache={} ", name, e);
            vertx.setTimer(config.warmupRetryIntervalMillis(), t -> warmUpAsync(availableRetries - 1));
        }
    }
}
