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
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.exceptions.SchemaUnavailableException;
import org.apache.cassandra.sidecar.metrics.CacheStatsCounter;
import org.jetbrains.annotations.VisibleForTesting;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_SCHEMA_INITIALIZED;

/**
 * Caches information needed for authenticating sidecar users.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public abstract class AuthCache<K, V>
{
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final String name;
    private final Function<K, V> loadFunction;
    private final Supplier<Map<K, V>> bulkLoadFunction;
    private final CacheConfiguration config;
    private final CacheStatsCounter cacheMetrics;
    private final TaskExecutorPool internalPool;
    private final TaskExecutorPool servicePool;
    // cache is null when AuthCache is disabled
    private volatile LoadingCache<K, V> cache;
    protected final Vertx vertx;

    protected AuthCache(String name,
                        Vertx vertx,
                        ExecutorPools executorPools,
                        Function<K, V> loadFunction,
                        Supplier<Map<K, V>> bulkLoadFunction,
                        CacheConfiguration cacheConfiguration,
                        CacheStatsCounter cacheMetrics)
    {
        this.name = name;
        this.vertx = vertx;
        this.internalPool = executorPools.internal();
        this.servicePool = executorPools.service();
        this.loadFunction = loadFunction;
        this.bulkLoadFunction = bulkLoadFunction;
        this.config = cacheConfiguration;
        this.cacheMetrics = Objects.requireNonNull(cacheMetrics, "cacheMetrics is required");

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
     * Retrieves a value from the cache asynchronously. The method first attempts to return an existing value using
     * getIfPresent(). If the value is missing, it schedules getCached, which delegates to
     * {@link LoadingCache#get(Object)} to load and populate the cache, if cache is disabled the value is fetched using
     * load function. This design permits concurrent calls for a small time window. It is a good balance between code
     * simplicity and performance. The cache retrieval is offloaded to a worker thread to avoid blocking the event loop.
     *
     * @param k key
     * @return A {@link Future} containing the current value of {@code K} if cached or loaded.
     * <p>
     * See {@link LoadingCache#get(Object)} for possible exceptions.
     */
    public Future<V> get(K k)
    {
        V value = config.enabled() ? cache.getIfPresent(k) : null;
        if (value != null)
        {
            return Future.succeededFuture(value);
        }
        return servicePool.executeBlocking(() -> getCached(k), false);
    }

    /**
     * Helper method to retrieve value from cache synchronously. Used by {@link #get(K)} which offloads
     * this to a worker thread.
     *
     * @param k key
     * @return current value of {@code K} if cached or loaded
     */
    private V getCached(K k)
    {
        if (!config.enabled())
        {
            return loadFunction.apply(k);
        }
        return cache.get(k);
    }

    /**
     * Retrieves all cached entries asynchronously. If cache is enabled, returns early with map view. If not bulk loads
     * with bulkLoadFunction. The bulkLoadFunction retrieval is offloaded to a worker thread to avoid blocking the
     * event loop.
     *
     * @return A {@link Future} containing a map of cached key-value pairs
     */
    public Future<Map<K, V>> getAll()
    {
        if (config.enabled())
        {
            return Future.succeededFuture(Collections.unmodifiableMap(cache.asMap()));
        }
        return servicePool.executeBlocking(bulkLoadFunction::get, false);
    }

    /**
     * Invalidate a key.
     * @param k key to invalidate
     */
    public void invalidate(K k)
    {
        if (cache != null)
        {
            cache.invalidate(k);
            logger.info("Cache entry with key={} has been invalidated", k);
        }
    }

    /**
     * Invalidate multiple keys.
     * @param keys keys to invalidate
     */
    public void invalidateAll(Iterable<? extends K> keys)
    {
        if (cache != null)
        {
            cache.invalidateAll(keys);
            logger.info("Cache entries with keys={} have been invalidated from cache={}", keys, this.name);
        }
    }

    /**
     * Invalidate all entries in this cache
     */
    public void invalidateAll()
    {
        if (cache != null)
        {
            cache.invalidateAll();
            logger.info("All cache entries from {} have been invalidated", this.name);
        }
    }

    private LoadingCache<K, V> initCache()
    {
        if (config.refreshAfterWrite() == null && config.expireAfterAccess() == null)
        {
            throw new ConfigurationException(name +
                                             " must be configured with either refreshAfterWrite or expireAfterAccess");
        }

        Caffeine<Object, Object> cacheBuilder
        = Caffeine.newBuilder()
                  .recordStats(() -> cacheMetrics)
                  .maximumSize(config.maximumSize());
        if (config.refreshAfterWrite() != null)
        {
            cacheBuilder.refreshAfterWrite(config.refreshAfterWrite().quantity(), config.refreshAfterWrite().unit());
        }
        if (config.expireAfterAccess() != null)
        {
            cacheBuilder.expireAfterAccess(config.expireAfterAccess().quantity(), config.expireAfterAccess().unit());
        }
        return cacheBuilder.build(loadFunction::apply);
    }

    private void configureSidecarServerEventListener()
    {
        EventBus eventBus = vertx.eventBus();

        // Initiating warmup at ON_SIDECAR_SCHEMA_INITIALIZED because load functions interact with the database.
        // These functions may either directly execute CQL statements or use AbstractSchema. Therefore, it's preferable
        // to wait for the schema to be ready, as it ensures CQL is also ready.
        eventBus.localConsumer(ON_SIDECAR_SCHEMA_INITIALIZED.address(), message -> warmUpAsync(config.warmupRetries()));
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
            logger.info("Cache={} not enabled, skipping pre-warming", name);
            return;
        }

        if (availableRetries < 1)
        {
            logger.warn("Retries exhausted, unexpected error pre-warming cache={}", name);
            return;
        }

        try
        {
            long startTimeNanos = System.nanoTime();
            cache.putAll(bulkLoadFunction.get());
            logger.info("Cache={} warmup completed successfully in {} nanoseconds",
                        name, System.nanoTime() - startTimeNanos);
        }
        catch (SchemaUnavailableException sue)
        {
            logger.warn(sue.getMessage() + ". Skip warming up cache");
        }
        catch (Exception e)
        {
            logger.warn("Unexpected error encountered during pre-warming of cache={} ", name, e);
            vertx.setTimer(config.warmupRetryInterval().toMillis(), t -> warmUpAsync(availableRetries - 1));
        }
    }
}
