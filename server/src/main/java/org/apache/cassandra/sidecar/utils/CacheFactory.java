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

import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import org.apache.cassandra.sidecar.acl.authorization.AuthorizationCacheKey;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * A factory for caches used in Sidecar
 */
@Singleton
public class CacheFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CacheFactory.class);

    public static final String ENDPOINT_AUTHORIZATION_CACHE_NAME = "endpoint_authorization_cache";

    private final Cache<SSTableImporter.ImportOptions, Future<Void>> ssTableImportCache;
    private final AsyncCache<AuthorizationCacheKey, Boolean> endpointAuthorizationCache;

    @Inject
    public CacheFactory(SidecarConfiguration configuration,
                        SSTableImporter ssTableImporter,
                        SidecarMetrics sidecarMetrics)
    {
        this(configuration, ssTableImporter, sidecarMetrics, Ticker.systemTicker());
    }

    @VisibleForTesting
    CacheFactory(SidecarConfiguration configuration, SSTableImporter ssTableImporter, SidecarMetrics sidecarMetrics,
                 Ticker ticker)
    {
        this.ssTableImportCache = initSSTableImportCache(configuration.serviceConfiguration()
                                                                      .sstableImportConfiguration()
                                                                      .cacheConfiguration(),
                                                         ssTableImporter, ticker);
        this.endpointAuthorizationCache = initEndpointAuthorizationCache(configuration, sidecarMetrics, ticker);
    }

    /**
     * @return the cache used for the SSTableImport requests
     */
    public Cache<SSTableImporter.ImportOptions, Future<Void>> ssTableImportCache()
    {
        return ssTableImportCache;
    }

    /**
     * @return the cache used for authorization requests
     */
    public AsyncCache<AuthorizationCacheKey, Boolean> endpointAuthorizationCache()
    {
        return endpointAuthorizationCache;
    }

    /**
     * Initializes the SSTable Import Cache using the provided {@code configuration} and {@code ticker}
     * for the cache
     *
     * @param configuration   the Cache configuration parameters
     * @param ssTableImporter the reference to the SSTable importer singleton
     * @param ticker          the ticker for the cache
     * @return the initialized cache
     */
    protected Cache<SSTableImporter.ImportOptions, Future<Void>>
    initSSTableImportCache(CacheConfiguration configuration, SSTableImporter ssTableImporter, Ticker ticker)
    {
        long maximumSize = configuration.maximumSize();
        LOGGER.info("Building SSTable Import Cache with expireAfterAccess={}, maxSize={}",
                    configuration.expireAfterAccess(), maximumSize);
        return Caffeine.newBuilder()
                       .ticker(ticker)
                       .executor(MoreExecutors.directExecutor())
                       .expireAfterAccess(configuration.expireAfterAccess().quantity(), configuration.expireAfterAccess().unit())
                       .maximumSize(maximumSize)
                       .recordStats()
                       .removalListener((RemovalListener<SSTableImporter.ImportOptions, Future<Void>>)
                                        (options, result, cause) -> {
                                            LOGGER.debug("Removed entry '{}' with options '{}' from SSTable Import " +
                                                         "Cache and cause {}", result, options, cause);
                                            ssTableImporter.cancelImport(options);
                                        }
                       )
                       .build();
    }

    /**
     * Initializes the Authorization Cache using the provided {@code configuration}. We want to create only one
     * instance of authorization cache.
     *
     * @param sidecarConfiguration the Sidecar configuration
     * @param sidecarMetrics       the Sidecar metrics registry
     * @return instance of {@link AsyncCache} for caching authorization requests
     */
    private AsyncCache<AuthorizationCacheKey, Boolean> initEndpointAuthorizationCache(SidecarConfiguration sidecarConfiguration,
                                                                                      SidecarMetrics sidecarMetrics,
                                                                                      Ticker ticker)
    {
        if (!sidecarConfiguration.accessControlConfiguration().enabled()
            || !sidecarConfiguration.accessControlConfiguration().permissionCacheConfiguration().enabled())
        {
            return null;
        }
        CacheConfiguration permissionCacheConfig = sidecarConfiguration.accessControlConfiguration()
                                                                       .permissionCacheConfiguration();
        if (permissionCacheConfig.expireAfterAccess() == null)
        {
            throw new ConfigurationException("Authorization handler cache must be configured with expireAfterAccess");
        }
        LOGGER.info("Building Authorization Cache with expireAfterAccess={}, maxSize={}",
                    permissionCacheConfig.expireAfterAccess(), permissionCacheConfig.maximumSize());
        return Caffeine.newBuilder()
                       .ticker(ticker)
                       .expireAfterAccess(permissionCacheConfig.expireAfterAccess().quantity(),
                                          permissionCacheConfig.expireAfterAccess().unit())
                       .maximumSize(permissionCacheConfig.maximumSize())
                       .recordStats(() -> sidecarMetrics.server().cache().authorizationCacheMetrics)
                       .buildAsync();
    }
}
