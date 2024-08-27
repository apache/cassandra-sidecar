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

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.ext.auth.authorization.AndAuthorization;
import org.apache.cassandra.sidecar.auth.authorization.SystemAuthDatabaseAccessor;
import org.apache.cassandra.sidecar.config.AuthorizerConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.RoleToSidecarPermissionsConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * A factory for caches used in Sidecar
 */
@Singleton
public class CacheFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CacheFactory.class);

    private final Cache<SSTableImporter.ImportOptions, Future<Void>> ssTableImportCache;
    protected final AsyncLoadingCache<String, Boolean> rolesToSuperUserCache;
    protected final AsyncLoadingCache<String, String> identityToRolesCache;
    protected final AsyncLoadingCache<String, AndAuthorization> roleToPermissionsCache;
    protected final SystemAuthDatabaseAccessor systemAuthDatabaseAccessor;
    protected final AuthorizerConfiguration authorizerConfiguration;

    @Inject
    public CacheFactory(SidecarConfiguration sidecarConfiguration,
                        ServiceConfiguration serviceConfiguration,
                        SSTableImporter ssTableImporter,
                        SystemAuthDatabaseAccessor systemAuthDatabaseAccessor)
    {
        this(sidecarConfiguration, serviceConfiguration, ssTableImporter, Ticker.systemTicker(), systemAuthDatabaseAccessor);
    }

    @VisibleForTesting
    CacheFactory(SidecarConfiguration sidecarConfiguration,
                ServiceConfiguration serviceConfiguration,
                 SSTableImporter ssTableImporter,
                 Ticker ticker,
                 SystemAuthDatabaseAccessor systemAuthDatabaseAccessor)
    {
        this.ssTableImportCache = initSSTableImportCache(serviceConfiguration.sstableImportConfiguration().cacheConfiguration(),
                                                         ssTableImporter,
                                                         ticker);

        CacheConfiguration permissionsCacheConfig = serviceConfiguration.refreshPermissionCachesConfiguration().cacheConfiguration();
        this.systemAuthDatabaseAccessor = systemAuthDatabaseAccessor;
        this.rolesToSuperUserCache = initRolesToSuperUserCache(permissionsCacheConfig, ticker);
        this.identityToRolesCache = initIdentityToRolesCache(permissionsCacheConfig, ticker);
        this.roleToPermissionsCache = initRoleToPermissionsCache(permissionsCacheConfig, ticker);
        this.authorizerConfiguration = sidecarConfiguration.authorizerConfiguration();
    }

    /**
     * @return the cache used for the SSTableImport requests
     */
    public Cache<SSTableImporter.ImportOptions, Future<Void>> ssTableImportCache()
    {
        return ssTableImportCache;
    }

    public AsyncLoadingCache<String, Boolean> rolesToSuperUserCache()
    {
        return rolesToSuperUserCache;
    }

    public AsyncLoadingCache<String, String> identityToRolesCache()
    {
        return identityToRolesCache;
    }

    public AsyncLoadingCache<String, AndAuthorization> roleToPermissionsCache()
    {
        return roleToPermissionsCache;
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
        Duration expireAfterAccessDuration = Duration.of(configuration.expireAfterAccessMillis(), ChronoUnit.MILLIS);
        long maximumSize = configuration.maximumSize();
        LOGGER.info("Building SSTable Import Cache with expireAfterAccess={}, maxSize={}",
                    expireAfterAccessDuration, maximumSize);
        return Caffeine.newBuilder()
                       .ticker(ticker)
                       .executor(MoreExecutors.directExecutor())
                       .expireAfterAccess(expireAfterAccessDuration)
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

    protected AsyncLoadingCache<String, Boolean> initRolesToSuperUserCache(CacheConfiguration cacheConfiguration, Ticker ticker)
    {
        LOGGER.info("Initializing 'roles' cache");
        assert systemAuthDatabaseAccessor != null;
        return Caffeine.newBuilder()
                       .ticker(ticker)
                       .maximumSize(cacheConfiguration.maximumSize())
                       .expireAfterWrite(Duration.of(cacheConfiguration.expireAfterAccessMillis(), ChronoUnit.SECONDS))
                       .removalListener((key, value, cause) ->
                                        LOGGER.debug("Removed entry '{}' with key '{}' from {} with cause {}",
                                                     value, key, "roles", cause))
                       .buildAsync(systemAuthDatabaseAccessor::isSuperUser);
    }

    protected AsyncLoadingCache<String, String> initIdentityToRolesCache(CacheConfiguration cacheConfiguration, Ticker ticker)
    {
        LOGGER.info("Initializing 'identity_to_role' cache");
        assert systemAuthDatabaseAccessor != null;
        return Caffeine.newBuilder()
                       .ticker(ticker)
                       .maximumSize(cacheConfiguration.maximumSize())
                       .expireAfterWrite(Duration.of(cacheConfiguration.expireAfterAccessMillis(), ChronoUnit.SECONDS))
                       .removalListener((key, value, cause) ->
                                        LOGGER.debug("Removed entry '{}' with key '{}' from {} with cause {}",
                                                     value, key, "identity_to_role", cause))
                       .buildAsync(systemAuthDatabaseAccessor::findRoleFromIdentity);
    }

    protected AsyncLoadingCache<String, AndAuthorization> initRoleToPermissionsCache(CacheConfiguration cacheConfiguration, Ticker ticker)
    {
        LOGGER.info("Initializing 'role_permissions' cache");
        assert systemAuthDatabaseAccessor != null;
        return Caffeine.newBuilder()
                       .ticker(ticker)
                       .maximumSize(cacheConfiguration.maximumSize())
                       .expireAfterWrite(Duration.of(cacheConfiguration.expireAfterAccessMillis(), ChronoUnit.SECONDS))
                       .removalListener((key, value, cause) ->
                                        LOGGER.debug("Removed entry '{}' with key '{}' from {} with cause {}",
                                                     value, key, "role_permissions", cause))
                       .buildAsync(key -> {
                           AndAuthorization authFromCassandra = systemAuthDatabaseAccessor.findPermissionsFromResourceRole(key);
                           if (authFromCassandra == null)
                           {
                               for (RoleToSidecarPermissionsConfiguration authFromSidecar : authorizerConfiguration.roleToSidecarPermissions())
                               {
                                   if (authFromSidecar.role().equals(key))
                                   {
                                       return authFromSidecar.permissions();
                                   }
                               }
                           }
                           return authFromCassandra;
                       });
    }
}
