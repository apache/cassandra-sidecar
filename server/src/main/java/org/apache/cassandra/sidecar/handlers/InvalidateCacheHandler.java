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
package org.apache.cassandra.sidecar.handlers;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.AuthCache;
import org.apache.cassandra.sidecar.acl.IdentityToRoleCache;
import org.apache.cassandra.sidecar.acl.authorization.AuthorizationCacheKey;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.acl.authorization.RoleAuthorizationsCache;
import org.apache.cassandra.sidecar.acl.authorization.SuperUserCache;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.utils.CacheFactory;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.modules.ApiModule.OK_STATUS;
import static org.apache.cassandra.sidecar.utils.CacheFactory.ENDPOINT_AUTHORIZATION_CACHE_NAME;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Provides REST API for invalidating caches.
 * Supports full cache invalidation or selective key-based invalidation where applicable.
 * Currently, supports authentication and authorization caches including identity-to-role mappings,
 * role authorizations, superuser cache and endpoint authorization cache.
 */
@Singleton
public class InvalidateCacheHandler extends AbstractHandler<InvalidateCacheHandler.Params> implements AccessProtected
{
    private static final String IDENTITY_TO_ROLE_CACHE_NO_UNDERSCORE = "identitytorolecache";
    private static final String SUPER_USER_CACHE_NO_UNDERSCORE = "superusercache";
    private static final String ROLE_AUTHORIZATIONS_CACHE_NO_UNDERSCORE = "roleauthorizationscache";
    private static final String ENDPOINT_AUTHORIZATION_CACHE_NO_UNDERSCORE = "endpointauthorizationcache";

    private final IdentityToRoleCache identityToRoleCache;
    private final RoleAuthorizationsCache roleAuthorizationsCache;
    private final SuperUserCache superUserCache;
    private final Cache<AuthorizationCacheKey, Future<Boolean>> endpointAuthorizationCache;

    @Inject
    public InvalidateCacheHandler(InstanceMetadataFetcher metadataFetcher,
                                  ExecutorPools executorPools,
                                  CassandraInputValidator validator,
                                  IdentityToRoleCache identityToRoleCache,
                                  RoleAuthorizationsCache roleAuthorizationsCache,
                                  SuperUserCache superUserCache,
                                  CacheFactory cacheFactory)
    {
        super(metadataFetcher, executorPools, validator);
        this.identityToRoleCache = identityToRoleCache;
        this.roleAuthorizationsCache = roleAuthorizationsCache;
        this.superUserCache = superUserCache;
        this.endpointAuthorizationCache = cacheFactory.endpointAuthorizationCache();
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.INVALIDATE_CACHE.toAuthorization());
    }

    @Override
    protected Params extractParamsOrThrow(RoutingContext context)
    {
        String cacheName = context.pathParam("cacheName");
        List<String> keys = context.queryParam("keys");
        return new Params(cacheName, keys);
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  Params params)
    {
        String cacheName = params.cacheName;
        boolean invalidateAll = params.invalidateAll();

        switch (cacheName.toLowerCase())
        {
            case IdentityToRoleCache.NAME:
            case IDENTITY_TO_ROLE_CACHE_NO_UNDERSCORE:
                invalidateAuthCache(identityToRoleCache, params, true);
                break;
            case SuperUserCache.NAME:
            case SUPER_USER_CACHE_NO_UNDERSCORE:
                invalidateAuthCache(superUserCache, params, true);
                break;
            case RoleAuthorizationsCache.NAME:
            case ROLE_AUTHORIZATIONS_CACHE_NO_UNDERSCORE:
                invalidateAuthCache(roleAuthorizationsCache, params, false);
                break;
            case ENDPOINT_AUTHORIZATION_CACHE_NAME:
            case ENDPOINT_AUTHORIZATION_CACHE_NO_UNDERSCORE:
                if (!invalidateAll)
                {
                    throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                            "endpoint_authorization_cache does not support selective key invalidation");
                }
                endpointAuthorizationCache.invalidateAll();
                break;
            default:
                throw wrapHttpException(HttpResponseStatus.NOT_FOUND,
                                        "Unknown cache: " + cacheName);
        }

        logger.info("Cache {} invalidated successfully. Keys: {}", cacheName,
                    invalidateAll ? "all" : params.keys);
        context.json(OK_STATUS);
    }

    /**
     * Generic method to invalidate an AuthCache with optional list of keys for invalidation
     *
     * @param cache                         AuthCache to invalidate
     * @param params                        params containing cache name and keys
     * @param supportsSelectiveInvalidation whether cache supports selective key invalidation
     * @param <V>                           the cache value type
     */
    private <V> void invalidateAuthCache(AuthCache<String, V> cache, Params params, boolean supportsSelectiveInvalidation)
    {
        if (!supportsSelectiveInvalidation && !params.invalidateAll())
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    params.cacheName + " does not support selective key invalidation");
        }

        if (params.invalidateAll())
        {
            cache.invalidateAll();
        }
        else
        {
            cache.invalidateAll(params.keys);
        }
    }

    /**
     * Simple holder class for cache invalidation parameters
     */
    protected static class Params
    {
        final String cacheName;
        final List<String> keys;

        Params(String cacheName, List<String> keys)
        {
            this.cacheName = cacheName;
            this.keys = keys;
        }

        boolean invalidateAll()
        {
            return keys == null || keys.isEmpty();
        }
    }
}
