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
import java.util.Set;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
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
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Provides REST API for invalidating authentication and authorization caches.
 * Supports invalidating identity-to-role mappings, role authorizations,
 * super user cache, and endpoint authorization cache.
 */
@Singleton
public class InvalidateCacheHandler extends AbstractHandler<String> implements AccessProtected
{
    private final IdentityToRoleCache identityToRoleCache;
    private final RoleAuthorizationsCache roleAuthorizationsCache;
    private final SuperUserCache superUserCache;
    private final AsyncCache<AuthorizationCacheKey, Boolean> endpointAuthorizationCache;

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
    protected String extractParamsOrThrow(RoutingContext context)
    {
        return context.pathParam("cacheName");
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  String cacheName)
    {
        switch (cacheName.toLowerCase())
        {
            case "identity_to_role_cache":
            case "identitytorolecache":
                identityToRoleCache.invalidateAll();
                break;
            case "role_permissions_cache":
            case "roleauthorizationscache":
                roleAuthorizationsCache.invalidateAll();
                break;
            case "super_user_cache":
            case "superusercache":
                superUserCache.invalidateAll();
                break;
            case "endpoint_authorization_cache":
            case "endpointauthorizationcache":
                endpointAuthorizationCache.synchronous().invalidateAll();
                break;
            default:
                context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND,
                                               "Unknown cache: " + cacheName));
                return;
        }

        logger.info("Cache {} invalidated successfully", cacheName);
        context.json(OK_STATUS);
    }
}
