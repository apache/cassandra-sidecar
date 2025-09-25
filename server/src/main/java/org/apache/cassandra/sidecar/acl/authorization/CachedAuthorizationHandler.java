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

package org.apache.cassandra.sidecar.acl.authorization;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.AuthorizationContext;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthorizationHandler;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.handler.impl.AuthorizationHandlerImpl;
import org.apache.cassandra.sidecar.acl.AdminIdentityResolver;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.metrics.CacheStatsCounter;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.server.AuthMetrics;

import static org.apache.cassandra.sidecar.utils.AuthUtils.extractIdentities;

/**
 * Verifies user has required authorizations. Allows admin identities to bypass authorization checks.
 */
public class CachedAuthorizationHandler extends AuthorizationHandlerImpl
{
    private static final HttpException FORBIDDEN_EXCEPTION = new HttpException(403);
    private final AccessControlConfiguration accessControlConfiguration;
    private final AuthorizationParameterValidateHandler authZParameterValidateHandler;
    private final AdminIdentityResolver adminIdentityResolver;
    private final AuthMetrics authMetrics;
    private final CacheStatsCounter cacheMetrics;
    private final Cache<AuthorizationCacheKey, Boolean> authorizationCache;
    // This is overridden since Vert.x does not expose this
    private BiConsumer<RoutingContext, AuthorizationContext> variableHandler;
    private final List<AuthorizationCacheKey> keys = new ArrayList<>();

    public CachedAuthorizationHandler(AccessControlConfiguration accessControlConfiguration,
                                      AuthorizationParameterValidateHandler authZParameterValidateHandler,
                                      AdminIdentityResolver adminIdentityResolver,
                                      Authorization authorization,
                                      SidecarMetrics sidecarMetrics)
    {
        super(authorization);
        this.accessControlConfiguration = accessControlConfiguration;
        this.authZParameterValidateHandler = authZParameterValidateHandler;
        this.adminIdentityResolver = adminIdentityResolver;
        this.authMetrics = sidecarMetrics.server().auth();
        this.cacheMetrics = sidecarMetrics.server().cache().authorizationCacheMetrics;
        this.authorizationCache = initCache();
    }

    @Override
    public void handle(RoutingContext ctx)
    {
        long startTimeNanos = System.nanoTime();
        authZParameterValidateHandler.handle(ctx);
        if (ctx.failed()) // failed due to validation
        {
            return;
        }

        User user = ctx.user();
        AuthorizationContext authorizationContext = AuthorizationContext.create(user);
        if (this.variableHandler != null)
        {
            this.variableHandler.accept(ctx, authorizationContext);
        }

        AtomicBoolean ctxNextCalled = new AtomicBoolean(false);

        AuthorizationCacheKey key = AuthorizationCacheKey.create(authorizationContext);
        keys.add(key);
        Boolean authorized = authorizationCache.get(key, k -> {
            List<String> identities = extractIdentities(user);

            // Admin identities bypass route specific authorization checks
            if (!identities.isEmpty() && identities.stream().anyMatch(adminIdentityResolver::isAdmin))
            {
                return true;
            }

            super.handle(ctx);
            if (!ctx.failed())
            {
                ctxNextCalled.set(true);
                long durationNanos = System.nanoTime() - startTimeNanos;
                authMetrics.authorizationTime.metric.update(durationNanos, TimeUnit.NANOSECONDS);
                return true;
            }
            return false;
        });

        // We avoid calling ctx.next() and ctx.fail() when it is already done during cache value computation
        if (Boolean.TRUE.equals(authorized))
        {
            if (!ctxNextCalled.get())
            {
                ctx.next();
            }
        }
        else
        {
            if (!ctx.failed())
            {
                ctx.fail(403, FORBIDDEN_EXCEPTION);
            }
        }
    }

    private <K, V> Cache<K, V> initCache()
    {
        CacheConfiguration permissionCacheConfig = accessControlConfiguration.permissionCacheConfiguration();
        if (permissionCacheConfig.expireAfterAccess() == null)
        {
            throw new ConfigurationException("Authorization handler cache must be configured with expireAfterAccess");
        }
        Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder()
                                                        .maximumSize(permissionCacheConfig.maximumSize())
                                                        .recordStats(() -> cacheMetrics);
        if (permissionCacheConfig.expireAfterAccess() != null)
        {
            cacheBuilder.expireAfterAccess(permissionCacheConfig.expireAfterAccess().quantity(),
                                           permissionCacheConfig.expireAfterAccess().unit());
        }
        return cacheBuilder.build();
    }

    @Override
    public AuthorizationHandler variableConsumer(BiConsumer<RoutingContext, AuthorizationContext> handler)
    {
        this.variableHandler = handler;
        super.variableConsumer(handler);
        return this;
    }
}
