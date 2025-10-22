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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncCache;
import io.vertx.core.Future;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.AuthorizationContext;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthorizationHandler;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.handler.impl.AuthorizationHandlerImpl;
import org.apache.cassandra.sidecar.acl.AdminIdentityResolver;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.server.AuthMetrics;

import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.vertx.core.Future.fromCompletionStage;
import static org.apache.cassandra.sidecar.utils.AuthUtils.extractIdentities;

/**
 * {@link CachedAuthorizationHandler} caches all authorization requests using {@link AuthorizationCacheKey}.
 */
public class CachedAuthorizationHandler extends AuthorizationHandlerImpl
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CachedAuthorizationHandler.class);

    // uniquely identities CachedAuthorizationHandler across different routes. Having same handlerId can lead
    // to permission bypass across routes.
    private static final AtomicInteger HANDLER_ID_GEN = new AtomicInteger(0);
    private static final HttpException FORBIDDEN_EXCEPTION = new HttpException(403);
    private final int handlerId;
    private final AccessControlConfiguration accessControlConfiguration;
    private final AuthorizationParameterValidateHandler authZParameterValidateHandler;
    private final AdminIdentityResolver adminIdentityResolver;
    private final AuthMetrics authMetrics;
    private final AsyncCache<AuthorizationCacheKey, Boolean> authorizationCache;

    // This is overridden since Vert.x does not expose this
    private BiConsumer<RoutingContext, AuthorizationContext> variableHandler;

    public CachedAuthorizationHandler(AccessControlConfiguration accessControlConfiguration,
                                      AuthorizationParameterValidateHandler authZParameterValidateHandler,
                                      AdminIdentityResolver adminIdentityResolver,
                                      Authorization authorization,
                                      SidecarMetrics sidecarMetrics,
                                      AsyncCache<AuthorizationCacheKey, Boolean> authorizationCache)
    {
        this(HANDLER_ID_GEN.getAndIncrement(), accessControlConfiguration, authZParameterValidateHandler,
             adminIdentityResolver, authorization, sidecarMetrics, authorizationCache);
    }

    @VisibleForTesting
    public CachedAuthorizationHandler(int handlerId,
                                      AccessControlConfiguration accessControlConfiguration,
                                      AuthorizationParameterValidateHandler authZParameterValidateHandler,
                                      AdminIdentityResolver adminIdentityResolver,
                                      Authorization authorization,
                                      SidecarMetrics sidecarMetrics,
                                      AsyncCache<AuthorizationCacheKey, Boolean> authorizationCache)
    {
        super(authorization);
        this.handlerId = handlerId;
        this.accessControlConfiguration = accessControlConfiguration;
        this.authZParameterValidateHandler = authZParameterValidateHandler;
        this.adminIdentityResolver = adminIdentityResolver;
        this.authMetrics = sidecarMetrics.server().auth();
        this.authorizationCache = authorizationCache;
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

        AtomicBoolean ctxNextCalled = new AtomicBoolean(false);
        Future<Boolean> authorizationFuture
        = fromCompletionStage(checkAuthorization(ctx, ctxNextCalled, startTimeNanos));

        authorizationFuture
        .onSuccess(authorized -> {
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
                    ctx.fail(FORBIDDEN.code(), FORBIDDEN_EXCEPTION);
                }
            }
        })
        .onFailure(cause -> {
            LOGGER.error("Error encountered during authorization cache computation", cause);
            if (!ctx.failed())
            {
                ctx.fail(FORBIDDEN.code(), FORBIDDEN_EXCEPTION);
            }
        });
    }

    @Override
    public AuthorizationHandler variableConsumer(BiConsumer<RoutingContext, AuthorizationContext> handler)
    {
        this.variableHandler = handler;
        super.variableConsumer(handler);
        return this;
    }

    private CompletableFuture<Boolean> checkAuthorization(RoutingContext ctx, AtomicBoolean ctxNextCalled,
                                                          long startTimeNanos)
    {
        if (!this.accessControlConfiguration.permissionCacheConfiguration().enabled())
        {
            // We perform authorization checks everytime if caching is disabled
            return CompletableFuture.completedFuture(isUserAuthorized(ctx, ctxNextCalled, startTimeNanos));
        }

        AuthorizationCacheKey key = createAuthorizationKey(ctx);
        return authorizationCache.get(key, k -> isUserAuthorized(ctx, ctxNextCalled, startTimeNanos));
    }

    private AuthorizationCacheKey createAuthorizationKey(RoutingContext ctx)
    {
        User user = ctx.user();
        AuthorizationContext authorizationContext = AuthorizationContext.create(user);
        if (this.variableHandler != null)
        {
            this.variableHandler.accept(ctx, authorizationContext);
        }
        return AuthorizationCacheKey.create(handlerId, authorizationContext);
    }

    private boolean isUserAuthorized(RoutingContext ctx, AtomicBoolean ctxNextCalled, long startTimeNanos)
    {
        User user = ctx.user();
        List<String> identities = extractIdentities(user);

        // Admin identities bypass route specific authorization checks
        if (isAdmin(identities))
        {
            return true;
        }

        super.handle(ctx);
        if (!ctx.failed())
        {
            ctxNextCalled.set(true);
            long durationNanos = System.nanoTime() - startTimeNanos;
            // authorization time recorded here is only taking into account authorizations that are not cached
            authMetrics.authorizationTime.metric.update(durationNanos, TimeUnit.NANOSECONDS);
            return true;
        }
        return false;
    }

    private boolean isAdmin(List<String> identities)
    {
        for (String identity : identities)
        {
            if (adminIdentityResolver.isAdmin(identity))
            {
                return true;
            }
        }
        return false;
    }
}
