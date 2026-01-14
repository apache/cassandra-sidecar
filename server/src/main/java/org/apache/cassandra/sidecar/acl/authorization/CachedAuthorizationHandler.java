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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.AuthorizationContext;
import io.vertx.ext.auth.authorization.AuthorizationProvider;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthorizationHandler;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.acl.AdminIdentityResolver;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.server.AuthMetrics;
import org.jetbrains.annotations.VisibleForTesting;

import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.apache.cassandra.sidecar.utils.AuthUtils.extractIdentities;

/**
 * {@link CachedAuthorizationHandler} caches all authorization requests using {@link AuthorizationCacheKey}.
 */
public class CachedAuthorizationHandler implements AuthorizationHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CachedAuthorizationHandler.class);

    // uniquely identities CachedAuthorizationHandler across different routes. Having same handlerId can lead
    // to permission bypass across routes.
    private static final AtomicInteger HANDLER_ID_GEN = new AtomicInteger(0);
    private final int handlerId;
    private final AccessControlConfiguration accessControlConfiguration;
    private final AdminIdentityResolver adminIdentityResolver;
    private final AuthMetrics authMetrics;
    private final Cache<AuthorizationCacheKey, Future<Boolean>> authorizationCache;

    protected final Authorization authorization;
    protected final Collection<AuthorizationProvider> authorizationProviders;
    protected BiConsumer<RoutingContext, AuthorizationContext> variableHandler;

    public CachedAuthorizationHandler(AccessControlConfiguration accessControlConfiguration,
                                      AdminIdentityResolver adminIdentityResolver,
                                      Authorization authorization,
                                      SidecarMetrics sidecarMetrics,
                                      Cache<AuthorizationCacheKey, Future<Boolean>> authorizationCache)
    {
        this(HANDLER_ID_GEN.getAndIncrement(), accessControlConfiguration, adminIdentityResolver,
             authorization, sidecarMetrics, authorizationCache);
    }

    @VisibleForTesting
    public CachedAuthorizationHandler(int handlerId,
                                      AccessControlConfiguration accessControlConfiguration,
                                      AdminIdentityResolver adminIdentityResolver,
                                      Authorization authorization,
                                      SidecarMetrics sidecarMetrics,
                                      Cache<AuthorizationCacheKey, Future<Boolean>> authorizationCache)
    {
        this.authorization = Objects.requireNonNull(authorization, "authorization cannot be null");
        this.authorizationProviders = new ArrayList<>();
        this.handlerId = handlerId;
        this.accessControlConfiguration = accessControlConfiguration;
        this.adminIdentityResolver = adminIdentityResolver;
        this.authMetrics = sidecarMetrics.server().auth();
        this.authorizationCache = authorizationCache;
    }

    @Override
    public void handle(RoutingContext context)
    {
        long startTimeNanos = System.nanoTime();
        User user = context.user();
        if (user == null)
        {
            context.fail(FORBIDDEN.code(), new HttpException(FORBIDDEN.code()));
            return;
        }

        try
        {
            // this handler can perform asynchronous operations
            if (!context.request().isEnded())
            {
                context.request().pause();
            }

            // create the authorization context
            AuthorizationContext authorizationContext = AuthorizationContext.create(user);
            if (variableHandler != null)
            {
                variableHandler.accept(context, authorizationContext);
            }

            Context originCtx = context.vertx().getOrCreateContext();
            checkAuthorization(authorizationContext, startTimeNanos)
            .onSuccess(ignored -> {
                originCtx.runOnContext(v -> {
                    if (!context.request().isEnded())
                    {
                        context.request().resume();
                    }
                    context.next();
                });
            })
            .onFailure(cause -> {
                LOGGER.error("Authorization failed for user='{}'", user, cause);
                originCtx.runOnContext(v -> {
                    // resume as the error handler may allow this request to become valid again
                    if (!context.request().isEnded())
                    {
                        context.request().resume();
                    }
                    context.fail(FORBIDDEN.code(), cause);
                });
            });
        }
        catch (Throwable e)
        {
            // resume as the error handler may allow this request to become valid again
            if (!context.request().isEnded())
            {
                context.request().resume();
            }
            context.fail(e);
        }
    }

    @Override
    public AuthorizationHandler variableConsumer(BiConsumer<RoutingContext, AuthorizationContext> handler)
    {
        variableHandler = handler;
        return this;
    }

    @Override
    public AuthorizationHandler addAuthorizationProvider(AuthorizationProvider authorizationProvider)
    {
        authorizationProviders.add(Objects.requireNonNull(authorizationProvider,
                                                          "authorizationProvider cannot be null"));
        return this;
    }

    protected Future<Boolean> checkAuthorization(AuthorizationContext authorizationContext, long startTimeNanos)
    {
        if (!accessControlConfiguration.permissionCacheConfiguration().enabled())
        {
            // We perform authorization checks everytime if caching is disabled
            return authorizeUser(authorizationContext, startTimeNanos);
        }

        AuthorizationCacheKey key = AuthorizationCacheKey.create(handlerId, authorizationContext);
        // Cache.get() returns the cached Future<Boolean> directly. This is simpler than AsyncCache which
        // would wrap the result in a CompletableFuture, requiring unwrapping via compose(future -> future).
        // The synchronous Cache API provides thread-safe deduplication of concurrent loads for the same key.
        return authorizationCache.get(key, k -> authorizeUser(authorizationContext, startTimeNanos));
    }

    protected Future<Boolean> authorizeUser(AuthorizationContext authorizationContext, long startTimeNanos)
    {
        List<String> identities = extractIdentities(authorizationContext.user());
        return isAdmin(identities)
               .compose(isAdmin -> {
                   // Admin identities bypass route specific authorization checks
                   if (isAdmin)
                   {
                       return Future.succeededFuture(true);
                   }

                   Promise<Boolean> promise = Promise.promise();
                   // check or fetch authorizations
                   checkOrFetchAuthorizations(promise, authorizationContext, authorizationProviders.iterator());

                   Future<Boolean> future = promise.future();
                   future.onSuccess(ignored -> {
                       long durationNanos = System.nanoTime() - startTimeNanos;
                       // authorization time recorded here is only taking into account authorizations that are not cached
                       // and only successful authorizations are recorded
                       authMetrics.authorizationTime.metric.update(durationNanos, TimeUnit.NANOSECONDS);
                   });
                   return future;
        });
    }

    /**
     * this method checks that the specified authorization match the current content.
     * It doesn't fetch all providers at once in order to do early-out, but rather tries to be smart and fetch authorizations one provider at a time
     *
     * @param promise              the promise
     * @param authorizationContext the current authorization context
     * @param providers            the providers iterator
     */
    protected void checkOrFetchAuthorizations(Promise<Boolean> promise, AuthorizationContext authorizationContext, Iterator<AuthorizationProvider> providers)
    {
        if (authorization.match(authorizationContext))
        {
            promise.complete(true); // Authorization succeeds
            return;
        }

        User user = authorizationContext.user();

        // there was no match, in this case we do the following:
        // 1) contact the next provider we haven't contacted yet
        // 2) if there is a match, get out right away otherwise repeat 1)
        while (providers.hasNext())
        {
            AuthorizationProvider provider = providers.next();
            // we haven't fetched authorization from this provider yet
            if (!user.authorizations().getProviderIds().contains(provider.getId()))
            {
                try
                {
                    provider.getAuthorizations(user, authorizationResult -> {
                        if (authorizationResult.failed())
                        {
                            LOGGER.warn("An error occurred getting authorization - providerId='{}'", provider.getId(), authorizationResult.cause());
                            // note that we don't 'record' the fact that we tried to fetch the authorization provider. therefore, it will be re-fetched later-on
                        }
                        checkOrFetchAuthorizations(promise, authorizationContext, providers);
                    });
                }
                catch (Exception e)
                {
                    // if getAuthorizations throws we want to handle this case
                    // otherwise the promise will be lost
                    LOGGER.error("Exception thrown while retrieving authorizations - providerId='{}'", provider.getId(), e);
                    checkOrFetchAuthorizations(promise, authorizationContext, providers);
                }
                // get out right now as the callback will decide what to do next
                return;
            }
        }
        // exhausted all authorization providers
        promise.fail(new HttpException(FORBIDDEN.code()));
    }

    protected Future<Boolean> isAdmin(List<String> identities)
    {
        if (identities.isEmpty())
        {
            return Future.succeededFuture(false);
        }
        List<Future<Boolean>> adminFutures = new ArrayList<>(identities.size());
        for (String identity : identities)
        {
            Future<Boolean> adminFuture
            = adminIdentityResolver.isAdmin(identity)
                                   .compose(adminValue -> adminValue
                                                          ? Future.succeededFuture() : Future.failedFuture("Not admin"));
            adminFutures.add(adminFuture);
        }
        return Future.any(adminFutures)
                     .compose(success -> Future.succeededFuture(true),
                              failure -> Future.succeededFuture(false));
    }
}
