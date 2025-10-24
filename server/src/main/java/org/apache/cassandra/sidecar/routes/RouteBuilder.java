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

package org.apache.cassandra.sidecar.routes;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import com.github.benmanes.caffeine.cache.AsyncCache;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.auth.authorization.AndAuthorization;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.AuthorizationContext;
import io.vertx.ext.auth.authorization.AuthorizationProvider;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthorizationHandler;
import io.vertx.ext.web.handler.BodyHandler;
import org.apache.cassandra.sidecar.acl.AdminIdentityResolver;
import org.apache.cassandra.sidecar.acl.authorization.AuthorizationCacheKey;
import org.apache.cassandra.sidecar.acl.authorization.AuthorizationParameterValidateHandler;
import org.apache.cassandra.sidecar.acl.authorization.CachedAuthorizationHandler;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.KEYSPACE;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.TABLE;
import static org.apache.cassandra.sidecar.routes.RoutingContextUtils.SC_QUALIFIED_TABLE_NAME;

/**
 * Builder for building authorized routes
 */
public class RouteBuilder
{
    private final AccessControlConfiguration accessControlConfiguration;
    private final AuthorizationProvider authorizationProvider;
    private final AdminIdentityResolver adminIdentityResolver;
    private final AuthorizationParameterValidateHandler authZParameterValidateHandler;
    private final SidecarMetrics sidecarMetrics;
    private final AsyncCache<AuthorizationCacheKey, Boolean> authorizationCache;
    private boolean setBodyHandler;
    private boolean accessProtected = true;
    private final List<Handler<RoutingContext>> handlers = new ArrayList<>();

    private RouteBuilder(Factory factory)
    {
        this.accessControlConfiguration = factory.accessControlConfiguration;
        this.authorizationProvider = factory.authorizationProvider;
        this.adminIdentityResolver = factory.adminIdentityResolver;
        this.authZParameterValidateHandler = factory.authZParameterValidateHandler;
        this.sidecarMetrics = factory.sidecarMetrics;
        this.authorizationCache = factory.authorizationCache;
    }

    /**
     * Sets if BodyHandler should be created for the route.
     *
     * @param setBodyHandler boolean flag indicating if route requires BodyHandler
     * @return a reference to {@link RouteBuilder} for chaining
     */
    public RouteBuilder setBodyHandler(Boolean setBodyHandler)
    {
        this.setBodyHandler = setBodyHandler;
        return this;
    }

    /**
     * Adds handler to handler chain of route. Handlers are ordered, they are called in order they are set in chain.
     *
     * @param handler handler for route
     * @return a reference to {@link RouteBuilder} for chaining
     */
    public RouteBuilder handler(Handler<RoutingContext> handler)
    {
        this.handlers.add(handler);
        return this;
    }

    /**
     * Indicate the route does not require authorization.
     * <p>Note that the method is to be accessed by {@link Factory} only
     * @return a reference to {@link RouteBuilder} for chaining
     */
    RouteBuilder setUnauthorized()
    {
        this.accessProtected = false;
        return this;
    }

    /**
     * Builds an authorized route. Adds {@link io.vertx.ext.web.handler.AuthorizationHandler} at top of handler
     * chain if access control is enabled.
     */
    public SettableVertxRoute build()
    {
        return new SettableVertxRoute()
        {
            @Override
            public void mountTo(Router router)
            {
                HttpMethod method = httpMethod();
                String endpoint = routeURI();
                Objects.requireNonNull(method, "Http method must be set");
                Preconditions.checkArgument(endpoint != null && !endpoint.isEmpty(), "Endpoint must be set");
                Preconditions.checkArgument(!handlers.isEmpty(), "Handler chain can not be empty");

                Route route = router.route(method, endpoint);

                // BodyHandler must be placed before Authorization handler
                // See io.vertx.ext.web.impl.RouteState.Priority
                if (setBodyHandler)
                {
                    route.handler(BodyHandler.create());
                }

                if (accessControlConfiguration.enabled())
                {
                    // authorization handler added before route specific handler chain
                    AuthorizationHandler authorizationHandler =
                    new CachedAuthorizationHandler(accessControlConfiguration, authZParameterValidateHandler,
                                                   adminIdentityResolver, requiredAuthorization(), sidecarMetrics,
                                                   authorizationCache);
                    authorizationHandler.addAuthorizationProvider(authorizationProvider);
                    authorizationHandler.variableConsumer(routeGenericVariableConsumer());

                    route.handler(authorizationHandler);
                }

                handlers.forEach(route::handler);
            }
        };
    }

    @VisibleForTesting
    public Authorization requiredAuthorization()
    {
        Set<Authorization> requiredAuthorizations = handlers
                                                    .stream()
                                                    .filter(handler -> handler instanceof AccessProtected)
                                                    .map(handler -> (AccessProtected) handler)
                                                    .flatMap(handler -> handler.requiredAuthorizations().stream())
                                                    .collect(Collectors.toSet());
        if (accessProtected && requiredAuthorizations.isEmpty())
        {
            throw new ConfigurationException("Authorized route must have required authorizations declared");
        }
        if (!accessProtected && !requiredAuthorizations.isEmpty())
        {
            throw new ConfigurationException("Unauthorized route must not have required authorizations declared");
        }
        AndAuthorization andAuthorization = AndAuthorization.create();
        requiredAuthorizations.forEach(andAuthorization::addAuthorization);
        return andAuthorization;
    }

    @VisibleForTesting
    public BiConsumer<RoutingContext, AuthorizationContext> routeGenericVariableConsumer()
    {
        return (routingCtx, authZContext) -> {
            Optional<QualifiedTableName> optional = RoutingContextUtils.getAsOptional(routingCtx, SC_QUALIFIED_TABLE_NAME);
            String keyspace = null;
            String table = null;
            if (optional.isPresent())
            {
                QualifiedTableName qualifiedTableName = optional.get();
                keyspace = qualifiedTableName.keyspace();
                table = qualifiedTableName.tableName();
            }

            if (keyspace != null)
            {
                authZContext.variables().add(KEYSPACE, keyspace);
            }
            if (table != null)
            {
                authZContext.variables().add(TABLE, table);
            }
        };
    }

    /**
     * Factory to produce {@link RouteBuilder}
     */
    public static class Factory
    {
        private final AccessControlConfiguration accessControlConfiguration;
        private final AuthorizationProvider authorizationProvider;
        private final AdminIdentityResolver adminIdentityResolver;
        private final AuthorizationParameterValidateHandler authZParameterValidateHandler;
        private final SidecarMetrics sidecarMetrics;
        private final AsyncCache<AuthorizationCacheKey, Boolean> authorizationCache;

        public Factory(AccessControlConfiguration accessControlConfiguration,
                       AuthorizationProvider authorizationProvider,
                       AdminIdentityResolver adminIdentityResolver,
                       AuthorizationParameterValidateHandler authZParameterValidateHandler,
                       SidecarMetrics sidecarMetrics,
                       AsyncCache<AuthorizationCacheKey, Boolean> authorizationCache)
        {
            this.accessControlConfiguration = accessControlConfiguration;
            this.authorizationProvider = authorizationProvider;
            this.adminIdentityResolver = adminIdentityResolver;
            this.authZParameterValidateHandler = authZParameterValidateHandler;
            this.sidecarMetrics = sidecarMetrics;
            this.authorizationCache = authorizationCache;
        }

        public RouteBuilder builderForRoute()
        {
            return new RouteBuilder(this);
        }

        public VertxRoute buildRouteWithHandler(Handler<RoutingContext> handler)
        {
            return builderForRoute().handler(handler).build();
        }

        public RouteBuilder builderForUnauthorizedRoute()
        {
            return builderForRoute().setUnauthorized();
        }
    }
}
