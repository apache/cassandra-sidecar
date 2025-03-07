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

package org.apache.cassandra.sidecar.acl.authentication;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.oauth2.OAuth2Options;
import io.vertx.ext.auth.oauth2.providers.OpenIDConnectAuth;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.impl.AuthenticationHandlerImpl;
import io.vertx.ext.web.handler.impl.OAuth2AuthHandlerImpl;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;

import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.apache.cassandra.sidecar.utils.AuthUtils.CASSANDRA_ROLES_ATTRIBUTE_NAME;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * {@link ReloadingJwtAuthenticationHandler} validates JWT token of a user. It handles periodically calling
 * {@link OpenIDConnectAuth} discover to fetch latest configuration and handles reloading {@link OAuth2AuthHandlerImpl}.
 * It can be chained with other {@link io.vertx.ext.web.handler.AuthenticationHandler} implementations.
 */
public class ReloadingJwtAuthenticationHandler
extends AuthenticationHandlerImpl<ReloadingJwtAuthenticationHandler.NoOpAuthenticationProvider>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadingJwtAuthenticationHandler.class);

    AtomicReference<OAuth2AuthHandlerImpl> delegateHandler = new AtomicReference<>();

    private final Vertx vertx;
    private final JwtParameters jwtParameters;
    private final JwtRoleProcessor roleProcessor;

    public ReloadingJwtAuthenticationHandler(Vertx vertx,
                                             JwtParameters jwtParameters,
                                             JwtRoleProcessor roleProcessor,
                                             PeriodicTaskExecutor periodicTaskExecutor)
    {
        super(NoOpAuthenticationProvider.INSTANCE);
        this.vertx = vertx;
        this.jwtParameters = jwtParameters;
        this.roleProcessor = roleProcessor;

        periodicTaskExecutor.schedule(new OAuth2AuthHandlerGenerateTask());
    }

    @Override
    public void authenticate(RoutingContext context, Handler<AsyncResult<User>> handler)
    {
        OAuth2AuthHandlerImpl oAuth2AuthHandler = delegateHandler.get();
        if (oAuth2AuthHandler == null)
        {
            handler.handle(Future.failedFuture(wrapHttpException(SERVICE_UNAVAILABLE,
                                                                 "JWT authentication handler unavailable")));
            return;
        }

        oAuth2AuthHandler.authenticate(context, authN -> {
            if (authN.failed())
            {
                handler.handle(Future.failedFuture(wrapHttpException(UNAUTHORIZED, authN.cause())));
                return;
            }

            User user = authN.result();
            JsonObject decodedToken = user.attributes().containsKey("accessToken")
                                      ? user.attributes().getJsonObject("accessToken")
                                      : user.attributes().getJsonObject("idToken");

            if (decodedToken == null)
            {
                handler.handle(Future.failedFuture(wrapHttpException(UNAUTHORIZED,
                                                                     "Could not process decoded JWT token")));
                return;
            }

            List<String> roles = extractCassandraRoles(decodedToken);
            if (!roles.isEmpty())
            {
                user.attributes().put(CASSANDRA_ROLES_ATTRIBUTE_NAME, roles);
            }
            handler.handle(Future.succeededFuture(user));
        });
    }

    /**
     * {@link NoOpAuthenticationProvider} is used in {@link ReloadingJwtAuthenticationHandler}.
     * {@link ReloadingJwtAuthenticationHandler} delegates authenticating user to delegate handler.
     * Hence it uses no op authentication provider
     */
    protected static class NoOpAuthenticationProvider implements AuthenticationProvider
    {
        public static final NoOpAuthenticationProvider INSTANCE = new NoOpAuthenticationProvider();

        private NoOpAuthenticationProvider()
        {
        }

        @Override
        public void authenticate(JsonObject credentials, Handler<AsyncResult<User>> resultHandler)
        {
            resultHandler.handle(Future.succeededFuture());
        }
    }

    private List<String> extractCassandraRoles(JsonObject decodedToken)
    {
        try
        {
            return roleProcessor.processRoles(decodedToken);
        }
        catch (Exception e)
        {
            LOGGER.debug("Error processing cassandra role from JWT token", e);
        }
        return List.of();
    }

    /**
     * Periodic task to generate {@link OAuth2AuthHandlerImpl} with refreshed configuration from
     * {@link OpenIDConnectAuth} discover.
     */
    private class OAuth2AuthHandlerGenerateTask implements PeriodicTask
    {
        private final String taskName
        = String.format("OAuth2AuthHandlerGenerateTask_%s_%s", jwtParameters.site(), jwtParameters.clientId());

        @Override
        public DurationSpec delay()
        {
            return jwtParameters.configDiscoverInterval();
        }

        @Override
        public String name()
        {
            return taskName;
        }

        @Override
        public void execute(Promise<Void> promise)
        {
            if (!jwtParameters.enabled())
            {
                delegateHandler.set(null);
                return;
            }

            OAuth2Options options = new OAuth2Options().setSite(jwtParameters.site())
                                                       .setClientId(jwtParameters.clientId());

            OpenIDConnectAuth.discover(vertx, options)
                             .onSuccess(oAuthProvider -> {
                                 OAuth2AuthHandlerImpl handler = new OAuth2AuthHandlerImpl(vertx, oAuthProvider, null);
                                 if (!jwtParameters.scopes().isEmpty())
                                 {
                                     handler.withScopes(jwtParameters.scopes());
                                 }
                                 delegateHandler.set(handler);
                                 promise.complete();
                             })
                             .onFailure(cause -> {
                                 LOGGER.error("Error encountered during OpenID discovery", cause);
                                 promise.fail(cause);
                             });
        }
    }
}
