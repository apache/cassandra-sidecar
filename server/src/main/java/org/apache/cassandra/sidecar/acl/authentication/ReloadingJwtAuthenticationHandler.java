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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.auth.oauth2.OAuth2Options;
import io.vertx.ext.auth.oauth2.providers.OpenIDConnectAuth;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.handler.impl.AuthenticationHandlerImpl;
import io.vertx.ext.web.handler.impl.AuthenticationHandlerInternal;
import io.vertx.ext.web.handler.impl.JWTAuthHandlerImpl;
import io.vertx.ext.web.handler.impl.OAuth2AuthHandlerImpl;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.metrics.server.AuthMetrics;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.jetbrains.annotations.NotNull;

import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.apache.cassandra.sidecar.common.http.SidecarHttpHeaderNames.AUTH_ROLE;
import static org.apache.cassandra.sidecar.common.utils.StringUtils.isNotEmpty;
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

    @VisibleForTesting
    final AtomicReference<AuthenticationHandlerInternal> delegateHandler = new AtomicReference<>();
    private final Vertx vertx;
    private final JwtParameters jwtParameters;
    private final JwtRoleProcessor roleProcessor;
    private final AuthMetrics metrics;

    public ReloadingJwtAuthenticationHandler(Vertx vertx,
                                             JwtParameters jwtParameters,
                                             JwtRoleProcessor roleProcessor,
                                             PeriodicTaskExecutor periodicTaskExecutor,
                                             AuthMetrics metrics)
    {
        super(NoOpAuthenticationProvider.INSTANCE);
        this.vertx = vertx;
        this.jwtParameters = jwtParameters;
        this.roleProcessor = roleProcessor;
        this.metrics = metrics;
        if (jwtParameters.jwtAuthType().equals(JwtParameters.AuthType.STATELESS))
        {
            periodicTaskExecutor.schedule(buildPeriodicStatelessJwtRefreshTask(vertx, jwtParameters));
        }
        else if (jwtParameters.jwtAuthType().equals(JwtParameters.AuthType.OAUTH))
        {
            periodicTaskExecutor.schedule(new OAuth2AuthHandlerGenerateTask());
        }
        else
        {
            throw new IllegalStateException("Unsupported JWT Auth Type: " + jwtParameters.jwtAuthType());
        }
    }

    private @NotNull PeriodicStatelessJwtRefreshTask buildPeriodicStatelessJwtRefreshTask(Vertx vertx, JwtParameters jwtParameters)
    {
        WebClientOptions options = new WebClientOptions()
                .setSsl(jwtParameters.site().startsWith("https"));

        if (jwtParameters.keystorePath() != null)
        {
            if (jwtParameters.keystorePassword().isEmpty())
            {
                throw new IllegalArgumentException("JWT keystore password required when setting JWT keystore path.");
            }
            options.setKeyStoreOptions(new JksOptions()
                    .setPath(jwtParameters.keystorePath())
                    .setPassword(jwtParameters.keystorePassword())
            );
        }
        if (jwtParameters.truststorePath() != null)
        {
            if (jwtParameters.truststorePassword().isEmpty())
            {
                throw new IllegalArgumentException("JWT truststore password required when setting JWT truststore path.");
            }
            options.setTrustStoreOptions(new JksOptions()
                    .setPath(jwtParameters.truststorePath())
                    .setPassword(jwtParameters.truststorePassword())
            );
        }
        WebClient webClient = WebClient.create(vertx, options);
        return new PeriodicStatelessJwtRefreshTask(webClient);
    }

    @Override
    public void authenticate(RoutingContext context, Handler<AsyncResult<User>> handler)
    {
        AuthenticationHandlerInternal authHandler = delegateHandler.get();
        if (authHandler == null)
        {
            handler.handle(Future.failedFuture(wrapHttpException(SERVICE_UNAVAILABLE,
                                                                 "JWT authentication handler unavailable")));
            return;
        }

        authHandler.authenticate(context, authN -> {
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
            String roleIntended = context.request().getHeader(AUTH_ROLE);

            if (isNotEmpty(roleIntended) && !roles.contains(roleIntended))
            {
                String errMsg = String.format("User not authorized for role %s", roleIntended);
                handler.handle(Future.failedFuture(wrapHttpException(UNAUTHORIZED, errMsg)));
                return;
            }

            List<String> rolesToAdd = isNotEmpty(roleIntended) ? List.of(roleIntended) : roles;
            user.attributes().put(CASSANDRA_ROLES_ATTRIBUTE_NAME, rolesToAdd);
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
        public DurationSpec initialDelay()
        {
            return SecondBoundConfiguration.ZERO;
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
                promise.complete();
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

    private class PeriodicStatelessJwtRefreshTask implements PeriodicTask
    {
        private final String taskName = String.format("PeriodicStatelessJwtRefreshTask_%s", jwtParameters.site());
        private final WebClient webClient;

        private PeriodicStatelessJwtRefreshTask(WebClient webClient)
        {
            this.webClient = webClient;
        }

        @Override
        public DurationSpec delay()
        {
            return jwtParameters.configDiscoverInterval();
        }

        @Override
        public DurationSpec initialDelay()
        {
            return SecondBoundConfiguration.ZERO;
        }


        @Override
        public void execute(Promise<Void> promise)
        {
            if (!jwtParameters.enabled())
            {
                delegateHandler.set(null);
                promise.complete();
                return;
            }
            String jwtPemUri = jwtParameters.site();
            HttpRequest<Buffer> request = webClient.getAbs(jwtPemUri);
            if (jwtParameters.pemProviderJwt() != null)
            {
                request.bearerTokenAuthentication(jwtParameters.pemProviderJwt());
            }

            request.send()
                    .onSuccess(ar -> {
                        String pem = ar.bodyAsString();
                        JWTAuthOptions jwtAuthOptions = new JWTAuthOptions()
                                                        .addPubSecKey(new PubSecKeyOptions()
                                                                      .setAlgorithm("RS256")
                                                                      .setBuffer(pem));
                        JWTAuth auth = JWTAuth.create(vertx, jwtAuthOptions);
                        AuthenticationHandlerInternal jwtAuthHandlerDelegate = new JWTAuthHandlerImpl(auth, null);
                        delegateHandler.set(jwtAuthHandlerDelegate);
                        metrics.jwtPemRefreshSuccesses.metric.inc();
                        promise.complete();
                    }).onFailure(cause -> {
                        LOGGER.error("Error encountered when refreshing stateless JWT PEM material.", cause);
                        metrics.jwtPemRefreshFailures.metric.inc();
                        promise.fail(cause);
                    });
        }

        @Override
        public String name()
        {
            return taskName;
        }
    }
}
