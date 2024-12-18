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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.oauth2.OAuth2Auth;
import io.vertx.ext.auth.oauth2.OAuth2Options;
import io.vertx.ext.auth.oauth2.providers.OpenIDConnectAuth;
import io.vertx.ext.web.handler.impl.AuthenticationHandlerInternal;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.jetbrains.annotations.VisibleForTesting;

import static org.apache.cassandra.sidecar.common.utils.StringUtils.isNotEmpty;
import static org.apache.cassandra.sidecar.common.utils.StringUtils.isNullOrEmpty;

/**
 * {@link AuthenticationHandlerFactory} implementation for providing JWT authentication handler
 * {@link JwtOAuth2AuthenticationHandler}. {@link JwtOAuth2AuthenticationHandler} uses Vert.x's
 * {@link io.vertx.ext.web.handler.impl.OAuth2AuthHandlerImpl} which does
 * {@link io.vertx.ext.auth.authentication.TokenCredentials} validation.
 */
@Singleton
public class JwtAuthenticationHandlerFactory implements AuthenticationHandlerFactory
{
    protected static final String SITE_PARAM_KEY = "site";
    protected static final String CLIENT_ID_PARAM_KEY = "client_id";
    protected static final String SCOPE_SEPARATOR_PARAM_KEY = "scope_separator";
    protected static final String SCOPES_SUPPORTED_PARAM_KEY = "scopes_supported";

    private static final Logger LOGGER = LoggerFactory.getLogger(JwtAuthenticationHandlerFactory.class);
    private static final String SITE_SUFFIX = "/.well-known/openid-configuration";
    private static final String DEFAULT_SCOPE_SEPARATOR = ",";

    private final JwtRoleProcessor roleProcessor;
    private final ExecutorPools executorPools;

    @Inject
    public JwtAuthenticationHandlerFactory(JwtRoleProcessor roleProcessor,
                                           ExecutorPools executorPools)
    {
        this.roleProcessor = roleProcessor;
        this.executorPools = executorPools;
    }

    @Override
    public AuthenticationHandlerInternal create(Vertx vertx,
                                                AccessControlConfiguration accessControlConfiguration,
                                                Map<String, String> parameters) throws ConfigurationException
    {
        validate(parameters);

        String site = removeSiteSuffix(parameters);
        OAuth2Options oAuth2Options = new OAuth2Options()
                                      .setSite(site)
                                      .setClientId(parameters.get(CLIENT_ID_PARAM_KEY));

        OAuth2Auth oauthProvider = getAuthenticationProvider(vertx, oAuth2Options);
        List<String> scopes = buildScopes(parameters);
        return new JwtOAuth2AuthenticationHandler(vertx, oauthProvider, scopes, roleProcessor);
    }

    void validate(Map<String, String> parameters) throws ConfigurationException
    {
        if (parameters == null)
        {
            throw new ConfigurationException("Parameters cannot be null for JwtAuthenticationHandlerFactory");
        }

        validateParameterPresence(parameters, SITE_PARAM_KEY);
        validateParameterPresence(parameters, CLIENT_ID_PARAM_KEY);
    }

    private void validateParameterPresence(Map<String, String> parameters, String paramKey)
    {
        if (!parameters.containsKey(paramKey) || isNullOrEmpty(parameters.get(paramKey)))
        {
            throw new ConfigurationException(String.format("Missing %s parameter for JwtOAuth2AuthenticationHandler creation",
                                                           paramKey));
        }
    }

    private String removeSiteSuffix(Map<String, String> parameters)
    {
        String site = parameters.get(SITE_PARAM_KEY);
        if(site.endsWith(SITE_SUFFIX))
        {
            LOGGER.info("Removing site suffix {}, it is added during OpenID discover", SITE_SUFFIX);
            return site.substring(0, site.length() - SITE_SUFFIX.length());
        }
        return site;
    }

    private List<String> buildScopes(Map<String, String> parameters)
    {
        List<String> scopes = new ArrayList<>();
        if (isNotEmpty(parameters.get(SCOPES_SUPPORTED_PARAM_KEY)))
        {
            String delimiter = isNotEmpty(parameters.get(SCOPE_SEPARATOR_PARAM_KEY))
                               ? parameters.get(SCOPE_SEPARATOR_PARAM_KEY)
                               : DEFAULT_SCOPE_SEPARATOR;
            scopes.addAll(Arrays.asList(parameters.get(SCOPES_SUPPORTED_PARAM_KEY).split(delimiter)));
        }
        return scopes;
    }

    @VisibleForTesting
    OAuth2Auth getAuthenticationProvider(Vertx vertx, OAuth2Options options)
    {
        Promise<OAuth2Auth> promise = Promise.promise();
        executorPools.service()
                     .executeBlocking(() -> OpenIDConnectAuth.discover(vertx, options))
                     .onSuccess(resp -> {
                         promise.complete(resp.result());
                     })
                     .onFailure(cause -> {
                         LOGGER.error("Error encountered during OpenID discovery", cause);
                         promise.fail(cause);
                     });
        return promise.future().toCompletionStage().toCompletableFuture().join();
    }
}
