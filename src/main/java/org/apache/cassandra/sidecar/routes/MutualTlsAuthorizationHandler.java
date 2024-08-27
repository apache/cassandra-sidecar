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
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.AndAuthorization;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.AuthorizationContext;
import io.vertx.ext.auth.authorization.AuthorizationProvider;
import io.vertx.ext.auth.authorization.RoleBasedAuthorization;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthorizationHandler;
import org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions;
import org.apache.cassandra.sidecar.auth.authorization.RequiredPermissionsProvider;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler for authorization in MutualTLS to check if a user has the correct authorizations
 */
public class MutualTlsAuthorizationHandler implements AuthorizationHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MutualTlsAuthorizationHandler.class);
    private final List<AuthorizationProvider> providers;
    private final AndAuthorization superuserAuthorizations;
    RequiredPermissionsProvider requiredPermissionsProvider;

    @Inject
    public MutualTlsAuthorizationHandler(RequiredPermissionsProvider requiredPermissionsProvider)
    {
        this.requiredPermissionsProvider = requiredPermissionsProvider;
        providers = new ArrayList<>();
        superuserAuthorizations = AndAuthorization.create();
        for (MutualTlsPermissions perm : MutualTlsPermissions.ALL)
        {
            superuserAuthorizations.addAuthorization(RoleBasedAuthorization.create(perm.name()).setResource("ALL KEYSPACES"));
        }
    }

    /**
     * Adds a provider that shall be used to retrieve the required authorizations for the user to attest.
     * Multiple calls are allowed to retrieve authorizations from many sources.
     *
     * @param authorizationProvider a provider.
     * @return fluent self.
     */
    public AuthorizationHandler addAuthorizationProvider(AuthorizationProvider authorizationProvider)
    {
        this.providers.add(authorizationProvider);
        return this;
    }

    /**
     * Provide a simple handler to extract needed variables.
     * As it may be useful to allow/deny access based on the value of a request param one can do:
     * {@code (routingCtx, authCtx) -> authCtx.variables().addAll(routingCtx.request().params()) }
     * <p>
     * Or for example the remote address:
     * {@code (routingCtx, authCtx) -> authCtx.result.variables().add(VARIABLE_REMOTE_IP, routingCtx.request().connection().remoteAddress()) }
     *
     * Not used because we do not intend perform validation based on request variables for AuthZ
     *
     * @param handler a bi consumer.
     * @return fluent self.
     */
    public AuthorizationHandler variableConsumer(BiConsumer<RoutingContext, AuthorizationContext> handler)
    {
        // We do not intend to perform request params validations for AuthZ
        return null;
    }

    private void verifyUserPermissions(RoutingContext event, Iterator<AuthorizationProvider> providerIterator, Authorization requiredAuthorizations)
    {
        while (providerIterator.hasNext())
        {
            AuthorizationProvider provider = providerIterator.next();
            User user = event.user();
            if (user != null && !user.authorizations().getProviderIds().contains(provider.getId()))
            {
                provider.getAuthorizations(event.user(), (authorizationResult) -> {
                    if (authorizationResult.failed())
                    {
                        LOGGER.warn("An error occurred getting authorization - providerID: " + provider.getId(), authorizationResult.cause());
                    }
                });
                if (requiredAuthorizations.match(event.user()) || this.superuserAuthorizations.match(event.user()))
                {
                    event.next();
                    return;
                }
            }
        }
        event.fail(wrapHttpException(HttpResponseStatus.UNAUTHORIZED, "Not Authorized"));
    }

    /**
     * Something has happened, so handle it.
     *
     * @param event the event to handle
     */
    public void handle(RoutingContext event)
    {
        if (event.user() == null)
        {
            event.fail(new RoutingContextUtils.RoutingContextException("No user provided"));
        }

        Iterator<AuthorizationProvider> providerIterator = providers.iterator();

        String placeholderPath = event.normalizedPath();
        for (String placeholder : event.pathParams().keySet())
        {
            placeholderPath = placeholderPath.replaceFirst(event.pathParam(placeholder), ":" + placeholder);
        }

        String verb = event.request().method().name();

        AndAuthorization requiredAuthorizations =
        requiredPermissionsProvider.requiredPermissions(verb + " " + placeholderPath, event.pathParams());

        if (requiredAuthorizations == null)
        {
            requiredAuthorizations = AndAuthorization.create();
        }

        verifyUserPermissions(event, providerIterator, requiredAuthorizations);
    }
}
