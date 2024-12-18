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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.oauth2.OAuth2Auth;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.handler.impl.OAuth2AuthHandlerImpl;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.apache.cassandra.sidecar.utils.AuthUtils.CASSANDRA_ROLES_ATTRIBUTE_NAME;
import static org.apache.cassandra.sidecar.utils.AuthUtils.CASSANDRA_ROLE_SPLITTER;

/**
 * {@link JwtOAuth2AuthenticationHandler} validates JWT token of a user. It can be chained with other
 * {@link io.vertx.ext.web.handler.AuthenticationHandler} implementations. Validation of the token passes if a
 * cassandra role is associated with the user.
 */
public class JwtOAuth2AuthenticationHandler extends OAuth2AuthHandlerImpl
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JwtOAuth2AuthenticationHandler.class);
    private final JwtRoleProcessor roleProcessor;

    public JwtOAuth2AuthenticationHandler(Vertx vertx,
                                          OAuth2Auth authProvider,
                                          List<String> scopes,
                                          JwtRoleProcessor roleProcessor)
    {
        super(vertx, authProvider, null);
        this.roleProcessor = roleProcessor;
        withScopes(scopes);
    }

    @Override
    public void authenticate(RoutingContext context, Handler<AsyncResult<User>> handler)
    {
        super.authenticate(context, authN -> {
            if (authN.failed())
            {
                handler.handle(Future.failedFuture(new HttpException(UNAUTHORIZED.code(), authN.cause())));
                return;
            }

            User user = authN.result();
            JsonObject decodedToken = user.attributes().containsKey("accessToken")
                                      ? user.attributes().getJsonObject("accessToken")
                                      : user.attributes().getJsonObject("idToken");

            if (decodedToken == null)
            {
                handler.handle(Future.failedFuture(new HttpException(UNAUTHORIZED.code(),
                                                                     "Could not process decoded JWT token")));
                return;
            }

            try
            {
                List<String> roles = roleProcessor.processRoles(decodedToken);
                if (!roles.isEmpty())
                {
                    user.attributes().put(CASSANDRA_ROLES_ATTRIBUTE_NAME, String.join(CASSANDRA_ROLE_SPLITTER, roles));
                }
                handler.handle(Future.succeededFuture(user));
            }
            catch (Exception e)
            {
                LOGGER.debug("Error processing cassandra role from JWT token", e);
                handler.handle(Future.failedFuture(new HttpException(UNAUTHORIZED.code(),
                                                                     "Error processing cassandra role from token")));
            }
        });
    }
}
