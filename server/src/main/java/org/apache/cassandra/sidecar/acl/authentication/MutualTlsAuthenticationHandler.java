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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.CertificateCredentials;
import io.vertx.ext.auth.mtls.MutualTlsAuthentication;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.impl.AuthenticationHandlerImpl;
import org.apache.cassandra.sidecar.acl.IdentityToRoleCache;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.apache.cassandra.sidecar.utils.AuthUtils.CASSANDRA_ROLES_ATTRIBUTE_NAME;
import static org.apache.cassandra.sidecar.utils.AuthUtils.extractIdentities;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler for verifying user certificates for Mutual TLS authentication. {@link MutualTlsAuthenticationHandler} can be
 * chained with other {@link io.vertx.ext.web.handler.AuthenticationHandler} implementations.
 */
public class MutualTlsAuthenticationHandler extends AuthenticationHandlerImpl<MutualTlsAuthentication>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MutualTlsAuthenticationHandler.class);
    private final IdentityToRoleCache identityToRoleCache;

    public MutualTlsAuthenticationHandler(MutualTlsAuthentication authProvider,
                                          IdentityToRoleCache identityToRoleCache)
    {
        super(authProvider);
        this.identityToRoleCache = identityToRoleCache;
    }

    @Override
    public void authenticate(RoutingContext ctx, Handler<AsyncResult<User>> handler)
    {
        if (!ctx.request().isSSL())
        {
            ctx.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end();
            return;
        }

        CertificateCredentials certificateCredentials = CertificateCredentials.fromHttpRequest(ctx.request());

        authProvider.authenticate(certificateCredentials)
                    .recover(cause -> { // converts any exception to unauthorized http exception
                        throw wrapHttpException(UNAUTHORIZED, cause);
                    })
                    .andThen(authN-> {
                        if (authN.failed())
                        {
                            handler.handle(Future.failedFuture(wrapHttpException(UNAUTHORIZED, authN.cause())));
                            return;
                        }

                        List<String> identities = extractIdentities(authN.result());
                        List<String> roles = extractCassandraRoles(identities);
                        if (!roles.isEmpty())
                        {
                            authN.result().attributes().put(CASSANDRA_ROLES_ATTRIBUTE_NAME, roles);
                        }
                        handler.handle(authN);
                    });
    }

    private List<String> extractCassandraRoles(List<String> identities)
    {
        List<String> roles = new ArrayList<>();
        try
        {
            for (String identity : identities)
            {
                String role = identityToRoleCache.get(identity);
                if (role != null)
                {
                    roles.add(role);
                }
            }
        }
        catch (Exception e)
        {
            LOGGER.debug("Could not retrieve roles associated with the identities", e);
        }
        return roles;
    }
}
