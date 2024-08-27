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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.mtls.MutualTlsCredentials;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * A handler that provides MutualTLS Authentication for Cassandra Sidecar API endpoints
 */
@Singleton
public class MutualTlsAuthenticationHandler implements AuthenticationHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MutualTlsAuthenticationHandler.class);

    protected final AuthenticationProvider authProvider;

    @Inject
    public MutualTlsAuthenticationHandler(AuthenticationProvider authProvider)
    {
        this.authProvider = authProvider;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handle(RoutingContext context)
    {
        MutualTlsCredentials credentials = new MutualTlsCredentials(context.request());

        authProvider.authenticate(credentials, authN -> {
            if (authN.succeeded())
            {
                if (authN.result() == null)
                {
                    LOGGER.warn("No user present at authentication");
                    context.fail(wrapHttpException(HttpResponseStatus.UNAUTHORIZED, "No user presented"));
                }
                context.setUser(authN.result());
                context.next();
            }
            else
            {
                context.fail(wrapHttpException(HttpResponseStatus.UNAUTHORIZED, "Not Authenticated"));
            }
        });
    }
}
