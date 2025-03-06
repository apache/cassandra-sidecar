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

import java.util.Map;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Vertx;
import io.vertx.ext.web.handler.impl.AuthenticationHandlerInternal;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;

/**
 * {@link AuthenticationHandlerFactory} implementation for providing JWT authentication handler
 * {@link ReloadingJwtAuthenticationHandler}. {@link ReloadingJwtAuthenticationHandler} uses Vert.x's
 * {@link io.vertx.ext.web.handler.impl.OAuth2AuthHandlerImpl} which does
 * {@link io.vertx.ext.auth.authentication.TokenCredentials} validation.
 */
@Singleton
public class JwtAuthenticationHandlerFactory implements AuthenticationHandlerFactory
{
    private final JwtRoleProcessor roleProcessor;
    private final PeriodicTaskExecutor periodicTaskExecutor;

    @Inject
    public JwtAuthenticationHandlerFactory(JwtRoleProcessor roleProcessor,
                                           PeriodicTaskExecutor periodicTaskExecutor)
    {
        this.roleProcessor = roleProcessor;
        this.periodicTaskExecutor = periodicTaskExecutor;
    }

    @Override
    public AuthenticationHandlerInternal create(Vertx vertx,
                                                AccessControlConfiguration accessControlConfiguration,
                                                Map<String, String> parameters) throws ConfigurationException
    {
        JwtParameters jwtParameters = parameterParser(parameters);

        return new ReloadingJwtAuthenticationHandler(vertx,
                                                     jwtParameters,
                                                     roleProcessor,
                                                     periodicTaskExecutor);
    }

    protected JwtParameters parameterParser(Map<String, String> parameters)
    {
        return new JwtParameterExtractor(parameters);
    }
}
