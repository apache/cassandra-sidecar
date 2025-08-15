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

import io.vertx.core.Vertx;
import io.vertx.ext.web.handler.impl.AuthenticationHandlerInternal;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

/**
 * Factory class for creating {@link AuthenticationHandlerInternal} instances.
 */
public interface AuthenticationHandlerFactory
{
    /**
     * Creates instances of {@link AuthenticationHandlerInternal}. ChainAuthHandlerImpl in Vertx supports only
     * implementations of AuthenticationHandlerInternal. When ChainAuthHandlerImpl is fixed to handle generic
     * AuthenticationHandler implementations, we can update this method to return
     * implementations of AuthenticationHandler.
     *
     * @param vertx                         instance of Vertx
     * @param accessControlConfiguration    Configuration for creating authentication handler
     * @param parameters                    Parameters for creating {@link AuthenticationHandlerInternal} implementation
     * @return a newly created instance of {@link AuthenticationHandlerInternal}.
     * @throws ConfigurationException if handler cannot be created
     */
    AuthenticationHandlerInternal create(Vertx vertx,
                                         AccessControlConfiguration accessControlConfiguration,
                                         Map<String, String> parameters) throws ConfigurationException;
    
    /**
     * Validates that this authentication handler factory can be used with the given configuration.
     *
     * @param sidecarConfiguration the sidecar configuration to validate against
     * @throws ConfigurationException if the authentication handler factory cannot be used with the given configuration
     */
    default void validatePrerequisites(SidecarConfiguration sidecarConfiguration) throws ConfigurationException
    {
    }
}
