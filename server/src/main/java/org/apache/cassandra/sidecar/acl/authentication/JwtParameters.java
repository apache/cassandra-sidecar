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

import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;

/**
 * Interface defines JWT parameters needed for {@link ReloadingJwtAuthenticationHandler} handler creation
 */
public interface JwtParameters
{
    /**
     * @return boolean flag indicating if JWT authentication is enabled
     */
    boolean enabled();

    /**
     * @return site to dynamically retrieve the configuration information of an OpenID provider
     */
    String site();

    /**
     * @return clientId is a unique identifier used to identity applications/users
     */
    String clientId();

    /**
     * @return scopes granted to a user
     */
    List<String> scopes();

    /**
     * @return interval at which {@link io.vertx.ext.auth.oauth2.providers.OpenIDConnectAuth} discover is called to
     * dynamically retrieve configuration information of an OpenID provider.
     */
    SecondBoundConfiguration configDiscoverInterval();

    /**
     * @return The configured method of JWT authentication to use. Defaults to oauth if not supplied.
     */
    AuthType jwtAuthType();

    /**
     * @return Optional path to a keystore to provide mutual TLS to PEM public key provider service
     */
    String keystorePath();

    /**
     * @return Optional password for the provided keystore
     */
    String keystorePassword();

    /**
     * @return Optional path to a truststore to validate SSL certs from PEM public key provider service
     */
    String truststorePath();

    /**
     * @return Optional password for the provided truststore
     */
    String truststorePassword();

    /**
     * @return Optional JWT to authenticate to PEM public key provider service
     */
    String pemProviderJwt();


    /**
     * Supported types of JWT authentication. Today Oauth and Stateless JWT token authentication methods are supported.
     */
    enum AuthType
    {
        OAUTH,
        STATELESS
    }
}
