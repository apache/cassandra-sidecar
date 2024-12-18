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

import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.ext.auth.oauth2.OAuth2Auth;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.yaml.TestServiceConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

import static org.apache.cassandra.sidecar.acl.authentication.JwtAuthenticationHandlerFactory.CLIENT_ID_PARAM_KEY;
import static org.apache.cassandra.sidecar.acl.authentication.JwtAuthenticationHandlerFactory.SCOPES_SUPPORTED_PARAM_KEY;
import static org.apache.cassandra.sidecar.acl.authentication.JwtAuthenticationHandlerFactory.SITE_PARAM_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Test for {@link JwtAuthenticationHandlerFactory}
 */
class JWTAuthenticationHandlerFactoryTest
{
    Vertx vertx = Vertx.vertx();
    AccessControlConfiguration mockConfig = mock(AccessControlConfiguration.class);
    JwtRoleProcessor mockRoleProcessor = mock(JwtRoleProcessor.class);

    @Test
    void testHandlerCreatedWithSuffix()
    {
        ServiceConfiguration serviceConfig = TestServiceConfiguration.newInstance();
        ExecutorPools pool = new ExecutorPools(vertx, serviceConfig);

        OAuth2Auth mockAuthProvider = mock(OAuth2Auth.class);
        JwtAuthenticationHandlerFactory spyFactory = spy(new JwtAuthenticationHandlerFactory(mockRoleProcessor, pool));
        doReturn(mockAuthProvider).when(spyFactory).getAuthenticationProvider(any(), any());
        assertThat(spyFactory.create(vertx, mockConfig, Map.of(SITE_PARAM_KEY,
                                                               "http:authorize.com/.well-known/openid-configuration",
                                                               CLIENT_ID_PARAM_KEY,
                                                               "any",
                                                               SCOPES_SUPPORTED_PARAM_KEY,
                                                               "email,phone")))
        .isInstanceOf(JwtOAuth2AuthenticationHandler.class);
    }

    @Test
    void testInvalidParameters()
    {
        ExecutorPools mockPool = mock(ExecutorPools.class);
        JwtAuthenticationHandlerFactory factory = new JwtAuthenticationHandlerFactory(mockRoleProcessor, mockPool);
        assertThatThrownBy(() -> factory.create(vertx, mockConfig, null))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("Parameters cannot be null for JwtAuthenticationHandlerFactory");

        assertThatThrownBy(() -> factory.create(vertx, mockConfig, Map.of()))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("Missing site parameter for JwtOAuth2AuthenticationHandler creation");

        assertThatThrownBy(() -> factory.create(vertx, mockConfig, Map.of("site", "x.com")))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("Missing client_id parameter for JwtOAuth2AuthenticationHandler creation");
    }
}
