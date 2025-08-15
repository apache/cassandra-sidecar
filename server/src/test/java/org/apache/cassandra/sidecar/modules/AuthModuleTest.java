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

package org.apache.cassandra.sidecar.modules;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.acl.authentication.AuthenticationHandlerFactory;
import org.apache.cassandra.sidecar.acl.authentication.AuthenticationHandlerFactoryRegistry;
import org.apache.cassandra.sidecar.acl.authentication.MutualTlsAuthenticationHandlerFactory;
import org.apache.cassandra.sidecar.acl.authorization.RoleBasedAuthorizationProvider;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.ParameterizedClassConfiguration;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link AuthModule}
 */
class AuthModuleTest
{
    @Test
    void testAuthorizationProviderWithSchemaEnabled()
    {
        AuthModule authModule = new AuthModule();
        SidecarConfiguration mockSidecarConfig = createSidecarConfiguration(true, true, true);
        
        // Should not throw when schema is enabled
        authModule.authorizationProvider(mockSidecarConfig, null);
    }

    @Test
    void testRoleBasedAuthorizationProviderWithSchemaDisabled()
    {
        AuthModule authModule = new AuthModule();
        SidecarConfiguration mockSidecarConfig = createSidecarConfiguration(true, true, false);
        
        // Should throw when RoleBasedAuthorizationProvider is used but schema is disabled
        assertThatThrownBy(() -> authModule.authorizationProvider(mockSidecarConfig, null))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage(RoleBasedAuthorizationProvider.class.getName() + 
                   " requires sidecar schema to be enabled for role permissions storage");
    }

    @Test
    void testChainAuthHandlerValidatesPrerequisites()
    {
        AuthModule authModule = new AuthModule();
        Vertx mockVertx = mock(Vertx.class);

        SidecarConfiguration mockSidecarConfig = createSidecarConfiguration(true, false, false);

        AuthenticationHandlerFactoryRegistry mockRegistry = mock(AuthenticationHandlerFactoryRegistry.class);
        AuthenticationHandlerFactory mtlsFactory = new MutualTlsAuthenticationHandlerFactory(null, null);
        when(mockRegistry.getFactory(any())).thenReturn(mtlsFactory);
        
        // Should throw ConfigurationException when factory validates prerequisites
        assertThatThrownBy(() -> authModule.chainAuthHandler(mockVertx, mockSidecarConfig, mockRegistry))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("mTLS auth requires sidecar schema to be enabled for role processing");
    }

    private SidecarConfiguration createSidecarConfiguration(boolean authenticationEnabled,
                                                            boolean authorizationEnabled,
                                                            boolean schemaEnabled)
    {
        SidecarConfiguration mockSidecarConfig = mock(SidecarConfiguration.class);
        ServiceConfiguration mockServiceConfig = mock(ServiceConfiguration.class);
        SchemaKeyspaceConfiguration mockSchemaConfig = mock(SchemaKeyspaceConfiguration.class);
        AccessControlConfiguration mockAccessControlConfig = mock(AccessControlConfiguration.class);
        ParameterizedClassConfiguration mockAuthenticatorConfig = mock(ParameterizedClassConfiguration.class);
        ParameterizedClassConfiguration mockAuthorizerConfig = mock(ParameterizedClassConfiguration.class);
        
        when(mockSidecarConfig.serviceConfiguration()).thenReturn(mockServiceConfig);
        when(mockServiceConfig.schemaKeyspaceConfiguration()).thenReturn(mockSchemaConfig);
        when(mockSchemaConfig.isEnabled()).thenReturn(schemaEnabled);
        when(mockSidecarConfig.accessControlConfiguration()).thenReturn(mockAccessControlConfig);
        when(mockAccessControlConfig.enabled()).thenReturn(true);

        if (authenticationEnabled)
        {
            when(mockAccessControlConfig.authenticatorsConfiguration()).thenReturn(List.of(mockAuthenticatorConfig));
            when(mockAuthenticatorConfig.className()).thenReturn(MutualTlsAuthenticationHandlerFactory.class.getName());
        }

        if (authorizationEnabled)
        {
            when(mockAccessControlConfig.authorizerConfiguration()).thenReturn(mockAuthorizerConfig);
            when(mockAuthorizerConfig.className()).thenReturn(RoleBasedAuthorizationProvider.class.getName());
        }
        return mockSidecarConfig;
    }
}
