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
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link JwtAuthenticationHandlerFactory}
 */
class JWTAuthenticationHandlerFactoryTest
{
    Vertx mockVertx = mock(Vertx.class);
    AccessControlConfiguration mockConfig = mock(AccessControlConfiguration.class);
    JwtRoleProcessor mockRoleProcessor = mock(JwtRoleProcessor.class);

    @Test
    void testInvalidParameters()
    {
        PeriodicTaskExecutor mockTaskExecutor = mock(PeriodicTaskExecutor.class);
        JwtAuthenticationHandlerFactory factory = new JwtAuthenticationHandlerFactory(mockRoleProcessor, mockTaskExecutor);
        assertThatThrownBy(() -> factory.create(mockVertx, mockConfig, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("JWT parameters can not be null");

        assertThatThrownBy(() -> factory.create(mockVertx, mockConfig, Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing site JWT parameter");

        assertThatThrownBy(() -> factory.create(mockVertx, mockConfig, Map.of("site", "www.apache.org")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing client_id JWT parameter");
    }

    @Test
    void testValidatePrerequisitesWithSchemaDisabled()
    {
        PeriodicTaskExecutor mockTaskExecutor = mock(PeriodicTaskExecutor.class);
        JwtAuthenticationHandlerFactory factory = new JwtAuthenticationHandlerFactory(mockRoleProcessor, mockTaskExecutor);
        
        SidecarConfiguration mockSidecarConfig = mock(SidecarConfiguration.class);
        ServiceConfiguration mockServiceConfig = mock(ServiceConfiguration.class);
        SchemaKeyspaceConfiguration mockSchemaConfig = mock(SchemaKeyspaceConfiguration.class);
        
        when(mockSidecarConfig.serviceConfiguration()).thenReturn(mockServiceConfig);
        when(mockServiceConfig.schemaKeyspaceConfiguration()).thenReturn(mockSchemaConfig);
        when(mockSchemaConfig.isEnabled()).thenReturn(false);
        
        // Should throw exception when schema is disabled
        assertThatThrownBy(() -> factory.validatePrerequisites(mockSidecarConfig))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("JwtAuthenticationHandlerFactory requires Sidecar schema to be enabled for role processing");
    }
}
