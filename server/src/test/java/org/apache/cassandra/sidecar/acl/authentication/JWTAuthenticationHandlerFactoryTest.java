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
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

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
        assertThatThrownBy(() -> factory.create(mockVertx, mockConfig, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("JWT parameters can not be null");

        assertThatThrownBy(() -> factory.create(mockVertx, mockConfig, Map.of(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing site JWT parameter");

        assertThatThrownBy(() -> factory.create(mockVertx, mockConfig, Map.of("site", "www.apache.org"), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing client_id JWT parameter");
    }
}
