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
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link ReloadingJwtAuthenticationHandler}
 */
class ReloadingJwtAuthenticationHandlerTest
{
    @Test
    void testDelegateHandlerNotSet()
    {
        Vertx mockVertx = mock(Vertx.class);
        JwtParameterExtractor parameterExtractor = new JwtParameterExtractor(Map.of("enabled", "true",
                                                                                    "site", "www.apache.org",
                                                                                    "client_id", "id"));
        JwtRoleProcessor mockRoleProcessor = mock(JwtRoleProcessor.class);
        when(mockRoleProcessor.processRoles(any())).thenReturn(List.of("test_role"));
        PeriodicTaskExecutor mockTaskExecutor = mock(PeriodicTaskExecutor.class);
        doNothing().when(mockTaskExecutor).schedule(any());
        ReloadingJwtAuthenticationHandler reloadingJwtAuthenticationHandler
        = new ReloadingJwtAuthenticationHandler(mockVertx, parameterExtractor, mockRoleProcessor, mockTaskExecutor);
        RoutingContext mockCtx = mock(RoutingContext.class);
        reloadingJwtAuthenticationHandler.authenticate(mockCtx, result -> {
            assertThat(result.failed()).isTrue();
            assertThat(result.cause()).hasMessage("Service Unavailable");
        });
    }

    @Test
    void testDelegateHandlerNotCreatedWhenJWTDisabled()
    {
        Vertx mockVertx = mock(Vertx.class);
        JwtParameterExtractor parameterExtractor = new JwtParameterExtractor(Map.of("enabled", "false",
                                                                                    "site", "www.apache.org",
                                                                                    "client_id", "id"));
        JwtRoleProcessor mockRoleProcessor = mock(JwtRoleProcessor.class);
        PeriodicTaskExecutor mockTaskExecutor = mock(PeriodicTaskExecutor.class);
        ReloadingJwtAuthenticationHandler reloadingJwtAuthenticationHandler
        = new ReloadingJwtAuthenticationHandler(mockVertx, parameterExtractor, mockRoleProcessor, mockTaskExecutor);
        RoutingContext mockCtx = mock(RoutingContext.class);
        reloadingJwtAuthenticationHandler.authenticate(mockCtx, result -> {
            assertThat(result.failed()).isTrue();
            assertThat(result.cause()).hasMessage("Service Unavailable");
        });
    }
}
