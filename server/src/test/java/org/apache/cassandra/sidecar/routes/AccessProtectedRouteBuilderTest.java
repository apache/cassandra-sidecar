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

import org.junit.jupiter.api.Test;

import io.vertx.core.http.HttpMethod;
import io.vertx.ext.auth.authorization.AuthorizationProvider;
import io.vertx.ext.web.Router;
import org.apache.cassandra.sidecar.acl.authorization.AdminIdentityResolver;
import org.apache.cassandra.sidecar.acl.authorization.AuthorizationParameterValidateHandler;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Test for {@link AccessProtectedRouteBuilder}
 */
class AccessProtectedRouteBuilderTest
{
    @Test
    void testRequiredParameters()
    {
        AccessControlConfiguration mockConfig = mock(AccessControlConfiguration.class);
        AuthorizationProvider mockAuthorizationProvider = mock(AuthorizationProvider.class);
        AdminIdentityResolver mockAdminIdentityResolver = mock(AdminIdentityResolver.class);
        AuthorizationParameterValidateHandler mockHandler = mock(AuthorizationParameterValidateHandler.class);
        AccessProtectedRouteBuilder accessProtectedRouteBuilder = new AccessProtectedRouteBuilder(mockConfig,
                                                                                                  mockAuthorizationProvider,
                                                                                                  mockAdminIdentityResolver,
                                                                                                  mockHandler);
        assertThatThrownBy(accessProtectedRouteBuilder::build).isInstanceOf(IllegalArgumentException.class);
        Router mockRouter = mock(Router.class);
        accessProtectedRouteBuilder.router(mockRouter);
        accessProtectedRouteBuilder.method(HttpMethod.GET);
        accessProtectedRouteBuilder.endpoint("/api/v1/__health");
        assertThatThrownBy(accessProtectedRouteBuilder::build).isInstanceOf(IllegalArgumentException.class);
    }
}
