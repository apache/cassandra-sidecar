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

package org.apache.cassandra.sidecar.acl.authorization;

import org.junit.jupiter.api.Test;

import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link AuthorizationWithAdminBypassHandler}
 */
class AuthorizationHandlerTest
{
    @Test
    void testMissingIdentity()
    {
        AdminIdentityResolver mockAdminIdentityResolver = mock(AdminIdentityResolver.class);
        Authorization mockAuthorization = mock(Authorization.class);
        AuthorizationParameterValidateHandler mockValidateHandler = mock(AuthorizationParameterValidateHandler.class);
        AuthorizationWithAdminBypassHandler authorizationHandler
        = new AuthorizationWithAdminBypassHandler(mockValidateHandler, mockAdminIdentityResolver, mockAuthorization);
        RoutingContext mockCtx = mock(RoutingContext.class);
        User user = User.fromName("test_user");
        when(mockCtx.user()).thenReturn(user);
        assertThatThrownBy(() -> authorizationHandler.handle(mockCtx))
        .isInstanceOf(HttpException.class);
    }
}
