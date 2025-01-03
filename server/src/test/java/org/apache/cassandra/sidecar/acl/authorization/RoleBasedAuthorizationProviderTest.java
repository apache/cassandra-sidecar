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

import java.util.Collections;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.mtls.impl.MutualTlsUser;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.acl.IdentityToRoleCache;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link RoleBasedAuthorizationProvider}
 */
@ExtendWith(VertxExtension.class)
public class RoleBasedAuthorizationProviderTest
{
    RoleAuthorizationsCache mockRolePermissionsCache;

    @BeforeEach
    void setup()
    {
        mockRolePermissionsCache = mock(RoleAuthorizationsCache.class);
    }

    @Test
    void testMissingIdentity()
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        RoleBasedAuthorizationProvider authorizationProvider = new RoleBasedAuthorizationProvider(mockIdentityToRoleCache,
                                                                                                  mockRolePermissionsCache);
        User user = User.fromName("test_user");
        assertThatThrownBy(() -> authorizationProvider.getAuthorizations(user))
        .isInstanceOf(HttpException.class)
        .hasMessage(HttpResponseStatus.FORBIDDEN.reasonPhrase());
    }

    @Test
    void testCassandraRoleNotFound()
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        RoleBasedAuthorizationProvider authorizationProvider = new RoleBasedAuthorizationProvider(mockIdentityToRoleCache,
                                                                                                  mockRolePermissionsCache);
        User user = MutualTlsUser.fromIdentities(Collections.singletonList("spiffe://cassandra/sidecar/test_user"));
        assertThatThrownBy(() -> authorizationProvider.getAuthorizations(user))
        .isInstanceOf(HttpException.class)
        .hasMessage(HttpResponseStatus.FORBIDDEN.reasonPhrase());
        when(mockIdentityToRoleCache.get("spiffe://cassandra/sidecar/test_user")).thenReturn("test_role");
        authorizationProvider.getAuthorizations(user);
    }

    @Test
    void testAuthorizationsFetched(VertxTestContext testContext)
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        when(mockIdentityToRoleCache.get("spiffe://cassandra/sidecar/test_user")).thenReturn("test_role");
        RoleAuthorizationsCache mockRolePermissionsCache = mock(RoleAuthorizationsCache.class);
        when(mockRolePermissionsCache.getAuthorizations("test_role"))
        .thenReturn(ImmutableSet.of(CassandraPermissions.CREATE.toAuthorization(), SidecarPermissions.CREATE_SNAPSHOT.toAuthorization()));
        RoleBasedAuthorizationProvider authorizationProvider = new RoleBasedAuthorizationProvider(mockIdentityToRoleCache,
                                                                                                  mockRolePermissionsCache);
        User user = MutualTlsUser.fromIdentities(Collections.singletonList("spiffe://cassandra/sidecar/test_user"));
        authorizationProvider.getAuthorizations(user)
                             .onComplete(v -> {
                                 assertThat(user.authorizations().get(authorizationProvider.getId()).size()).isEqualTo(2);
                                 testContext.completeNow();
                             });
    }
}
