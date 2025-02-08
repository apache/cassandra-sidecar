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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.ext.auth.User;
import io.vertx.ext.auth.mtls.impl.MutualTlsUser;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.acl.IdentityToRoleCache;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link RoleBasedAuthorizationProvider}
 */
@ExtendWith(VertxExtension.class)
class RoleBasedAuthorizationProviderTest
{
    RoleAuthorizationsCache mockRolePermissionsCache;
    PermissionFactory permissionFactory = new PermissionFactoryImpl();

    @BeforeEach
    void setup()
    {
        mockRolePermissionsCache = mock(RoleAuthorizationsCache.class);
    }

    @Test
    void testMissingIdentity(VertxTestContext testContext)
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        RoleBasedAuthorizationProvider authorizationProvider = new RoleBasedAuthorizationProvider(mockIdentityToRoleCache,
                                                                                                  mockRolePermissionsCache);
        User user = User.fromName("test_user");
        authorizationProvider.getAuthorizations(user)
                             .onSuccess(v -> testContext.failNow("With missing identity authorization provider is expected to return a failed future"))
                             .onFailure(cause -> testContext.completeNow());
    }

    @Test
    void testCassandraRoleNotFound() throws Exception
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        RoleAuthorizationsCache mockRolePermissionsCache = mock(RoleAuthorizationsCache.class);
        RoleBasedAuthorizationProvider authorizationProvider = new RoleBasedAuthorizationProvider(mockIdentityToRoleCache,
                                                                                                  mockRolePermissionsCache);
        User user = MutualTlsUser.fromIdentities(Collections.singletonList("spiffe://cassandra/sidecar/test_user"));

        CountDownLatch waitForEmptyAuthorizations = new CountDownLatch(1);
        authorizationProvider.getAuthorizations(user).onComplete(v -> waitForEmptyAuthorizations.countDown());
        assertThat(waitForEmptyAuthorizations.await(30, TimeUnit.SECONDS)).isTrue();

        assertThat(user.authorizations().get("RoleBasedAccessControl")).isEmpty();

        when(mockIdentityToRoleCache.get("spiffe://cassandra/sidecar/test_user")).thenReturn("test_role");
        when(mockRolePermissionsCache.getAuthorizations("test_role"))
        .thenReturn(Collections.singleton(permissionFactory.createPermission("RANDOM").toAuthorization()));

        CountDownLatch waitForAuthorizations = new CountDownLatch(1);
        authorizationProvider.getAuthorizations(user).onComplete(v -> waitForAuthorizations.countDown());
        assertThat(waitForAuthorizations.await(30, TimeUnit.SECONDS)).isTrue();

        assertThat(user.authorizations().get("RoleBasedAccessControl")).isNotEmpty();
    }

    @Test
    void testAuthorizationsFetched(VertxTestContext testContext)
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        when(mockIdentityToRoleCache.get("spiffe://cassandra/sidecar/test_user")).thenReturn("test_role");
        RoleAuthorizationsCache mockRolePermissionsCache = mock(RoleAuthorizationsCache.class);
        when(mockRolePermissionsCache.getAuthorizations("test_role"))
        .thenReturn(new HashSet<>(Arrays.asList(CassandraPermissions.CREATE.toAuthorization(), BasicPermissions.CREATE_SNAPSHOT.toAuthorization())));
        RoleBasedAuthorizationProvider authorizationProvider = new RoleBasedAuthorizationProvider(mockIdentityToRoleCache,
                                                                                                  mockRolePermissionsCache);
        User user = MutualTlsUser.fromIdentities(Collections.singletonList("spiffe://cassandra/sidecar/test_user"));
        authorizationProvider.getAuthorizations(user)
                             .onComplete(testContext.succeeding(v -> {
                                 assertThat(user.authorizations().get(authorizationProvider.getId())).hasSize(2);
                                 testContext.completeNow();
                             }));
    }
}
