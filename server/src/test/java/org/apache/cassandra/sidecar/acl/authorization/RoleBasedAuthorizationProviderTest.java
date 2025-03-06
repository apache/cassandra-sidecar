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
import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.ext.auth.User;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.apache.cassandra.sidecar.utils.AuthUtils.CASSANDRA_ROLES_ATTRIBUTE_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link RoleBasedAuthorizationProvider}
 */
@ExtendWith(VertxExtension.class)
class RoleBasedAuthorizationProviderTest
{
    RoleAuthorizationsCache mockRolePermissionsCache;

    @BeforeEach
    void setup()
    {
        mockRolePermissionsCache = mock(RoleAuthorizationsCache.class);
    }

    @Test
    void testMissingRoles(VertxTestContext testContext)
    {
        RoleBasedAuthorizationProvider authorizationProvider = new RoleBasedAuthorizationProvider(mockRolePermissionsCache);
        User user = User.fromName("test_user");
        authorizationProvider.getAuthorizations(user)
                             .onSuccess(v -> testContext.failNow("With missing cassandra role authorization provider is expected to return a failed future"))
                             .onFailure(cause -> testContext.completeNow());
    }

    @Test
    void testAuthorizationsFetched(VertxTestContext testContext)
    {
        RoleAuthorizationsCache mockRolePermissionsCache = mock(RoleAuthorizationsCache.class);
        when(mockRolePermissionsCache.getAuthorizations("test_role"))
        .thenReturn(new HashSet<>(Arrays.asList(CassandraPermissions.CREATE.toAuthorization(), BasicPermissions.CREATE_SNAPSHOT.toAuthorization())));
        RoleBasedAuthorizationProvider authorizationProvider = new RoleBasedAuthorizationProvider(mockRolePermissionsCache);
        User user = User.fromName("test_user");
        user.attributes().put(CASSANDRA_ROLES_ATTRIBUTE_NAME, List.of("test_role"));
        authorizationProvider.getAuthorizations(user)
                             .onComplete(testContext.succeeding(v -> {
                                 assertThat(user.authorizations().get(authorizationProvider.getId())).hasSize(2);
                                 testContext.completeNow();
                             }));
    }
}
