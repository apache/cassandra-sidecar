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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.acl.AdminIdentityResolver;
import org.apache.cassandra.sidecar.acl.IdentityToRoleCache;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link AdminIdentityResolver}
 */
@ExtendWith(VertxExtension.class)
class AdminIdentityResolverTest
{
    @Test
    void testAdminIdentityFromConfig(VertxTestContext testContext)
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        when(mockIdentityToRoleCache.get("spiffe://cassandra/sidecar/test_user"))
        .thenReturn(Future.succeededFuture(null));
        SuperUserCache mockSuperUserCache = mock(SuperUserCache.class);
        SidecarConfiguration mockConfig = mock(SidecarConfiguration.class);
        AccessControlConfiguration mockAclConfig = mock(AccessControlConfiguration.class);
        when(mockAclConfig.adminIdentities()).thenReturn(Collections.singleton("spiffe://cassandra/sidecar/admin"));
        when(mockConfig.accessControlConfiguration()).thenReturn(mockAclConfig);
        AdminIdentityResolver adminIdentityResolver = new AdminIdentityResolver(mockIdentityToRoleCache,
                                                                                mockSuperUserCache,
                                                                                mockConfig);
        Future.all(adminIdentityResolver.isAdmin("spiffe://cassandra/sidecar/admin"),
                   adminIdentityResolver.isAdmin("spiffe://cassandra/sidecar/test_user"))
              .onComplete(testContext.succeeding(compositeFuture -> {
                  testContext.verify(() -> {
                      assertThat(compositeFuture.<Boolean>resultAt(0)).isTrue();
                      assertThat(compositeFuture.<Boolean>resultAt(1)).isFalse();
                  });
                  testContext.completeNow();
              }));
    }

    @Test
    void testSuperUser(VertxTestContext testContext)
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        when(mockIdentityToRoleCache.get("spiffe://cassandra/sidecar/test_user"))
        .thenReturn(Future.succeededFuture("test_role"));
        when(mockIdentityToRoleCache.get("spiffe://cassandra/sidecar/admin"))
        .thenReturn(Future.succeededFuture(null));
        SuperUserCache mockSuperUserCache = mock(SuperUserCache.class);
        when(mockSuperUserCache.isSuperUser("test_role")).thenReturn(Future.succeededFuture(true));
        SidecarConfiguration mockConfig = mock(SidecarConfiguration.class);
        AccessControlConfiguration mockAclConfig = mock(AccessControlConfiguration.class);
        when(mockAclConfig.adminIdentities()).thenReturn(Collections.emptySet());
        when(mockConfig.accessControlConfiguration()).thenReturn(mockAclConfig);
        AdminIdentityResolver adminIdentityResolver = new AdminIdentityResolver(mockIdentityToRoleCache,
                                                                                mockSuperUserCache,
                                                                                mockConfig);
        Future.all(adminIdentityResolver.isAdmin("spiffe://cassandra/sidecar/test_user"),
                   adminIdentityResolver.isAdmin("spiffe://cassandra/sidecar/admin"))
              .onComplete(testContext.succeeding(compositeFuture -> {
                  testContext.verify(() -> {
                      assertThat(compositeFuture.<Boolean>resultAt(0)).isTrue();
                      assertThat(compositeFuture.<Boolean>resultAt(1)).isFalse();
                  });
                  testContext.completeNow();
              }));
    }

    @Test
    void testNonAdminIdentity(VertxTestContext testContext)
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        when(mockIdentityToRoleCache.get("spiffe://cassandra/sidecar/test_user")).thenReturn(Future.succeededFuture(null));
        SuperUserCache mockSuperUserCache = mock(SuperUserCache.class);
        SidecarConfiguration mockConfig = mock(SidecarConfiguration.class);
        AccessControlConfiguration mockAclConfig = mock(AccessControlConfiguration.class);
        when(mockAclConfig.adminIdentities()).thenReturn(Collections.emptySet());
        when(mockConfig.accessControlConfiguration()).thenReturn(mockAclConfig);
        AdminIdentityResolver adminIdentityResolver = new AdminIdentityResolver(mockIdentityToRoleCache,
                                                                                mockSuperUserCache,
                                                                                mockConfig);

        adminIdentityResolver.isAdmin("spiffe://cassandra/sidecar/test_user")
                             .onComplete(testContext.succeeding(isAdmin -> {
                                 testContext.verify(() -> assertThat(isAdmin).isFalse());
                                 testContext.completeNow();
                             }));
    }
}
