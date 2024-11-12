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

import org.apache.cassandra.sidecar.acl.IdentityToRoleCache;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link AdminIdentityResolver}
 */
class AdminIdentityResolverTest
{
    @Test
    void testAdminIdentityFromConfig()
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        SuperUserCache mockSuperUserCache = mock(SuperUserCache.class);
        SidecarConfiguration mockConfig = mock(SidecarConfiguration.class);
        AccessControlConfiguration mockAclConfig = mock(AccessControlConfiguration.class);
        when(mockAclConfig.adminIdentities()).thenReturn(Collections.singleton("spiffe://cassandra/sidecar/admin"));
        when(mockConfig.accessControlConfiguration()).thenReturn(mockAclConfig);
        AdminIdentityResolver adminIdentityResolver = new AdminIdentityResolver(mockIdentityToRoleCache,
                                                                                mockSuperUserCache,
                                                                                mockConfig);
        assertThat(adminIdentityResolver.isAdmin("spiffe://cassandra/sidecar/admin")).isTrue();
        assertThat(adminIdentityResolver.isAdmin("spiffe://cassandra/sidecar/test_user")).isFalse();
    }

    @Test
    void testSuperUser()
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        when(mockIdentityToRoleCache.get("spiffe://cassandra/sidecar/test_user")).thenReturn("test_role");
        SuperUserCache mockSuperUserCache = mock(SuperUserCache.class);
        when(mockSuperUserCache.isSuperUser("test_role")).thenReturn(true);
        SidecarConfiguration mockConfig = mock(SidecarConfiguration.class);
        AccessControlConfiguration mockAclConfig = mock(AccessControlConfiguration.class);
        when(mockAclConfig.adminIdentities()).thenReturn(Collections.emptySet());
        when(mockConfig.accessControlConfiguration()).thenReturn(mockAclConfig);
        AdminIdentityResolver adminIdentityResolver = new AdminIdentityResolver(mockIdentityToRoleCache,
                                                                                mockSuperUserCache,
                                                                                mockConfig);
        assertThat(adminIdentityResolver.isAdmin("spiffe://cassandra/sidecar/test_user")).isTrue();
        assertThat(adminIdentityResolver.isAdmin("spiffe://cassandra/sidecar/admin")).isFalse();
    }

    @Test
    void testNonAdminIdentity()
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        SuperUserCache mockSuperUserCache = mock(SuperUserCache.class);
        SidecarConfiguration mockConfig = mock(SidecarConfiguration.class);
        AccessControlConfiguration mockAclConfig = mock(AccessControlConfiguration.class);
        when(mockAclConfig.adminIdentities()).thenReturn(Collections.emptySet());
        when(mockConfig.accessControlConfiguration()).thenReturn(mockAclConfig);
        AdminIdentityResolver adminIdentityResolver = new AdminIdentityResolver(mockIdentityToRoleCache,
                                                                                mockSuperUserCache,
                                                                                mockConfig);
        assertThat(adminIdentityResolver.isAdmin("spiffe://cassandra/sidecar/test_user")).isFalse();
    }
}
