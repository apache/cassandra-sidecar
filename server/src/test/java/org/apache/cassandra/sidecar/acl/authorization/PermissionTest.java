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

import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.OrAuthorization;
import io.vertx.ext.auth.authorization.PermissionBasedAuthorization;
import io.vertx.ext.auth.authorization.WildcardPermissionBasedAuthorization;
import io.vertx.ext.auth.authorization.impl.PermissionBasedAuthorizationImpl;
import io.vertx.ext.auth.authorization.impl.WildcardPermissionBasedAuthorizationImpl;

import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.TABLE;
import static org.apache.cassandra.sidecar.utils.AuthUtils.permissionFromName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link Permission}
 */
class PermissionTest
{
    @Test
    void testValidActions()
    {
        assertThat(permissionFromName("CREATE_SNAPSHOT")).isInstanceOf(StandardPermission.class);
        assertThat(permissionFromName("OPERATE")).isInstanceOf(StandardPermission.class);
        assertThat(permissionFromName("CREATESNAPSHOT")).isInstanceOf(StandardPermission.class);
        assertThat(permissionFromName("SNAPSHOT:CREATE")).isInstanceOf(DomainAwarePermission.class);
        assertThat(permissionFromName("SNAPSHOT:CREATE,READ")).isInstanceOf(DomainAwarePermission.class);
        assertThat(permissionFromName("SNAPSHOT:CREATE:NEW")).isInstanceOf(DomainAwarePermission.class);
    }

    @Test
    void testInvalidActions()
    {
        assertThatThrownBy(() -> permissionFromName("")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> permissionFromName(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testToAuthorizationWithResource()
    {
        String expectedResource = TABLE.variableAwareResource();
        PermissionBasedAuthorization authorization
        = (PermissionBasedAuthorization) permissionFromName("CREATESNAPSHOT").toAuthorization(expectedResource);
        assertThat(authorization.getResource()).isEqualTo(expectedResource);
        WildcardPermissionBasedAuthorization wildcardAuthorization
        = (WildcardPermissionBasedAuthorization) permissionFromName("SNAPSHOT:CREATE").toAuthorization(expectedResource);
        assertThat(wildcardAuthorization.getResource()).isEqualTo(expectedResource);
    }

    @Test
    void testToAuthorizationWithEmptyResource()
    {
        PermissionBasedAuthorization authorization
        = (PermissionBasedAuthorization) permissionFromName("CREATESNAPSHOT").toAuthorization("");
        assertThat(authorization.getResource()).isNull();
        WildcardPermissionBasedAuthorization wildcardAuthorization
        = (WildcardPermissionBasedAuthorization) permissionFromName("SNAPSHOT:CREATE").toAuthorization("");
        assertThat(wildcardAuthorization.getResource()).isNull();
    }

    @Test
    void testInvalidWildcardActions()
    {
        assertThatThrownBy(() -> new DomainAwarePermission("*"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("DomainAwarePermission can not have * to avoid unpredictable behavior");

        assertThatThrownBy(() -> new DomainAwarePermission(":"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("DomainAwarePermission parts can not be empty");

        assertThatThrownBy(() -> new DomainAwarePermission("::"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("DomainAwarePermission parts can not be empty");

        assertThatThrownBy(() -> new DomainAwarePermission("a::d"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("DomainAwarePermission parts can not be empty");

        assertThatThrownBy(() -> new DomainAwarePermission("a"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("DomainAwarePermission must have : to divide domain and action");

        assertThatThrownBy(() -> new DomainAwarePermission("a,b,c"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("DomainAwarePermission must have : to divide domain and action");
    }

    @Test
    void testSettingResourceScope()
    {
        DataResourceScope keyspaceScoped = DataResourceScope.createWithKeyspaceScope();
        StandardPermission permissionWithScope = new StandardPermission("permission1").withScope(keyspaceScoped);
        Authorization permissionWithScopeAuthorization = permissionWithScope.toAuthorization("data/university");
        assertThat(permissionWithScopeAuthorization.verify(new PermissionBasedAuthorizationImpl("permission1")
                                                           .setResource("cluster"))).isFalse();
        assertThat(permissionWithScopeAuthorization.verify(new WildcardPermissionBasedAuthorizationImpl("permission1")
                                                           .setResource("data/{keyspace}"))).isFalse();
        assertThat(permissionWithScopeAuthorization.verify(new WildcardPermissionBasedAuthorizationImpl("permission1")
                                                           .setResource("data/university"))).isTrue();

        StandardPermission permissionWithoutScope = new StandardPermission("permission1");
        Authorization permissionWithoutScopeAuthorization
        = permissionWithoutScope.toAuthorization("data/university");
        assertThat(permissionWithoutScopeAuthorization.verify(new PermissionBasedAuthorizationImpl("permission1")
                                                              .setResource("cluster"))).isFalse();
        assertThat(permissionWithoutScopeAuthorization.verify(new WildcardPermissionBasedAuthorizationImpl("permission1")
                                                              .setResource("data/{keyspace}"))).isFalse();
        assertThat(permissionWithoutScopeAuthorization.verify(new WildcardPermissionBasedAuthorizationImpl("permission1")
                                                              .setResource("data/university"))).isTrue();

        // permission with scope and without resource scope behave similarly when resource used to retrieve
        // Authorization is same
        assertThat(permissionWithScopeAuthorization.verify(permissionWithoutScopeAuthorization)).isTrue();
        // with scope toAuthorization provides Authorization with expanded resources
        assertThat(permissionWithScope.toAuthorization()).isInstanceOf(OrAuthorization.class);
        // without scope toAuthorization provides Authorization with just the permission name, resource is not
        // validated
        assertThat(permissionWithoutScope.toAuthorization()).isInstanceOf(PermissionBasedAuthorization.class);
    }
}
