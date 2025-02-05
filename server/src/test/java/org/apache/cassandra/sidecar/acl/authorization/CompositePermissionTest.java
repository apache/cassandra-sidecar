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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.vertx.ext.auth.authorization.AndAuthorization;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.impl.PermissionBasedAuthorizationImpl;
import io.vertx.ext.auth.authorization.impl.WildcardPermissionBasedAuthorizationImpl;

import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.CLUSTER_SCOPE;
import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.DATA_SCOPE;
import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.KEYSPACE_SCOPE;
import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.OPERATION_SCOPE;
import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.TABLE_SCOPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link CompositePermission}
 */
class CompositePermissionTest
{
    @Test
    void testEmptyChildPermissions()
    {
        assertThatThrownBy(() -> new CompositePermission("feature", Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testResourceResolvedForChildPermissions()
    {
        List<Permission> permissions = new ArrayList<>();
        permissions.add(new StandardPermission("permission1", CLUSTER_SCOPE));
        permissions.add(new DomainAwarePermission("domain1:action1", CLUSTER_SCOPE));
        permissions.add(new DomainAwarePermission("domain2:action2", OPERATION_SCOPE));
        permissions.add(new DomainAwarePermission("domain3:action3", DATA_SCOPE));
        permissions.add(new DomainAwarePermission("domain4:action4", KEYSPACE_SCOPE));
        permissions.add(new DomainAwarePermission("domain5:action5", TABLE_SCOPE));

        CompositePermission compositePermission = new CompositePermission("composite", permissions);
        assertThat(compositePermission.childPermissions().size()).isEqualTo(6);
        AndAuthorization compositeAuthorization
        = (AndAuthorization) compositePermission.toAuthorization("data/university/student");
        Set<Authorization> resolvedAuthorizations = new HashSet<>(compositeAuthorization.getAuthorizations());
        assertThat(resolvedAuthorizations.contains(new PermissionBasedAuthorizationImpl("permission1")
                                                   .setResource("cluster"))).isTrue();
        assertThat(resolvedAuthorizations.contains(new WildcardPermissionBasedAuthorizationImpl("domain1:action1")
                                                   .setResource("cluster"))).isTrue();
        assertThat(resolvedAuthorizations.contains(new WildcardPermissionBasedAuthorizationImpl("domain2:action2")
                                                   .setResource("operation"))).isTrue();
        assertThat(resolvedAuthorizations.contains(new WildcardPermissionBasedAuthorizationImpl("domain3:action3")
                                                   .setResource("data"))).isTrue();
        assertThat(resolvedAuthorizations.contains(new WildcardPermissionBasedAuthorizationImpl("domain4:action4")
                                                   .setResource("data/university"))).isTrue();
        assertThat(resolvedAuthorizations.contains(new WildcardPermissionBasedAuthorizationImpl("domain5:action5")
                                                   .setResource("data/university/student"))).isTrue();
    }

    @Test
    void testCompositePermissionWithinCompositePermission()
    {
        List<Permission> permissions = new ArrayList<>();
        permissions.add(new StandardPermission("permission1", CLUSTER_SCOPE));
        permissions.add(new StandardPermission("permission2", KEYSPACE_SCOPE));

        CompositePermission compositePermission = new CompositePermission("permission3", permissions);

        List<Permission> combinedPermissions = new ArrayList<>();
        combinedPermissions.add(new StandardPermission("permission4", TABLE_SCOPE));
        combinedPermissions.add(compositePermission);

        CompositePermission combinedPermission = new CompositePermission("permission5", combinedPermissions);

        assertThat(combinedPermission.childPermissions().size()).isEqualTo(2);
        Authorization combinedAuthorization = combinedPermission.toAuthorization("data/university/student");
        assertThat(combinedAuthorization.verify(new PermissionBasedAuthorizationImpl("permission1")
                                                .setResource("cluster"))).isTrue();
        assertThat(combinedAuthorization.verify(new PermissionBasedAuthorizationImpl("permission2")
                                                .setResource("data/university"))).isTrue();
        assertThat(combinedAuthorization.verify(new PermissionBasedAuthorizationImpl("permission4")
                                                .setResource("data/university/student"))).isTrue();
    }
}
