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

import io.vertx.ext.auth.authorization.PermissionBasedAuthorization;
import io.vertx.ext.auth.authorization.WildcardPermissionBasedAuthorization;

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
        String expectedResource = VariableAwareResource.DATA_WITH_KEYSPACE_TABLE.resource();
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
}
