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

import static org.apache.cassandra.sidecar.acl.authorization.PermissionFactoryImpl.FORBIDDEN_PREFIX_ERR_MSG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link PermissionFactoryImpl}
 */
public class PermissionFactoryTest
{
    PermissionFactory permissionFactory = new PermissionFactoryImpl();

    @Test
    void testPermissionCreated()
    {
        assertThat(permissionFactory.createPermission("permission1")).isNotNull();
        assertThat(permissionFactory.createPermission("permission1"))
        .isInstanceOf(StandardPermission.class);
        assertThat(permissionFactory.createPermission("random"))
        .isInstanceOf(StandardPermission.class);
        assertThat(permissionFactory.createPermission("domain:action"))
        .isInstanceOf(DomainAwarePermission.class);
        assertThat(permissionFactory.createPermission("ANALYTICS:READ_DIRECT"))
        .isInstanceOf(CompositePermission.class);
    }

    @Test
    void testFeaturePermissionCreated()
    {
        assertThat(permissionFactory.createFeaturePermission("ANALYTICS:READ_DIRECT")).isNotNull();
        assertThat(permissionFactory.createFeaturePermission("ANALYTICS:READ_DIRECT").childPermissions().size()).isEqualTo(7);
        assertThat(permissionFactory.createFeaturePermission("ANALYTICS:WRITE_DIRECT").childPermissions().size()).isEqualTo(6);
        assertThat(permissionFactory.createFeaturePermission("ANALYTICS:WRITE_S3_COMPAT").childPermissions().size()).isEqualTo(6);
        assertThat(permissionFactory.createFeaturePermission("CDC").childPermissions().size()).isEqualTo(1);
    }

    @Test
    void testInvalidPermission()
    {
        assertThat(permissionFactory.createFeaturePermission("random")).isNull();
        assertThatThrownBy(() -> permissionFactory.createPermission("*:CREATE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(FORBIDDEN_PREFIX_ERR_MSG);
        assertThatThrownBy(() -> permissionFactory.createPermission("*:*"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(FORBIDDEN_PREFIX_ERR_MSG);
        assertThatThrownBy(() -> permissionFactory.createPermission("")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> permissionFactory.createPermission(" ")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> permissionFactory.createPermission(":*")).isInstanceOf(IllegalArgumentException.class);
    }
}
