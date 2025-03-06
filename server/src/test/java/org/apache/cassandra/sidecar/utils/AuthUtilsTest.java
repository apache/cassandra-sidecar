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

package org.apache.cassandra.sidecar.utils;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.vertx.ext.auth.User;

import static org.apache.cassandra.sidecar.utils.AuthUtils.CASSANDRA_ROLES_ATTRIBUTE_NAME;
import static org.apache.cassandra.sidecar.utils.AuthUtils.extractCassandraRoles;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link AuthUtils}
 */
class AuthUtilsTest
{
    @Test
    void testExtractingRoles()
    {
        User user = User.fromName("test_user");
        user.attributes().put(CASSANDRA_ROLES_ATTRIBUTE_NAME, List.of());
        assertThat(extractCassandraRoles(user)).isEmpty();

        user.attributes().put(CASSANDRA_ROLES_ATTRIBUTE_NAME, List.of("role1", "role2"));
        assertThat(extractCassandraRoles(user)).containsAll(Arrays.asList("role1", "role2"));

        user.attributes().put(CASSANDRA_ROLES_ATTRIBUTE_NAME, "role1,role2");
        assertThatThrownBy(() -> extractCassandraRoles(user)).isInstanceOf(ClassCastException.class);
    }

    @Test
    void testRolesSetToNull()
    {
        User user = User.fromName("test_user");
        user.attributes().put(CASSANDRA_ROLES_ATTRIBUTE_NAME, null);
        assertThat(extractCassandraRoles(user)).isEmpty();
    }
}
