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
import java.util.List;

import org.junit.jupiter.api.Test;

import io.vertx.core.MultiMap;
import io.vertx.ext.auth.User;

import static org.apache.cassandra.sidecar.utils.AuthUtils.CASSANDRA_ROLES_ATTRIBUTE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link AuthorizationCacheKeyImpl}
 */
class AuthorizationCacheKeyImplTest
{
    @Test
    void testEqualsSameUserSameVariables()
    {
        User user1 = createUser("user1", List.of("role1", "role2"));
        User user2 = createUser("user1", List.of("role1", "role2"));

        MultiMap variables1 = MultiMap.caseInsensitiveMultiMap().add("keyspace", "ks1");
        MultiMap variables2 = MultiMap.caseInsensitiveMultiMap().add("keyspace", "ks1");

        AuthorizationCacheKeyImpl key1 = new AuthorizationCacheKeyImpl(1, user1, variables1);
        AuthorizationCacheKeyImpl key2 = new AuthorizationCacheKeyImpl(1, user2, variables2);

        assertThat(key1).isEqualTo(key2);
        assertThat(key1.hashCode()).isEqualTo(key2.hashCode());
    }

    @Test
    void testEqualsSameUserNullVariables()
    {
        User user1 = createUser("user1", List.of("role1", "role2"));
        User user2 = createUser("user1", List.of("role1", "role2"));

        AuthorizationCacheKeyImpl key1 = new AuthorizationCacheKeyImpl(1, user1, null);
        AuthorizationCacheKeyImpl key2 = new AuthorizationCacheKeyImpl(1, user2, null);

        assertThat(key1).isEqualTo(key2);
        assertThat(key1.hashCode()).isEqualTo(key2.hashCode());
    }

    @Test
    void testEqualsEmptyVariables()
    {
        User user1 = createUser("user1", List.of("role1"));
        User user2 = createUser("user1", List.of("role1"));

        MultiMap variables1 = MultiMap.caseInsensitiveMultiMap();
        MultiMap variables2 = MultiMap.caseInsensitiveMultiMap();

        AuthorizationCacheKeyImpl key1 = new AuthorizationCacheKeyImpl(1, user1, variables1);
        AuthorizationCacheKeyImpl key2 = new AuthorizationCacheKeyImpl(1, user2, variables2);

        assertThat(key1).isEqualTo(key2);
        assertThat(key1.hashCode()).isEqualTo(key2.hashCode());
    }

    @Test
    void testNotEqualsDifferentRoles()
    {
        User user1 = createUser("user1", List.of("role1", "role2"));
        User user2 = createUser("user1", List.of("role1", "role3"));

        MultiMap variables = MultiMap.caseInsensitiveMultiMap().add("keyspace", "ks1");

        AuthorizationCacheKeyImpl key1 = new AuthorizationCacheKeyImpl(1, user1, variables);
        AuthorizationCacheKeyImpl key2 = new AuthorizationCacheKeyImpl(1, user2, variables);

        assertThat(key1).isNotEqualTo(key2);
        assertThat(key1.hashCode()).isNotEqualTo(key2.hashCode());
    }

    @Test
    void testNotEqualsDifferentVariables()
    {
        User user1 = createUser("user1", List.of("role1", "role2"));
        User user2 = createUser("user1", List.of("role1", "role2"));

        MultiMap variables1 = MultiMap.caseInsensitiveMultiMap().add("keyspace", "ks1");
        MultiMap variables2 = MultiMap.caseInsensitiveMultiMap().add("keyspace", "ks2");

        AuthorizationCacheKeyImpl key1 = new AuthorizationCacheKeyImpl(1, user1, variables1);
        AuthorizationCacheKeyImpl key2 = new AuthorizationCacheKeyImpl(1, user2, variables2);

        assertThat(key1).isNotEqualTo(key2);
        assertThat(key1.hashCode()).isNotEqualTo(key2.hashCode());
    }

    @Test
    void testNotEqualsOneWithVariablesOneWithout()
    {
        User user1 = createUser("user1", List.of("role1", "role2"));
        User user2 = createUser("user1", List.of("role1", "role2"));

        MultiMap variables1 = MultiMap.caseInsensitiveMultiMap().add("keyspace", "ks1");

        AuthorizationCacheKeyImpl key1 = new AuthorizationCacheKeyImpl(1, user1, variables1);
        AuthorizationCacheKeyImpl key2 = new AuthorizationCacheKeyImpl(1, user2, null);

        assertThat(key1).isNotEqualTo(key2);
        assertThat(key1.hashCode()).isNotEqualTo(key2.hashCode());
    }

    @Test
    void testVariablesMutationDoesNotAffectKey()
    {
        User user1 = createUser("user1", List.of("role1"));
        User user2 = createUser("user1", List.of("role1"));

        MultiMap variables1 = MultiMap.caseInsensitiveMultiMap().add("keyspace", "ks1");
        MultiMap variables2 = MultiMap.caseInsensitiveMultiMap().add("keyspace", "ks1");

        AuthorizationCacheKeyImpl key1 = new AuthorizationCacheKeyImpl(1, user1, variables1);
        AuthorizationCacheKeyImpl key2 = new AuthorizationCacheKeyImpl(1, user2, variables2);

        // Mutate original MultiMap
        variables1.add("table", "tb1");

        // Keys should still be equal because AuthorizationCacheKeyImpl creates a copy
        assertThat(key1).isEqualTo(key2);
    }

    @Test
    void testEqualsMultipleVariablesSameOrder()
    {
        User user1 = createUser("user1", List.of("role1"));
        User user2 = createUser("user1", List.of("role1"));

        MultiMap variables1 = MultiMap.caseInsensitiveMultiMap()
                                      .add("keyspace", "ks1")
                                      .add("table", "tb1");
        MultiMap variables2 = MultiMap.caseInsensitiveMultiMap()
                                      .add("keyspace", "ks1")
                                      .add("table", "tb1");

        AuthorizationCacheKeyImpl key1 = new AuthorizationCacheKeyImpl(1, user1, variables1);
        AuthorizationCacheKeyImpl key2 = new AuthorizationCacheKeyImpl(1, user2, variables2);

        assertThat(key1).isEqualTo(key2);
        assertThat(key1.hashCode()).isEqualTo(key2.hashCode());
    }

    @Test
    void testEqualsMultipleVariablesDifferentOrder()
    {
        User user1 = createUser("user1", List.of("role1"));
        User user2 = createUser("user1", List.of("role1"));

        MultiMap variables1 = MultiMap.caseInsensitiveMultiMap()
                                      .add("keyspace", "ks1")
                                      .add("table", "tb1");
        MultiMap variables2 = MultiMap.caseInsensitiveMultiMap()
                                      .add("table", "tb1")
                                      .add("keyspace", "ks1");

        AuthorizationCacheKeyImpl key1 = new AuthorizationCacheKeyImpl(1, user1, variables1);
        AuthorizationCacheKeyImpl key2 = new AuthorizationCacheKeyImpl(1, user2, variables2);

        // Should be equal regardless of insertion order
        assertThat(key1).isEqualTo(key2);
        assertThat(key1.hashCode()).isEqualTo(key2.hashCode());
    }

    @Test
    void testEqualsVariablesWithMultipleValues()
    {
        User user1 = createUser("user1", List.of("role1"));
        User user2 = createUser("user1", List.of("role1"));

        MultiMap variables1 = MultiMap.caseInsensitiveMultiMap()
                                      .add("keyspace", "ks1")
                                      .add("keyspace", "ks1");
        MultiMap variables2 = MultiMap.caseInsensitiveMultiMap()
                                      .add("keyspace", "ks1")
                                      .add("keyspace", "ks1");

        AuthorizationCacheKeyImpl key1 = new AuthorizationCacheKeyImpl(1, user1, variables1);
        AuthorizationCacheKeyImpl key2 = new AuthorizationCacheKeyImpl(1, user2, variables2);

        assertThat(key1).isEqualTo(key2);
        assertThat(key1.hashCode()).isEqualTo(key2.hashCode());
    }

    @Test
    void testNotEqualsVariablesWithDifferentMultipleValues()
    {
        User user1 = createUser("user1", List.of("role1"));
        User user2 = createUser("user1", List.of("role1"));

        MultiMap variables1 = MultiMap.caseInsensitiveMultiMap()
                                      .add("keyspace", "ks1")
                                      .add("keyspace", "ks2");
        MultiMap variables2 = MultiMap.caseInsensitiveMultiMap()
                                      .add("keyspace", "ks1")
                                      .add("keyspace", "ks3");

        AuthorizationCacheKeyImpl key1 = new AuthorizationCacheKeyImpl(1, user1, variables1);
        AuthorizationCacheKeyImpl key2 = new AuthorizationCacheKeyImpl(1, user2, variables2);

        assertThat(key1).isNotEqualTo(key2);
        assertThat(key1.hashCode()).isNotEqualTo(key2.hashCode());
    }

    @Test
    void testEqualsCaseInsensitiveVariables()
    {
        User user1 = createUser("user1", List.of("role1"));
        User user2 = createUser("user1", List.of("role1"));

        MultiMap variables1 = MultiMap.caseInsensitiveMultiMap().add("Keyspace", "ks1");
        MultiMap variables2 = MultiMap.caseInsensitiveMultiMap().add("keyspace", "ks1");

        AuthorizationCacheKeyImpl key1 = new AuthorizationCacheKeyImpl(1, user1, variables1);
        AuthorizationCacheKeyImpl key2 = new AuthorizationCacheKeyImpl(1, user2, variables2);

        assertThat(key1).isEqualTo(key2);
    }

    @Test
    void testEqualsEmptyRolesList()
    {
        User user1 = createUser("user1", Collections.emptyList());
        User user2 = createUser("user1", Collections.emptyList());

        MultiMap variables = MultiMap.caseInsensitiveMultiMap().add("keyspace", "ks1");

        AuthorizationCacheKeyImpl key1 = new AuthorizationCacheKeyImpl(1, user1, variables);
        AuthorizationCacheKeyImpl key2 = new AuthorizationCacheKeyImpl(1, user2, variables);

        assertThat(key1).isEqualTo(key2);
        assertThat(key1.hashCode()).isEqualTo(key2.hashCode());
    }

    @Test
    void testNotEqualsDifferentHandlerId()
    {
        User user1 = createUser("user1", List.of("role1"));
        User user2 = createUser("user1", List.of("role1"));

        MultiMap variables1 = MultiMap.caseInsensitiveMultiMap().add("keyspace", "ks1");
        MultiMap variables2 = MultiMap.caseInsensitiveMultiMap().add("keyspace", "ks1");

        AuthorizationCacheKeyImpl key1 = new AuthorizationCacheKeyImpl(1, user1, variables1);
        AuthorizationCacheKeyImpl key2 = new AuthorizationCacheKeyImpl(2, user2, variables2);

        assertThat(key1).isNotEqualTo(key2);
        assertThat(key1.hashCode()).isNotEqualTo(key2.hashCode());
    }

    private User createUser(String username, List<String> roles)
    {
        User user = User.fromName(username);
        user.attributes().put(CASSANDRA_ROLES_ATTRIBUTE_NAME, roles);
        return user;
    }
}
