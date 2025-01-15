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

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.impl.PermissionBasedAuthorizationImpl;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Test for AllowAll authorization setting
 */
@ExtendWith(VertxExtension.class)
class AllowAllAuthorizationTest
{
    AllowAllAuthorizationProvider provider;

    @BeforeEach
    void setup()
    {
        provider = new AllowAllAuthorizationProvider();
    }

    @Test
    void testAllowAllAuthorizationMatching()
    {
        Authorization authorization = new PermissionBasedAuthorizationImpl("test_permission");
        User user = User.fromName("test_user");
        assertThat(authorization.match(user)).isFalse();
        user.authorizations().add(provider.getId(), provider.authorization);
        assertThat(authorization.match(user)).isTrue();
        assertThat(provider.authorization.verify(authorization)).isTrue();
    }

    @Test
    void testAuthorizationsWithAllowAllProvider(VertxTestContext testContext)
    {
        User user = User.fromName("test_user");
        provider.getAuthorizations(user)
                .onComplete(v -> {
                    Authorization authorization = new PermissionBasedAuthorizationImpl("test_permission");
                    Set<Authorization> found = user.authorizations().get(provider.getId());
                    assertThat(found.size()).isOne();
                    assertThat(found.contains(provider.authorization)).isTrue();
                    assertThat(found.iterator().next().verify(authorization)).isTrue();
                    testContext.completeNow();
                });
    }
}
