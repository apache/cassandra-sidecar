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

package org.apache.cassandra.sidecar.acl.authentication;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.acl.IdentityToRoleCache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link JwtRoleProcessorImpl}
 */
@ExtendWith(VertxExtension.class)
class JwtRoleProcessorTest
{
    @Test
    void testRoleExtractedWithSubField(VertxTestContext testContext)
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        when(mockIdentityToRoleCache.get("user_id")).thenReturn(Future.succeededFuture("test_role"));
        JwtRoleProcessor roleProcessor = new JwtRoleProcessorImpl(mockIdentityToRoleCache);
        JsonObject decodedToken = new JsonObject();
        decodedToken.put("sub", "user_id");

        roleProcessor.processRoles(decodedToken)
                     .onSuccess(roles -> {
                         testContext.verify(() -> assertThat(roles).contains("test_role"));
                         testContext.completeNow();
                     })
                     .onFailure(testContext::failNow);
    }

    @Test
    void testRoleExtractedWithIdentityField(VertxTestContext testContext)
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        when(mockIdentityToRoleCache.get("user_id")).thenReturn(Future.succeededFuture("test_role"));
        JwtRoleProcessor roleProcessor = new JwtRoleProcessorImpl(mockIdentityToRoleCache);
        JsonObject decodedToken = new JsonObject();
        decodedToken.put("identity", "user_id");

        roleProcessor.processRoles(decodedToken)
                     .onSuccess(roles -> {
                         testContext.verify(() -> assertThat(roles).contains("test_role"));
                         testContext.completeNow();
                     })
                     .onFailure(testContext::failNow);
    }

    @Test
    void testRoleExtractedWithIdentitiesField(VertxTestContext testContext)
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        when(mockIdentityToRoleCache.get("user_id1")).thenReturn(Future.succeededFuture("test_role1"));
        when(mockIdentityToRoleCache.get("user_id2")).thenReturn(Future.succeededFuture("test_role2"));
        when(mockIdentityToRoleCache.get("user_id3")).thenReturn(Future.succeededFuture("test_role3"));
        JwtRoleProcessor roleProcessor = new JwtRoleProcessorImpl(mockIdentityToRoleCache);
        JsonObject decodedToken = new JsonObject();
        decodedToken.put("identities", Arrays.asList("user_id1", "user_id2", "user_id3"));
        roleProcessor.processRoles(decodedToken)
                     .onSuccess(roles -> {
                         testContext.verify(() ->
                             assertThat(roles).containsAll(Arrays.asList("test_role1", "test_role2", "test_role3"))
                         );
                         testContext.completeNow();
                     })
                     .onFailure(testContext::failNow);
    }

    @Test
    void testEmptyIdentitiesField(VertxTestContext testContext)
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        when(mockIdentityToRoleCache.get("user_id1")).thenReturn(Future.succeededFuture("test_role1"));
        when(mockIdentityToRoleCache.get("user_id2")).thenReturn(Future.succeededFuture("test_role2"));
        when(mockIdentityToRoleCache.get("user_id3")).thenReturn(Future.succeededFuture("test_role3"));
        JwtRoleProcessor roleProcessor = new JwtRoleProcessorImpl(mockIdentityToRoleCache);
        JsonObject decodedToken = new JsonObject();
        decodedToken.put("identities", List.of());
        roleProcessor.processRoles(decodedToken)
                     .onSuccess(roles -> {
                         testContext.verify(() -> assertThat(roles).isEmpty());
                         testContext.completeNow();
                     })
                     .onFailure(testContext::failNow);
    }

    @Test
    void testNonStringIdentitiesField(VertxTestContext testContext)
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        when(mockIdentityToRoleCache.get("1")).thenReturn(Future.succeededFuture("test_role1"));
        when(mockIdentityToRoleCache.get("2")).thenReturn(Future.succeededFuture("test_role2"));
        JwtRoleProcessor roleProcessor = new JwtRoleProcessorImpl(mockIdentityToRoleCache);
        JsonObject decodedToken = new JsonObject();
        decodedToken.put("identities", List.of("1", "2"));
        roleProcessor.processRoles(decodedToken)
                     .onSuccess(roles -> {
                         testContext.verify(() ->
                             assertThat(roles).containsAll(Arrays.asList("test_role1", "test_role2"))
                         );
                         testContext.completeNow();
                     })
                     .onFailure(testContext::failNow);
    }

    @Test
    void testUnrecognizedIdentity(VertxTestContext testContext)
    {
        IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
        JwtRoleProcessor roleProcessor = new JwtRoleProcessorImpl(mockIdentityToRoleCache);

        roleProcessor.processRoles(new JsonObject())
                     .onSuccess(roles -> {
                         testContext.verify(() -> assertThat(roles).isEmpty());
                         testContext.completeNow();
                     })
                     .onFailure(testContext::failNow);
    }
}
