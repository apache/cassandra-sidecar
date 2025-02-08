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

package org.apache.cassandra.sidecar.acl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.cassandra.sidecar.acl.authorization.PermissionFactoryImpl;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.SystemAuthDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SystemAuthSchema;

import static org.apache.cassandra.sidecar.ExecutorPoolsHelper.createdSharedTestPool;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_SCHEMA_INITIALIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test {@link IdentityToRoleCache}
 */
class IdentityToRoleCacheTest
{
    Vertx vertx;
    ExecutorPools executorPools;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        executorPools = createdSharedTestPool(vertx);
    }

    @Test
    void testFindRole()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test")).thenReturn("cassandra-role");
        when(mockDbAccessor.findAllIdentityToRoles()).thenReturn(Collections.singletonMap("spiffe://cassandra/sidecar/test", "cassandra-role"));
        SidecarConfiguration mockConfig = mockConfig();
        IdentityToRoleCache identityToRoleCache = new IdentityToRoleCache(vertx, executorPools, mockConfig, mockDbAccessor);
        assertThat(identityToRoleCache.containsKey("spiffe://cassandra/sidecar/test")).isTrue();
        assertThat(identityToRoleCache.get("spiffe://cassandra/sidecar/test")).isEqualTo("cassandra-role");
        assertThat(identityToRoleCache.getAll().size()).isOne();
    }

    @Test
    void testCacheDisabled()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test")).thenReturn("cassandra-role");
        when(mockDbAccessor.findAllIdentityToRoles()).thenReturn(Collections.singletonMap("spiffe://cassandra/sidecar/test", "cassandra-role"));
        SidecarConfiguration mockConfig = mockConfig();
        when(mockConfig.accessControlConfiguration().permissionCacheConfiguration().enabled()).thenReturn(false);
        IdentityToRoleCache identityToRoleCache = new IdentityToRoleCache(vertx, executorPools, mockConfig, mockDbAccessor);
        assertThat(identityToRoleCache.cache()).isNull();
        assertThat(identityToRoleCache.containsKey("spiffe://cassandra/sidecar/test")).isFalse();
        // loaded with load function
        assertThat(identityToRoleCache.get("spiffe://cassandra/sidecar/test")).isEqualTo("cassandra-role");
        assertThat(identityToRoleCache.getAll().size()).isOne();
    }

    @Test
    void testFindRoles()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test")).thenReturn("cassandra-role");
        when(mockDbAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test2")).thenReturn("cassandra-role2");
        Map<String, String> identityRoles = new HashMap<String, String>()
        { {
            put("spiffe://cassandra/sidecar/test", "cassandra-role");
            put("spiffe://cassandra/sidecar/test2", "cassandra-role2");
        } };
        when(mockDbAccessor.findAllIdentityToRoles()).thenReturn(identityRoles);
        SidecarConfiguration mockConfig = mockConfig();
        IdentityToRoleCache identityToRoleCache = new IdentityToRoleCache(vertx, executorPools, mockConfig, mockDbAccessor);
        assertThat(identityToRoleCache.containsKey("spiffe://cassandra/sidecar/test")).isTrue();
        assertThat(identityToRoleCache.containsKey("spiffe://cassandra/sidecar/test2")).isTrue();
        assertThat(identityToRoleCache.get("spiffe://cassandra/sidecar/test")).isEqualTo("cassandra-role");
        assertThat(identityToRoleCache.get("spiffe://cassandra/sidecar/test2")).isEqualTo("cassandra-role2");
        assertThat(identityToRoleCache.getAll().size()).isEqualTo(2);
    }

    @Test
    void testCacheWarming()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test")).thenReturn("cassandra-role");
        when(mockDbAccessor.findAllIdentityToRoles()).thenReturn(Collections.singletonMap("spiffe://cassandra/sidecar/test", "cassandra-role"));
        SidecarConfiguration mockConfig = mockConfig();
        IdentityToRoleCache identityToRoleCache = new IdentityToRoleCache(vertx, executorPools, mockConfig, mockDbAccessor);
        assertThat(identityToRoleCache.cache().asMap().size()).isZero();
        // warming cache
        identityToRoleCache.warmUp(5);
        assertThat(identityToRoleCache.getAll().size()).isOne();
        assertThat(identityToRoleCache.cache().asMap().size()).isOne();
        assertThat(identityToRoleCache.containsKey("spiffe://cassandra/sidecar/test")).isTrue();
        assertThat(identityToRoleCache.get("spiffe://cassandra/sidecar/test")).isEqualTo("cassandra-role");
    }

    @Test
    void testCacheWarmingOnSchemaReady()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test")).thenReturn("cassandra-role");
        when(mockDbAccessor.findAllIdentityToRoles()).thenReturn(Collections.singletonMap("spiffe://cassandra/sidecar/test", "cassandra-role"));

        SidecarConfiguration mockConfig = mockConfig();
        IdentityToRoleCache identityToRoleCache = new IdentityToRoleCache(vertx, executorPools, mockConfig, mockDbAccessor);
        assertThat(identityToRoleCache.cache().asMap().size()).isZero();

        // warming cache
        vertx.eventBus().publish(ON_SIDECAR_SCHEMA_INITIALIZED.address(), new JsonObject());

        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

        assertThat(identityToRoleCache.getAll().size()).isOne();
        assertThat(identityToRoleCache.cache().asMap().size()).isOne();
        assertThat(identityToRoleCache.containsKey("spiffe://cassandra/sidecar/test")).isTrue();
        assertThat(identityToRoleCache.get("spiffe://cassandra/sidecar/test")).isEqualTo("cassandra-role");
    }

    @Test
    void testNullEntriesFromSystemAuthDatabaseAccessor()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test")).thenReturn(null);
        when(mockDbAccessor.findAllIdentityToRoles()).thenReturn(Collections.emptyMap());

        SidecarConfiguration mockConfig = mockConfig();
        IdentityToRoleCache identityToRoleCache = new IdentityToRoleCache(vertx, executorPools, mockConfig, mockDbAccessor);

        assertThat(identityToRoleCache.containsKey("spiffe://cassandra/sidecar/test")).isFalse();
        assertThat(identityToRoleCache.get("spiffe://cassandra/sidecar/test")).isNull();
        assertThat(identityToRoleCache.getAll().size()).isZero();
    }

    @Test
    void testNoExceptionThrownWhenSchemaNotPrepared()
    {
        SystemAuthSchema systemAuthSchema = new SystemAuthSchema();
        CQLSessionProvider mockCqlSessionProvider = mock(CQLSessionProvider.class);
        SystemAuthDatabaseAccessor systemAuthDatabaseAccessor = new SystemAuthDatabaseAccessor(systemAuthSchema,
                                                                                               mockCqlSessionProvider,
                                                                                               new PermissionFactoryImpl());

        SidecarConfiguration mockConfig = mockConfig();

        IdentityToRoleCache identityToRoleCache = new IdentityToRoleCache(vertx, executorPools, mockConfig, systemAuthDatabaseAccessor);
        assertThat(identityToRoleCache.containsKey("spiffe://cassandra/sidecar/test")).isFalse();
    }

    private SidecarConfiguration mockConfig()
    {
        SidecarConfiguration mockConfig = mock(SidecarConfiguration.class);
        AccessControlConfiguration mockAccessControlConfig = mock(AccessControlConfiguration.class);
        when(mockConfig.accessControlConfiguration()).thenReturn(mockAccessControlConfig);
        CacheConfiguration mockCacheConfig = mock(CacheConfiguration.class);
        when(mockCacheConfig.enabled()).thenReturn(true);
        when(mockCacheConfig.expireAfterAccess()).thenReturn(MillisecondBoundConfiguration.parse("3s"));
        when(mockCacheConfig.maximumSize()).thenReturn(10L);
        when(mockCacheConfig.warmupRetries()).thenReturn(5);
        when(mockCacheConfig.warmupRetryInterval()).thenReturn(MillisecondBoundConfiguration.parse("1s"));
        when(mockAccessControlConfig.permissionCacheConfiguration()).thenReturn(mockCacheConfig);
        return mockConfig;
    }
}
