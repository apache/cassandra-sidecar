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
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.SystemAuthDatabaseAccessor;

import static org.apache.cassandra.sidecar.ExecutorPoolsHelper.createdSharedTestPool;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_SCHEMA_INITIALIZED;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link SuperUserCache}
 */
class SuperUserCacheTest
{
    Vertx vertx;
    ExecutorPools executorPools;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        executorPools = createdSharedTestPool(vertx);
    }

    @AfterEach
    void cleanup()
    {
        TestResourceReaper.create().with(vertx).with(executorPools).close();
    }

    @Test
    void testBulkLoad()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findAllRolesToSuperuserStatus()).thenReturn(ImmutableMap.of("test_role1", true,
                                                                                        "test_role2", false));
        SidecarConfiguration mockConfig = mockConfig();
        SuperUserCache cache = new SuperUserCache(vertx, executorPools, mockConfig, mockDbAccessor);
        assertThat(cache.getAll().size()).isZero();

        // warming cache
        vertx.eventBus().publish(ON_SIDECAR_SCHEMA_INITIALIZED.address(), new JsonObject());

        // wait for cache warming. system_auth.role_permissions table bulk loaded against a single key
        loopAssert(3, 100, () -> {
            assertThat(cache.getAll()).hasSize(2);
            assertThat(cache.isSuperUser("test_role1")).isTrue();
            assertThat(cache.isSuperUser("test_role2")).isFalse();
        });
    }

    @Test
    void testCacheDisabled()
    {
        Map<String, Boolean> superUserMap = new HashMap<>();
        superUserMap.put("test_role1", true);
        superUserMap.put("test_role2", false);
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.isSuperUser("test_role")).thenReturn(true);
        when(mockDbAccessor.findAllRolesToSuperuserStatus()).thenReturn(superUserMap);
        SidecarConfiguration mockConfig = mockConfig();
        when(mockConfig.accessControlConfiguration().permissionCacheConfiguration().enabled()).thenReturn(false);
        SuperUserCache superUserCache = new SuperUserCache(vertx, executorPools, mockConfig, mockDbAccessor);
        assertThat(superUserCache.get("test_role")).isTrue();
        assertThat(superUserCache.isSuperUser("test_role")).isTrue();
        assertThat(superUserCache.getAll().size()).isEqualTo(2);
    }

    @Test
    void testEmptyEntriesFetched()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findAllRolesToSuperuserStatus()).thenReturn(Collections.emptyMap());
        SidecarConfiguration mockConfig = mockConfig();
        SuperUserCache cache = new SuperUserCache(vertx, executorPools, mockConfig, mockDbAccessor);
        assertThat(cache.getAll().size()).isZero();

        // warming cache
        vertx.eventBus().publish(ON_SIDECAR_SCHEMA_INITIALIZED.address(), new JsonObject());

        // wait for cache warming. system_auth.role_permissions table bulk loaded against a single key
        loopAssert(3, 100, () -> assertThat(cache.getAll().size()).isZero());
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
