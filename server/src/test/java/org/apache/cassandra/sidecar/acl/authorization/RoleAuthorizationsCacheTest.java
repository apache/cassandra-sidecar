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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.SidecarPermissionsDatabaseAccessor;
import org.apache.cassandra.sidecar.db.SystemAuthDatabaseAccessor;

import static org.apache.cassandra.sidecar.ExecutorPoolsHelper.createdSharedTestPool;
import static org.apache.cassandra.sidecar.acl.authorization.RoleAuthorizationsCache.UNIQUE_CACHE_ENTRY;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_SCHEMA_INITIALIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link RoleAuthorizationsCache}
 */
class RoleAuthorizationsCacheTest
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
    void testCacheSizeAlwaysOne() throws InterruptedException
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.getAllRolesAndPermissions())
        .thenReturn(Collections.singletonMap("test_role1", Collections.singleton(CassandraPermissions.SELECT.toAuthorization())));
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);
        when(mockSidecarPermissionsAccessor.getAllRolesAndPermissions())
        .thenReturn(Collections.singletonMap("test_role1", Collections.singleton(SidecarPermissions.CREATE_SNAPSHOT.toAuthorization())));
        SidecarConfiguration mockConfig = mockConfig();
        RoleAuthorizationsCache cache = new RoleAuthorizationsCache(vertx,
                                                                    executorPools,
                                                                    mockConfig,
                                                                    mockDbAccessor,
                                                                    mockSidecarPermissionsAccessor);
        assertThat(cache.getAll().size()).isZero();
        assertThat(cache.getAuthorizations("test_role1").size()).isEqualTo(2);
        assertThat(cache.getAll().size()).isOne();

        when(mockSidecarPermissionsAccessor.getAllRolesAndPermissions())
        .thenReturn(ImmutableMap.of("test_role1", Collections.singleton(SidecarPermissions.CREATE_SNAPSHOT.toAuthorization()),
                                    "test_role2", Collections.singleton(SidecarPermissions.STREAM_SSTABLE.toAuthorization())));

        // wait for cache entries to be refreshed
        Thread.sleep(3000);

        // New entries fetched during refreshes
        assertThat(cache.getAuthorizations("test_role2").size()).isOne();
        assertThat(cache.getAll().size()).isOne();
    }

    @Test
    void testNotFoundUser() throws Exception
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.getAllRolesAndPermissions())
        .thenReturn(Collections.singletonMap("test_role1", Collections.singleton(CassandraPermissions.SELECT.toAuthorization())));
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);
        when(mockSidecarPermissionsAccessor.getAllRolesAndPermissions())
        .thenReturn(Collections.singletonMap("test_role1", Collections.singleton(SidecarPermissions.CREATE_SNAPSHOT.toAuthorization())));
        SidecarConfiguration mockConfig = mockConfig();
        RoleAuthorizationsCache cache = new RoleAuthorizationsCache(vertx,
                                                                    executorPools,
                                                                    mockConfig,
                                                                    mockDbAccessor,
                                                                    mockSidecarPermissionsAccessor);
        assertThat(cache.getAll().size()).isZero();

        // wait for cache entries to be refreshed
        Thread.sleep(3000);

        // New entries fetched during refreshes
        assertThat(cache.getAll().size()).isOne();
        assertThat(cache.getAuthorizations("test_role2")).isNull();
    }


    @Test
    void testBulkload() throws InterruptedException
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.getAllRolesAndPermissions())
        .thenReturn(ImmutableMap.of("test_role1", Collections.singleton(SidecarPermissions.CREATE_SNAPSHOT.toAuthorization()),
                                    "test_role2", Collections.singleton(SidecarPermissions.STREAM_SSTABLE.toAuthorization())));
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);
        SidecarConfiguration mockConfig = mockConfig();
        RoleAuthorizationsCache cache = new RoleAuthorizationsCache(vertx,
                                                                    executorPools,
                                                                    mockConfig,
                                                                    mockDbAccessor,
                                                                    mockSidecarPermissionsAccessor);
        assertThat(cache.getAll().size()).isZero();

        // warming cache
        vertx.eventBus().publish(ON_SIDECAR_SCHEMA_INITIALIZED.address(), new JsonObject());

        // wait for cache warming. system_auth.role_permissions table bulk loaded against a single key
        Thread.sleep(3000);
        assertThat(cache.getAll().size()).isOne();
        assertThat(cache.get(UNIQUE_CACHE_ENTRY).get("test_role1").size()).isOne();
        assertThat(cache.get(UNIQUE_CACHE_ENTRY).get("test_role2").size()).isOne();
    }

    @Test
    void testCacheDisabled()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.getAllRolesAndPermissions())
        .thenReturn(ImmutableMap.of("test_role1", Collections.singleton(SidecarPermissions.CREATE_SNAPSHOT.toAuthorization()),
                                    "test_role2", Collections.singleton(SidecarPermissions.STREAM_SSTABLE.toAuthorization())));
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);
        SidecarConfiguration mockConfig = mockConfig();
        when(mockConfig.accessControlConfiguration().permissionCacheConfiguration().enabled()).thenReturn(false);
        RoleAuthorizationsCache cache = new RoleAuthorizationsCache(vertx,
                                                                    executorPools,
                                                                    mockConfig,
                                                                    mockDbAccessor,
                                                                    mockSidecarPermissionsAccessor);
        assertThat(cache.getAuthorizations("test_role1").size()).isOne();
        assertThat(cache.getAuthorizations("test_role2").size()).isOne();
    }

    @Test
    void testEmptyEntriesFromSystemAuthDatabaseAccessor() throws InterruptedException
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.getAllRolesAndPermissions()).thenReturn(Collections.emptyMap());
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);
        SidecarConfiguration mockConfig = mockConfig();
        RoleAuthorizationsCache cache = new RoleAuthorizationsCache(vertx,
                                                                    executorPools,
                                                                    mockConfig,
                                                                    mockDbAccessor,
                                                                    mockSidecarPermissionsAccessor);
        assertThat(cache.getAll().size()).isZero();

        // warming cache
        vertx.eventBus().publish(ON_SIDECAR_SCHEMA_INITIALIZED.address(), new JsonObject());

        // wait for cache warming. system_auth.role_permissions table bulk loaded against a single key
        Thread.sleep(3000);
        assertThat(cache.getAll().size()).isOne();
        assertThat(cache.get(UNIQUE_CACHE_ENTRY).size()).isZero();
    }

    @Test
    void testSidecarPermissionsNotAddedWhenSchemaDisabled() throws InterruptedException
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.getAllRolesAndPermissions())
        .thenReturn(ImmutableMap.of("test_role1", Collections.singleton(CassandraPermissions.SELECT.toAuthorization()),
                                    "test_role2", Collections.singleton(CassandraPermissions.CREATE.toAuthorization())));
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);
        when(mockDbAccessor.getAllRolesAndPermissions())
        .thenReturn(ImmutableMap.of("test_role3", Collections.singleton(SidecarPermissions.CREATE_SNAPSHOT.toAuthorization())));
        SidecarConfiguration mockConfig = mockConfig();
        ServiceConfiguration mockServiceConfig = mock(ServiceConfiguration.class);
        SchemaKeyspaceConfiguration mockSchemaConfig = mock(SchemaKeyspaceConfiguration.class);
        when(mockSchemaConfig.isEnabled()).thenReturn(false);
        when(mockServiceConfig.schemaKeyspaceConfiguration()).thenReturn(mockSchemaConfig);
        when(mockConfig.serviceConfiguration()).thenReturn(mockServiceConfig);
        RoleAuthorizationsCache cache = new RoleAuthorizationsCache(vertx,
                                                                    executorPools,
                                                                    mockConfig,
                                                                    mockDbAccessor,
                                                                    mockSidecarPermissionsAccessor);
        assertThat(cache.getAll().size()).isZero();

        // warming cache
        vertx.eventBus().publish(ON_SIDECAR_SCHEMA_INITIALIZED.address(), new JsonObject());

        // wait for cache warming. system_auth.role_permissions table bulk loaded against a single key
        Thread.sleep(3000);
        assertThat(cache.getAll().size()).isOne();
        assertThat(cache.get(UNIQUE_CACHE_ENTRY).get("test_role1").size()).isOne();
        assertThat(cache.get(UNIQUE_CACHE_ENTRY).get("test_role2").size()).isOne();
        assertThat(cache.get(UNIQUE_CACHE_ENTRY).get("test_role3")).isNull();
    }

    private SidecarConfiguration mockConfig()
    {
        SidecarConfiguration mockConfig = mock(SidecarConfiguration.class);
        ServiceConfiguration mockServiceConfig = mock(ServiceConfiguration.class);
        SchemaKeyspaceConfiguration mockSchemaConfig = mock(SchemaKeyspaceConfiguration.class);
        when(mockSchemaConfig.isEnabled()).thenReturn(true);
        when(mockServiceConfig.schemaKeyspaceConfiguration()).thenReturn(mockSchemaConfig);
        when(mockConfig.serviceConfiguration()).thenReturn(mockServiceConfig);
        AccessControlConfiguration mockAccessControlConfig = mock(AccessControlConfiguration.class);
        when(mockConfig.accessControlConfiguration()).thenReturn(mockAccessControlConfig);
        CacheConfiguration mockCacheConfig = mock(CacheConfiguration.class);
        when(mockCacheConfig.enabled()).thenReturn(true);
        when(mockCacheConfig.expireAfterAccessMillis()).thenReturn(3000L);
        when(mockCacheConfig.maximumSize()).thenReturn(10L);
        when(mockCacheConfig.warmupRetries()).thenReturn(5);
        when(mockCacheConfig.warmupRetryIntervalMillis()).thenReturn(1000L);
        when(mockAccessControlConfig.permissionCacheConfiguration()).thenReturn(mockCacheConfig);
        return mockConfig;
    }
}
