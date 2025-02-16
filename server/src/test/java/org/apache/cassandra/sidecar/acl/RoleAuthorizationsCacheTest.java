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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authorization.Authorization;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.acl.authorization.CassandraPermissions;
import org.apache.cassandra.sidecar.acl.authorization.RoleAuthorizationsCache;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.SidecarPermissionsDatabaseAccessor;
import org.apache.cassandra.sidecar.db.SystemAuthDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;

import static org.apache.cassandra.sidecar.ExecutorPoolsHelper.createdSharedTestPool;
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
    SidecarSchema mockSidecarSchema;
    ExecutorPools executorPools;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        mockSidecarSchema = mock(SidecarSchema.class);
        when(mockSidecarSchema.isInitialized()).thenReturn(true);
        executorPools = createdSharedTestPool(vertx);
    }

    @AfterEach
    void cleanup()
    {
        TestResourceReaper.create().with(vertx).with(executorPools).close();
    }

    @Test
    void testCacheSizeAlwaysOne() throws InterruptedException
    {
        Map<String, Set<Authorization>> cassandraAuthorizations = new HashMap<>();
        cassandraAuthorizations.put("test_role1", new HashSet<>(Collections.singletonList(CassandraPermissions.SELECT.toAuthorization())));
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findAllRolesAndPermissions()).thenReturn(cassandraAuthorizations);
        Map<String, Set<Authorization>> sidecarAuthorizations = new HashMap<>();
        sidecarAuthorizations.put("test_role1", new HashSet<>(Collections.singletonList(BasicPermissions.CREATE_SNAPSHOT.toAuthorization())));
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);
        when(mockSidecarPermissionsAccessor.rolesToAuthorizations()).thenReturn(sidecarAuthorizations);
        SidecarConfiguration mockConfig = mockConfig();
        RoleAuthorizationsCache cache = new RoleAuthorizationsCache(vertx,
                                                                    executorPools,
                                                                    mockConfig,
                                                                    mockSidecarSchema,
                                                                    mockDbAccessor,
                                                                    mockSidecarPermissionsAccessor);
        assertThat(cache.getAll().size()).isZero();
        assertThat(cache.getAuthorizations("test_role1").size()).isEqualTo(2);
        assertThat(cache.getAll().size()).isOne();

        sidecarAuthorizations.put("test_role2", new HashSet<>(Collections.singletonList(BasicPermissions.STREAM_SNAPSHOT.toAuthorization())));
        when(mockSidecarPermissionsAccessor.rolesToAuthorizations()).thenReturn(sidecarAuthorizations);

        // wait for cache entries to be refreshed
        Thread.sleep(3000);

        // New entries fetched during refreshes
        assertThat(cache.getAuthorizations("test_role2").size()).isOne();
        assertThat(cache.getAll().size()).isOne();
    }

    @Test
    void testNotFoundUser()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        Map<String, Set<Authorization>> cassandraAuthorizations = new HashMap<>();
        cassandraAuthorizations.put("test_role1", new HashSet<>(Collections.singletonList(CassandraPermissions.SELECT.toAuthorization())));
        when(mockDbAccessor.findAllRolesAndPermissions()).thenReturn(cassandraAuthorizations);
        Map<String, Set<Authorization>> sidecarAuthorizations = new HashMap<>();
        sidecarAuthorizations.put("test_role1", new HashSet<>(Collections.singletonList(BasicPermissions.CREATE_SNAPSHOT.toAuthorization())));
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);
        when(mockSidecarPermissionsAccessor.rolesToAuthorizations()).thenReturn(sidecarAuthorizations);
        SidecarConfiguration mockConfig = mockConfig();
        RoleAuthorizationsCache cache = new RoleAuthorizationsCache(vertx,
                                                                    executorPools,
                                                                    mockConfig,
                                                                    mockSidecarSchema,
                                                                    mockDbAccessor,
                                                                    mockSidecarPermissionsAccessor);
        assertThat(cache.getAll().size()).isZero();

        cache.warmUp(5);

        // New entries fetched during refreshes
        assertThat(cache.getAll().size()).isOne();
        assertThat(cache.getAuthorizations("test_role2")).isNull();
    }

    @Test
    void testBulkload() throws InterruptedException
    {
        Map<String, Set<Authorization>> sidecarAuthorizations = new HashMap<>();
        sidecarAuthorizations.put("test_role1", new HashSet<>(Collections.singletonList(BasicPermissions.CREATE_SNAPSHOT.toAuthorization())));
        sidecarAuthorizations.put("test_role2", new HashSet<>(Collections.singletonList(BasicPermissions.STREAM_SNAPSHOT.toAuthorization())));
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findAllRolesAndPermissions()).thenReturn(sidecarAuthorizations);
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);
        SidecarConfiguration mockConfig = mockConfig();
        RoleAuthorizationsCache cache = new RoleAuthorizationsCache(vertx,
                                                                    executorPools,
                                                                    mockConfig,
                                                                    mockSidecarSchema,
                                                                    mockDbAccessor,
                                                                    mockSidecarPermissionsAccessor);
        assertThat(cache.getAll().size()).isZero();

        // warming cache
        vertx.eventBus().publish(ON_SIDECAR_SCHEMA_INITIALIZED.address(), new JsonObject());

        // wait for cache warming. system_auth.role_permissions table bulk loaded against a single key
        Thread.sleep(3000);
        assertThat(cache.getAll().size()).isOne();
        assertThat(cache.get("unique_cache_entry_key").get("test_role1").size()).isOne();
        assertThat(cache.get("unique_cache_entry_key").get("test_role2").size()).isOne();
    }

    @Test
    void testCacheDisabled()
    {
        Map<String, Set<Authorization>> sidecarAuthorizations = new HashMap<>();
        sidecarAuthorizations.put("test_role1", new HashSet<>(Collections.singletonList(BasicPermissions.CREATE_SNAPSHOT.toAuthorization())));
        sidecarAuthorizations.put("test_role2", new HashSet<>(Collections.singletonList(BasicPermissions.STREAM_SNAPSHOT.toAuthorization())));
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findAllRolesAndPermissions()).thenReturn(sidecarAuthorizations);
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);
        SidecarConfiguration mockConfig = mockConfig();
        CacheConfiguration mockCacheConfig = mock(CacheConfiguration.class);
        when(mockCacheConfig.enabled()).thenReturn(true);
        when(mockCacheConfig.expireAfterAccess()).thenReturn(MillisecondBoundConfiguration.parse("1s"));
        when(mockCacheConfig.maximumSize()).thenReturn(100L);
        when(mockConfig.accessControlConfiguration().permissionCacheConfiguration()).thenReturn(mockCacheConfig);
        RoleAuthorizationsCache cache = new RoleAuthorizationsCache(vertx,
                                                                    executorPools,
                                                                    mockConfig,
                                                                    mockSidecarSchema,
                                                                    mockDbAccessor,
                                                                    mockSidecarPermissionsAccessor);
        assertThat(cache.getAuthorizations("test_role1").size()).isOne();
        assertThat(cache.getAuthorizations("test_role2").size()).isOne();
    }

    @Test
    void testEmptyEntriesFromSystemAuthDatabaseAccessor() throws InterruptedException
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findAllRolesAndPermissions()).thenReturn(Collections.emptyMap());
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);
        SidecarConfiguration mockConfig = mockConfig();
        RoleAuthorizationsCache cache = new RoleAuthorizationsCache(vertx,
                                                                    executorPools,
                                                                    mockConfig,
                                                                    mockSidecarSchema,
                                                                    mockDbAccessor,
                                                                    mockSidecarPermissionsAccessor);
        assertThat(cache.getAll().size()).isZero();

        // warming cache
        vertx.eventBus().publish(ON_SIDECAR_SCHEMA_INITIALIZED.address(), new JsonObject());

        // wait for cache warming. system_auth.role_permissions table bulk loaded against a single key
        Thread.sleep(3000);
        assertThat(cache.getAll().size()).isOne();
        assertThat(cache.get("unique_cache_entry_key").size()).isZero();
    }

    @Test
    void testSidecarPermissionsNotAddedWhenSchemaDisabled()
    {
        Map<String, Set<Authorization>> cassandraAuthorizations = new HashMap<>();
        cassandraAuthorizations.put("test_role1", new HashSet<>(Collections.singletonList(CassandraPermissions.SELECT.toAuthorization())));
        cassandraAuthorizations.put("test_role2", new HashSet<>(Collections.singletonList(CassandraPermissions.CREATE.toAuthorization())));
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findAllRolesAndPermissions()).thenReturn(cassandraAuthorizations);
        Map<String, Set<Authorization>> sidecarAuthorizations = new HashMap<>();
        sidecarAuthorizations.put("test_role3", new HashSet<>(Collections.singletonList(BasicPermissions.CREATE_SNAPSHOT.toAuthorization())));
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);
        when(mockSidecarPermissionsAccessor.rolesToAuthorizations()).thenReturn(sidecarAuthorizations);
        SidecarConfiguration mockConfig = mockConfig();
        SidecarSchema mockSidecarSchema = mock(SidecarSchema.class);
        when(mockSidecarSchema.isInitialized()).thenReturn(false);
        RoleAuthorizationsCache cache = new RoleAuthorizationsCache(vertx,
                                                                    executorPools,
                                                                    mockConfig,
                                                                    mockSidecarSchema,
                                                                    mockDbAccessor,
                                                                    mockSidecarPermissionsAccessor);
        assertThat(cache.getAll().size()).isZero();

        // force warmup of cache
        cache.warmUp(5);

        assertThat(cache.getAll().size()).isOne();
        assertThat(cache.get("unique_cache_entry_key").get("test_role1").size()).isOne();
        assertThat(cache.get("unique_cache_entry_key").get("test_role2").size()).isOne();
        assertThat(cache.get("unique_cache_entry_key").get("test_role3")).isNull();
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
        when(mockCacheConfig.expireAfterAccess()).thenReturn(MillisecondBoundConfiguration.parse("1s"));
        when(mockCacheConfig.maximumSize()).thenReturn(10L);
        when(mockCacheConfig.warmupRetries()).thenReturn(5);
        when(mockCacheConfig.expireAfterAccess()).thenReturn(MillisecondBoundConfiguration.parse("1s"));
        when(mockAccessControlConfig.permissionCacheConfiguration()).thenReturn(mockCacheConfig);
        return mockConfig;
    }
}
