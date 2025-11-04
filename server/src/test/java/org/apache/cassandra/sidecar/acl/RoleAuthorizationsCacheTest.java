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

import com.github.benmanes.caffeine.cache.stats.CacheStats;
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
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetricsImpl;

import static org.apache.cassandra.sidecar.ExecutorPoolsHelper.createdSharedTestPool;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_SCHEMA_INITIALIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link RoleAuthorizationsCache}
 */
class RoleAuthorizationsCacheTest
{
    private static final MetricRegistryFactory FACTORY
    = new MetricRegistryFactory(RoleAuthorizationsCacheTest.class.getName(),
                                Collections.emptyList(),
                                Collections.emptyList());
    Vertx vertx;
    SidecarSchema mockSidecarSchema;
    ExecutorPools executorPools;
    SidecarMetrics sidecarMetrics;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        mockSidecarSchema = mock(SidecarSchema.class);
        when(mockSidecarSchema.isInitialized()).thenReturn(true);
        executorPools = createdSharedTestPool(vertx);
        sidecarMetrics = new SidecarMetricsImpl(FACTORY, null);
    }

    @AfterEach
    void cleanup()
    {
        TestResourceReaper.create().with(vertx).with(executorPools).close();
        FACTORY.getOrCreate().removeMatching((name, metric) -> true);
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
                                                                    mockSidecarPermissionsAccessor,
                                                                    sidecarMetrics);
        assertThat(cache.getAll().size()).isZero();
        assertThat(cache.getAuthorizations("test_role1").size()).isEqualTo(2);
        CacheStats initialCallStats = sidecarMetrics.server().cache().rolePermissionsCacheMetrics.snapshot();
        assertThat(initialCallStats.hitCount()).isZero();
        assertThat(initialCallStats.missCount()).isOne();
        assertThat(cache.getAll().size()).isOne();

        sidecarAuthorizations.put("test_role2", new HashSet<>(Collections.singletonList(BasicPermissions.STREAM_SNAPSHOT.toAuthorization())));
        when(mockSidecarPermissionsAccessor.rolesToAuthorizations()).thenReturn(sidecarAuthorizations);

        // wait for cache entries to be refreshed
        Thread.sleep(3000);

        // New entries fetched during refreshes
        assertThat(cache.getAuthorizations("test_role2").size()).isOne();
        CacheStats afterRefreshStats = sidecarMetrics.server().cache().rolePermissionsCacheMetrics.snapshot();
        assertThat(afterRefreshStats.hitCount()).isZero();
        assertThat(afterRefreshStats.missCount()).isOne();
        assertThat(cache.getAll().size()).isOne();
        assertThat(afterRefreshStats.evictionCount()).isOne();

        assertThat(cache.getAuthorizations("test_role2").size()).isOne();
        CacheStats validEntryStats = sidecarMetrics.server().cache().rolePermissionsCacheMetrics.snapshot();
        assertThat(validEntryStats.hitCount()).isOne();
        assertThat(validEntryStats.missCount()).isZero();

        // check for not existing role
        cache.getAuthorizations("non_existing_role");
        CacheStats afterMissStats = sidecarMetrics.server().cache().rolePermissionsCacheMetrics.snapshot();
        assertThat(afterMissStats.missCount()).isEqualTo(0);
        // It is a hit, since we load entire role_permissions table during each refresh
        assertThat(afterMissStats.hitCount()).isOne();
    }

    @Test
    void testMultipleLoadCacheStats()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findAllRolesAndPermissions()).thenReturn(new HashMap<>());
        Map<String, Set<Authorization>> sidecarAuthorizations = new HashMap<>();
        sidecarAuthorizations.put("test_role1", new HashSet<>(Collections.singletonList(BasicPermissions.CREATE_SNAPSHOT.toAuthorization())));
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);
        when(mockSidecarPermissionsAccessor.rolesToAuthorizations()).thenReturn(sidecarAuthorizations);
        // high cache expire time to test load stats
        SidecarConfiguration mockConfig = mockConfig(mockCacheConfig(false, true, "5m"));
        RoleAuthorizationsCache cache = new RoleAuthorizationsCache(vertx,
                                                                    executorPools,
                                                                    mockConfig,
                                                                    mockSidecarSchema,
                                                                    mockDbAccessor,
                                                                    mockSidecarPermissionsAccessor,
                                                                    sidecarMetrics);

        assertThat(cache.getAuthorizations("test_role1").size()).isEqualTo(1);
        assertThat(cache.getAuthorizations("test_role1").size()).isEqualTo(1);
        assertThat(cache.getAuthorizations("test_role1").size()).isEqualTo(1);
        assertThat(cache.getAuthorizations("test_role1").size()).isEqualTo(1);
        assertThat(cache.getAuthorizations("test_role1").size()).isEqualTo(1);

        CacheStats multipleRetrievalStats = sidecarMetrics.server().cache().rolePermissionsCacheMetrics.snapshot();
        assertThat(multipleRetrievalStats.hitCount()).isEqualTo(4);
        assertThat(multipleRetrievalStats.loadSuccessCount()).isEqualTo(1);
        assertThat(multipleRetrievalStats.loadFailureCount()).isEqualTo(0);
        assertThat(multipleRetrievalStats.missCount()).isEqualTo(1);
        assertThat(multipleRetrievalStats.loadCount()).isEqualTo(1);
    }

    @Test
    void testNotFoundUser()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        Map<String, Set<Authorization>> cassandraAuthorizations = cassandraAuthorizations();
        when(mockDbAccessor.findAllRolesAndPermissions()).thenReturn(cassandraAuthorizations);
        Map<String, Set<Authorization>> sidecarAuthorizations = sidecarAuthorizations();
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);
        when(mockSidecarPermissionsAccessor.rolesToAuthorizations()).thenReturn(sidecarAuthorizations);
        SidecarConfiguration mockConfig = mockConfig();
        RoleAuthorizationsCache cache = new RoleAuthorizationsCache(vertx,
                                                                    executorPools,
                                                                    mockConfig,
                                                                    mockSidecarSchema,
                                                                    mockDbAccessor,
                                                                    mockSidecarPermissionsAccessor,
                                                                    sidecarMetrics);
        assertThat(cache.getAll().size()).isZero();

        cache.warmUp(5);

        // New entries fetched during refreshes
        assertThat(cache.getAll().size()).isOne();
        assertThat(cache.getAuthorizations("not_found_user")).isNull();
    }

    @Test
    void testBulkload() throws InterruptedException
    {
        Map<String, Set<Authorization>> sidecarAuthorizations = sidecarAuthorizations();
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findAllRolesAndPermissions()).thenReturn(sidecarAuthorizations);
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);
        SidecarConfiguration mockConfig = mockConfig();
        RoleAuthorizationsCache cache = new RoleAuthorizationsCache(vertx,
                                                                    executorPools,
                                                                    mockConfig,
                                                                    mockSidecarSchema,
                                                                    mockDbAccessor,
                                                                    mockSidecarPermissionsAccessor,
                                                                    sidecarMetrics);
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
        Map<String, Set<Authorization>> sidecarAuthorizations = sidecarAuthorizations();
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
                                                                    mockSidecarPermissionsAccessor,
                                                                    sidecarMetrics);
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
                                                                    mockSidecarPermissionsAccessor,
                                                                    sidecarMetrics);
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
        Map<String, Set<Authorization>> cassandraAuthorizations = cassandraAuthorizations();
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findAllRolesAndPermissions()).thenReturn(cassandraAuthorizations);
        Map<String, Set<Authorization>> sidecarAuthorizations = sidecarAuthorizations();
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
                                                                    mockSidecarPermissionsAccessor,
                                                                    sidecarMetrics);
        assertThat(cache.getAll().size()).isZero();

        // force warmup of cache
        cache.warmUp(5);

        assertThat(cache.getAll().size()).isOne();
        assertThat(cache.get("unique_cache_entry_key").get("test_role1").size()).isOne();
        assertThat(cache.get("unique_cache_entry_key").get("test_role2").size()).isOne();
        assertThat(cache.get("unique_cache_entry_key").get("test_role3")).isNull();
    }

    @Test
    void testCacheLoadTime()
    {
        Map<String, Set<Authorization>> cassandraAuthorizations = cassandraAuthorizations();
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findAllRolesAndPermissions()).thenReturn(cassandraAuthorizations);
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);

        SidecarConfiguration mockConfig = mockConfig();
        RoleAuthorizationsCache cache = new RoleAuthorizationsCache(vertx,
                                                                    executorPools,
                                                                    mockConfig,
                                                                    mockSidecarSchema,
                                                                    mockDbAccessor,
                                                                    mockSidecarPermissionsAccessor,
                                                                    sidecarMetrics);

        CacheStats initialStats = sidecarMetrics.server().cache().rolePermissionsCacheMetrics.snapshot();
        assertThat(initialStats.loadCount()).isZero();
        assertThat(initialStats.totalLoadTime()).isZero();

        cache.getAuthorizations("test_role1");

        CacheStats afterLoadStats = sidecarMetrics.server().cache().rolePermissionsCacheMetrics.snapshot();
        assertThat(afterLoadStats.loadCount()).isOne();
        assertThat(afterLoadStats.totalLoadTime()).isGreaterThan(0);

        double averageLoadTime = afterLoadStats.averageLoadPenalty();
        assertThat(averageLoadTime).isGreaterThan(0);
    }

    @Test
    void testCacheLoadFailureStats()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findAllRolesAndPermissions()).thenThrow(new RuntimeException("Database connection failed"));
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);

        SidecarConfiguration mockConfig = mockConfig();
        RoleAuthorizationsCache cache = new RoleAuthorizationsCache(vertx,
                                                                    executorPools,
                                                                    mockConfig,
                                                                    mockSidecarSchema,
                                                                    mockDbAccessor,
                                                                    mockSidecarPermissionsAccessor,
                                                                    sidecarMetrics);

        CacheStats initialStats = sidecarMetrics.server().cache().rolePermissionsCacheMetrics.snapshot();
        assertThat(initialStats.loadFailureCount()).isZero();

        try
        {
            cache.getAuthorizations("test_role1");
        }
        catch (Exception e)
        {
            // ignore exception
        }

        CacheStats afterFailureStats = sidecarMetrics.server().cache().rolePermissionsCacheMetrics.snapshot();
        assertThat(afterFailureStats.loadFailureCount()).isEqualTo(1);
        assertThat(afterFailureStats.loadCount()).isEqualTo(1);
        assertThat(afterFailureStats.loadSuccessCount()).isEqualTo(0);
    }

    @Test
    void testCacheWithInvalidCacheConfig()
    {
        Map<String, Set<Authorization>> cassandraAuthorizations = cassandraAuthorizations();
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findAllRolesAndPermissions()).thenReturn(cassandraAuthorizations);
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);

        // Configure cache with both expireAfterAccess and refreshAfterWrite as null
        SidecarConfiguration mockConfig = mockConfig(mockCacheConfig(false, false, "1s"));

        assertThatThrownBy(
        () -> new RoleAuthorizationsCache(vertx,
                                          executorPools,
                                          mockConfig,
                                          mockSidecarSchema,
                                          mockDbAccessor,
                                          mockSidecarPermissionsAccessor,
                                          sidecarMetrics))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("role_permissions_cache must be configured with either refreshAfterWrite or expireAfterAccess");
    }

    @Test
    void testCacheWithOnlyExpireAfterAccess()
    {
        Map<String, Set<Authorization>> cassandraAuthorizations = cassandraAuthorizations();
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findAllRolesAndPermissions()).thenReturn(cassandraAuthorizations);
        SidecarPermissionsDatabaseAccessor mockSidecarPermissionsAccessor = mock(SidecarPermissionsDatabaseAccessor.class);

        // Configure cache with only expireAfterAccess
        CacheConfiguration mockCacheConfig = mockCacheConfig(true, false, "1s");
        SidecarConfiguration mockConfig = mockConfig(mockCacheConfig);

        RoleAuthorizationsCache authorizationsCache
        = new RoleAuthorizationsCache(vertx, executorPools, mockConfig, mockSidecarSchema, mockDbAccessor,
                                      mockSidecarPermissionsAccessor, sidecarMetrics);
        assertThat(authorizationsCache).isNotNull();
    }

    private CacheConfiguration mockCacheConfig()
    {
        return mockCacheConfig(true, true, "1s");
    }

    private CacheConfiguration mockCacheConfig(boolean setExpire, boolean setRefresh, String time)
    {
        CacheConfiguration mockCacheConfig = mock(CacheConfiguration.class);
        when(mockCacheConfig.enabled()).thenReturn(true);
        when(mockCacheConfig.expireAfterAccess()).thenReturn(setExpire ? MillisecondBoundConfiguration.parse("1s") : null);
        when(mockCacheConfig.refreshAfterWrite()).thenReturn(setRefresh ? MillisecondBoundConfiguration.parse("1s") : null);
        when(mockCacheConfig.maximumSize()).thenReturn(10L);
        when(mockCacheConfig.warmupRetries()).thenReturn(5);
        when(mockCacheConfig.warmupRetryInterval()).thenReturn(MillisecondBoundConfiguration.parse("1s"));
        return mockCacheConfig;
    }

    private SidecarConfiguration mockConfig()
    {
        return mockConfig(mockCacheConfig());
    }

    private SidecarConfiguration mockConfig(CacheConfiguration cacheConfiguration)
    {
        SidecarConfiguration mockConfig = mock(SidecarConfiguration.class);
        ServiceConfiguration mockServiceConfig = mock(ServiceConfiguration.class);
        SchemaKeyspaceConfiguration mockSchemaConfig = mock(SchemaKeyspaceConfiguration.class);
        when(mockSchemaConfig.isEnabled()).thenReturn(true);
        when(mockServiceConfig.schemaKeyspaceConfiguration()).thenReturn(mockSchemaConfig);
        when(mockConfig.serviceConfiguration()).thenReturn(mockServiceConfig);
        AccessControlConfiguration mockAccessControlConfig = mock(AccessControlConfiguration.class);
        when(mockConfig.accessControlConfiguration()).thenReturn(mockAccessControlConfig);
        when(mockAccessControlConfig.permissionCacheConfiguration()).thenReturn(cacheConfiguration);
        return mockConfig;
    }

    private Map<String, Set<Authorization>> sidecarAuthorizations()
    {
        Map<String, Set<Authorization>> sidecarAuthorizations = new HashMap<>();
        sidecarAuthorizations.put("test_role1", new HashSet<>(Collections.singletonList(BasicPermissions.CREATE_SNAPSHOT.toAuthorization())));
        sidecarAuthorizations.put("test_role2", new HashSet<>(Collections.singletonList(BasicPermissions.STREAM_SNAPSHOT.toAuthorization())));
        return sidecarAuthorizations;
    }

    private Map<String, Set<Authorization>> cassandraAuthorizations()
    {
        Map<String, Set<Authorization>> cassandraAuthorizations = new HashMap<>();
        cassandraAuthorizations.put("test_role1", new HashSet<>(Collections.singletonList(CassandraPermissions.SELECT.toAuthorization())));
        cassandraAuthorizations.put("test_role2", new HashSet<>(Collections.singletonList(CassandraPermissions.CREATE.toAuthorization())));
        return cassandraAuthorizations;
    }
}
