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
import org.junit.jupiter.api.extension.ExtendWith;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
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
@ExtendWith(VertxExtension.class)
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
    void testCacheSizeAlwaysOne(VertxTestContext testContext)
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
        cache.getAll()
             .compose(allEntries -> {
                 testContext.verify(() -> assertThat(allEntries.size()).isZero());
                 return cache.getAuthorizations("test_role1");
             })
             .compose(authorizations -> {
                 testContext.verify(() -> {
                     CacheStats initialCallStats = sidecarMetrics.server().cache().roleAuthorizationsCacheMetrics.snapshot();
                     assertThat(authorizations.size()).isEqualTo(2);
                     // Cache load records 2 misses: one from getIfPresent, one from cache.get
                     assertThat(initialCallStats.hitCount()).isZero();
                     assertThat(initialCallStats.missCount()).isEqualTo(2);
                 });
                 return cache.getAll();
             })
             .compose(allEntries -> {
                 testContext.verify(() -> assertThat(allEntries.size()).isOne());
                 // Add new role and wait for cache refresh
                 sidecarAuthorizations.put("test_role2",
                                           new HashSet<>(Collections.singletonList(BasicPermissions.
                                                                                   STREAM_SNAPSHOT.
                                                                                   toAuthorization())));
                 when(mockSidecarPermissionsAccessor.rolesToAuthorizations()).thenReturn(sidecarAuthorizations);
                 return vertx.timer(3000);
             })
             .compose(timerId -> cache.getAuthorizations("test_role2"))
             .compose(authorizations -> {
                 testContext.verify(() -> {
                     assertThat(authorizations.size()).isOne();
                     CacheStats afterRefreshStats = sidecarMetrics.server().cache().roleAuthorizationsCacheMetrics.snapshot();
                     assertThat(afterRefreshStats.hitCount()).isZero();
                     assertThat(afterRefreshStats.missCount()).isEqualTo(2);
                     assertThat(afterRefreshStats.evictionCount()).isOne();
                 });
                 return Future.all(cache.getAuthorizations("test_role2"),
                                   cache.getAuthorizations("non_existing_role"));
             })
             .onSuccess(cf -> {
                 Set<Authorization> testRole2Authorizations = cf.resultAt(0);
                 Set<Authorization> nonExistingRoleAuthorizations = cf.resultAt(1);
                 testContext.verify(() -> {
                     assertThat(testRole2Authorizations.size()).isOne();
                     assertThat(nonExistingRoleAuthorizations.size()).isZero();
                     CacheStats validEntryStats = sidecarMetrics.server().cache().roleAuthorizationsCacheMetrics.snapshot();
                     // Fetch for non_existing_role is a hit, since we load entire role_permissions table during each refresh
                     assertThat(validEntryStats.hitCount()).isEqualTo(2);
                     assertThat(validEntryStats.missCount()).isZero();
                 });
                 testContext.completeNow();
             })
             .onFailure(testContext::failNow);
    }

    @Test
    void testNotFoundUser(VertxTestContext testContext)
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
        cache.getAll()
             .compose(allEntries -> {
                 testContext.verify(() -> assertThat(allEntries.size()).isZero());
                 cache.warmUp(5);
                 return cache.getAll();
             })
             .compose(allEntries -> {
                 testContext.verify(() -> assertThat(allEntries.size()).isOne());
                 return cache.getAuthorizations("not_found_user");
             })
             .onSuccess(result -> {
                 testContext.verify(() -> assertThat(result).isEmpty());
                 testContext.completeNow();
             })
             .onFailure(testContext::failNow);
    }

    @Test
    void testBulkload(VertxTestContext testContext)
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
        cache.getAll()
             .compose(allEntries -> {
                 testContext.verify(() -> assertThat(allEntries.size()).isZero());
                 // warming cache
                 vertx.eventBus().publish(ON_SIDECAR_SCHEMA_INITIALIZED.address(), new JsonObject());
                 // wait for cache warming. system_auth.role_permissions table bulk loaded against a single key
                 return vertx.timer(3000);
             })
             .compose(timerId -> cache.getAll())
             .compose(allEntries -> {
                 testContext.verify(() -> assertThat(allEntries.size()).isOne());
                 return cache.get("unique_cache_entry_key");
             })
             .onSuccess(cacheEntry -> {
                 testContext.verify(() -> {
                     assertThat(cacheEntry.get("test_role1").size()).isOne();
                     assertThat(cacheEntry.get("test_role2").size()).isOne();
                 });
                 testContext.completeNow();
             })
             .onFailure(testContext::failNow);
    }

    @Test
    void testCacheDisabled(VertxTestContext testContext)
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
        Future.all(
            cache.getAuthorizations("test_role1"),
            cache.getAuthorizations("test_role2")
        )
        .onSuccess(results -> {
            testContext.verify(() -> {
                assertThat(results.<Set<Authorization>>resultAt(0).size()).isOne();
                assertThat(results.<Set<Authorization>>resultAt(1).size()).isOne();
            });
            testContext.completeNow();
        })
        .onFailure(testContext::failNow);
    }

    @Test
    void testEmptyEntriesFromSystemAuthDatabaseAccessor(VertxTestContext testContext)
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
        cache.getAll()
             .compose(allEntries -> {
                 testContext.verify(() -> assertThat(allEntries.size()).isZero());
                 // warming cache
                 vertx.eventBus().publish(ON_SIDECAR_SCHEMA_INITIALIZED.address(), new JsonObject());
                 // wait for cache warming. system_auth.role_permissions table bulk loaded against a single key
                 return vertx.timer(3000);
             })
             .compose(timerId -> cache.getAll())
             .compose(allEntries -> {
                 testContext.verify(() -> assertThat(allEntries.size()).isOne());
                 return cache.get("unique_cache_entry_key");
             })
             .onSuccess(cacheEntry -> {
                 testContext.verify(() -> assertThat(cacheEntry.size()).isZero());
                 testContext.completeNow();
             })
             .onFailure(testContext::failNow);
    }

    @Test
    void testSidecarPermissionsNotAddedWhenSchemaDisabled(VertxTestContext testContext)
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
        cache.getAll()
             .compose(allEntries -> {
                 testContext.verify(() -> assertThat(allEntries.size()).isZero());
                 // force warmup of cache
                 cache.warmUp(5);
                 return cache.getAll();
             })
             .compose(allEntries -> {
                 testContext.verify(() -> assertThat(allEntries.size()).isOne());
                 return cache.get("unique_cache_entry_key");
             })
             .onSuccess(cacheEntry -> {
                 testContext.verify(() -> {
                     assertThat(cacheEntry.get("test_role1").size()).isOne();
                     assertThat(cacheEntry.get("test_role2").size()).isOne();
                     assertThat(cacheEntry.get("test_role3")).isNull();
                 });
                 testContext.completeNow();
             })
             .onFailure(testContext::failNow);
    }

    @Test
    void testCacheLoadTime(VertxTestContext testContext)
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

        CacheStats initialStats = sidecarMetrics.server().cache().roleAuthorizationsCacheMetrics.snapshot();
        testContext.verify(() -> {
            assertThat(initialStats.loadCount()).isZero();
            assertThat(initialStats.totalLoadTime()).isZero();
        });

        cache.getAuthorizations("test_role1")
             .onSuccess(result -> {
                 testContext.verify(() -> {
                     CacheStats afterLoadStats = sidecarMetrics.server().cache().roleAuthorizationsCacheMetrics.snapshot();
                     assertThat(afterLoadStats.loadCount()).isOne();
                     assertThat(afterLoadStats.totalLoadTime()).isGreaterThan(0);
                     double averageLoadTime = afterLoadStats.averageLoadPenalty();
                     assertThat(averageLoadTime).isGreaterThan(0);
                 });
                 testContext.completeNow();
             })
             .onFailure(testContext::failNow);
    }

    @Test
    void testCacheLoadFailureStats(VertxTestContext testContext)
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

        CacheStats initialStats = sidecarMetrics.server().cache().roleAuthorizationsCacheMetrics.snapshot();
        testContext.verify(() -> assertThat(initialStats.loadFailureCount()).isZero());

        cache.getAuthorizations("test_role1")
             .onComplete(ar -> {
                 testContext.verify(() -> {
                     CacheStats afterFailureStats = sidecarMetrics.server().cache().roleAuthorizationsCacheMetrics.snapshot();
                     assertThat(afterFailureStats.loadFailureCount()).isEqualTo(1);
                     assertThat(afterFailureStats.loadCount()).isEqualTo(1);
                     assertThat(afterFailureStats.loadSuccessCount()).isEqualTo(0);
                 });
                 testContext.completeNow();
             });
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
        .hasMessageContaining("role_authorizations_cache must be configured with either refreshAfterWrite or expireAfterAccess");
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
