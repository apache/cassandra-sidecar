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

package org.apache.cassandra.sidecar.accesscontrol;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.db.SystemAuthDatabaseAccessor;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_ALL_CASSANDRA_CQL_READY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test {@link IdentityToRoleCache}
 */
class IdentityToRoleCacheTest
{
    Vertx vertx;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
    }

    @Test
    void testFindRole()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test")).thenReturn("cassandra-role");
        when(mockDbAccessor.findAllIdentityToRoles()).thenReturn(Collections.singletonMap("spiffe://cassandra/sidecar/test", "cassandra-role"));
        CacheConfiguration mockConfig = mockCacheConfig();
        IdentityToRoleCache identityToRoleCache = new IdentityToRoleCache(vertx, mockConfig, mockDbAccessor);
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
        CacheConfiguration mockConfig = mock(CacheConfiguration.class);
        when(mockConfig.enabled()).thenReturn(false);
        IdentityToRoleCache identityToRoleCache = new IdentityToRoleCache(vertx, mockConfig, mockDbAccessor);
        assertThat(identityToRoleCache.cache).isNull();
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
        CacheConfiguration mockConfig = mockCacheConfig();
        IdentityToRoleCache identityToRoleCache = new IdentityToRoleCache(vertx, mockConfig, mockDbAccessor);
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
        CacheConfiguration mockConfig = mockCacheConfig();
        IdentityToRoleCache identityToRoleCache = new IdentityToRoleCache(vertx, mockConfig, mockDbAccessor);
        assertThat(identityToRoleCache.cache.asMap().size()).isZero();
        // warming cache
        identityToRoleCache.warm();
        assertThat(identityToRoleCache.getAll().size()).isOne();
        assertThat(identityToRoleCache.cache.asMap().size()).isOne();
        assertThat(identityToRoleCache.containsKey("spiffe://cassandra/sidecar/test")).isTrue();
        assertThat(identityToRoleCache.get("spiffe://cassandra/sidecar/test")).isEqualTo("cassandra-role");
    }

    @Test
    void testCacheWarmingOnCqlReady()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test")).thenReturn("cassandra-role");
        when(mockDbAccessor.findAllIdentityToRoles()).thenReturn(Collections.singletonMap("spiffe://cassandra/sidecar/test", "cassandra-role"));

        CacheConfiguration mockConfig = mockCacheConfig();
        IdentityToRoleCache identityToRoleCache = new IdentityToRoleCache(vertx, mockConfig, mockDbAccessor);
        assertThat(identityToRoleCache.cache.asMap().size()).isZero();

        // warming cache
        vertx.eventBus().publish(ON_ALL_CASSANDRA_CQL_READY.address(), new JsonObject());

        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

        assertThat(identityToRoleCache.getAll().size()).isOne();
        assertThat(identityToRoleCache.cache.asMap().size()).isOne();
        assertThat(identityToRoleCache.containsKey("spiffe://cassandra/sidecar/test")).isTrue();
        assertThat(identityToRoleCache.get("spiffe://cassandra/sidecar/test")).isEqualTo("cassandra-role");
    }

    private CacheConfiguration mockCacheConfig()
    {
        CacheConfiguration mockConfig = mock(CacheConfiguration.class);
        when(mockConfig.enabled()).thenReturn(true);
        when(mockConfig.expireAfterAccessMillis()).thenReturn(3000L);
        when(mockConfig.maximumSize()).thenReturn(10L);
        when(mockConfig.warmupRetries()).thenReturn(5);
        when(mockConfig.warmupRetryIntervalMillis()).thenReturn(1000L);
        return mockConfig;
    }
}
