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
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.db.SystemAuthDatabaseAccessor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test {@link IdentityRoleCache}
 */
class IdentityRoleCacheTest
{
    @Test
    void testFindRole()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test")).thenReturn("cassandra-role");
        when(mockDbAccessor.findAllIdentityRoles()).thenReturn(Collections.singletonMap("spiffe://cassandra/sidecar/test", "cassandra-role"));
        CacheConfiguration mockConfig = mockCacheConfig();
        IdentityRoleCache identityRoleCache = new IdentityRoleCache(mockConfig, mockDbAccessor);
        assertThat(identityRoleCache.contains("spiffe://cassandra/sidecar/test")).isTrue();
        assertThat(identityRoleCache.get("spiffe://cassandra/sidecar/test")).isEqualTo("cassandra-role");
        assertThat(identityRoleCache.getAll().size()).isOne();
    }

    @Test
    void testCacheDisabled()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test")).thenReturn("cassandra-role");
        when(mockDbAccessor.findAllIdentityRoles()).thenReturn(Collections.singletonMap("spiffe://cassandra/sidecar/test", "cassandra-role"));
        CacheConfiguration mockConfig = mock(CacheConfiguration.class);
        when(mockConfig.enabled()).thenReturn(false);
        IdentityRoleCache identityRoleCache = new IdentityRoleCache(mockConfig, mockDbAccessor);
        assertThat(identityRoleCache.cache).isNull();
        assertThat(identityRoleCache.contains("spiffe://cassandra/sidecar/test")).isFalse();
        // loaded with load function
        assertThat(identityRoleCache.get("spiffe://cassandra/sidecar/test")).isEqualTo("cassandra-role");
        assertThat(identityRoleCache.getAll().size()).isZero();
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
        when(mockDbAccessor.findAllIdentityRoles()).thenReturn(identityRoles);
        CacheConfiguration mockConfig = mockCacheConfig();
        IdentityRoleCache identityRoleCache = new IdentityRoleCache(mockConfig, mockDbAccessor);
        assertThat(identityRoleCache.contains("spiffe://cassandra/sidecar/test")).isTrue();
        assertThat(identityRoleCache.contains("spiffe://cassandra/sidecar/test2")).isTrue();
        assertThat(identityRoleCache.get("spiffe://cassandra/sidecar/test")).isEqualTo("cassandra-role");
        assertThat(identityRoleCache.get("spiffe://cassandra/sidecar/test2")).isEqualTo("cassandra-role2");
        assertThat(identityRoleCache.getAll().size()).isEqualTo(2);
    }

    @Test
    void testUpdateExpireAfterMillisEntries()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test")).thenReturn("cassandra-role");
        when(mockDbAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test2")).thenReturn("cassandra-role2");
        Map<String, String> identityRoles = new HashMap<String, String>()
        { {
            put("spiffe://cassandra/sidecar/test", "cassandra-role");
            put("spiffe://cassandra/sidecar/test2", "cassandra-role2");
        } };
        when(mockDbAccessor.findAllIdentityRoles()).thenReturn(identityRoles);

        CacheConfiguration mockConfig = mockCacheConfig();
        IdentityRoleCache identityRoleCache = new IdentityRoleCache(mockConfig, mockDbAccessor);

        assertThat(identityRoleCache.getAll().size()).isZero();
        assertThat(identityRoleCache.get("spiffe://cassandra/sidecar/test")).isEqualTo("cassandra-role");
        assertThat(identityRoleCache.get("spiffe://cassandra/sidecar/test2")).isEqualTo("cassandra-role2");
        assertThat(identityRoleCache.getAll().size()).isEqualTo(2);

        identityRoleCache.setExpireAfterMillis(0L);
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

        assertThat(identityRoleCache.getAll().size()).isEqualTo(0);
    }

    @Test
    void testUpdateMaxEntries()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test")).thenReturn("cassandra-role");
        when(mockDbAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test2")).thenReturn("cassandra-role2");
        Map<String, String> identityRoles = new HashMap<String, String>()
        { {
            put("spiffe://cassandra/sidecar/test", "cassandra-role");
            put("spiffe://cassandra/sidecar/test2", "cassandra-role2");
        } };
        when(mockDbAccessor.findAllIdentityRoles()).thenReturn(identityRoles);

        CacheConfiguration mockConfig = mockCacheConfig();
        IdentityRoleCache identityRoleCache = new IdentityRoleCache(mockConfig, mockDbAccessor);

        assertThat(identityRoleCache.getAll().size()).isZero();
        assertThat(identityRoleCache.get("spiffe://cassandra/sidecar/test")).isEqualTo("cassandra-role");
        assertThat(identityRoleCache.get("spiffe://cassandra/sidecar/test2")).isEqualTo("cassandra-role2");
        assertThat(identityRoleCache.getAll().size()).isEqualTo(2);

        identityRoleCache.setMaxEntries(0L);
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

        assertThat(identityRoleCache.getAll().size()).isEqualTo(0);
    }

    @Test
    void testCacheWarming()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test")).thenReturn("cassandra-role");
        when(mockDbAccessor.findAllIdentityRoles()).thenReturn(Collections.singletonMap("spiffe://cassandra/sidecar/test", "cassandra-role"));
        CacheConfiguration mockConfig = mockCacheConfig();
        IdentityRoleCache identityRoleCache = new IdentityRoleCache(mockConfig, mockDbAccessor);
        assertThat(identityRoleCache.getAll().size()).isZero();
        // warming cache
        identityRoleCache.warm();
        assertThat(identityRoleCache.getAll().size()).isOne();
        assertThat(identityRoleCache.contains("spiffe://cassandra/sidecar/test")).isTrue();
        assertThat(identityRoleCache.get("spiffe://cassandra/sidecar/test")).isEqualTo("cassandra-role");
    }

    @Test
    void testInvalidateNotSupported()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        CacheConfiguration mockConfig = mockCacheConfig();
        IdentityRoleCache identityRoleCache = new IdentityRoleCache(mockConfig, mockDbAccessor);
        assertThatThrownBy(() -> identityRoleCache.invalidate("k")).isInstanceOf(UnsupportedOperationException.class);
    }

    private CacheConfiguration mockCacheConfig()
    {
        CacheConfiguration mockConfig = mock(CacheConfiguration.class);
        when(mockConfig.enabled()).thenReturn(true);
        when(mockConfig.expireAfterAccessMillis()).thenReturn(3000L);
        when(mockConfig.maximumSize()).thenReturn(10L);
        return mockConfig;
    }
}
