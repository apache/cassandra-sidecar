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

package org.apache.cassandra.sidecar.utils;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.testing.FakeTicker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.AuthorizationContext;
import org.apache.cassandra.sidecar.acl.authorization.AuthorizationCacheKey;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.SSTableImportConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.AccessControlConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.CacheConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SSTableImportConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.TestServiceConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetricsImpl;

import static org.apache.cassandra.sidecar.utils.AuthUtils.CASSANDRA_ROLES_ATTRIBUTE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link CacheFactory} class
 */
class CacheFactoryTest
{
    static final MillisecondBoundConfiguration SSTABLE_IMPORT_EXPIRE_AFTER_ACCESS = MillisecondBoundConfiguration.parse("2h");
    static final MillisecondBoundConfiguration AUTHORIZATION_EXPIRE_AFTER_ACCESS = MillisecondBoundConfiguration.parse("5m");
    static final MetricRegistryFactory FACTORY = new MetricRegistryFactory(CacheFactoryTest.class.getName(), List.of(), List.of());
    static final long SSTABLE_IMPORT_CACHE_MAX_SIZE = 10L;
    static final long AUTHORIZATION_CACHE_MAX_SIZE = 10L;
    private CacheFactory cacheFactory;
    private FakeTicker fakeTicker;

    @BeforeEach
    void setup()
    {
        fakeTicker = new FakeTicker();

        CacheConfiguration ssTableImportCacheConfiguration = CacheConfigurationImpl.builder()
                                                                                   .expireAfterAccess(SSTABLE_IMPORT_EXPIRE_AFTER_ACCESS)
                                                                                   .maximumSize(SSTABLE_IMPORT_CACHE_MAX_SIZE)
                                                                                   .build();

        CacheConfiguration authorizationCacheConfiguration = CacheConfigurationImpl.builder()
                                                                                   .expireAfterAccess(AUTHORIZATION_EXPIRE_AFTER_ACCESS)
                                                                                   .maximumSize(AUTHORIZATION_CACHE_MAX_SIZE)
                                                                                   .enabled(true)
                                                                                   .build();

        SSTableImportConfiguration ssTableImportConfiguration =
        new SSTableImportConfigurationImpl(ssTableImportCacheConfiguration);
        ServiceConfiguration serviceConfiguration =
        TestServiceConfiguration.builder()
                                .sstableImportConfiguration(ssTableImportConfiguration)
                                .build();
        AccessControlConfiguration accessControlConfiguration
        = AccessControlConfigurationImpl.builder()
                                        .enabled(true)
                                        .permissionCacheConfiguration(authorizationCacheConfiguration)
                                        .build();
        SidecarConfiguration sidecarConfiguration = mock(SidecarConfiguration.class);
        when(sidecarConfiguration.accessControlConfiguration()).thenReturn(accessControlConfiguration);
        when(sidecarConfiguration.serviceConfiguration()).thenReturn(serviceConfiguration);
        SSTableImporter mockSSTableImporter = mock(SSTableImporter.class);
        SidecarMetrics sidecarMetrics = new SidecarMetricsImpl(FACTORY, null);
        cacheFactory = new CacheFactory(sidecarConfiguration, mockSSTableImporter, sidecarMetrics, fakeTicker::read);
    }

    @Test
    void testSSTableImportCacheExpiration() throws ExecutionException, InterruptedException
    {
        Cache<SSTableImporter.ImportOptions, Future<Void>> cache = cacheFactory.ssTableImportCache();
        SSTableImporter.ImportOptions type1Options1 = buildImportOptions("ks", "tbl", "uuid");
        SSTableImporter.ImportOptions type1Options2 = buildImportOptions("ks", "tbl", "uuid");
        SSTableImporter.ImportOptions type1Options3 = buildImportOptions("ks", "tbl", "uuid");
        SSTableImporter.ImportOptions type1Options4 = buildImportOptions("ks", "tbl", "uuid");
        SSTableImporter.ImportOptions type1Options5 = buildImportOptions("ks", "tbl", "uuid");
        SSTableImporter.ImportOptions type2Options1 = buildImportOptions("ks2", "tbl2", "uuid");

        Void result1 = ssTableImportCacheEntry(cache, type1Options1, mock(Void.class));
        Void type2Result1 = ssTableImportCacheEntry(cache, type2Options1, mock(Void.class));

        assertThat(result1).isNotNull()
                           .isNotSameAs(type2Result1);

        // advance ticker 1 minute
        fakeTicker.advance(1, TimeUnit.MINUTES);

        // should get the same instance, type1Options2.equals(type1Options1)
        Void result2 = ssTableImportCacheEntry(cache, type1Options2, mock(Void.class));
        assertThat(result2).isSameAs(result1); // same instance

        // advance ticker 1 hour
        fakeTicker.advance(1, TimeUnit.HOURS);

        // should still get the same instance
        Void result3 = ssTableImportCacheEntry(cache, type1Options3, mock(Void.class));
        assertThat(result3).isSameAs(result1);

        // advance ticker 1 hour and 59 minutes and 59 seconds
        fakeTicker.advance(1, TimeUnit.HOURS);
        fakeTicker.advance(59, TimeUnit.MINUTES);
        fakeTicker.advance(59, TimeUnit.SECONDS);

        // should still get the same instance
        Void result4 = ssTableImportCacheEntry(cache, type1Options4, mock(Void.class));
        assertThat(result4).isSameAs(result1);

        // advance ticker for 2 hours
        fakeTicker.advance(2, TimeUnit.HOURS);

        // should get a different instance
        Void result5 = ssTableImportCacheEntry(cache, type1Options5, mock(Void.class));
        assertThat(result5).isNotSameAs(result1);
    }

    @Test
    void testSSTableImportCacheLimit() throws ExecutionException, InterruptedException
    {
        Cache<SSTableImporter.ImportOptions, Future<Void>> cache = cacheFactory.ssTableImportCache();
        cache.invalidateAll(); // make sure our cache is emptied out before testing
        int n = (int) SSTABLE_IMPORT_CACHE_MAX_SIZE * 2;
        for (int i = 0; i < n; i++)
        {
            Void mockVoid = mock(Void.class);
            SSTableImporter.ImportOptions importOptions = buildImportOptions("ks" + i, "tbl" + i, "uuid" + i);
            Void result = ssTableImportCacheEntry(cache, importOptions, mockVoid);
            assertThat(result).isNotNull();
        }
        assertThat(cache.estimatedSize()).isLessThanOrEqualTo(SSTABLE_IMPORT_CACHE_MAX_SIZE);
    }

    @Test
    void testConcurrentThreadsAccessingSameKey() throws InterruptedException, ExecutionException
    {
        Cache<SSTableImporter.ImportOptions, Future<Void>> cache = cacheFactory.ssTableImportCache();
        final int nThreads = 20;
        final ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        final Void[] voidArray = new Void[nThreads];
        final CountDownLatch latch = new CountDownLatch(nThreads);

        for (int i = 0; i < nThreads; i++)
        {
            final int finalI = i;
            pool.submit(() -> {
                try
                {
                    // Invoke getDirectory roughly at the same time
                    latch.countDown();
                    latch.await();
                    SSTableImporter.ImportOptions importOptions = buildImportOptions("ks", "tbl", "uuid");
                    // The first thread to win creates the object, the rest should get the same instance
                    voidArray[finalI] = ssTableImportCacheEntry(cache, importOptions, mock(Void.class));
                    fakeTicker.advance(1, TimeUnit.MINUTES);
                }
                catch (InterruptedException | ExecutionException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }

        pool.shutdown();
        assertThat(pool.awaitTermination(1, TimeUnit.MINUTES)).isTrue();

        for (int i = 1; i < nThreads; i++)
        {
            assertThat(voidArray[i]).isSameAs(voidArray[0]);
        }

        // advance ticker for 4 hours
        fakeTicker.advance(4, TimeUnit.HOURS);
        SSTableImporter.ImportOptions importOptions = buildImportOptions("ks", "tbl", "uuid");
        assertThat(ssTableImportCacheEntry(cache, importOptions, mock(Void.class))).isNotSameAs(voidArray[0]);
    }

    @Test
    void testEndpointAuthorizationCacheExpiration() throws ExecutionException, InterruptedException
    {
        AsyncCache<AuthorizationCacheKey, Future<Boolean>> cache = cacheFactory.endpointAuthorizationCache();
        AuthorizationCacheKey key1
        = createAuthorizationCacheKey(1, "user1", List.of("role1"), "ks1", "tbl1");
        AuthorizationCacheKey key2
        = createAuthorizationCacheKey(1, "user1", List.of("role1"), "ks1", "tbl1");
        AuthorizationCacheKey key3
        = createAuthorizationCacheKey(1, "user1", List.of("role1"), "ks1", "tbl1");
        AuthorizationCacheKey key4
        = createAuthorizationCacheKey(1, "user1", List.of("role1"), "ks1", "tbl1");
        AuthorizationCacheKey otherKey
        = createAuthorizationCacheKey(2, "user2", List.of("role2"), "ks2", "tbl2");

        Boolean result1 = authorizationCacheEntry(cache, key1, true);
        Boolean otherResult = authorizationCacheEntry(cache, otherKey, false);

        assertThat(result1).isTrue();
        assertThat(otherResult).isFalse();

        // advance ticker 2 minutes
        fakeTicker.advance(2, TimeUnit.MINUTES);

        // should get the same cached result
        Boolean result2 = authorizationCacheEntry(cache, key2, false);
        assertThat(result2).isTrue(); // cached value, not the new value

        // advance ticker 1 minutes and 59 seconds (total: 3m 59s)
        fakeTicker.advance(1, TimeUnit.MINUTES);
        fakeTicker.advance(59, TimeUnit.SECONDS);

        // should still get the same cached result
        Boolean result3 = authorizationCacheEntry(cache, key3, false);
        assertThat(result3).isTrue();

        fakeTicker.advance(10, TimeUnit.MINUTES);

        // should get a new value (cache expired)
        Boolean result4 = authorizationCacheEntry(cache, key4, false);
        assertThat(result4).isFalse(); // new value
    }

    @Test
    void testEndpointAuthorizationCacheDifferentKeys() throws ExecutionException, InterruptedException
    {
        AsyncCache<AuthorizationCacheKey, Future<Boolean>> cache = cacheFactory.endpointAuthorizationCache();

        // Different handler IDs
        AuthorizationCacheKey key1
        = createAuthorizationCacheKey(1, "user1", List.of("role1"), "ks1", "tbl1");
        AuthorizationCacheKey key2
        = createAuthorizationCacheKey(2, "user1", List.of("role1"), "ks1", "tbl1");

        Boolean result1 = authorizationCacheEntry(cache, key1, true);
        Boolean result2 = authorizationCacheEntry(cache, key2, false);

        assertThat(result1).isTrue();
        assertThat(result2).isFalse(); // different handler ID = different cache entry
    }

    @Test
    void testEndpointAuthorizationCacheThrowsExceptionWhenExpireAfterAccessIsMissing()
    {
        CacheConfiguration ssTableImportCacheConfiguration
        = CacheConfigurationImpl.builder()
                                .expireAfterAccess(SSTABLE_IMPORT_EXPIRE_AFTER_ACCESS)
                                .maximumSize(SSTABLE_IMPORT_CACHE_MAX_SIZE)
                                .build();

        // Authorization cache configuration WITHOUT expireAfterAccess
        CacheConfiguration authorizationCacheConfiguration
        = CacheConfigurationImpl.builder().maximumSize(AUTHORIZATION_CACHE_MAX_SIZE).enabled(true).build();

        SSTableImportConfiguration ssTableImportConfiguration =
        new SSTableImportConfigurationImpl(ssTableImportCacheConfiguration);
        ServiceConfiguration serviceConfiguration =
        TestServiceConfiguration.builder()
                                .sstableImportConfiguration(ssTableImportConfiguration)
                                .build();
        AccessControlConfiguration accessControlConfiguration
        = AccessControlConfigurationImpl.builder()
                                        .enabled(true)
                                        .permissionCacheConfiguration(authorizationCacheConfiguration)
                                        .build();
        SidecarConfiguration sidecarConfiguration = mock(SidecarConfiguration.class);
        when(sidecarConfiguration.accessControlConfiguration()).thenReturn(accessControlConfiguration);
        when(sidecarConfiguration.serviceConfiguration()).thenReturn(serviceConfiguration);
        SSTableImporter mockSSTableImporter = mock(SSTableImporter.class);
        SidecarMetrics sidecarMetrics = new SidecarMetricsImpl(FACTORY, null);

        assertThatThrownBy(() -> new CacheFactory(sidecarConfiguration, mockSSTableImporter, sidecarMetrics, fakeTicker::read))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Authorization handler cache must be configured with expireAfterAccess");
    }

    private Void ssTableImportCacheEntry(Cache<SSTableImporter.ImportOptions, Future<Void>> cache,
                                         SSTableImporter.ImportOptions key, Void value)
    throws ExecutionException, InterruptedException
    {
        Future<Void> voidFuture = cache.get(key, k -> Future.succeededFuture(value));
        assertThat(voidFuture).isNotNull();
        return voidFuture.toCompletionStage().toCompletableFuture().get();
    }

    private static SSTableImporter.ImportOptions buildImportOptions(String keyspace, String tableName, String uuid)
    {
        return new SSTableImporter.ImportOptions.Builder()
               .keyspace(keyspace)
               .tableName(tableName)
               .directory("/tmp/" + uuid)
               .uploadId(uuid)
               .host("localhost")
               .build();
    }

    private AuthorizationCacheKey createAuthorizationCacheKey(int handlerId, String username, List<String> roles,
                                                              String keyspace, String table)
    {
        User user = User.fromName(username);
        user.attributes().put(CASSANDRA_ROLES_ATTRIBUTE_NAME, roles);
        MultiMap variables = MultiMap.caseInsensitiveMultiMap()
                                     .add("keyspace", keyspace)
                                     .add("table", table);
        AuthorizationContext authContext = AuthorizationContext.create(user);
        variables.forEach(entry -> authContext.variables().add(entry.getKey(), entry.getValue()));
        return AuthorizationCacheKey.create(handlerId, authContext);
    }

    private Boolean authorizationCacheEntry(AsyncCache<AuthorizationCacheKey, Future<Boolean>> cache,
                                            AuthorizationCacheKey key,
                                            Boolean value) throws ExecutionException, InterruptedException
    {
        CompletableFuture<Future<Boolean>> future = cache.get(key, k -> Future.succeededFuture(value));
        assertThat(future).isNotNull();
        return future.get().toCompletionStage().toCompletableFuture().get();
    }
}
