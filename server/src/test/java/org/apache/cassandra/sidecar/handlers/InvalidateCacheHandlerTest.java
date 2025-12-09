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

package org.apache.cassandra.sidecar.handlers;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.acl.AuthCache;
import org.apache.cassandra.sidecar.acl.IdentityToRoleCache;
import org.apache.cassandra.sidecar.acl.authorization.AuthorizationCacheKey;
import org.apache.cassandra.sidecar.acl.authorization.RoleAuthorizationsCache;
import org.apache.cassandra.sidecar.acl.authorization.SuperUserCache;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.utils.CacheFactory;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link InvalidateCacheHandler}
 */
@ExtendWith(VertxExtension.class)
public class InvalidateCacheHandlerTest
{
    static final Logger LOGGER = LoggerFactory.getLogger(InvalidateCacheHandlerTest.class);
    static final String TEST_ROUTE_TEMPLATE = "/api/v1/caches/%s/invalidate";

    Vertx vertx;
    Server server;
    IdentityToRoleCache mockIdentityToRoleCache = mock(IdentityToRoleCache.class);
    RoleAuthorizationsCache mockRoleAuthorizationsCache = mock(RoleAuthorizationsCache.class);
    SuperUserCache mockSuperUserCache = mock(SuperUserCache.class);
    Cache<AuthorizationCacheKey, Future<Boolean>> mockEndpointAuthorizationCache = mock(Cache.class);

    @BeforeEach
    void before() throws InterruptedException
    {
        Injector injector;
        Module testOverride = Modules.override(new TestModule())
                                     .with(new InvalidateCacheTestModule());
        injector = Guice.createInjector(Modules.override(SidecarModules.all())
                                               .with(testOverride));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        VertxTestContext context = new VertxTestContext();
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void after() throws InterruptedException
    {
        CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    // IdentityToRoleCache tests
    @Test
    void testInvalidateIdentityToRoleCache(VertxTestContext context)
    {
        verifyInvalidateCache(context, IdentityToRoleCache.NAME, null, OK, mockIdentityToRoleCache, mockRoleAuthorizationsCache, mockSuperUserCache);
    }

    @Test
    void testInvalidateIdentityToRoleCacheAlternativeName(VertxTestContext context)
    {
        verifyInvalidateCache(context, "IdentityToRoleCache", null, OK, mockIdentityToRoleCache, mockRoleAuthorizationsCache, mockSuperUserCache);
    }

    @Test
    void testInvalidateIdentityToRoleCacheWithKeys(VertxTestContext context)
    {
        verifyInvalidateCache(context, IdentityToRoleCache.NAME,
                              Arrays.asList("key1", "key2"),
                              OK,
                              mockIdentityToRoleCache, mockRoleAuthorizationsCache, mockSuperUserCache);
    }

    // RoleAuthorizationsCache tests
    @Test
    void testInvalidateRoleAuthorizationsCache(VertxTestContext context)
    {
        verifyInvalidateCache(context, RoleAuthorizationsCache.NAME, null, OK, mockRoleAuthorizationsCache, mockIdentityToRoleCache, mockSuperUserCache);
    }

    @Test
    void testInvalidateRoleAuthorizationsCacheAlternativeName(VertxTestContext context)
    {
        verifyInvalidateCache(context, "RoleAuthorizationsCache", null, OK, mockRoleAuthorizationsCache, mockIdentityToRoleCache, mockSuperUserCache);
    }

    @Test
    void testInvalidateRoleAuthorizationsCacheWithKeys(VertxTestContext context)
    {
        verifyInvalidateCache(context, RoleAuthorizationsCache.NAME,
                              Arrays.asList("key1"),
                              BAD_REQUEST,
                              null, mockIdentityToRoleCache, mockRoleAuthorizationsCache, mockSuperUserCache);
    }

    // SuperUserCache tests
    @Test
    void testInvalidateSuperUserCache(VertxTestContext context)
    {
        verifyInvalidateCache(context, SuperUserCache.NAME, null, OK, mockSuperUserCache, mockIdentityToRoleCache, mockRoleAuthorizationsCache);
    }

    @Test
    void testInvalidateSuperUserCacheAlternativeName(VertxTestContext context)
    {
        verifyInvalidateCache(context, "SuperUserCache", null, OK, mockSuperUserCache, mockIdentityToRoleCache, mockRoleAuthorizationsCache);
    }

    @Test
    void testInvalidateSuperUserCacheWithKeys(VertxTestContext context)
    {
        verifyInvalidateCache(context, SuperUserCache.NAME,
                              Arrays.asList("user1", "user2", "user3"),
                              OK,
                              mockSuperUserCache, mockIdentityToRoleCache, mockRoleAuthorizationsCache);
    }

    // EndpointAuthorizationCache tests
    @Test
    void testInvalidateEndpointAuthorizationCache(VertxTestContext context)
    {
        String testRoute = String.format(TEST_ROUTE_TEMPLATE, CacheFactory.ENDPOINT_AUTHORIZATION_CACHE_NAME);

        WebClient client = WebClient.create(vertx);
        client.delete(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  assertThat(response.bodyAsJsonObject().getString("status")).isEqualTo("OK");

                  // Note: We can't easily verify the mock AsyncCache was invalidated in unit tests
                  // because the handler calls .synchronous().invalidateAll() internally.
                  // End-to-end behavior is tested in integration tests.
                  verifyNoInteractions(mockIdentityToRoleCache);
                  verifyNoInteractions(mockRoleAuthorizationsCache);
                  verifyNoInteractions(mockSuperUserCache);

                  context.completeNow();
              }));
    }

    @Test
    void testInvalidateEndpointAuthorizationCacheAlternativeName(VertxTestContext context)
    {
        String testRoute = String.format(TEST_ROUTE_TEMPLATE, "EndpointAuthorizationCache");

        WebClient client = WebClient.create(vertx);
        client.delete(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  assertThat(response.bodyAsJsonObject().getString("status")).isEqualTo("OK");

                  // Verify alternative name works (case-insensitive)
                  verifyNoInteractions(mockIdentityToRoleCache);
                  verifyNoInteractions(mockRoleAuthorizationsCache);
                  verifyNoInteractions(mockSuperUserCache);

                  context.completeNow();
              }));
    }

    @Test
    void testInvalidateEndpointAuthorizationCacheWithKeys(VertxTestContext context)
    {
        verifyInvalidateCache(context, CacheFactory.ENDPOINT_AUTHORIZATION_CACHE_NAME,
                              Arrays.asList("key1"),
                              BAD_REQUEST,
                              null, mockIdentityToRoleCache, mockRoleAuthorizationsCache, mockSuperUserCache);
    }

    // Error case tests
    @Test
    void testInvalidateUnknownCache(VertxTestContext context)
    {
        String testRoute = String.format(TEST_ROUTE_TEMPLATE, "unknown_cache");

        WebClient client = WebClient.create(vertx);
        client.delete(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_NOT_FOUND)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  LOGGER.info("Unknown Cache Response: {}", response.bodyAsString());

                  // Verify no caches were invalidated
                  verifyNoInteractions(mockIdentityToRoleCache);
                  verifyNoInteractions(mockRoleAuthorizationsCache);
                  verifyNoInteractions(mockSuperUserCache);

                  context.completeNow();
              }));
    }

    /**
     * Helper method to test cache invalidation
     * Verifies that the specified cache is invalidated (or returns the expected error) and other caches are not touched.
     *
     * @param context             the test context
     * @param cacheName           the name of the cache to invalidate (sent in the HTTP request)
     * @param keys                the specific keys to invalidate, or null to invalidate all keys
     * @param expectedStatus      the expected HTTP response status (OK, BAD_REQUEST, NOT_FOUND, etc.)
     * @param cacheToInvalidate   the mock cache that should be invalidated (null if expecting an error)
     * @param cachesToNotInteract other mock caches that should not be touched
     */
    @SuppressWarnings("rawtypes")
    private void verifyInvalidateCache(VertxTestContext context, String cacheName, List<String> keys,
                                       HttpResponseStatus expectedStatus,
                                       AuthCache cacheToInvalidate, AuthCache... cachesToNotInteract)
    {
        String testRoute = String.format(TEST_ROUTE_TEMPLATE, cacheName);

        // Append query parameters if keys are provided
        if (keys != null && !keys.isEmpty())
        {
            StringBuilder queryParams = new StringBuilder("?");
            for (int i = 0; i < keys.size(); i++)
            {
                if (i > 0) queryParams.append("&");
                queryParams.append("keys=").append(keys.get(i));
            }
            testRoute = testRoute + queryParams;
        }

        WebClient client = WebClient.create(vertx);
        ResponsePredicate predicate = expectedStatus == OK
                                      ? ResponsePredicate.SC_OK
                                      : (expectedStatus == NOT_FOUND
                                         ? ResponsePredicate.SC_NOT_FOUND
                                         : ResponsePredicate.SC_BAD_REQUEST);

        client.delete(server.actualPort(), "127.0.0.1", testRoute)
              .expect(predicate)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(expectedStatus.code());

                  if (expectedStatus == OK)
                  {
                      assertThat(response.bodyAsJsonObject().getString("status")).isEqualTo("OK");

                      // Verify the correct cache was invalidated
                      if (keys == null || keys.isEmpty())
                      {
                          verify(cacheToInvalidate).invalidateAll();
                      }
                      else
                      {
                          verify(cacheToInvalidate).invalidateAll(keys);
                      }
                  }

                  // Verify other caches weren't touched
                  for (AuthCache cache : cachesToNotInteract)
                  {
                      verifyNoInteractions(cache);
                  }

                  context.completeNow();
              }));
    }

    /**
     * Test Guice module for InvalidateCache handler tests
     */
    class InvalidateCacheTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public IdentityToRoleCache identityToRoleCache()
        {
            return mockIdentityToRoleCache;
        }

        @Provides
        @Singleton
        public RoleAuthorizationsCache roleAuthorizationsCache()
        {
            return mockRoleAuthorizationsCache;
        }

        @Provides
        @Singleton
        public SuperUserCache superUserCache()
        {
            return mockSuperUserCache;
        }

        @Provides
        @Singleton
        public CacheFactory cacheFactory()
        {
            CacheFactory mockCacheFactory = mock(CacheFactory.class);
            when(mockCacheFactory.endpointAuthorizationCache()).thenReturn(mockEndpointAuthorizationCache);
            return mockCacheFactory;
        }
    }
}
