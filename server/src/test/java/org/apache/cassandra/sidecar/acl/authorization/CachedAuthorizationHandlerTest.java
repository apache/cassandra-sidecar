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

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.AndAuthorization;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.AuthorizationProvider;
import io.vertx.ext.auth.authorization.PermissionBasedAuthorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.AdminIdentityResolver;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetricsImpl;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.RoutingContextUtils;

import static org.apache.cassandra.sidecar.utils.AuthUtils.CASSANDRA_ROLES_ATTRIBUTE_NAME;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for {@link CachedAuthorizationHandler}
 */
class CachedAuthorizationHandlerTest
{
    private AccessControlConfiguration mockAccessControlConfig;
    private AuthorizationParameterValidateHandler mockValidateHandler;
    private AdminIdentityResolver mockAdminIdentityResolver;
    private CacheConfiguration mockCacheConfig;
    private SidecarMetrics metrics;
    private Authorization testAuthorization;
    private RouteBuilder.Factory routeBuilderFactory;

    @BeforeEach
    void setUp()
    {
        mockAccessControlConfig = mock(AccessControlConfiguration.class);
        mockValidateHandler = mock(AuthorizationParameterValidateHandler.class);
        mockAdminIdentityResolver = mock(AdminIdentityResolver.class);
        mockCacheConfig = mock(CacheConfiguration.class);

        MetricRegistryFactory registryFactory
        = new MetricRegistryFactory("cassandra_sidecar", List.of(), List.of());
        metrics = new SidecarMetricsImpl(registryFactory, null);

        // Default cache configuration
        when(mockCacheConfig.expireAfterAccess()).thenReturn(MillisecondBoundConfiguration.parse("3s"));
        when(mockCacheConfig.maximumSize()).thenReturn(1000L);
        when(mockAccessControlConfig.permissionCacheConfiguration()).thenReturn(mockCacheConfig);

        testAuthorization = AndAuthorization.create()
                                            .addAuthorization(PermissionBasedAuthorization.create("MODIFY"));

        AuthorizationProvider mockAuthorizationProvider = mock(AuthorizationProvider.class);
        routeBuilderFactory = new RouteBuilder.Factory(mockAccessControlConfig, mockAuthorizationProvider,
                                                       mockAdminIdentityResolver, mockValidateHandler, metrics);
    }

    @AfterEach
    void tearDown()
    {
        registry().removeMatching((name, metric) -> true);
    }

    @Test
    void testMissingExpireAfterAccess()
    {
        when(mockCacheConfig.expireAfterAccess()).thenReturn(null);

        assertThatThrownBy(() -> new CachedAuthorizationHandler(mockAccessControlConfig, mockValidateHandler,
                                                                mockAdminIdentityResolver, testAuthorization,
                                                                metrics))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("Authorization handler cache must be configured with expireAfterAccess");
    }

    @Test
    void testAdminBypassesAuthorization()
    {
        when(mockCacheConfig.expireAfterAccess()).thenReturn(MillisecondBoundConfiguration.parse("5m"));
        when(mockAdminIdentityResolver.isAdmin("admin-identity")).thenReturn(true);

        Authorization expected = PermissionBasedAuthorization.create("CREATE");
        CachedAuthorizationHandler handler
        = new CachedAuthorizationHandler(mockAccessControlConfig, mockValidateHandler, mockAdminIdentityResolver,
                                         expected, metrics);

        RoutingContext mockContext = createMockContext("admin-user", "admin-identity", "admin-role");
        verifySuccess(handler, mockContext);
    }

    @Test
    void testMultipleIdentitiesOneIsAdmin()
    {
        CachedAuthorizationHandler handler
        = new CachedAuthorizationHandler(mockAccessControlConfig, mockValidateHandler, mockAdminIdentityResolver,
                                         testAuthorization, metrics);

        RoutingContext mockContext
        = createMockContext("user1", List.of("identity1", "admin-identity"), List.of("admin-role"));
        when(mockAdminIdentityResolver.isAdmin("identity1")).thenReturn(false);
        when(mockAdminIdentityResolver.isAdmin("admin-identity")).thenReturn(true);

        verifySuccess(handler, mockContext);
    }

    @Test
    void testNonAdminRequiresAuthorization()
    {
        CachedAuthorizationHandler handler
        = new CachedAuthorizationHandler(mockAccessControlConfig, mockValidateHandler, mockAdminIdentityResolver,
                                         testAuthorization, metrics);

        RoutingContext mockContext = createMockContext("user2", "identity2", "role2");
        when(mockAdminIdentityResolver.isAdmin("identity2")).thenReturn(false);

        verifySuccess(handler, mockContext);
    }

    @Test
    void testNonAdminDifferentPermissionForbidden()
    {
        Authorization expected = PermissionBasedAuthorization.create("CREATE");
        CachedAuthorizationHandler handler
        = new CachedAuthorizationHandler(mockAccessControlConfig, mockValidateHandler, mockAdminIdentityResolver,
                                         expected, metrics);

        RoutingContext mockContext = createMockContext("user3", "identity3", "role3");
        when(mockAdminIdentityResolver.isAdmin("identity3")).thenReturn(false);

        verifyFailure(handler, mockContext);
    }

    @Test
    void testCacheHitSameUser()
    {
        CachedAuthorizationHandler handler
        = new CachedAuthorizationHandler(mockAccessControlConfig, mockValidateHandler, mockAdminIdentityResolver,
                                         testAuthorization, metrics);

        RoutingContext mockContext1 = createMockContext("user4", "identity4", "role4");
        RoutingContext mockContext2 = createMockContext("user4", "identity4", "role4");
        when(mockAdminIdentityResolver.isAdmin("identity4")).thenReturn(false);

        verifySuccess(handler, mockContext1);

        for (int i = 0; i < 5; i++)
        {
            handler.handle(mockContext2);
        }

        verify(mockContext2, times(5)).next();

        // Verify cache hit for mockContext2
        CacheStats multipleCallStats = metrics.server().cache().authorizationCacheMetrics.snapshot();
        assertThat(multipleCallStats.missCount()).isEqualTo(0);
        assertThat(multipleCallStats.hitCount()).isEqualTo(5);
    }

    @Test
    void testCacheMissDifferentUsers()
    {
        CachedAuthorizationHandler handler
        = new CachedAuthorizationHandler(mockAccessControlConfig, mockValidateHandler, mockAdminIdentityResolver,
                                         testAuthorization, metrics);

        RoutingContext mockContext1 = createMockContext("user5", "identity5", "role5");
        RoutingContext mockContext2 = createMockContext("user5", "identity6", "role6");
        when(mockAdminIdentityResolver.isAdmin(any())).thenReturn(false);

        verifySuccess(handler, mockContext1);

        handler.handle(mockContext2);

        verify(mockContext2).next();

        // Verify cache miss for different user
        CacheStats differentUserStats = metrics.server().cache().authorizationCacheMetrics.snapshot();
        assertThat(differentUserStats.missCount()).isEqualTo(1);
        assertThat(differentUserStats.hitCount()).isEqualTo(0);
    }

    @Test
    void testCacheHitSameUserSameResource()
    {
        CachedAuthorizationHandler handler
        = new CachedAuthorizationHandler(mockAccessControlConfig, mockValidateHandler, mockAdminIdentityResolver,
                                         testAuthorization, metrics);

        handler.variableConsumer(routeBuilderFactory.builderForRoute().routeGenericVariableConsumer());

        QualifiedTableName table = new QualifiedTableName("ks", "tbl");
        RoutingContext mockContext1 = createMockContext("user6", "identity7", "role7");
        when(mockContext1.get("SC_QUALIFIED_TABLE_NAME")).thenReturn(table);

        RoutingContext mockContext2 = createMockContext("user6", "identity7", "role7");
        RoutingContextUtils.put(mockContext2, RoutingContextUtils.SC_QUALIFIED_TABLE_NAME, table);
        when(mockContext2.get("SC_QUALIFIED_TABLE_NAME")).thenReturn(table);

        verifySuccess(handler, mockContext1);

        handler.handle(mockContext2);

        CacheStats sameUserSameResourceCallStats = metrics.server().cache().authorizationCacheMetrics.snapshot();
        assertThat(sameUserSameResourceCallStats.missCount()).isEqualTo(0);
        assertThat(sameUserSameResourceCallStats.hitCount()).isEqualTo(1);
    }

    @Test
    void testCacheMissSameUserDifferentResources()
    {
        CachedAuthorizationHandler handler
        = new CachedAuthorizationHandler(mockAccessControlConfig, mockValidateHandler, mockAdminIdentityResolver,
                                         testAuthorization, metrics);

        handler.variableConsumer(routeBuilderFactory.builderForRoute().routeGenericVariableConsumer());

        RoutingContext mockContext1 = createMockContext("user7", "identity8", "role8");
        QualifiedTableName table1 = new QualifiedTableName("ks1", "tbl1");
        when(mockContext1.get("SC_QUALIFIED_TABLE_NAME")).thenReturn(table1);

        RoutingContext mockContext2 = createMockContext("user7", "identity8", "role8");
        QualifiedTableName table2 = new QualifiedTableName("ks2", "tbl2");
        when(mockContext2.get("SC_QUALIFIED_TABLE_NAME")).thenReturn(table2);

        when(mockAdminIdentityResolver.isAdmin(any())).thenReturn(false);

        verifySuccess(handler, mockContext1);

        CacheStats baselineStats = metrics.server().cache().authorizationCacheMetrics.snapshot();
        handler.handle(mockContext2);

        CacheStats sameUserDifferentResourceCallStats = metrics.server().cache().authorizationCacheMetrics.snapshot();
        assertThat(sameUserDifferentResourceCallStats.missCount() - baselineStats.missCount()).isEqualTo(1);
        assertThat(sameUserDifferentResourceCallStats.hitCount()).isEqualTo(0);
    }

    @Test
    void testValidationFailure()
    {
        CachedAuthorizationHandler handler
        = new CachedAuthorizationHandler(mockAccessControlConfig, mockValidateHandler, mockAdminIdentityResolver,
                                         testAuthorization, metrics);

        RoutingContext mockContext = createMockContext("user8", "identity9", "role9");

        // Mock validation handler to fail the context
        doAnswer(invocation -> {
            RoutingContext ctx = invocation.getArgument(0);
            when(ctx.failed()).thenReturn(true);
            return null;
        }).when(mockValidateHandler).handle(any(RoutingContext.class));

        handler.handle(mockContext);

        // Should not proceed to authorization or call next()
        verify(mockContext, times(0)).next();

        // Verify no cache operations when validation fails
        assertThat(metrics.server().cache().authorizationCacheMetrics.snapshot().missCount()).isEqualTo(0);
        assertThat(metrics.server().cache().authorizationCacheMetrics.snapshot().hitCount()).isEqualTo(0);
    }

    @Test
    void testEmptyIdentitiesWithPermission()
    {
        CachedAuthorizationHandler handler
        = new CachedAuthorizationHandler(mockAccessControlConfig, mockValidateHandler, mockAdminIdentityResolver,
                                         testAuthorization, metrics);

        RoutingContext mockContext = createMockContext("user9", List.of(), List.of());
        when(mockAdminIdentityResolver.isAdmin(any())).thenReturn(false);

        verifySuccess(handler, mockContext);
    }

    @Test
    void testEmptyIdentitiesWithoutPermission()
    {
        Authorization expected = PermissionBasedAuthorization.create("CREATE");
        CachedAuthorizationHandler handler
        = new CachedAuthorizationHandler(mockAccessControlConfig, mockValidateHandler, mockAdminIdentityResolver,
                                         expected, metrics);

        RoutingContext mockContext = createMockContext("user10", List.of(), List.of());
        when(mockAdminIdentityResolver.isAdmin(any())).thenReturn(false);

        verifyFailure(handler, mockContext);
    }

    private RoutingContext createMockContext(String username, String identity, String role)
    {
        return createMockContext(username, List.of(identity), List.of(role));
    }

    private RoutingContext createMockContext(String username, List<String> identities, List<String> roles)
    {
        RoutingContext mockContext = mock(RoutingContext.class);
        HttpServerRequest mockServerRequest = mock(HttpServerRequest.class);
        when(mockServerRequest.isEnded()).thenReturn(false);
        when(mockServerRequest.pause()).thenReturn(mockServerRequest);
        when(mockServerRequest.resume()).thenReturn(mockServerRequest);
        when(mockContext.request()).thenReturn(mockServerRequest);
        User mockUser = createMockUser(username, identities, roles);
        when(mockContext.user()).thenReturn(mockUser);

        // Track failed state - starts as false, becomes true when fail() is called
        when(mockContext.failed()).thenReturn(false);
        doAnswer(invocation -> {
            when(mockContext.failed()).thenReturn(true);
            return mockContext;
        }).when(mockContext).fail(any(Integer.class), any(Throwable.class));

        return mockContext;
    }

    private User createMockUser(String username, List<String> identities, List<String> roles)
    {
        User mockUser = User.fromName(username);
        mockUser.principal().put("identities", String.join(",", identities));
        mockUser.attributes().put(CASSANDRA_ROLES_ATTRIBUTE_NAME, roles);
        mockUser.authorizations().add("test-provider", PermissionBasedAuthorization.create("MODIFY"));
        return mockUser;
    }

    private void verifyRequest(CachedAuthorizationHandler handler, RoutingContext mockContext)
    {
        verifyRequest(handler, mockContext, true, 0);
    }

    private void verifySuccess(CachedAuthorizationHandler handler, RoutingContext mockContext)
    {
        verifyRequest(handler, mockContext);
    }

    private void verifyFailure(CachedAuthorizationHandler handler, RoutingContext mockContext)
    {
        verifyRequest(handler, mockContext, false, HttpResponseStatus.FORBIDDEN.code());
    }

    private void verifyRequest(CachedAuthorizationHandler handler, RoutingContext mockContext,
                               boolean success, int statusCode)
    {
        handler.handle(mockContext);

        if (success)
        {
            verify(mockContext).next();
        }
        else
        {
            verify(mockContext, times(1)).fail(eq(statusCode), any(Throwable.class));
        }

        // Verify cache miss on first admin request
        CacheStats firstCallStats = metrics.server().cache().authorizationCacheMetrics.snapshot();
        assertThat(firstCallStats.missCount()).isEqualTo(1);
        assertThat(firstCallStats.hitCount()).isEqualTo(0);
        assertThat(firstCallStats.loadSuccessCount()).isEqualTo(1);
        assertThat(firstCallStats.loadCount()).isEqualTo(1);

        for (int i = 0; i < 5; i++)
        {
            // Reset failed state before each subsequent call to allow handler to process
            when(mockContext.failed()).thenReturn(false);
            handler.handle(mockContext);
        }

        if (success)
        {
            verify(mockContext, times(6)).next();
        }
        else
        {
            verify(mockContext, times(6)).fail(eq(statusCode), any(Throwable.class));
        }

        // Verify cache hit on subsequent requests
        CacheStats multipleCallStats = metrics.server().cache().authorizationCacheMetrics.snapshot();
        assertThat(multipleCallStats.missCount()).isEqualTo(0);
        assertThat(multipleCallStats.hitCount()).isEqualTo(5);
        assertThat(multipleCallStats.loadSuccessCount()).isEqualTo(1);
        assertThat(multipleCallStats.loadCount()).isEqualTo(1);
    }
}
