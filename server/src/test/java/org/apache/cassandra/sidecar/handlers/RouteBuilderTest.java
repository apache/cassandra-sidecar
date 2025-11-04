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

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.auth.authorization.AuthorizationProvider;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.AdminIdentityResolver;
import org.apache.cassandra.sidecar.acl.authorization.AuthorizationParameterValidateHandler;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetricsImpl;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.RouteBuilder.Factory;
import org.apache.cassandra.sidecar.routes.SettableVertxRoute;
import org.apache.cassandra.sidecar.utils.CacheFactory;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.SSTableImporter;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link RouteBuilder}
 */
class RouteBuilderTest
{
    MetricRegistryFactory metricRegistryFactory = new MetricRegistryFactory("cassandra_sidecar", List.of(), List.of());
    SidecarMetrics metrics = new SidecarMetricsImpl(metricRegistryFactory, null);

    @Test
    void testRequiredParameters()
    {
        CacheConfiguration permissionCacheConfiguration = mock(CacheConfiguration.class);
        when(permissionCacheConfiguration.maximumSize()).thenReturn(5L);
        when(permissionCacheConfiguration.expireAfterAccess()).thenReturn(MillisecondBoundConfiguration.parse("5m"));
        AccessControlConfiguration mockConfig = mock(AccessControlConfiguration.class);
        when(mockConfig.permissionCacheConfiguration()).thenReturn(permissionCacheConfiguration);
        AuthorizationProvider mockAuthorizationProvider = mock(AuthorizationProvider.class);
        AdminIdentityResolver mockAdminIdentityResolver = mock(AdminIdentityResolver.class);
        AuthorizationParameterValidateHandler mockHandler = mock(AuthorizationParameterValidateHandler.class);

        ServiceConfiguration serviceConfiguration = new ServiceConfigurationImpl();
        SidecarConfiguration sidecarConfiguration = mock(SidecarConfiguration.class);
        when(sidecarConfiguration.serviceConfiguration()).thenReturn(serviceConfiguration);
        when(sidecarConfiguration.accessControlConfiguration()).thenReturn(mockConfig);
        SSTableImporter sstableImporter = mock(SSTableImporter.class);
        CacheFactory cacheFactory = new CacheFactory(sidecarConfiguration, sstableImporter, metrics);

        Factory factory = new Factory(mockConfig, mockAuthorizationProvider, mockAdminIdentityResolver,
                                      mockHandler, metrics, cacheFactory.endpointAuthorizationCache());
        RouteBuilder routeBuilder = factory.builderForRoute();
        Router mockRouter = mock(Router.class);
        SettableVertxRoute route = routeBuilder.build();
        route.setHttpMethod(HttpMethod.GET);
        route.setRouteURI("/api/v1/__health");
        assertThatThrownBy(() -> route.mountTo(mockRouter))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Handler chain can not be empty");
    }

    @Test
    void testBuildAccessProtectedRouteWithoutRequiredAuthorizations()
    {
        Router mockRouter = mock(Router.class);
        CassandraHealthHandler handler = new CassandraHealthHandler(mock(InstanceMetadataFetcher.class),
                                                                    mock(ExecutorPools.class),
                                                                    mock(CassandraInputValidator.class));
        testRoute(handler,
                  Factory::builderForRoute,
                  route -> {
                      route.setHttpMethod(HttpMethod.GET);
                      route.setRouteURI(ApiEndpointsV1.HEALTH_ROUTE);
                      assertThatThrownBy(() -> route.mountTo(mockRouter))
                      .isInstanceOf(ConfigurationException.class)
                      .hasMessage("Authorized route must have required authorizations declared");
                  });
    }

    @Test
    void testBuildUnprotectedRouteWithRequiredAuthorizations()
    {
        Router mockRouter = mock(Router.class);
        GossipInfoHandler handler = new GossipInfoHandler(mock(InstanceMetadataFetcher.class),
                                                          mock(ExecutorPools.class));
        testRoute(handler,
                  Factory::builderForUnauthorizedRoute,
                  route -> {
                      route.setHttpMethod(HttpMethod.GET);
                      route.setRouteURI(ApiEndpointsV1.GOSSIP_ROUTE);
                      assertThatThrownBy(() -> route.mountTo(mockRouter))
                      .isInstanceOf(ConfigurationException.class)
                      .hasMessage("Unauthorized route must not have required authorizations declared");
                  });
    }

    private void testRoute(Handler<RoutingContext> handler,
                           Function<Factory, RouteBuilder> routeBuilderFunction,
                           Consumer<SettableVertxRoute> test)
    {
        CacheConfiguration permissionCacheConfiguration = mock(CacheConfiguration.class);
        when(permissionCacheConfiguration.maximumSize()).thenReturn(5L);
        when(permissionCacheConfiguration.expireAfterAccess()).thenReturn(MillisecondBoundConfiguration.parse("5m"));
        AccessControlConfiguration mockConfig = mock(AccessControlConfiguration.class);
        when(mockConfig.permissionCacheConfiguration()).thenReturn(permissionCacheConfiguration);
        when(mockConfig.enabled()).thenReturn(true);
        AuthorizationProvider mockAuthorizationProvider = mock(AuthorizationProvider.class);
        AdminIdentityResolver mockAdminIdentityResolver = mock(AdminIdentityResolver.class);
        AuthorizationParameterValidateHandler mockHandler = mock(AuthorizationParameterValidateHandler.class);

        ServiceConfiguration serviceConfiguration = new ServiceConfigurationImpl();
        SidecarConfiguration sidecarConfiguration = mock(SidecarConfiguration.class);
        when(sidecarConfiguration.serviceConfiguration()).thenReturn(serviceConfiguration);
        when(sidecarConfiguration.accessControlConfiguration()).thenReturn(mockConfig);
        SSTableImporter sstableImporter = mock(SSTableImporter.class);
        CacheFactory cacheFactory = new CacheFactory(sidecarConfiguration, sstableImporter, metrics);

        Factory factory = new Factory(mockConfig, mockAuthorizationProvider, mockAdminIdentityResolver,
                                      mockHandler, metrics, cacheFactory.endpointAuthorizationCache());
        RouteBuilder routeBuilder = routeBuilderFunction.apply(factory);
        SettableVertxRoute route = routeBuilder.handler(handler).build();
        test.accept(route);
    }
}
