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

package org.apache.cassandra.sidecar.handlers.livemigration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.HelperTestModules.InstanceMetadataTestModule;
import org.apache.cassandra.sidecar.HelperTestModules.RoutingContextTestModule;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LiveMigrationApiEnableDisableHandlerTest
{
    private static final InstanceMetadata SOURCE_META = createMockInstanceMeta(100001, "localhost", "/tmp/dummy");
    private static final InstanceMetadata DEST_META = createMockInstanceMeta(100002, "localhost2", "/tmp/dummy2");
    private static final InstanceMetadata NON_MIGRATION_INSTANCE_META = createMockInstanceMeta(100003, "localhost3", "/tmp/dummy3");
    private static final List<InstanceMetadata> INSTANCE_METADATA_LIST = new ArrayList<>(2)
    {{
        add(SOURCE_META);
        add(DEST_META);
        add(NON_MIGRATION_INSTANCE_META);
    }};
    private static final Map<String, String> MIGRATION_MAP = new HashMap<>()
    {{
        put(SOURCE_META.host(), DEST_META.host());
    }};

    private static InstanceMetadata createMockInstanceMeta(int id, String host, String rootDir)
    {

        return InstanceMetadataImpl.builder()
                                   .id(id)
                                   .host(host)
                                   .port(9042)
                                   .dataDirs(List.of(rootDir + "/data0", rootDir + "/data1"))
                                   .hintsDir(rootDir + "/hints")
                                   .commitlogDir(rootDir + "/commitlog")
                                   .savedCachesDir(rootDir + "/saved_caches")
                                   .stagingDir(rootDir + "/staging")
                                   .delegate(mock(CassandraAdapterDelegate.class))
                                   .metricRegistry(mock(MetricRegistry.class))
                                   .build();
    }

    Injector getInjector()
    {
        return Guice.createInjector(new TestModule(INSTANCE_METADATA_LIST));
    }

    @Test
    public void testIsSource()
    {
        final Injector injector = getInjector();
        configureReqLocalHost(injector, SOURCE_META);

        final LiveMigrationApiEnableDisableHandler enableDisableHandler = injector.getInstance(LiveMigrationApiEnableDisableHandler.class);
        final RoutingContext routingContext = injector.getInstance(RoutingContext.class);
        enableDisableHandler.isSource(routingContext);
        verify(routingContext, times(1)).next();
        verify(routingContext, times(0)).response();

        configureReqLocalHost(injector, DEST_META);
        enableDisableHandler.isSource(routingContext);
        verify(routingContext, times(1)).fail(eq(404));
    }

    @Test
    public void testIsDestination()
    {
        final Injector injector = getInjector();
        configureReqLocalHost(injector, DEST_META);

        final LiveMigrationApiEnableDisableHandler enableDisableHandler = injector.getInstance(LiveMigrationApiEnableDisableHandler.class);
        final RoutingContext routingContext = injector.getInstance(RoutingContext.class);
        enableDisableHandler.isDestination(routingContext);
        verify(routingContext, times(1)).next();
        verify(routingContext, times(0)).response();

        configureReqLocalHost(injector, SOURCE_META);
        enableDisableHandler.isDestination(routingContext);
        verify(routingContext, times(1)).fail(eq(404));
    }

    @Test
    public void testNonMigrationHost()
    {
        final Injector injector = getInjector();
        final LiveMigrationApiEnableDisableHandler enableDisableHandler = injector.getInstance(LiveMigrationApiEnableDisableHandler.class);
        final RoutingContext routingContext = injector.getInstance(RoutingContext.class);

        configureReqLocalHost(injector, NON_MIGRATION_INSTANCE_META);
        enableDisableHandler.isSource(routingContext);
        verify(routingContext, times(1)).fail(eq(404));
    }

    private void configureReqLocalHost(final Injector injector, final InstanceMetadata source)
    {
        final SocketAddress socketAddress = injector.getInstance(SocketAddress.class);
        final HttpServerRequest serverRequest = injector.getInstance(HttpServerRequest.class);
        when(socketAddress.host()).thenReturn(source.host());
        when(serverRequest.host()).thenReturn(source.host());
    }

    private static class TestModule extends AbstractModule
    {
        private final List<InstanceMetadata> instanceMetadataList;

        public TestModule(List<InstanceMetadata> instanceMetadataList)
        {
            this.instanceMetadataList = instanceMetadataList;
        }

        @Override
        protected void configure()
        {
            bind(Vertx.class).toInstance(Vertx.vertx());
            LiveMigrationMap mockLiveMigrationMap = () -> MIGRATION_MAP;
            bind(LiveMigrationMap.class).toInstance(mockLiveMigrationMap);
            install(new RoutingContextTestModule());
            install(new InstanceMetadataTestModule(instanceMetadataList));
        }
    }
}
