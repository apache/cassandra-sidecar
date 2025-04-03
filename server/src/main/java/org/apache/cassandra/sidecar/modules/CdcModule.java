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

package org.apache.cassandra.sidecar.modules;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoMap;
import org.apache.cassandra.sidecar.cdc.CdcLogCache;
import org.apache.cassandra.sidecar.client.SidecarInstancesProvider;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.coordination.DynamicSidecarInstancesProvider;
import org.apache.cassandra.sidecar.coordination.InnerDcTokenAdjacentPeerProvider;
import org.apache.cassandra.sidecar.coordination.SidecarHttpHealthProvider;
import org.apache.cassandra.sidecar.coordination.SidecarPeerHealthMonitorTask;
import org.apache.cassandra.sidecar.coordination.SidecarPeerHealthProvider;
import org.apache.cassandra.sidecar.coordination.SidecarPeerProvider;
import org.apache.cassandra.sidecar.db.schema.ConfigsSchema;
import org.apache.cassandra.sidecar.db.schema.SystemViewsSchema;
import org.apache.cassandra.sidecar.db.schema.TableSchema;
import org.apache.cassandra.sidecar.handlers.cdc.AllServiceConfigHandler;
import org.apache.cassandra.sidecar.handlers.cdc.DeleteServiceConfigHandler;
import org.apache.cassandra.sidecar.handlers.cdc.ListCdcDirHandler;
import org.apache.cassandra.sidecar.handlers.cdc.StreamCdcSegmentHandler;
import org.apache.cassandra.sidecar.handlers.cdc.UpdateServiceConfigHandler;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.PeriodicTaskMapKeys;
import org.apache.cassandra.sidecar.modules.multibindings.TableSchemaMapKeys;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;
import org.apache.cassandra.sidecar.tasks.CdcRawDirectorySpaceCleaner;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.utils.SidecarClientProvider;

/**
 * Provides Cassandra change-data capture (CDC) publishing capability
 */
public class CdcModule extends AbstractModule
{
    @ProvidesIntoMap
    @KeyClassMapKey(PeriodicTaskMapKeys.SidecarPeerHealthMonitorTaskKey.class)
    PeriodicTask sidecarPeerHealthMonitorTask(SidecarPeerHealthMonitorTask task)
    {
        // Wire SidecarPeerHealthMonitorTask singleton into mapBinder
        return task;
    }

    @ProvidesIntoMap
    @KeyClassMapKey(PeriodicTaskMapKeys.CdcRawDirectorySpaceCleanerTaskKey.class)
    PeriodicTask cdcRawDirectorySpaceCleanercPeriodicTask(CdcRawDirectorySpaceCleaner cleanerTask)
    {
        return cleanerTask;
    }

    @ProvidesIntoMap
    @KeyClassMapKey(TableSchemaMapKeys.ConfigsSchemaKey.class)
    TableSchema configsSchema(ServiceConfiguration serviceConfiguration)
    {
        return new ConfigsSchema(serviceConfiguration);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(TableSchemaMapKeys.SystemViewsSchemaKey.class)
    TableSchema systemViewssSchema(SystemViewsSchema schema)
    {
        return schema;
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.ListCdcSegmentsRouteKey.class)
    VertxRoute listCdcSegmentsRoute(RouteBuilder.Factory factory,
                                    ListCdcDirHandler listCdcDirHandler)
    {
        return factory.buildRouteWithHandler(listCdcDirHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.StreamCdcSegmentRouteKey.class)
    VertxRoute streamCdcSegmentRoute(RouteBuilder.Factory factory,
                                     StreamCdcSegmentHandler streamCdcSegmentHandler)
    {
        return factory.buildRouteWithHandler(streamCdcSegmentHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.GetAllServiceConfigurationsRouteKey.class)
    VertxRoute getAllServiceConfigurationsRoute(RouteBuilder.Factory factory,
                                                AllServiceConfigHandler allServiceConfigHandler)
    {
        return factory.buildRouteWithHandler(allServiceConfigHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.UpdateServiceConfigurationRouteKey.class)
    VertxRoute updateServiceConfigurationRoute(RouteBuilder.Factory factory,
                                               UpdateServiceConfigHandler updateServiceConfigHandler)
    {
        return factory.builderForRoute().setBodyHandler(true).handler(updateServiceConfigHandler).build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.DeleteServiceConfigurationRouteKey.class)
    VertxRoute deleteServiceConfigurationRoute(RouteBuilder.Factory factory,
                                               DeleteServiceConfigHandler deleteServiceConfigHandler)
    {
        return factory.buildRouteWithHandler(deleteServiceConfigHandler);
    }

    @Provides
    @Singleton
    CdcLogCache cdcLogCache(ExecutorPools executorPools,
                            InstancesMetadata instancesMetadata,
                            SidecarConfiguration sidecarConfig)
    {
        return new CdcLogCache(executorPools, instancesMetadata, sidecarConfig);
    }

    @Provides
    @Singleton
    public SidecarPeerHealthProvider sidecarHealthProvider(SidecarClientProvider sidecarClientProvider)
    {
        return new SidecarHttpHealthProvider(sidecarClientProvider);
    }

    @Provides
    @Singleton
    public SidecarPeerProvider sidecarPeerProvider(InnerDcTokenAdjacentPeerProvider innerDcTokenAdjacentPeerProvider)
    {
        return innerDcTokenAdjacentPeerProvider;
    }

    @Provides
    @Singleton
    public SidecarInstancesProvider sidecarInstancesProvider(InstancesMetadata instancesMetadata, ServiceConfiguration serviceConfiguration)
    {
        return new DynamicSidecarInstancesProvider(instancesMetadata, serviceConfiguration);
    }
}
