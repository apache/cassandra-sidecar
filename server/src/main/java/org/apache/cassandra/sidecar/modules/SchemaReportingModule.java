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

import org.apache.commons.lang3.concurrent.LazyInitializer;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoMap;
import datahub.client.rest.RestEmitter;
import datahub.client.rest.RestEmitterConfig;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.utils.ThrowableUtils;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SchemaReportingConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.datahub.EmitterFactory;
import org.apache.cassandra.sidecar.datahub.IdentifiersProvider;
import org.apache.cassandra.sidecar.datahub.SchemaReporter;
import org.apache.cassandra.sidecar.datahub.SchemaReportingTask;
import org.apache.cassandra.sidecar.handlers.ReportSchemaHandler;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.PeriodicTaskMapKeys;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

/**
 * Instantiates the objects necessary for converting and reporting the cluster's
 * current schema in a DataHub-compatible format either on schedule or as requested
 */
public class SchemaReportingModule extends AbstractModule
{
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.DataHubSchemaReportingRouteKey.class)
    VertxRoute schemaReportingRoute(@NotNull RouteBuilder.Factory factory,
                                    @NotNull ReportSchemaHandler handler)
    {
        return factory.buildRouteWithHandler(handler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(PeriodicTaskMapKeys.SchemaReportingTaskKey.class)
    PeriodicTask schemaReportingTask(@NotNull SidecarConfiguration configuration,
                                     @NotNull CQLSessionProvider session,
                                     @NotNull SchemaReporter reporter,
                                     @NotNull ExecutorPools executors)
    {
        return new SchemaReportingTask(configuration,
                                       session,
                                       reporter,
                                       executors.internal());
    }

    @Provides
    @Singleton
    IdentifiersProvider identifiersProvider(@NotNull InstanceMetadataFetcher fetcher)
    {
        LazyInitializer<String> cluster = new LazyInitializer<>()
        {
            @Override
            @NotNull
            protected String initialize()
            {
                return fetcher.callOnFirstAvailableInstance(instance -> instance.delegate()
                                                                                .storageOperations()
                                                                                .clusterName());
            }
        };

        return new IdentifiersProvider()
        {
            @Override
            @NotNull
            public String cluster()
            {
                return ThrowableUtils.supplier(cluster::get).get();
            }
        };
    }

    @Provides
    @Singleton
    EmitterFactory emitterFactory(@NotNull SidecarConfiguration sidecarConfiguration)
    {
        SchemaReportingConfiguration reporterConfiguration = sidecarConfiguration.schemaReportingConfiguration();
        RestEmitterConfig emitterConfiguration = RestEmitterConfig.builder()
                                                                  .server(reporterConfiguration.endpoint())
                                                                  .maxRetries(reporterConfiguration.maxRetries())
                                                                  .build();

        return () -> new RestEmitter(emitterConfiguration);
    }
}
