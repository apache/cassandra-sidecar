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
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.ProvidesIntoMap;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.locator.CachedLocalTokenRanges;
import org.apache.cassandra.sidecar.cluster.locator.LocalTokenRangesProvider;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.schema.RestoreJobsSchema;
import org.apache.cassandra.sidecar.db.schema.RestoreRangesSchema;
import org.apache.cassandra.sidecar.db.schema.RestoreSlicesSchema;
import org.apache.cassandra.sidecar.db.schema.TableSchema;
import org.apache.cassandra.sidecar.handlers.DiskSpaceProtectionHandler;
import org.apache.cassandra.sidecar.handlers.restore.AbortRestoreJobHandler;
import org.apache.cassandra.sidecar.handlers.restore.CreateRestoreJobHandler;
import org.apache.cassandra.sidecar.handlers.restore.CreateRestoreSliceHandler;
import org.apache.cassandra.sidecar.handlers.restore.RestoreJobProgressHandler;
import org.apache.cassandra.sidecar.handlers.restore.RestoreJobSummaryHandler;
import org.apache.cassandra.sidecar.handlers.restore.RestoreRequestValidationHandler;
import org.apache.cassandra.sidecar.handlers.restore.UpdateRestoreJobHandler;
import org.apache.cassandra.sidecar.handlers.validations.ValidateTableExistenceHandler;
import org.apache.cassandra.sidecar.modules.multibindings.ClassKey;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.MultiBindingUtils;
import org.apache.cassandra.sidecar.modules.multibindings.PeriodicTaskMapKeys;
import org.apache.cassandra.sidecar.modules.multibindings.TableSchemaMapKeys;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.restore.RestoreJobDiscoverer;
import org.apache.cassandra.sidecar.restore.RestoreProcessor;
import org.apache.cassandra.sidecar.restore.RingTopologyRefresher;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;

/**
 * Provides the capability of restoring data (SSTables) from S3 into Cassandra
 */
public class RestoreJobModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        MapBinder<Class<? extends ClassKey>, PeriodicTask> periodicTaskMapBinder =
        MultiBindingUtils.newClassKeyClassMapBinder(binder(), PeriodicTask.class);
        // The bindings using DSL if the bound type, e.g. RestoreJobDiscoverer, is referenced _directly_ by other components.
        periodicTaskMapBinder.addBinding(PeriodicTaskMapKeys.RestoreJobDiscovererKey.class).to(RestoreJobDiscoverer.class);
        periodicTaskMapBinder.addBinding(PeriodicTaskMapKeys.RestoreProcessorKey.class).to(RestoreProcessor.class);
        periodicTaskMapBinder.addBinding(PeriodicTaskMapKeys.RingTopologyRefresherKey.class).to(RingTopologyRefresher.class);
    }

    @Provides
    @Singleton
    LocalTokenRangesProvider localTokenRangesProvider(InstancesMetadata instancesMetadata, DnsResolver dnsResolver)
    {
        return new CachedLocalTokenRanges(instancesMetadata, dnsResolver);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(TableSchemaMapKeys.RestoreJobsSchemaKey.class)
    TableSchema restoreJobsSchema(SidecarConfiguration configuration)
    {
        return new RestoreJobsSchema(configuration.serviceConfiguration().schemaKeyspaceConfiguration(),
                                     configuration.restoreJobConfiguration().restoreJobTablesTtl());
    }

    @ProvidesIntoMap
    @KeyClassMapKey(TableSchemaMapKeys.RestoreSlicesSchemaKey.class)
    TableSchema restoreSlicesSchema(SidecarConfiguration configuration)
    {
        return new RestoreSlicesSchema(configuration.serviceConfiguration().schemaKeyspaceConfiguration(),
                                       configuration.restoreJobConfiguration().restoreJobTablesTtl());
    }

    @ProvidesIntoMap
    @KeyClassMapKey(TableSchemaMapKeys.RestoreRangesSchemaKey.class)
    TableSchema restoreRangesSchema(SidecarConfiguration configuration)
    {
        return new RestoreRangesSchema(configuration.serviceConfiguration().schemaKeyspaceConfiguration(),
                                       configuration.restoreJobConfiguration().restoreJobTablesTtl());
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CreateRestoreJobRouteKey.class)
    VertxRoute createRestoreJobsRoute(RouteBuilder.Factory factory,
                                      ValidateTableExistenceHandler validateTableExistence,
                                      RestoreRequestValidationHandler validateRestoreJobRequest,
                                      CreateRestoreJobHandler createRestoreJobHandler)
    {
        return factory.builderForRoute()
                      .setBodyHandler(true)
                      .handler(validateTableExistence)
                      .handler(validateRestoreJobRequest)
                      .handler(createRestoreJobHandler)
                      .build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CreateRestoreSliceRouteKey.class)
    VertxRoute createRestoreJobSlicesRoute(RouteBuilder.Factory factory,
                                           ValidateTableExistenceHandler validateTableExistence,
                                           RestoreRequestValidationHandler validateRestoreJobRequest,
                                           DiskSpaceProtectionHandler diskSpaceProtection,
                                           CreateRestoreSliceHandler createRestoreSliceHandler)
    {
        return factory.builderForRoute()
                      .setBodyHandler(true)
                      .handler(diskSpaceProtection) // reject creating slice if short of disk space
                      .handler(validateTableExistence)
                      .handler(validateRestoreJobRequest)
                      .handler(createRestoreSliceHandler)
                      .build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.GetRestoreJobSummaryRouteKey.class)
    VertxRoute restoreJobSummaryRoute(RouteBuilder.Factory factory,
                                      ValidateTableExistenceHandler validateTableExistence,
                                      RestoreRequestValidationHandler validateRestoreJobRequest,
                                      RestoreJobSummaryHandler restoreJobSummaryHandler)
    {
        return factory.builderForRoute()
                      .handler(validateTableExistence)
                      .handler(validateRestoreJobRequest)
                      .handler(restoreJobSummaryHandler)
                      .build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.UpdateRestoreJobRouteKey.class)
    VertxRoute updateRestoreJobRoute(RouteBuilder.Factory factory,
                                     ValidateTableExistenceHandler validateTableExistence,
                                     RestoreRequestValidationHandler validateRestoreJobRequest,
                                     UpdateRestoreJobHandler updateRestoreJobHandler)
    {
        return factory.builderForRoute()
                      .setBodyHandler(true)
                      .handler(validateTableExistence)
                      .handler(validateRestoreJobRequest)
                      .handler(updateRestoreJobHandler)
                      .build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.AbortRestoreJobRouteKey.class)
    VertxRoute abortRestoreJobRoute(RouteBuilder.Factory factory,
                                    ValidateTableExistenceHandler validateTableExistence,
                                    RestoreRequestValidationHandler validateRestoreJobRequest,
                                    AbortRestoreJobHandler abortRestoreJobHandler)
    {
        // The route should use POST because aborting a job is updating the job status
        return factory.builderForRoute()
                      .setBodyHandler(true)
                      .handler(validateTableExistence)
                      .handler(validateRestoreJobRequest)
                      .handler(abortRestoreJobHandler)
                      .build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.GetRestoreJobProgressRouteKey.class)
    VertxRoute restoreJobProgressRoute(RouteBuilder.Factory factory,
                                       ValidateTableExistenceHandler validateTableExistence,
                                       RestoreRequestValidationHandler validateRestoreJobRequest,
                                       RestoreJobProgressHandler restoreJobProgressHandler)
    {
        return factory.builderForRoute()
                      .handler(validateTableExistence)
                      .handler(validateRestoreJobRequest)
                      .handler(restoreJobProgressHandler)
                      .build();
    }
}
