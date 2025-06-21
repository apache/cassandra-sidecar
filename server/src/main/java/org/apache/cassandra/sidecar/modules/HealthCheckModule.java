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
import com.google.inject.multibindings.ProvidesIntoMap;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.handlers.CassandraHealthHandler;
import org.apache.cassandra.sidecar.handlers.GossipHealthHandler;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.PeriodicTaskMapKeys;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;
import org.apache.cassandra.sidecar.tasks.HealthCheckPeriodicTask;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;

/**
 * Provides the capability to perform health checks on various components in both Sidecar and Cassandra
 */
public class HealthCheckModule extends AbstractModule
{
    @ProvidesIntoMap
    @KeyClassMapKey(PeriodicTaskMapKeys.HealthCheckPeriodicTaskKey.class)
    PeriodicTask healthCheckPeriodicTask(SidecarConfiguration configuration,
                                         InstancesMetadata instancesMetadata,
                                         ExecutorPools executorPools,
                                         SidecarMetrics metrics)
    {
        return new HealthCheckPeriodicTask(configuration, instancesMetadata, executorPools, metrics);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.SidecarHealthRouteKey.class)
    VertxRoute sidecarHealthRoute(RouteBuilder.Factory factory)
    {
        return factory.builderForUnauthorizedRoute()
                      .handler(context -> context.json(ApiModule.OK_STATUS))
                      .build();
    }

    @Deprecated
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraHealthRouteKey.class)
    VertxRoute cassandraHealthRoute(RouteBuilder.Factory factory,
                                    CassandraHealthHandler cassandraHealthHandler)
    {
        // Backwards compatibility for the Cassandra health endpoint
        return factory.builderForUnauthorizedRoute()
                      .handler(cassandraHealthHandler)
                      .build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraNativeHealthRouteKey.class)
    VertxRoute cassandraNativeHealthRoute(RouteBuilder.Factory factory,
                                          CassandraHealthHandler cassandraHealthHandler)
    {
        return factory.builderForUnauthorizedRoute()
                      .handler(cassandraHealthHandler)
                      .build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraJmxHealthRouteKey.class)
    VertxRoute cassandraJmxHealthRoute(RouteBuilder.Factory factory,
                                       CassandraHealthHandler cassandraHealthHandler)
    {
        return factory.builderForUnauthorizedRoute()
                      .handler(cassandraHealthHandler)
                      .build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraGossipHealthRouteKey.class)
    VertxRoute cassandraGossipHealthRoute(RouteBuilder.Factory factory,
                                          GossipHealthHandler gossipHealthHandler)
    {
        return factory.builderForUnauthorizedRoute()
                      .handler(gossipHealthHandler)
                      .build();
    }
}
