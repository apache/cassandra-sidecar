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
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationApiEnableDisableHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationListInstanceFilesHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationMap;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationMapSidecarConfigImpl;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;

/**
 * Module for supporting LiveMigration feature.
 */
public class LiveMigrationModule extends AbstractModule
{

    @Override
    protected void configure()
    {
        bind(LiveMigrationMap.class).to(LiveMigrationMapSidecarConfigImpl.class);
    }


    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationListInstanceFilesRouteKey.class)
    VertxRoute listInstanceFiles(RouteBuilder.Factory factory,
                                 LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                                 LiveMigrationListInstanceFilesHandler liveMigrationListInstanceFilesHandler)
    {
        return factory.builderForRoute()
                      .handler(liveMigrationApiEnableDisableHandler::isSourceOrDestination)
                      .handler(liveMigrationListInstanceFilesHandler)
                      .build();
    }
}
