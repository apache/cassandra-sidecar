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
import org.apache.cassandra.sidecar.handlers.FileStreamHandler;
import org.apache.cassandra.sidecar.handlers.StreamSSTableComponentHandler;
import org.apache.cassandra.sidecar.handlers.snapshots.ClearSnapshotHandler;
import org.apache.cassandra.sidecar.handlers.snapshots.CreateSnapshotHandler;
import org.apache.cassandra.sidecar.handlers.snapshots.ListSnapshotHandler;
import org.apache.cassandra.sidecar.handlers.sstableuploads.SSTableCleanupHandler;
import org.apache.cassandra.sidecar.handlers.sstableuploads.SSTableImportHandler;
import org.apache.cassandra.sidecar.handlers.sstableuploads.SSTableUploadHandler;
import org.apache.cassandra.sidecar.handlers.validations.ValidateTableExistenceHandler;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;

/**
 * Provides the capability to access SSTables in the companion Cassandra node(s).
 * <ul>
 *     <li>Read capability: routes to take snapshots, list and download sstables from snapshots, remove snapshots</li>
 *     <li>Write capability: upload and import SSTables</li>
 * </ul>
 */
public class SSTablesAccessModule extends AbstractModule
{
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.StreamSSTableComponentsRouteKey.class)
    VertxRoute streamSSTableComponentsRoute(RouteBuilder.Factory factory,
                                            StreamSSTableComponentHandler streamSSTableComponentHandler,
                                            FileStreamHandler fileStreamHandler)
    {
        return factory.builderForRoute()
                      .handler(streamSSTableComponentHandler)
                      .handler(fileStreamHandler)
                      .build();
    }

    @Deprecated
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.DeprecatedStreamSSTableComponentsRouteKey.class)
    VertxRoute deprecatedStreamSSTableComponentsRoute(RouteBuilder.Factory factory,
                                                      StreamSSTableComponentHandler streamSSTableComponentHandler,
                                                      FileStreamHandler fileStreamHandler)
    {
        return factory.builderForRoute()
                      .handler(streamSSTableComponentHandler)
                      .handler(fileStreamHandler)
                      .build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.StreamSSTableComponentsWithSecondaryIndexRouteKey.class)
    VertxRoute streamSSTableComponentsWithSecondaryIndexRoute(RouteBuilder.Factory factory,
                                                              StreamSSTableComponentHandler streamSSTableComponentHandler,
                                                              FileStreamHandler fileStreamHandler)
    {
        return factory.builderForRoute()
                      .handler(streamSSTableComponentHandler)
                      .handler(fileStreamHandler)
                      .build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CreateSnapshotRouteKey.class)
    VertxRoute createSnapshotRouteKey(RouteBuilder.Factory factory,
                                      CreateSnapshotHandler createSnapshotHandler)
    {
        return factory.buildRouteWithHandler(createSnapshotHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.ListSnapshotRouteKey.class)
    VertxRoute listSnapshotRouteKey(RouteBuilder.Factory factory,
                                    ListSnapshotHandler listSnapshotHandler)
    {
        return factory.buildRouteWithHandler(listSnapshotHandler);
    }

    @Deprecated
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.DeprecatedListSnapshotRouteKey.class)
    VertxRoute deprecatedListSnapshotRouteKey(RouteBuilder.Factory factory,
                                              ListSnapshotHandler listSnapshotHandler)
    {
        return factory.buildRouteWithHandler(listSnapshotHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.ClearSnapshotRouteKey.class)
    VertxRoute clearSnapshotRouteKey(RouteBuilder.Factory factory,
                                     ValidateTableExistenceHandler validateTableExistence,
                                     ClearSnapshotHandler clearSnapshotHandler)
    {
        return factory.builderForRoute()
                      // Leverage the validateTableExistence. Currently, JMX does not validate for non-existent keyspace.
                      // Additionally, the current JMX implementation to clear snapshots does not support passing a table
                      // as a parameter.
                      .handler(validateTableExistence)
                      .handler(clearSnapshotHandler)
                      .build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.SSTableUploadRouteKey.class)
    VertxRoute sstableUploadRoute(RouteBuilder.Factory factory,
                                  SSTableUploadHandler sstableUploadHandler)
    {
        return factory.buildRouteWithHandler(sstableUploadHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.SSTableImportRouteKey.class)
    VertxRoute sstableImportRoute(RouteBuilder.Factory factory,
                                  SSTableImportHandler sstableImportHandler)
    {
        return factory.buildRouteWithHandler(sstableImportHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.SSTableCleanupRouteKey.class)
    VertxRoute sstableCleanupRoute(RouteBuilder.Factory factory,
                                   SSTableCleanupHandler sstableCleanupHandler)
    {
        return factory.buildRouteWithHandler(sstableCleanupHandler);
    }
}
