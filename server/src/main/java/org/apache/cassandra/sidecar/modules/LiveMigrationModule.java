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
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.Path;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.response.InstanceFilesListResponse;
import org.apache.cassandra.sidecar.common.response.LiveMigrationTaskResponse;
import org.apache.cassandra.sidecar.handlers.FileStreamHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationApiEnableDisableHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationCancelDataCopyTaskHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationCreateDataCopyTaskHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationFileStreamHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationGetAllDataCopyTasksHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationGetDataCopyTaskHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationListInstanceFilesHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationMap;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationMapSidecarConfigImpl;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationTaskFactory;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationTaskFactoryImpl;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/**
 * Module for supporting LiveMigration feature.
 */
@Path("/")
public class LiveMigrationModule extends AbstractModule
{

    @Override
    protected void configure()
    {
        bind(LiveMigrationMap.class).to(LiveMigrationMapSidecarConfigImpl.class);
        bind(LiveMigrationTaskFactory.class).to(LiveMigrationTaskFactoryImpl.class);
    }

    @GET
    @Path(ApiEndpointsV1.LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE)
    @Operation(summary = "Create data copy task",
               description = "Creates a new data copy task for live migration")
    @APIResponse(description = "Data copy task created successfully",
                 responseCode = "202",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @APIResponse(responseCode = "403",
                 description = "Live migration not enabled or node not configured as destination",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationCreateDataCopyTaskRouteKey.class)
    public VertxRoute createDataCopyTaskRoute(RouteBuilder.Factory factory,
                                              LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                                              LiveMigrationCreateDataCopyTaskHandler liveMigrationCreateDataCopyTaskHandler)
    {
        return factory.builderForRoute()
                      .setBodyHandler(true)
                      .handler(liveMigrationApiEnableDisableHandler::isDestination)
                      .handler(liveMigrationCreateDataCopyTaskHandler)
                      .build();
    }

    @PATCH
    @Operation(summary = "Cancel data copy task",
    description = "Cancels an existing data copy task for live migration")
    @APIResponse(description = "Data copy task cancelled successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = LiveMigrationTaskResponse.class)))
    @APIResponse(responseCode = "403",
                 description = "Live migration not enabled or node not configured as destination",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @APIResponse(responseCode = "404",
                 description = "Data copy task not found",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationCancelDataCopyTaskRouteKey.class)
    public VertxRoute cancelDataCopyTaskRoute(RouteBuilder.Factory factory,
                                              LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                                              LiveMigrationCancelDataCopyTaskHandler liveMigrationCancelDataCopyTaskHandler)
    {
        return factory.builderForRoute()
                      .handler(liveMigrationApiEnableDisableHandler::isDestination)
                      .handler(liveMigrationCancelDataCopyTaskHandler)
                      .build();
    }

    @GET
    @Operation(summary = "Get data copy task",
               description = "Retrieves the status and details of a specific data copy task by task ID")
    @APIResponse(description = "Data copy task retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = LiveMigrationTaskResponse.class)))
    @APIResponse(responseCode = "403",
                 description = "Live migration not enabled or node not configured as destination",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @APIResponse(responseCode = "404",
                 description = "Data copy task not found",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationGetDataCopyTaskRouteKey.class)
    public VertxRoute getDataCopyTaskRoute(RouteBuilder.Factory factory,
                                           LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                                           LiveMigrationGetDataCopyTaskHandler liveMigrationGetDataCopyTaskHandler)
    {
        return factory.builderForRoute()
                      .handler(liveMigrationApiEnableDisableHandler::isDestination)
                      .handler(liveMigrationGetDataCopyTaskHandler)
                      .build();
    }

    @GET
    @Operation(summary = "Get all data copy tasks",
    description = "Retrieves all data copy tasks for live migration on the current node")
    @APIResponse(description = "Data copy tasks retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.ARRAY)))
    @APIResponse(responseCode = "403",
                 description = "Live migration not enabled or node not configured as destination",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationGetAllDataCopyTasksRouteKey.class)
    public VertxRoute getAllDataCopyTasksRoute(RouteBuilder.Factory factory,
                                               LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                                               LiveMigrationGetAllDataCopyTasksHandler liveMigrationGetAllDataCopyTasksHandler)
    {
        return factory.builderForRoute()
                      .handler(liveMigrationApiEnableDisableHandler::isDestination)
                      .handler(liveMigrationGetAllDataCopyTasksHandler)
                      .build();
    }

    @GET
    @Path(ApiEndpointsV1.LIVE_MIGRATION_FILE_TRANSFER_ROUTE)
    @Operation(summary = "Stream file for live migration",
               description = "Streams a file for live migration data transfer")
    @APIResponse(description = "File stream for live migration initiated successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/octet-stream",
                 schema = @Schema(type = SchemaType.STRING)))
    @APIResponse(responseCode = "403",
                 description = "Live migration not enabled or file access denied",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationFileStreamHandlerRouteKey.class)
    public VertxRoute downloadFileRoute(RouteBuilder.Factory factory,
                                        LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                                        LiveMigrationFileStreamHandler liveMigrationFileStreamHandler,
                                        FileStreamHandler fileStreamHandler)
    {
        return factory.builderForRoute()
                      .handler(liveMigrationApiEnableDisableHandler::isSource)
                      .handler(liveMigrationFileStreamHandler)
                      .handler(fileStreamHandler)
                      .build();
    }

    @GET
    @Path(ApiEndpointsV1.LIVE_MIGRATION_FILES_ROUTE)
    @Operation(summary = "List instance files",
               description = "Lists files available on an instance for live migration purposes")
    @APIResponse(description = "Instance files listed successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = InstanceFilesListResponse.class)))
    @APIResponse(responseCode = "403",
                 description = "Live migration not enabled or node not configured for migration",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationListInstanceFilesRouteKey.class)
    public VertxRoute listInstanceFiles(RouteBuilder.Factory factory,
                                        LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                                        LiveMigrationListInstanceFilesHandler liveMigrationListInstanceFilesHandler)
    {
        return factory.builderForRoute()
                      .handler(liveMigrationApiEnableDisableHandler::isSourceOrDestination)
                      .handler(liveMigrationListInstanceFilesHandler)
                      .build();
    }
}
