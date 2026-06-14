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
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.response.DigestResponse;
import org.apache.cassandra.sidecar.common.response.InstanceFilesListResponse;
import org.apache.cassandra.sidecar.common.response.LiveMigrationDataCopyResponse;
import org.apache.cassandra.sidecar.common.response.LiveMigrationFilesVerificationResponse;
import org.apache.cassandra.sidecar.common.response.LiveMigrationStatus;
import org.apache.cassandra.sidecar.common.response.LiveMigrationTaskCreationResponse;
import org.apache.cassandra.sidecar.handlers.FileStreamHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationApiEnableDisableHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationCancelDataCopyTaskHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationCancelFilesVerificationTaskHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationConcurrencyLimitHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationCreateDataCopyTaskHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationCreateFilesVerificationTaskHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDigestHandlerWrapper;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationFileResolveHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationGetAllDataCopyTasksHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationGetAllFilesVerificationTasksHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationGetDataCopyTaskHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationGetFilesVerificationTaskHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationListInstanceFilesHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationMap;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationMapSidecarConfigImpl;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationStatusClearHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationStatusCompleteHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationStatusGetHandler;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationDataCopyTaskFactoryImpl;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationFileDownloadPreCheck;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationFilesVerificationTaskFactory;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationStatusTracker;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationStatusTrackerImpl;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationTaskFactory;
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
        bind(LiveMigrationTaskFactory.class).to(LiveMigrationDataCopyTaskFactoryImpl.class);
        bind(LiveMigrationFilesVerificationTaskFactory.class);
        bind(LiveMigrationStatusTracker.class).to(LiveMigrationStatusTrackerImpl.class);
        bind(LiveMigrationFileDownloadPreCheck.class).toInstance(LiveMigrationFileDownloadPreCheck.DEFAULT);
    }

    @POST
    @Path(ApiEndpointsV1.LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE)
    @Operation(summary = "Create data copy task",
    description = "Creates a new data copy task for live migration")
    @APIResponse(description = "Data copy task created successfully",
    responseCode = "202",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @APIResponse(responseCode = "404",
    description = "Live migration not enabled or node not configured as destination",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @APIResponse(responseCode = "409",
    description = "Cannot accept data copy task as another task is in progress",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationCreateDataCopyTaskRouteKey.class)
    VertxRoute createDataCopyTaskRoute(RouteBuilder.Factory factory,
                                       LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                                       LiveMigrationCreateDataCopyTaskHandler liveMigrationCreateDataCopyTaskHandler)
    {
        return factory.builderForRoute()
                      .setBodyHandler(true)
                      .handler(liveMigrationApiEnableDisableHandler::isDestination)
                      .handler(liveMigrationApiEnableDisableHandler::allowIfMigrationNotComplete)
                      .handler(liveMigrationCreateDataCopyTaskHandler)
                      .build();
    }

    @PATCH
    @Operation(summary = "Cancel data copy task",
    description = "Cancels an existing data copy task for live migration")
    @APIResponse(description = "Data copy task cancelled successfully",
    responseCode = "200",
    content = @Content(mediaType = "application/json",
    schema = @Schema(implementation = LiveMigrationDataCopyResponse.class)))
    @APIResponse(responseCode = "404",
    description = "Live migration not enabled, node not configured as destination, or data copy task not found",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationCancelDataCopyTaskRouteKey.class)
    VertxRoute cancelDataCopyTaskRoute(RouteBuilder.Factory factory,
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
    schema = @Schema(implementation = LiveMigrationDataCopyResponse.class)))
    @APIResponse(responseCode = "404",
    description = "Live migration not enabled, node not configured as destination, or data copy task not found",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationGetDataCopyTaskRouteKey.class)
    VertxRoute getDataCopyTaskRoute(RouteBuilder.Factory factory,
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
    @APIResponse(responseCode = "404",
    description = "Live migration not enabled or node not configured as destination",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationGetAllDataCopyTasksRouteKey.class)
    VertxRoute getAllDataCopyTasksRoute(RouteBuilder.Factory factory,
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
    description = "Streams a file for live migration data transfer. " +
                  "Optionally returns file digest when digestAlgorithm query parameter is provided")
    @APIResponse(description = "File stream for live migration initiated successfully (when digestAlgorithm param is absent)",
    responseCode = "200",
    content = @Content(mediaType = "application/octet-stream",
    schema = @Schema(type = SchemaType.STRING)))
    @APIResponse(description = "File digest calculated successfully (when digestAlgorithm param is present)",
    responseCode = "200",
    content = @Content(mediaType = "application/json",
    schema = @Schema(implementation = DigestResponse.class)))
    @APIResponse(responseCode = "400",
    description = "Invalid path parameter (e.g., non-numeric directory index) or a directory was requested",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @APIResponse(responseCode = "403",
    description = "Resolved path is outside the configured live-migration directories",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @APIResponse(responseCode = "404",
    description = "Live migration not enabled, node not configured as source, or requested file not found",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @APIResponse(responseCode = "503",
    description = "Concurrency limit reached for file requests",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @APIResponse(responseCode = "500",
    description = "Failed to calculate digest",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationFileStreamHandlerRouteKey.class)
    VertxRoute liveMigrationFileRoute(RouteBuilder.Factory factory,
                                      LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                                      LiveMigrationConcurrencyLimitHandler concurrencyLimitHandler,
                                      LiveMigrationFileResolveHandler liveMigrationFileResolveHandler,
                                      FileStreamHandler fileStreamHandler,
                                      LiveMigrationDigestHandlerWrapper liveMigrationDigestHandlerWrapper)
    {
        return factory.builderForRoute()
                      .handler(liveMigrationApiEnableDisableHandler::isSource)
                      .handler(liveMigrationApiEnableDisableHandler::allowIfMigrationNotComplete)
                      .handler(concurrencyLimitHandler)
                      .handler(liveMigrationFileResolveHandler)
                      .handler(liveMigrationDigestHandlerWrapper)
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
    @APIResponse(responseCode = "404",
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

    @POST
    @Path(ApiEndpointsV1.LIVE_MIGRATION_STATUS_ROUTE)
    @Operation(summary = "Updates live migration status",
    description = "Updates live migration status as COMPLETED for requested instance")
    @APIResponse(description = "Live migration status updated successfully",
    responseCode = "200",
    content = @Content(mediaType = "application/json",
    schema = @Schema(implementation = LiveMigrationStatus.class)))
    @APIResponse(responseCode = "400",
    description = "When tried to update live migration status when it is already marked as COMPLETED",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @APIResponse(responseCode = "503",
    description = "When could not update live migration status as COMPLETED",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationStatusUpdateRouteKey.class)
    VertxRoute getStatusUpdateRoute(RouteBuilder.Factory factory,
                                    LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                                    LiveMigrationStatusCompleteHandler statusCompleteHandler)
    {
        return factory.builderForRoute()
                      .handler(liveMigrationApiEnableDisableHandler::isSourceOrDestination)
                      .handler(statusCompleteHandler)
                      .build();
    }

    @GET
    @Path(ApiEndpointsV1.LIVE_MIGRATION_STATUS_ROUTE)
    @Operation(summary = "Get live migration status",
    description = "Get the status of the live migration")
    @APIResponse(description = "Live migration status retrieved successfully",
    responseCode = "200",
    content = @Content(mediaType = "application/json",
    schema = @Schema(implementation = LiveMigrationStatus.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationStatusRouteKey.class)
    VertxRoute getStatusRoute(RouteBuilder.Factory factory,
                              LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                              LiveMigrationStatusGetHandler statusHandler)
    {
        return factory.builderForRoute()
                      .handler(liveMigrationApiEnableDisableHandler::isSourceOrDestination)
                      .handler(statusHandler)
                      .build();
    }

    @DELETE
    @Path(ApiEndpointsV1.LIVE_MIGRATION_STATUS_ROUTE)
    @Operation(summary = "Deletes live migration status",
    description = "Deletes live migration status for requested instance. " +
                  "It should be called after clearing the live migration map configuration only.")
    @APIResponse(description = "Live migration status deleted successfully",
    responseCode = "200",
    content = @Content(mediaType = "application/json",
    schema = @Schema(implementation = LiveMigrationStatus.class)))
    @APIResponse(responseCode = "403",
    description = "When tried to delete live migration status before clearing the live migration map",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @APIResponse(responseCode = "400",
    description = "When tried to delete Live migration status before without updating the status as COMPLETED",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @APIResponse(responseCode = "503",
    description = "When faced some issue while deleting the live migration status",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationStatusDeleteRouteKey.class)
    VertxRoute deleteStatusRoute(RouteBuilder.Factory factory,
                                 LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                                 LiveMigrationStatusClearHandler statusDeleteHandler)
    {
        return factory.builderForRoute()
                      .handler(liveMigrationApiEnableDisableHandler::neitherSourceNorDestination)
                      .handler(statusDeleteHandler)
                      .build();
    }

    @POST
    @Path(ApiEndpointsV1.LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
    @Operation(summary = "Create files verification task",
    description = "Creates a new files verification task")
    @APIResponse(description = "Files verification task created successfully",
    responseCode = "202",
    content = @Content(mediaType = "application/json",
    schema = @Schema(implementation = LiveMigrationTaskCreationResponse.class)))
    @APIResponse(responseCode = "404",
    description = "Live migration not enabled or node not configured as destination",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @APIResponse(responseCode = "409",
    description = "Cannot accept files verification task as another task is in progress",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationCreateFilesDigestVerificationTaskRouteKey.class)
    public VertxRoute createFilesVerificationTask(RouteBuilder.Factory factory,
                                                  LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                                                  LiveMigrationCreateFilesVerificationTaskHandler verificationTaskHandler)
    {
        return factory.builderForRoute()
                      .setBodyHandler(true)
                      .handler(liveMigrationApiEnableDisableHandler::isDestination)
                      .handler(liveMigrationApiEnableDisableHandler::allowIfMigrationNotComplete)
                      .handler(verificationTaskHandler)
                      .build();
    }

    @GET
    @Path(value = ApiEndpointsV1.LIVE_MIGRATION_FILES_VERIFICATION_TASK_ROUTE)
    @Operation(summary = "Get files verification task",
    description = "Retrieves the files verification task by task ID")
    @APIResponse(description = "Files verification task retrieved successfully",
    responseCode = "200",
    content = @Content(mediaType = "application/json",
    schema = @Schema(implementation = LiveMigrationFilesVerificationResponse.class)))
    @APIResponse(responseCode = "404",
    description = "Live migration not enabled, node not configured as destination, or files verification task not found",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationGetFilesVerificationTaskRouteKey.class)
    VertxRoute getFilesVerificationTask(RouteBuilder.Factory factory,
                                        LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                                        LiveMigrationGetFilesVerificationTaskHandler getVerificationTaskHandler)
    {
        return factory.builderForRoute()
                      .handler(liveMigrationApiEnableDisableHandler::isDestination)
                      .handler(getVerificationTaskHandler)
                      .build();
    }

    @GET
    @Path(value = ApiEndpointsV1.LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
    @Operation(summary = "Get all files verification tasks",
    description = "Retrieves all live migration file verification tasks of the current node")
    @APIResponse(description = "File verification tasks retrieved successfully",
    responseCode = "200",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.ARRAY, implementation = LiveMigrationFilesVerificationResponse.class)))
    @APIResponse(responseCode = "404",
    description = "Live migration not enabled or node not configured as destination",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationGetAllFilesVerificationTasksRouteKey.class)
    VertxRoute getAllFilesVerificationTask(RouteBuilder.Factory factory,
                                           LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                                           LiveMigrationGetAllFilesVerificationTasksHandler getAllVerificationTaskHandler)
    {
        return factory.builderForRoute()
                      .handler(liveMigrationApiEnableDisableHandler::isDestination)
                      .handler(getAllVerificationTaskHandler)
                      .build();
    }

    @PATCH
    @Path(value = ApiEndpointsV1.LIVE_MIGRATION_FILES_VERIFICATION_TASK_ROUTE)
    @Operation(summary = "Cancel files verification task",
    description = "Cancels an existing live migration files verification task")
    @APIResponse(description = "Files verification task cancelled successfully",
    responseCode = "200",
    content = @Content(mediaType = "application/json",
    schema = @Schema(implementation = LiveMigrationFilesVerificationResponse.class)))
    @APIResponse(responseCode = "404",
    description = "Live migration not enabled, node not configured as destination, or files verification task not found",
    content = @Content(mediaType = "application/json",
    schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationCancelFilesVerificationTaskRouteKey.class)
    VertxRoute cancelFilesVerificationTaskRoute(RouteBuilder.Factory factory,
                                                LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                                                LiveMigrationCancelFilesVerificationTaskHandler cancelFilesVerificationTaskHandler)
    {
        return factory.builderForRoute()
                      .handler(liveMigrationApiEnableDisableHandler::isDestination)
                      .handler(cancelFilesVerificationTaskHandler)
                      .build();
    }
}
