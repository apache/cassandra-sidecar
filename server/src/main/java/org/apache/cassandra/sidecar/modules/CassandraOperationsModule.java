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
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import org.apache.cassandra.sidecar.adapters.base.db.schema.ConnectedClientsSchema;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.ApiEndpointsV2;
import org.apache.cassandra.sidecar.common.response.CompactionStatsResponse;
import org.apache.cassandra.sidecar.common.response.CompactionStopResponse;
import org.apache.cassandra.sidecar.common.response.ConnectedClientStatsResponse;
import org.apache.cassandra.sidecar.common.response.GossipInfoResponse;
import org.apache.cassandra.sidecar.common.response.ListOperationalJobsResponse;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.response.SchemaResponse;
import org.apache.cassandra.sidecar.common.response.StreamStatsResponse;
import org.apache.cassandra.sidecar.common.response.TableStatsResponse;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.response.v2.V2NodeSettings;
import org.apache.cassandra.sidecar.db.schema.TableSchema;
import org.apache.cassandra.sidecar.handlers.CompactionStatsHandler;
import org.apache.cassandra.sidecar.handlers.CompactionStopHandler;
import org.apache.cassandra.sidecar.handlers.ConnectedClientStatsHandler;
import org.apache.cassandra.sidecar.handlers.GossipInfoHandler;
import org.apache.cassandra.sidecar.handlers.GossipUpdateHandler;
import org.apache.cassandra.sidecar.handlers.KeyspaceRingHandler;
import org.apache.cassandra.sidecar.handlers.KeyspaceSchemaHandler;
import org.apache.cassandra.sidecar.handlers.ListOperationalJobsHandler;
import org.apache.cassandra.sidecar.handlers.NativeUpdateHandler;
import org.apache.cassandra.sidecar.handlers.NodeDecommissionHandler;
import org.apache.cassandra.sidecar.handlers.NodeDrainHandler;
import org.apache.cassandra.sidecar.handlers.NodeMoveHandler;
import org.apache.cassandra.sidecar.handlers.OperationalJobHandler;
import org.apache.cassandra.sidecar.handlers.RepairHandler;
import org.apache.cassandra.sidecar.handlers.RingHandler;
import org.apache.cassandra.sidecar.handlers.SchemaHandler;
import org.apache.cassandra.sidecar.handlers.StreamStatsHandler;
import org.apache.cassandra.sidecar.handlers.TableStatsHandler;
import org.apache.cassandra.sidecar.handlers.TokenRangeReplicaMapHandler;
import org.apache.cassandra.sidecar.handlers.cassandra.NodeSettingsHandler;
import org.apache.cassandra.sidecar.handlers.v2.cassandra.V2NodeSettingsHandler;
import org.apache.cassandra.sidecar.handlers.validations.ValidateTableExistenceHandler;
import org.apache.cassandra.sidecar.job.DisabledOperationalJobCoordinator;
import org.apache.cassandra.sidecar.job.InMemoryOperationalJobTracker;
import org.apache.cassandra.sidecar.job.OperationalJobCoordinator;
import org.apache.cassandra.sidecar.job.OperationalJobTracker;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.TableSchemaMapKeys;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/**
 * Provides the capability to query and invoke Cassandra operations
 */
@Path("/")
public class CassandraOperationsModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(OperationalJobTracker.class).to(InMemoryOperationalJobTracker.class);
        bind(OperationalJobCoordinator.class).to(DisabledOperationalJobCoordinator.class);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(TableSchemaMapKeys.SystemViewsClientsSchemaKey.class)
    TableSchema systemViewsClientsSchema()
    {
        return new ConnectedClientsSchema();
    }

    @GET
    @Path(ApiEndpointsV1.CONNECTED_CLIENT_STATS_ROUTE)
    @Operation(summary = "Get connected client stats",
               description = "Returns statistics about connected clients to the Cassandra node")
    @APIResponse(description = "Connected client statistics retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = ConnectedClientStatsResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraConnectedClientStatsRouteKey.class)
    VertxRoute cassandraConnectedClientStatsRoute(RouteBuilder.Factory factory,
                                                  ConnectedClientStatsHandler connectedClientStatsHandler)
    {
        return factory.buildRouteWithHandler(connectedClientStatsHandler);
    }

    @GET
    @Path(ApiEndpointsV1.COMPACTION_STATS_ROUTE)
    @Operation(summary = "Get compaction statistics",
               description = "Returns compaction statistics for the Cassandra node")
    @APIResponse(description = "Compaction statistics retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = CompactionStatsResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraCompactionStatsRouteKey.class)
    VertxRoute cassandraCompactionStatsRoute(RouteBuilder.Factory factory,
                                             CompactionStatsHandler compactionStatsHandler)
    {
        return factory.buildRouteWithHandler(compactionStatsHandler);
    }

    @PUT
    @Path(ApiEndpointsV1.COMPACTION_STOP_ROUTE)
    @Operation(summary = "Stop compaction operation",
               description = "Stops a compaction operation on the Cassandra node")
    @APIResponse(description = "Compaction stop operation completed",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = CompactionStopResponse.class)))
    @APIResponse(responseCode = "400",
                 description = "Invalid request - malformed JSON body or \n invalid compaction parameters")
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraCompactionStopRouteKey.class)
    VertxRoute cassandraCompactionStopRoute(RouteBuilder.Factory factory,
                                            CompactionStopHandler compactionStopHandler)
    {
        return factory.builderForRoute()
                      .setBodyHandler(true)  // IMPORTANT: needed for JSON body parsing
                      .handler(compactionStopHandler)
                      .build();
    }

    @GET
    @Path(ApiEndpointsV1.OPERATIONAL_JOB_ROUTE)
    @Operation(summary = "Get operational job status",
               description = "Returns the status of a specific operational job running on the node")
    @APIResponse(description = "Operational job status retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = OperationalJobResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraOperationalJobRouteKey.class)
    VertxRoute cassandraOperationalJobRoute(RouteBuilder.Factory factory,
                                            OperationalJobHandler operationalJobHandler)
    {
        return factory.buildRouteWithHandler(operationalJobHandler);
    }

    @GET
    @Path(ApiEndpointsV1.LIST_OPERATIONAL_JOBS_ROUTE)
    @Operation(summary = "List operational jobs",
               description = "Returns a list of all operational jobs running on the node")
    @APIResponse(description = "Operational jobs listed successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = ListOperationalJobsResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.ListCassandraOperationalJobRouteKey.class)
    VertxRoute listCassandraOperationalJobRoute(RouteBuilder.Factory factory,
                                                ListOperationalJobsHandler listOperationalJobsHandler)
    {
        return factory.buildRouteWithHandler(listOperationalJobsHandler);
    }

    @PUT
    @Path(ApiEndpointsV1.NODE_DECOMMISSION_ROUTE)
    @Operation(summary = "Get node decommission status",
               description = "Returns the decommission status of a Cassandra node")
    @APIResponse(description = "Node decommission status retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = OperationalJobResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraNodeDecommissionRouteKey.class)
    VertxRoute cassandraNodeDecommissionRoute(RouteBuilder.Factory factory,
                                              NodeDecommissionHandler nodeDecommissionHandler)
    {
        return factory.buildRouteWithHandler(nodeDecommissionHandler);
    }

    @PUT
    @Path(ApiEndpointsV1.NODE_DRAIN_ROUTE)
    @Operation(summary = "Drain node",
               description = "Drains the Cassandra node by flushing memtables and stopping writes")
    @APIResponse(description = "Node drain operation completed successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = OperationalJobResponse.class)))
    @APIResponse(description = "Node drain operation initiated successfully",
                 responseCode = "202",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = OperationalJobResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraNodeDrainRouteKey.class)
    VertxRoute cassandraNodeDrainRoute(RouteBuilder.Factory factory,
                                       NodeDrainHandler nodeDrainHandler)
    {
        return factory.buildRouteWithHandler(nodeDrainHandler);
    }

    @PUT
    @Path(ApiEndpointsV1.NODE_MOVE_ROUTE)
    @Operation(summary = "Move node to new token",
               description = "Moves the Cassandra node to a new token in the ring")
    @APIResponse(description = "Node move operation completed successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = OperationalJobResponse.class)))
    @APIResponse(description = "Node move operation initiated successfully",
                 responseCode = "202",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = OperationalJobResponse.class)))
    @APIResponse(description = "Conflicting node move job encountered",
                 responseCode = "409",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = OperationalJobResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraNodeMoveRouteKey.class)
    VertxRoute cassandraNodeMoveRoute(RouteBuilder.Factory factory,
                                      NodeMoveHandler nodeMoveHandler)
    {
        return factory.builderForRoute()
                      .setBodyHandler(true)
                      .handler(nodeMoveHandler)
                      .build();
    }

    @GET
    @Path(ApiEndpointsV1.REPAIR_ROUTE)
    @Operation(summary = "Trigger a repair operation",
               description = "Returns the status of the repair operation on the Cassandra node")
    @APIResponse(description = "Repair operation status retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = OperationalJobResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraRepairRouteKey.class)
    VertxRoute cassandraRepairRoute(RouteBuilder.Factory factory,
                                    ValidateTableExistenceHandler validateKeyspaceExistence,
                                    RepairHandler repairhandler)
    {
        return factory.builderForRoute()
                      .setBodyHandler(true)
                      // We only require validation of keyspace here. It is guaranteed that the URI will provide a
                      // keyspace to validate
                      .handler(validateKeyspaceExistence)
                      .handler(repairhandler)
                      .build();
    }

    @GET
    @Path(ApiEndpointsV1.STREAM_STATS_ROUTE)
    @Operation(summary = "Get stream statistics",
    description = "Returns streaming statistics for the Cassandra node")
    @APIResponse(description = "Stream statistics retrieved successfully",
    responseCode = "200",
    content = @Content(mediaType = "application/json",
    schema = @Schema(implementation = StreamStatsResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraStreamStatsRouteKey.class)
    VertxRoute cassandraStreamStatsRoute(RouteBuilder.Factory factory,
                                         StreamStatsHandler streamStatsHandler)
    {
        return factory.buildRouteWithHandler(streamStatsHandler);
    }

    @GET
    @Path(ApiEndpointsV1.NODE_SETTINGS_ROUTE)
    @Operation(summary = "Get node settings",
               description = "Returns configuration settings for the Cassandra node")
    @APIResponse(description = "Node settings retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = Object.class, example = "{\"status\": \"OK\", \"message\": \"Node settings retrieved successfully\"}")))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraNodeSettingsRouteKey.class)
    VertxRoute cassandraNodeSettings(RouteBuilder.Factory factory,
                                     NodeSettingsHandler nodeSettingsHandler)
    {
        // Node settings endpoint is not Access protected. Any user who can log in into Cassandra is able to view
        // node settings information. Since sidecar and cassandra share list of authenticated identities, sidecar's
        // authenticated users can also read node settings information.
        return factory.builderForUnauthorizedRoute()
                      .handler(nodeSettingsHandler)
                      .build();
    }


    @GET
    @Path(ApiEndpointsV2.NODE_SETTINGS_ROUTE)
    @Operation(summary = "Get node settings",
                description = "Returns configuration settings for the Cassandra node.")
    @APIResponse(description = "Node settings retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = V2NodeSettings.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.V2CassandraNodeSettingsRouteKey.class)
    VertxRoute v2CassandraNodeSettings(RouteBuilder.Factory factory, V2NodeSettingsHandler v2NodeSettingsHandler)
    {
        return factory.buildRouteWithHandler(v2NodeSettingsHandler);
    }

    @GET
    @Path(ApiEndpointsV1.ALL_KEYSPACES_SCHEMA_ROUTE)
    @Operation(summary = "Get all keyspaces schema",
               description = "Returns the schema information for all keyspaces in the Cassandra cluster")
    @APIResponse(description = "Schema information retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = SchemaResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.AllKeyspacesSchemaRouteKey.class)
    VertxRoute cassandraSchemaRoute(RouteBuilder.Factory factory,
                                    SchemaHandler schemaHandler)
    {
        return factory.buildRouteWithHandler(schemaHandler);
    }

    @Deprecated
    @GET
    @Path(ApiEndpointsV1.DEPRECATED_ALL_KEYSPACES_SCHEMA_ROUTE)
    @Operation(summary = "Get all keyspaces schema (deprecated)",
               description = "Returns the schema information for all keyspaces in the Cassandra cluster. This endpoint is deprecated.")
    @APIResponse(description = "Schema information retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = SchemaResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.DeprecatedAllKeyspacesSchemaRouteKey.class)
    VertxRoute deprecatedCassandraSchemaRoute(RouteBuilder.Factory factory,
                                              SchemaHandler schemaHandler)
    {
        return factory.buildRouteWithHandler(schemaHandler);
    }

    @GET
    @Path(ApiEndpointsV1.KEYSPACE_SCHEMA_ROUTE)
    @Operation(summary = "Get keyspace schema",
               description = "Returns the schema information for a specific keyspace")
    @APIResponse(description = "Keyspace schema retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = SchemaResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.KeyspaceSchemaRouteKey.class)
    VertxRoute cassandraKeyspaceSchemaRoute(RouteBuilder.Factory factory,
                                            KeyspaceSchemaHandler keyspaceSchemaHandler)
    {
        return factory.buildRouteWithHandler(keyspaceSchemaHandler);
    }

    @Deprecated
    @GET
    @Path(ApiEndpointsV1.DEPRECATED_KEYSPACE_SCHEMA_ROUTE)
    @Operation(summary = "Get keyspace schema (deprecated)",
               description = "Returns the schema information for a specific keyspace. This endpoint is deprecated.")
    @APIResponse(description = "Keyspace schema retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = SchemaResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.DeprecatedKeyspaceSchemaRouteKey.class)
    VertxRoute deprecatedCassandraKeyspaceSchemaRoute(RouteBuilder.Factory factory,
                                                      KeyspaceSchemaHandler keyspaceSchemaHandler)
    {
        return factory.buildRouteWithHandler(keyspaceSchemaHandler);
    }

    @GET
    @Path(ApiEndpointsV1.RING_ROUTE)
    @Operation(summary = "Get cluster ring information",
               description = "Returns information about the Cassandra cluster ring topology")
    @APIResponse(description = "Ring information retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = RingResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraRingRouteKey.class)
    VertxRoute cassandraRingRoute(RouteBuilder.Factory factory,
                                  RingHandler ringHandler)
    {
        return factory.buildRouteWithHandler(ringHandler);
    }

    @GET
    @Path(ApiEndpointsV1.RING_WITH_KEYSPACE_ROUTE)
    @Operation(summary = "Get Cassandra Ring by Keyspace")
    @APIResponse(description = "Cassandra cluster ring information of a specific Keyspace",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = RingResponse.class)))
    @APIResponse(responseCode = "500", description = "Internal server error on getting the ring view")
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraRingWithKeyspaceRouteKey.class)
    VertxRoute cassandraRingWithKeyspaceRoute(RouteBuilder.Factory factory,
                                              KeyspaceRingHandler keyspaceRingHandler)
    {
        return factory.buildRouteWithHandler(keyspaceRingHandler);
    }

    @GET
    @Path(ApiEndpointsV1.KEYSPACE_TOKEN_MAPPING_ROUTE)
    @Operation(summary = "Get token range replica mapping",
               description = "Returns the replica mapping for token ranges in a specific keyspace")
    @APIResponse(description = "Token range replica mapping retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = TokenRangeReplicasResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraTokenRangeReplicaMapRouteKey.class)
    VertxRoute cassandraTokenRangeReplicaMapRoute(RouteBuilder.Factory factory,
                                                  TokenRangeReplicaMapHandler tokenRangeReplicaMapHandler)
    {
        return factory.buildRouteWithHandler(tokenRangeReplicaMapHandler);
    }

    @GET
    @Path(ApiEndpointsV1.GOSSIP_ROUTE)
    @Operation(summary = "Get gossip information",
               description = "Returns gossip information about the Cassandra cluster")
    @APIResponse(description = "Gossip information retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = GossipInfoResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraGossipInfoRouteKey.class)
    VertxRoute cassandraGossipInfoRoute(RouteBuilder.Factory factory,
                                        GossipInfoHandler gossipInfoHandler)
    {
        return factory.buildRouteWithHandler(gossipInfoHandler);
    }

    @GET
    @Path(ApiEndpointsV1.TABLE_STATS_ROUTE)
    @Operation(summary = "Get table statistics",
               description = "Returns statistics for a specific table")
    @APIResponse(description = "Table statistics retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = TableStatsResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.TableStatsRouteKey.class)
    VertxRoute tableStatsRoute(RouteBuilder.Factory factory,
                               ValidateTableExistenceHandler validateTableExistenceHandler,
                               TableStatsHandler tableStatsHandler)
    {
        return factory.builderForRoute()
                      .handler(validateTableExistenceHandler)
                      .handler(tableStatsHandler).build();
    }

    @PUT
    @Path(ApiEndpointsV1.GOSSIP_ROUTE)
    @Operation(summary = "Update node gossip state",
               description = "Updates the gossip state of the Cassandra node")
    @APIResponse(description = "Node gossip state updated successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = Object.class, example = "{\"status\": \"OK\", \"message\": \"Node gossip state updated successfully\"}")))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.UpdateNodeGossipStateRouteKey.class)
    VertxRoute cassandraChangeGossipStateRoute(RouteBuilder.Factory factory,
                                    GossipUpdateHandler nodeGossipHandler)
    {
        return factory.builderForRoute()
                      .setBodyHandler(true)
                      .handler(nodeGossipHandler)
                      .build();
    }

    @PUT
    @Path(ApiEndpointsV1.CASSANDRA_NATIVE_ROUTE)
    @Operation(summary = "Update node native state",
               description = "Updates the native protocol state of the Cassandra node")
    @APIResponse(description = "Node native state updated successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = Object.class, example = "{\"status\": \"OK\", \"message\": \"Node native state updated successfully\"}")))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.UpdateNodeNativeStateRouteKey.class)
    VertxRoute cassandraChangeNativeStateRoute(RouteBuilder.Factory factory,
                                    NativeUpdateHandler nodeNativeHandler)
    {
        return factory.builderForRoute()
                      .setBodyHandler(true)
                      .handler(nodeNativeHandler)
                      .build();
    }
}
