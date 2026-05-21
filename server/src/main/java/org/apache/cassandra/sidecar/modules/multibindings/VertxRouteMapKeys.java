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

package org.apache.cassandra.sidecar.modules.multibindings;

import io.vertx.core.http.HttpMethod;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.ApiEndpointsV2;

/**
 * Class keys in the {@link com.google.inject.multibindings.MapBinder} to {@link org.apache.cassandra.sidecar.routes.VertxRoute} objects
 */
public interface VertxRouteMapKeys
{
    /** Handlers that are in the global scope, not binding to any specific route **/
    interface GlobalChainAuthHandlerKey extends ClassKey {}
    interface GlobalUtilityHandlerKey extends ClassKey {}
    interface GlobalErrorHandlerKey extends ClassKey {}
    /*-------*/

    /** Alphabetically sorted list of keys **/
    interface AbortRestoreJobRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.POST;
        String ROUTE_URI = ApiEndpointsV1.ABORT_RESTORE_JOB_ROUTE;
    }
    interface AllKeyspacesSchemaRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.ALL_KEYSPACES_SCHEMA_ROUTE;
    }
    interface CassandraConnectedClientStatsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.CONNECTED_CLIENT_STATS_ROUTE;
    }
    interface CassandraCompactionStatsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.COMPACTION_STATS_ROUTE;
    }
    interface CassandraCompactionStopRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String ROUTE_URI = ApiEndpointsV1.COMPACTION_STOP_ROUTE;
    }
    interface CassandraGossipHealthRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.GOSSIP_HEALTH_ROUTE;
    }
    interface CassandraGossipInfoRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.GOSSIP_ROUTE;
    }
    interface CassandraHealthRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.CASSANDRA_HEALTH_ROUTE;
    }
    interface CassandraJmxHealthRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.CASSANDRA_JMX_HEALTH_ROUTE;
    }
    interface CassandraNativeHealthRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.CASSANDRA_NATIVE_HEALTH_ROUTE;
    }
    interface CassandraNodeDecommissionRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String ROUTE_URI = ApiEndpointsV1.NODE_DECOMMISSION_ROUTE;
    }
    interface CassandraNodeDrainRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String ROUTE_URI = ApiEndpointsV1.NODE_DRAIN_ROUTE;
    }
    interface CassandraNodeMoveRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String ROUTE_URI = ApiEndpointsV1.NODE_MOVE_ROUTE;
    }
    interface CassandraNodeSettingsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.NODE_SETTINGS_ROUTE;
    }
    interface CassandraOperationalJobRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.OPERATIONAL_JOB_ROUTE;
    }
    interface CassandraRepairRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String ROUTE_URI = ApiEndpointsV1.REPAIR_ROUTE;
    }
    interface CassandraRingRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.RING_ROUTE;
    }
    interface CassandraRingWithKeyspaceRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.RING_WITH_KEYSPACE_ROUTE;
    } // Tells the data ownership for the specific keyspace
    interface CassandraStreamStatsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.STREAM_STATS_ROUTE;
    }
    interface CassandraTokenRangeReplicaMapRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.KEYSPACE_TOKEN_MAPPING_ROUTE;
    }
    interface ClearSnapshotRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.DELETE;
        String ROUTE_URI = ApiEndpointsV1.SNAPSHOTS_ROUTE;
    }
    interface CreateRestoreJobRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.POST;
        String ROUTE_URI = ApiEndpointsV1.CREATE_RESTORE_JOB_ROUTE;
    }
    interface CreateRestoreSliceRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.POST;
        String ROUTE_URI = ApiEndpointsV1.RESTORE_JOB_SLICES_ROUTE;
    }
    interface CreateSnapshotRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String ROUTE_URI = ApiEndpointsV1.SNAPSHOTS_ROUTE;
    }
    interface DataHubSchemaReportingRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String ROUTE_URI = ApiEndpointsV1.REPORT_SCHEMA_ROUTE;
    }
    interface DeleteServiceConfigurationRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.DELETE;
        String ROUTE_URI = ApiEndpointsV1.SERVICE_CONFIG_ROUTE;
    }
    interface DeprecatedAllKeyspacesSchemaRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.DEPRECATED_ALL_KEYSPACES_SCHEMA_ROUTE;
    }
    interface DeprecatedKeyspaceSchemaRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.DEPRECATED_KEYSPACE_SCHEMA_ROUTE;
    }
    interface DeprecatedListSnapshotRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.DEPRECATED_SNAPSHOTS_ROUTE;
    }
    interface DeprecatedStreamSSTableComponentsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.DEPRECATED_COMPONENTS_ROUTE;
    }
    interface GetAllServiceConfigurationsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.SERVICES_CONFIG_ROUTE;
    }
    interface GetRestoreJobProgressRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.RESTORE_JOB_PROGRESS_ROUTE;
    }
    interface GetRestoreJobSummaryRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.RESTORE_JOB_ROUTE;
    }
    interface InvalidateCacheKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.DELETE;
        String ROUTE_URI = ApiEndpointsV1.INVALIDATE_CACHE_ROUTE;
    }
    interface KeyspaceSchemaRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.KEYSPACE_SCHEMA_ROUTE;
    }
    interface LifecycleInfoRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.LIFECYCLE_ROUTE;
    }
    interface LifecycleUpdateRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String ROUTE_URI = ApiEndpointsV1.LIFECYCLE_ROUTE;
    }
    interface ListCassandraOperationalJobRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.LIST_OPERATIONAL_JOBS_ROUTE;
    }
    interface ListCdcSegmentsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.LIST_CDC_SEGMENTS_ROUTE;
    }
    interface ListSnapshotRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.SNAPSHOTS_ROUTE;
    }
    interface OpenApiJsonRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.OPENAPI_JSON_ROUTE;
    }
    interface OpenApiYamlRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.OPENAPI_YAML_ROUTE;
    }
    interface OpenApiHtmlRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.OPENAPI_HTML_ROUTE;
    }
    interface LiveMigrationCancelDataCopyTaskRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PATCH;
        String ROUTE_URI = ApiEndpointsV1.LIVE_MIGRATION_DATA_COPY_TASK_ROUTE;
    }
    interface LiveMigrationCancelFilesVerificationTaskRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PATCH;
        String ROUTE_URI = ApiEndpointsV1.LIVE_MIGRATION_FILES_VERIFICATION_TASK_ROUTE;
    }
    interface LiveMigrationCreateDataCopyTaskRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.POST;
        String ROUTE_URI = ApiEndpointsV1.LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE;
    }
    interface LiveMigrationGetAllDataCopyTasksRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE;
    }
    interface LiveMigrationGetAllFilesVerificationTasksRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE;
    }
    interface LiveMigrationGetDataCopyTaskRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.LIVE_MIGRATION_DATA_COPY_TASK_ROUTE;
    }
    interface LiveMigrationGetFilesVerificationTaskRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.LIVE_MIGRATION_FILES_VERIFICATION_TASK_ROUTE;
    }
    interface LiveMigrationFileStreamHandlerRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.LIVE_MIGRATION_FILE_TRANSFER_ROUTE;
    }
    interface LiveMigrationCreateFilesDigestVerificationTaskRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.POST;
        String ROUTE_URI = ApiEndpointsV1.LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE;
    }
    interface LiveMigrationListInstanceFilesRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.LIVE_MIGRATION_FILES_ROUTE;
    }
    interface LiveMigrationStatusRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.LIVE_MIGRATION_STATUS_ROUTE;
    }
    interface LiveMigrationStatusUpdateRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.POST;
        String ROUTE_URI = ApiEndpointsV1.LIVE_MIGRATION_STATUS_ROUTE;
    }
    interface LiveMigrationStatusDeleteRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.DELETE;
        String ROUTE_URI = ApiEndpointsV1.LIVE_MIGRATION_STATUS_ROUTE;
    }
    interface SSTableCleanupRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.DELETE;
        String ROUTE_URI = ApiEndpointsV1.SSTABLE_CLEANUP_ROUTE;
    }
    interface SSTableImportRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String ROUTE_URI = ApiEndpointsV1.SSTABLE_IMPORT_ROUTE;
    }
    interface SSTableUploadRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String ROUTE_URI = ApiEndpointsV1.SSTABLE_UPLOAD_ROUTE;
    }
    interface SidecarHealthRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.HEALTH_ROUTE;
    }
    interface StreamCdcSegmentRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.STREAM_CDC_SEGMENTS_ROUTE;
    }
    interface StreamSSTableComponentsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.COMPONENTS_ROUTE;
    }
    interface StreamSSTableComponentsWithSecondaryIndexRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.COMPONENTS_WITH_SECONDARY_INDEX_ROUTE_SUPPORT;
    }
    interface SystemDiskInfoRoute extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.SYSTEM_DISK_INFO_ROUTE;
    }
    interface TableStatsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.TABLE_STATS_ROUTE;
    }
    interface TimeSkewRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.TIME_SKEW_ROUTE;
    }
    interface UpdateNodeGossipStateRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String    ROUTE_URI   = ApiEndpointsV1.GOSSIP_ROUTE;
    }
    interface UpdateNodeNativeStateRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String    ROUTE_URI   = ApiEndpointsV1.CASSANDRA_NATIVE_ROUTE;
    }
    interface UpdateRestoreJobRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PATCH;
        String ROUTE_URI = ApiEndpointsV1.RESTORE_JOB_ROUTE;
    }
    interface UpdateServiceConfigurationRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String ROUTE_URI = ApiEndpointsV1.SERVICE_CONFIG_ROUTE;
    }
    interface V2CassandraNodeSettingsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV2.NODE_SETTINGS_ROUTE;
    }
}
