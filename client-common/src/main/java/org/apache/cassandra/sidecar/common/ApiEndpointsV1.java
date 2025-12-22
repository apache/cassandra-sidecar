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

package org.apache.cassandra.sidecar.common;

/**
 * A constants container class for API endpoints of version 1.
 */
public final class ApiEndpointsV1
{
    public static final String API = "/api";
    public static final String API_V1 = API + "/v1";
    public static final String API_V1_ALL_ROUTES = API_V1 + "/.*";

    public static final String HEALTH = "/__health";
    public static final String CASSANDRA = "/cassandra";

    public static final String NATIVE = "/native";
    public static final String JMX = "/jmx";
    public static final String KEYSPACE = "keyspace";
    public static final String TABLE = "table";
    public static final String KEYSPACE_PATH_PARAM = ":" + KEYSPACE;
    public static final String TABLE_PATH_PARAM = ":" + TABLE;
    public static final String SNAPSHOT_PATH_PARAM = ":snapshot";
    public static final String COMPONENT_PATH_PARAM = ":component";
    public static final String INDEX_PATH_PARAM = ":index";
    public static final String UPLOAD_ID_PATH_PARAM = ":uploadId";
    public static final String JOB_ID_PATH_PARAM = ":jobId";

    public static final String OPERATIONAL_JOB_ID_PATH_PARAM = ":operationId";

    public static final String PER_KEYSPACE = "/keyspaces/" + KEYSPACE_PATH_PARAM;
    public static final String PER_TABLE = "/tables/" + TABLE_PATH_PARAM;
    public static final String PER_SNAPSHOT = "/snapshots/" + SNAPSHOT_PATH_PARAM;
    public static final String PER_COMPONENT = "/components/" + COMPONENT_PATH_PARAM;
    public static final String PER_SECONDARY_INDEX_COMPONENT = "/components/" + INDEX_PATH_PARAM
                                                               + "/" + COMPONENT_PATH_PARAM;
    public static final String PER_UPLOAD = "/uploads/" + UPLOAD_ID_PATH_PARAM;

    public static final String HEALTH_ROUTE = API_V1 + HEALTH;

    /**
     * @deprecated in favor of {@link #CASSANDRA_NATIVE_HEALTH_ROUTE}
     */
    @Deprecated
    public static final String CASSANDRA_HEALTH_ROUTE = API_V1 + CASSANDRA + HEALTH;
    public static final String CASSANDRA_NATIVE_ROUTE = API_V1 + CASSANDRA + NATIVE;
    public static final String CASSANDRA_NATIVE_HEALTH_ROUTE = CASSANDRA_NATIVE_ROUTE + HEALTH;
    public static final String CASSANDRA_JMX_HEALTH_ROUTE = API_V1 + CASSANDRA + JMX + HEALTH;

    @Deprecated  // NOTE: Uses singular forms of "keyspace" and "table"
    public static final String DEPRECATED_SNAPSHOTS_ROUTE = API_V1 + "/keyspace/" + KEYSPACE_PATH_PARAM +
                                                            "/table/" + TABLE_PATH_PARAM +
                                                            PER_SNAPSHOT;
    @Deprecated
    public static final String DEPRECATED_COMPONENTS_ROUTE = DEPRECATED_SNAPSHOTS_ROUTE +
                                                             "/component/" + COMPONENT_PATH_PARAM;

    // Replaces DEPRECATED_SNAPSHOT_ROUTE
    public static final String SNAPSHOTS_ROUTE = API_V1 + PER_KEYSPACE + PER_TABLE + PER_SNAPSHOT;
    // Replaces DEPRECATED_COMPONENTS_ROUTE
    public static final String COMPONENTS_ROUTE = SNAPSHOTS_ROUTE + PER_COMPONENT;
    public static final String COMPONENTS_WITH_SECONDARY_INDEX_ROUTE_SUPPORT = SNAPSHOTS_ROUTE
                                                                               + PER_SECONDARY_INDEX_COMPONENT;

    @Deprecated
    public static final String DEPRECATED_ALL_KEYSPACES_SCHEMA_ROUTE = API_V1 + "/schema/keyspaces";
    @Deprecated
    public static final String DEPRECATED_KEYSPACE_SCHEMA_ROUTE = API_V1 + "/schema" + PER_KEYSPACE;

    // Replaces DEPRECATED_ALL_KEYSPACES_SCHEMA_ROUTE
    public static final String ALL_KEYSPACES_SCHEMA_ROUTE = API_V1 + CASSANDRA + "/schema";

    // Replaces DEPRECATED_KEYSPACE_SCHEMA_ROUTE
    public static final String KEYSPACE_SCHEMA_ROUTE = API_V1 + PER_KEYSPACE + "/schema";
    public static final String NODE_SETTINGS_ROUTE = API_V1 + CASSANDRA + "/settings";

    public static final String RING_ROUTE = API_V1 + CASSANDRA + "/ring";
    public static final String RING_WITH_KEYSPACE_ROUTE = RING_ROUTE + PER_KEYSPACE;

    public static final String SSTABLE_UPLOAD_ROUTE = API_V1 + PER_UPLOAD + PER_KEYSPACE + PER_TABLE + PER_COMPONENT;
    public static final String SSTABLE_IMPORT_ROUTE = API_V1 + PER_UPLOAD + PER_KEYSPACE + PER_TABLE + "/import";
    public static final String SSTABLE_CLEANUP_ROUTE = API_V1 + PER_UPLOAD;

    public static final String GOSSIP_ROUTE = API_V1 + CASSANDRA + "/gossip";
    public static final String GOSSIP_HEALTH_ROUTE = GOSSIP_ROUTE + HEALTH;
    public static final String TIME_SKEW_ROUTE = API_V1 + "/time-skew";

    public static final String KEYSPACE_TOKEN_MAPPING_ROUTE = API_V1 + PER_KEYSPACE + "/token-range-replicas";

    // Blob Transport Extension
    public static final String RESTORE_JOBS = "/restore-jobs";
    public static final String SLICES = "/slices";
    public static final String ABORT = "/abort";
    public static final String PROGRESS = "/progress";
    public static final String FETCH_POLICY_QUERY_PARAM = "fetch-policy";
    public static final String PER_RESTORE_JOB = RESTORE_JOBS + "/" + JOB_ID_PATH_PARAM;
    public static final String CREATE_RESTORE_JOB_ROUTE = API_V1 + PER_KEYSPACE + PER_TABLE + RESTORE_JOBS;
    public static final String RESTORE_JOB_ROUTE = API_V1 + PER_KEYSPACE + PER_TABLE + PER_RESTORE_JOB;
    public static final String RESTORE_JOB_SLICES_ROUTE = RESTORE_JOB_ROUTE + SLICES;
    public static final String ABORT_RESTORE_JOB_ROUTE = RESTORE_JOB_ROUTE + ABORT;
    public static final String RESTORE_JOB_PROGRESS_ROUTE = RESTORE_JOB_ROUTE + PROGRESS;

    // Cache related APIs
    public static final String CACHE_NAME_PARAM = ":cacheName";
    public static final String INVALIDATE_CACHE_ROUTE = API_V1 + "/caches/" + CACHE_NAME_PARAM + "/invalidate";

    // CDC APIs
    public static final String CDC_PATH = "/cdc";
    public static final String SEGMENT_PATH_PARAM = ":segment";
    public static final String LIST_CDC_SEGMENTS_ROUTE = API_V1 + CDC_PATH + "/segments";
    public static final String STREAM_CDC_SEGMENTS_ROUTE = LIST_CDC_SEGMENTS_ROUTE + "/" + SEGMENT_PATH_PARAM;

    // Schema Reporting
    private static final String REPORT_SCHEMA = "/datahub/schemas";
    public static final String REPORT_SCHEMA_ROUTE = API_V1 + REPORT_SCHEMA;

    public static final String SERVICES_PATH = "/services";
    public static final String SERVICE_PARAM = ":service";
    public static final String CONFIG = "/config";
    public static final String SERVICE_CONFIG_ROUTE = API_V1 + SERVICES_PATH + "/" + SERVICE_PARAM + CONFIG;
    public static final String SERVICES_CONFIG_ROUTE = API_V1 + SERVICES_PATH;

    public static final String CONNECTED_CLIENT_STATS_ROUTE = API_V1 + CASSANDRA + "/stats/connected-clients";
    public static final String COMPACTION_STATS_ROUTE = API_V1 + CASSANDRA + "/stats/compaction";

    private static final String OPERATION_ROUTE = "/operations";
    private static final String OPERATIONAL_JOBS = "/operational-jobs";
    private static final String PER_OPERATIONAL_JOB = OPERATIONAL_JOBS + '/' + OPERATIONAL_JOB_ID_PATH_PARAM;
    public static final String LIST_OPERATIONAL_JOBS_ROUTE = API_V1 + CASSANDRA + OPERATIONAL_JOBS;
    public static final String OPERATIONAL_JOB_ROUTE = API_V1 + CASSANDRA + PER_OPERATIONAL_JOB;
    public static final String NODE_DECOMMISSION_ROUTE = API_V1 + CASSANDRA + "/operations/decommission";
    public static final String NODE_MOVE_ROUTE = API_V1 + CASSANDRA + "/operations/move";
    public static final String NODE_DRAIN_ROUTE = API_V1 + CASSANDRA + "/operations/drain";
    public static final String STREAM_STATS_ROUTE = API_V1 + CASSANDRA + "/stats/streams";
    public static final String TABLE_STATS_ROUTE = API_V1 + CASSANDRA + PER_KEYSPACE + PER_TABLE + "/stats";
    public static final String COMPACTION_STOP_ROUTE = API_V1 + CASSANDRA + OPERATION_ROUTE + "/compaction/stop";

    // Live Migration APIs
    public static final String LIVE_MIGRATION_API_PREFIX = API_V1 + "/live-migration";

    public static final String LIVE_MIGRATION_FILES_ROUTE = LIVE_MIGRATION_API_PREFIX + "/files";

    public static final String DIR_TYPE_PARAM = "dirType";
    public static final String DIR_INDEX_PARAM = "dirIndex";
    // API endpoint allows files transfer of specific directories handled by Cassandra during live migration operation.
    // dirType path parameter    : The type of directory (data, commitlog, etc.)
    // dirIndex path parameter   : The index of the directory
    // The remaining path ('/*') : Represents the relative path within the specified directory
    public static final String LIVE_MIGRATION_FILE_TRANSFER_ROUTE = LIVE_MIGRATION_FILES_ROUTE + "/:" + DIR_TYPE_PARAM
                                                                    + "/:" + DIR_INDEX_PARAM + "/*";

    public static final String LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE = LIVE_MIGRATION_API_PREFIX + "/data-copy-tasks";
    public static final String LIVE_MIGRATION_DATA_COPY_TASK_ROUTE = LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE + "/:taskId";
    public static final String LIVE_MIGRATION_STATUS_ROUTE = LIVE_MIGRATION_API_PREFIX + "/status";


    public static final String OPENAPI_JSON_ROUTE = "/spec/openapi.json";
    public static final String OPENAPI_YAML_ROUTE = "/spec/openapi.yaml";
    // With the wildcard, the index.html page under resources/docs/openapi/index.html will render
    // when a user visits /openapi
    public static final String OPENAPI_HTML_ROUTE = "/openapi/*";

    // Lifecycle APIs
    public static final String LIFECYCLE_ROUTE = API_V1 + CASSANDRA + "/lifecycle";

    private static final String SYSTEM_API_PREFIX = API_V1 + "/system";
    public static final String SYSTEM_DISK_INFO_ROUTE = SYSTEM_API_PREFIX + "/disk-info";

    private ApiEndpointsV1()
    {
        throw new IllegalStateException(getClass() + " is a constants container and shall not be instantiated");
    }
}
