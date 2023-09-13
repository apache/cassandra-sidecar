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

    public static final String HEALTH = "/__health";
    public static final String CASSANDRA = "/cassandra";
    public static final String KEYSPACE_PATH_PARAM = ":keyspace";
    public static final String TABLE_PATH_PARAM = ":table";
    public static final String SNAPSHOT_PATH_PARAM = ":snapshot";
    public static final String COMPONENT_PATH_PARAM = ":component";
    public static final String UPLOAD_ID_PATH_PARAM = ":uploadId";

    public static final String PER_KEYSPACE = "/keyspaces/" + KEYSPACE_PATH_PARAM;
    public static final String PER_TABLE = "/tables/" + TABLE_PATH_PARAM;
    public static final String PER_SNAPSHOT = "/snapshots/" + SNAPSHOT_PATH_PARAM;
    public static final String PER_COMPONENT = "/components/" + COMPONENT_PATH_PARAM;
    public static final String PER_UPLOAD = "/uploads/" + UPLOAD_ID_PATH_PARAM;

    public static final String HEALTH_ROUTE = API_V1 + HEALTH;
    public static final String CASSANDRA_HEALTH_ROUTE = API_V1 + CASSANDRA + HEALTH;

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
    public static final String RING_ROUTE_PER_KEYSPACE = RING_ROUTE + PER_KEYSPACE;

    public static final String SSTABLE_UPLOAD_ROUTE = API_V1 + PER_UPLOAD + PER_KEYSPACE + PER_TABLE + PER_COMPONENT;
    public static final String SSTABLE_IMPORT_ROUTE = API_V1 + PER_UPLOAD + PER_KEYSPACE + PER_TABLE + "/import";
    public static final String SSTABLE_CLEANUP_ROUTE = API_V1 + PER_UPLOAD;

    public static final String GOSSIP_INFO_ROUTE = API_V1 + CASSANDRA + "/gossip";
    public static final String TIME_SKEW_ROUTE = API_V1 + "/time-skew";

    public static final String KEYSPACE_TOKEN_MAPPING_ROUTE = API_V1 + PER_KEYSPACE + "/token-range-replicas";

    private ApiEndpointsV1()
    {
        throw new IllegalStateException(getClass() + " is a constants container and shall not be instantiated");
    }
}
