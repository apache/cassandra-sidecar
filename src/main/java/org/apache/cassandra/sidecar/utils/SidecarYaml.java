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

package org.apache.cassandra.sidecar.utils;

/**
 * 1. Stores the property keys used to retrieve information from sidecar.yaml file.
 * 2. Utilities that help to read yaml
 */
public class SidecarYaml
{
    public static final String HOST = "sidecar.host";
    public static final String PORT = "sidecar.port";
    public static final String HEALTH_CHECK_INTERVAL = "healthcheck.poll_freq_millis";
    public static final String KEYSTORE_PATH = "sidecar.ssl.keystore.path";
    public static final String KEYSTORE_PASSWORD = "sidecar.ssl.keystore.password";
    public static final String TRUSTSTORE_PATH = "sidecar.ssl.truststore.path";
    public static final String TRUSTSTORE_PASSWORD = "sidecar.ssl.truststore.password";
    public static final String SSL_ENABLED = "sidecar.ssl.enabled";
    public static final String STREAM_REQUESTS_PER_SEC = "sidecar.throttle.stream_requests_per_sec";
    public static final String THROTTLE_TIMEOUT_SEC = "sidecar.throttle.timeout_sec";
    public static final String THROTTLE_DELAY_SEC = "sidecar.throttle.delay_sec";
    public static final String ALLOWABLE_SKEW_IN_MINUTES = "sidecar.allowable_time_skew_in_minutes";
    public static final String REQUEST_IDLE_TIMEOUT_MILLIS = "sidecar.request_idle_timeout_millis";
    public static final String REQUEST_TIMEOUT_MILLIS = "sidecar.request_timeout_millis";
    public static final String MIN_FREE_SPACE_PERCENT_FOR_UPLOAD = "sidecar.sstable_uploads.min_free_space_percent";
    public static final String CONCURRENT_UPLOAD_LIMIT = "sidecar.sstable_upload.concurrent_upload_limit";
    public static final String SSTABLE_IMPORT_POLL_INTERVAL_MILLIS = "sidecar.sstable_import.poll_interval_millis";
    public static final String SSTABLE_IMPORT_CACHE_CONFIGURATION = "sidecar.sstable_import.cache";

    // v1 cassandra instance key constants
    public static final String CASSANDRA_INSTANCE = "cassandra";

    // v2 cassandra instances key constants
    public static final String CASSANDRA_INSTANCES = "cassandra_instances";
    public static final String CASSANDRA_INSTANCE_ID = "id";
    public static final String CASSANDRA_INSTANCE_HOST = "host";
    public static final String CASSANDRA_INSTANCE_PORT = "port";
    public static final String CASSANDRA_INSTANCE_DATA_DIRS = "data_dirs";
    public static final String CASSANDRA_INSTANCE_UPLOADS_STAGING_DIR = "uploads_staging_dir";
    public static final String CASSANDRA_JMX_HOST = "jmx_host";
    public static final String CASSANDRA_JMX_PORT = "jmx_port";
    public static final String CASSANDRA_JMX_ROLE = "jmx_role";
    public static final String CASSANDRA_JMX_ROLE_PASSWORD = "jmx_role_password";
    public static final String CASSANDRA_JMX_SSL_ENABLED = "jmx_ssl_enabled";

    // validation for cassandra inputs
    public static final String CASSANDRA_INPUT_VALIDATION = "cassandra_input_validation";
    public static final String CASSANDRA_FORBIDDEN_KEYSPACES = "forbidden_keyspaces";
    public static final String CASSANDRA_ALLOWED_CHARS_FOR_DIRECTORY = "allowed_chars_for_directory";
    public static final String CASSANDRA_ALLOWED_CHARS_FOR_COMPONENT_NAME = "allowed_chars_for_component_name";
    public static final String CASSANDRA_ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME =
    "allowed_chars_for_restricted_component_name";

    // cache configuration
    public static final String CACHE_EXPIRE_AFTER_ACCESS_MILLIS = "expire_after_access_millis";
    public static final String CACHE_MAXIMUM_SIZE = "maximum_size";

    // worker pools configuration
    private static final String WORKER_POOL_PREFIX = "sidecar.worker_pools.";
    public static final String WORKER_POOL_FOR_SERVICE = WORKER_POOL_PREFIX + "service";
    public static final String WORKER_POOL_FOR_INTERNAL = WORKER_POOL_PREFIX + "interal";
    public static final String WORKER_POOL_NAME = "name";
    public static final String WORKER_POOL_SIZE = "size";
    public static final String WORKER_POOL_MAX_EXECUTION_TIME_MILLIS = "max_execution_time_millis";
}
