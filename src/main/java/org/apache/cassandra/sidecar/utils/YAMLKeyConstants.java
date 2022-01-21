package org.apache.cassandra.sidecar.utils;

/**
 * Stores keys used to retrieve information from sidecar.yaml file.
 */
public class YAMLKeyConstants
{
    public static final String HOST = "sidecar.host";
    public static final String PORT = "sidecar.port";
    public static final String HEALTH_CHECK_FREQ = "healthcheck.poll_freq_millis";
    public static final String KEYSTORE_PATH = "sidecar.ssl.keystore.path";
    public static final String KEYSTORE_PASSWORD = "sidecar.ssl.keystore.password";
    public static final String TRUSTSTORE_PATH = "sidecar.ssl.truststore.path";
    public static final String TRUSTSTORE_PASSWORD = "sidecar.ssl.truststore.password";
    public static final String SSL_ENABLED = "sidecar.ssl.enabled";
    public static final String STREAM_REQUESTS_PER_SEC = "sidecar.throttle.stream_requests_per_sec";
    public static final String THROTTLE_TIMEOUT_SEC = "sidecar.throttle.timeout_sec";
    public static final String THROTTLE_DELAY_SEC = "sidecar.throttle.delay_sec";

    // v1 cassandra instance key constants
    public static final String CASSANDRA_INSTANCE = "cassandra";

    // v2 cassandra instances key constants
    public static final String CASSANDRA_INSTANCES = "cassandra_instances";
    public static final String CASSANDRA_INSTANCE_ID = "id";
    public static final String CASSANDRA_INSTANCE_HOST = "host";
    public static final String CASSANDRA_INSTANCE_PORT = "port";
    public static final String CASSANDRA_INSTANCE_DATA_DIRS = "data_dirs";
}
