package org.apache.cassandra.sidecar.common;

/**
 * A constants container class for API endpoints of version 2.
 */
public class ApiEndpointsV2
{
    public static final String API = "/api";
    public static final String API_V2 = API + "/v2";
    public static final String CASSANDRA = "/cassandra";
    public static final String NODE_SETTINGS_ROUTE = API_V2 + CASSANDRA + "/settings";
}
