package org.apache.cassandra.sidecar.adapters.base.jmx;

/**
 * An interface that pulls methods from the Cassandra Metrics Proxy
 */
public interface MetricsJmxOperations
{
    String METRICS_OBJ_TYPE_KEYSPACE_FORMAT = "org.apache.cassandra.metrics:type=Keyspace,keyspace=%s,name=%s";
    String METRICS_OBJ_TYPE_KEYSPACE_TABLE_FORMAT = "org.apache.cassandra.metrics:type=Table,keyspace=%s,scope=%s,name=%s";
    // org.apache.cassandra.metrics:type=Table,keyspace=cql_test_keyspace,scope=table_00,name=LiveSSTableCount
    String METRICS_OBJ_TYPE_TABLE_FORMAT = "org.apache.cassandra.metrics:type=Table,name=%s";
    String METRICS_OBJ_TYPE_INDEX_FORMAT = "org.apache.cassandra.metrics:type=IndexTable,name=%s";

    // Retrieves the value of the metric of type Gauge
    Object getValue();

    long getCount();
}
