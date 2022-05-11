package org.apache.cassandra.sidecar.cluster.instance;

import java.util.List;

import org.apache.cassandra.sidecar.common.CQLSession;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;

/**
 * Metadata of an instance
 */
public interface InstanceMetadata
{
    /**
     * Instance id.
     */
    int id();

    /**
     * Host address of cassandra instance.
     */
    String host();

    /**
     * Port number of cassandra instance.
     */
    int port();

    /**
     * List of data directories of cassandra instance.
     */
    List<String> dataDirs();

    /**
     * CQLSession for connecting with instance.
     */
    CQLSession session();

    /**
     * Delegate specific for the instance.
     */
    CassandraAdapterDelegate delegate();
}
