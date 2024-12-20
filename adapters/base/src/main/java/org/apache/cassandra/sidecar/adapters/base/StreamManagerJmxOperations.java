package org.apache.cassandra.sidecar.adapters.base;

import java.util.Set;
import javax.management.openmbean.CompositeData;

/**
 * An interface that pulls methods from the Cassandra Stream manager Proxy
 */
public interface StreamManagerJmxOperations
{

    String STREAM_MANAGER_OBJ_NAME = "org.apache.cassandra.net:type=StreamManager";
    Set<CompositeData> getCurrentStreams();
}
