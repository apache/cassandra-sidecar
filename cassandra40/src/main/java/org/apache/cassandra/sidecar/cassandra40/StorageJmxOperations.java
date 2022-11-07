package org.apache.cassandra.sidecar.cassandra40;

import java.util.Map;

/**
 * An interface that pulls a method from the Cassandra Storage Service Proxy
 */
interface StorageJmxOperations
{
    /**
     * Takes the snapshot of a multiple column family from different keyspaces. A snapshot name must be specified.
     *
     * @param tag      the tag given to the snapshot; may not be null or empty
     * @param options  map of options, for example ttl, skipFlush
     * @param entities list of keyspaces / tables in the form of empty | ks1 ks2 ... | ks1.cf1,ks2.cf2,...
     */
    void takeSnapshot(String tag, Map<String, String> options, String... entities);
}
