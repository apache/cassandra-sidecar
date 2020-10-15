package org.apache.cassandra.sidecar.cdc.output;

import java.io.Closeable;
import org.apache.cassandra.sidecar.cdc.Change;

/**
 * Interface for emitting Cassandra PartitionUpdates
 */
public interface Output extends Closeable
{
    void emitChange(Change change) throws Exception;
}
