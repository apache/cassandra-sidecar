package org.apache.cassandra.sidecar.common.data;

/**
 * Holder class for the {@code org.apache.cassandra.sidecar.routes.StreamSSTableComponentHandler}
 * request parameters
 */
public class StreamSSTableComponentRequest extends SSTableComponent
{
    private final String snapshotName;

    /**
     * Constructor for the holder class
     *
     * @param keyspace      the keyspace in Cassandra
     * @param tableName     the table name in Cassandra
     * @param snapshotName  the name of the snapshot
     * @param componentName the name of the SSTable component
     */
    public StreamSSTableComponentRequest(String keyspace, String tableName, String snapshotName, String componentName)
    {
        super(keyspace, tableName, componentName);
        this.snapshotName = validationUtils.validateSnapshotName(snapshotName);
    }

    /**
     * @return the name of the snapshot
     */
    public String getSnapshotName()
    {
        return snapshotName;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "StreamSSTableComponentRequest{" +
               "keyspace='" + getKeyspace() + '\'' +
               ", tableName='" + getTableName() + '\'' +
               ", snapshot='" + snapshotName + '\'' +
               ", componentName='" + getComponentName() + '\'' +
               '}';
    }
}
