package org.apache.cassandra.sidecar.common.data;

import org.apache.cassandra.sidecar.common.utils.ValidationUtils;

/**
 * Represents an SSTable component that includes a keyspace, table name and component name
 */
public class SSTableComponent extends QualifiedTableName
{
    private final String componentName;

    /**
     * Constructor for the holder class
     *
     * @param keyspace      the keyspace in Cassandra
     * @param tableName     the table name in Cassandra
     * @param componentName the name of the SSTable component
     */
    public SSTableComponent(String keyspace, String tableName, String componentName)
    {
        super(keyspace, tableName);
        this.componentName = ValidationUtils.validateComponentName(componentName);
    }

    /**
     * @return the name of the SSTable component
     */
    public String getComponentName()
    {
        return componentName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "SSTableComponent{" +
               "keyspace='" + getKeyspace() + '\'' +
               ", tableName='" + getTableName() + '\'' +
               ", componentName='" + componentName + '\'' +
               '}';
    }
}
