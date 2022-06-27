package org.apache.cassandra.sidecar.common.data;

import org.apache.cassandra.sidecar.common.utils.ValidationUtils;

/**
 * Contains the keyspace and table name in Cassandra
 */
public class QualifiedTableName
{
    private final String keyspace;
    private final String tableName;

    /**
     * Constructs a qualified name with the given {@code keyspace} and {@code tableName}
     *
     * @param keyspace  the keyspace in Cassandra
     * @param tableName the table name in Cassandra
     */
    public QualifiedTableName(String keyspace, String tableName)
    {
        this.keyspace = ValidationUtils.validateKeyspaceName(keyspace);
        this.tableName = ValidationUtils.validateTableName(tableName);
    }

    /**
     * @return the keyspace in Cassandra
     */
    public String getKeyspace()
    {
        return keyspace;
    }

    /**
     * @return the table name in Cassandra
     */
    public String getTableName()
    {
        return tableName;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return keyspace + "." + tableName;
    }
}
