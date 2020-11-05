package org.apache.cassandra.sidecar.cdc;

import javax.inject.Singleton;

import com.datastax.driver.core.AggregateMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.FunctionMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.MaterializedViewMetadata;
import com.datastax.driver.core.SchemaChangeListener;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;
import com.google.inject.Inject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
//import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
//import org.apache.cassandra.schema.Schema;
//import org.apache.cassandra.schema.TableId;
//import org.apache.cassandra.schema.Tables;

/**
 * Schema change listener to update CDC reader's keyspace/column family metadata.
 */
@Singleton
public class CDCSchemaChangeListener implements SchemaChangeListener
{
    private static final Logger logger = LoggerFactory.getLogger(SchemaChangeListener.class);

    @Inject
    public CDCSchemaChangeListener()
    {
    }

    // TODO : Add/modify/remove KeySpaces
    @Override
    public void onKeyspaceAdded(KeyspaceMetadata keyspace)
    {

    }

    @Override
    public void onKeyspaceRemoved(KeyspaceMetadata keyspace)
    {

    }

    @Override
    public void onKeyspaceChanged(KeyspaceMetadata current, KeyspaceMetadata previous)
    {

    }

    // Tables
    @Override
    public void onTableAdded(TableMetadata table)
    {
//        try
//        {
//            org.apache.cassandra.schema.KeyspaceMetadata keyspaceMetadata = Schema.instance
//                    .getKeyspaceMetadata(table.getKeyspace().getName());
//            org.apache.cassandra.schema.TableMetadata newTable = CreateTableStatement.parse(table.asCQLQuery(),
//                            table.getKeyspace().getName())
//                    .build(TableId.fromUUID(table.getId()));
//            Tables tables = keyspaceMetadata.tables
//                    .with(newTable);
//            org.apache.cassandra.schema.KeyspaceMetadata updatedKeyspaceMetadata = keyspaceMetadata
//                    .withSwapped(tables);
//            Schema.instance.load(updatedKeyspaceMetadata);
//        }
//        catch (Exception ex)
//        {
//            logger.error("Failed to update metadata on onTableAdded : {}", ex.getMessage());
//        }
    }

    @Override
    public void onTableRemoved(TableMetadata table)
    {
//        try
//        {
//            org.apache.cassandra.schema.KeyspaceMetadata keyspaceMetadata = Schema.instance
//                    .getKeyspaceMetadata(table.getKeyspace().getName());
//            Tables tables = keyspaceMetadata.tables
//                    .without(table.getName());
//            org.apache.cassandra.schema.KeyspaceMetadata updatedKeyspaceMetadata = keyspaceMetadata
//                    .withSwapped(tables);
//            Schema.instance.load(updatedKeyspaceMetadata);
//        }
//        catch (Exception ex)
//        {
//            logger.error("Failed to update metadata on onTableRemoved : {}", ex.getMessage());
//        }
    }

    @Override
    public void onTableChanged(TableMetadata current, TableMetadata previous)
    {
        onTableRemoved(previous);
        onTableAdded(current);
    }

    // Types
    @Override
    public void onUserTypeAdded(UserType type)
    {

    }

    @Override
    public void onUserTypeRemoved(UserType type)
    {

    }

    @Override
    public void onUserTypeChanged(UserType current, UserType previous)
    {

    }

    @Override
    public void onFunctionAdded(FunctionMetadata function)
    {

    }

    @Override
    public void onFunctionRemoved(FunctionMetadata function)
    {

    }

    @Override
    public void onFunctionChanged(FunctionMetadata current, FunctionMetadata previous)
    {

    }

    @Override
    public void onAggregateAdded(AggregateMetadata aggregate)
    {

    }

    @Override
    public void onAggregateRemoved(AggregateMetadata aggregate)
    {

    }

    @Override
    public void onAggregateChanged(AggregateMetadata current, AggregateMetadata previous)
    {

    }

    @Override
    public void onMaterializedViewAdded(MaterializedViewMetadata view)
    {

    }

    @Override
    public void onMaterializedViewRemoved(MaterializedViewMetadata view)
    {

    }

    @Override
    public void onMaterializedViewChanged(MaterializedViewMetadata current, MaterializedViewMetadata previous)
    {

    }

    @Override
    public void onRegister(Cluster cluster)
    {

    }

    @Override
    public void onUnregister(Cluster cluster)
    {

    }
}
