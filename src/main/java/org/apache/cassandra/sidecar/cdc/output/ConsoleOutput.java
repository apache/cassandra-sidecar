package org.apache.cassandra.sidecar.cdc.output;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.sidecar.cdc.Change;
/**
 * Null output for Cassandra PartitionUpdates.
 */
public class ConsoleOutput implements Output
{

    private static final Logger logger = LoggerFactory.getLogger(ConsoleOutput.class);

    @Inject
    ConsoleOutput()
    {
    }

    @Override
    public void emitChange(Change change)  throws Exception
    {
        if (change == null || change.getPartitionUpdateObject() == null)
        {
            return;
        }
        PartitionUpdate partition = change.getPartitionUpdateObject();
        logger.info("Handling a partition with the column family : {}", partition.metadata().name);
        String pkStr = partition.metadata().partitionKeyType.getString(partition.partitionKey()
                .getKey());
        logger.info("> Partition Key : {}", pkStr);

        if (partition.staticRow().columns().size() > 0)
        {
            logger.info("> -- Static columns : {} ", partition.staticRow().toString(partition.metadata(), false));
        }
        UnfilteredRowIterator ri = partition.unfilteredIterator();
        while (ri.hasNext())
        {
            Unfiltered r = ri.next();
            logger.info("> -- Row contents: {}", r.toString(partition.metadata(), false));
        }
    }

    @Override
    public void close()
    {
    }
}
