/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
