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

package org.apache.cassandra.sidecar.cdc;

import java.util.Comparator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.sidecar.ReplicationFactorSupplier;
import org.apache.cassandra.cdc.sidecar.SidecarCommitLogProvider;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.utils.FutureUtils;

/**
 * {@link ReplicationFactorSupplier} implementation that reads the actual replication factor
 * from Cassandra cluster metadata via {@link CdcOptions}, rather than using the default RF=1
 * SimpleStrategy fallback. Used by {@link SidecarCommitLogProvider} to build a correctly
 * replicated {@link org.apache.cassandra.spark.data.partitioner.CassandraRing}.
 */
public class SidecarReplicationFactorSupplier implements ReplicationFactorSupplier
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarReplicationFactorSupplier.class);
    private final CdcOptions cdcOptions;
    private final SchemaSupplier schemaSupplier;

    public SidecarReplicationFactorSupplier(CdcOptions cdcOptions, SchemaSupplier schemaSupplier)
    {
        this.cdcOptions = cdcOptions;
        this.schemaSupplier = schemaSupplier;
    }

    @Override
    public ReplicationFactor getReplicationFactor(String keyspace)
    {
        return cdcOptions.replicationFactor(keyspace);
    }

    @Override
    public ReplicationFactor getMaximalReplicationFactor()
    {
        String dc = cdcOptions.dc();
        Set<CqlTable> tables = FutureUtils.get(schemaSupplier.getCdcEnabledTables());
        return tables.stream()
                     .map(CqlTable::replicationFactor)
                     .filter(rf -> rf.getOptions().containsKey(dc))
                     .max(Comparator.comparingInt(rf -> rf.getOptions().get(dc)))
                     .orElseGet(() -> {
                         LOGGER.warn("No CDC-enabled tables found for DC '{}'; falling back to RF=3 SimpleStrategy", dc);
                         return ReplicationFactor.simpleStrategy(3);
                     });
    }
}
