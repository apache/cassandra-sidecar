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

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.sidecar.bridge.CassandraBridgeFactory;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.TableIdentifier;


/**
 * Supplies schema information for CDC processing.
 *
 * <p>Returns the set of user tables that need to be registered so that the bridge's
 * {@code Schema.instance} is complete enough to deserialize commit log mutations without
 * throwing {@code UnknownTableException}. This is necessary because a {@code BEGIN BATCH}
 * statement can co-locate writes to CDC-enabled and CDC-disabled tables under the same
 * partition key, producing a single {@code Mutation} in the commit log that references all
 * those tables. See {@link CdcBatchRiskAnalyzer} for how that set is decided.
 *
 * <p>Each returned {@link CqlTable} carries the correct CDC flag via {@link CqlTable#cdc()}.
 * Callers use this flag to decide which tables' events to publish to Kafka.
 */
public class CdcSchemaSupplier implements SchemaSupplier
{
    private final InstanceMetadataFetcher instanceMetadataFetcher;
    private final CassandraBridgeFactory cassandraBridgeFactory;
    private final CdcDatabaseAccessor cdcDatabaseAccessor;
    private final SidecarConfiguration sidecarConfiguration;
    private final ConcurrentHashMap<TableIdentifier, UUID> tableIdCache = new ConcurrentHashMap<>();

    public CdcSchemaSupplier(InstanceMetadataFetcher instanceMetadataFetcher,
                             CassandraBridgeFactory cassandraBridgeFactory,
                             CdcDatabaseAccessor cdcDatabaseAccessor,
                             SidecarConfiguration sidecarConfiguration)
    {
        this.instanceMetadataFetcher = instanceMetadataFetcher;
        this.cassandraBridgeFactory = cassandraBridgeFactory;
        this.cdcDatabaseAccessor = cdcDatabaseAccessor;
        this.sidecarConfiguration = sidecarConfiguration;
    }

    /**
     * Returns the set of user tables that need to be registered; see {@link CdcBatchRiskAnalyzer}.
     * Each {@link CqlTable} has {@link CqlTable#cdc()} set correctly from the CREATE TABLE
     * statement. Callers filter by {@code cdc()} to determine what to publish to Kafka.
     */
    @Override
    public CompletableFuture<Set<CqlTable>> getTables()
    {
        String schema = instanceMetadataFetcher.callOnFirstAvailableInstance(
                instance -> instance.delegate().metadata().exportSchemaAsString());
        NodeSettings nodeSettings = instanceMetadataFetcher.callOnFirstAvailableInstance(
                instance -> instance.delegate().nodeSettings());
        CassandraBridge cassandraBridge = cassandraBridgeFactory.get(nodeSettings.releaseVersion());
        Partitioner partitioner = getPartitioner(nodeSettings);
        boolean batchStatementsEnabled = sidecarConfiguration.serviceConfiguration().cdcConfiguration().batchStatementsEnabled();

        Set<CqlTable> allTables = CdcSchemaUtils.buildTablesToRegisterForCDC(schema, partitioner, tableIdCache,
                                                                     cdcDatabaseAccessor::getTableId,
                                                                     cassandraBridge, batchStatementsEnabled);
        return CompletableFuture.completedFuture(allTables);
    }

    private Partitioner getPartitioner(NodeSettings nodeSettings)
    {
        if (nodeSettings.partitioner().contains("."))
        {
            String[] splitPartitionerName = nodeSettings.partitioner().split("\\.");
            return Partitioner.valueOf(splitPartitionerName[splitPartitionerName.length - 1]);
        }
        return Partitioner.valueOf(nodeSettings.partitioner());
    }
}
