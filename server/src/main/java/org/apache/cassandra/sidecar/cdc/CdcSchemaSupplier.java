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

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.inject.Singleton;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.sidecar.bridge.CassandraBridgeFactory;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.utils.CdcUtil;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.CqlUtils;
import org.apache.cassandra.spark.utils.TableIdentifier;
import org.jetbrains.annotations.NotNull;


/**
 * Supplies schema information for CDC (Change Data Capture) enabled tables in a Cassandra cluster.
 *
 * <p>This class is responsible for:
 * <ul>
 *   <li>Retrieving and parsing the complete schema from Cassandra instances</li>
 *   <li>Identifying tables that have CDC enabled</li>
 *   <li>Building {@link CqlTable} representations with appropriate metadata</li>
 *   <li>Caching table IDs to optimize repeated lookups</li>
 *   <li>Extracting User Defined Types (UDTs) and replication factors</li>
 *   <li>Updating the CDC bridge with schema information</li>
 * </ul>
 *
 * <p>The schema supplier works by:
 * <ol>
 *   <li>Fetching the full schema export from any available Cassandra instance</li>
 *   <li>Parsing CREATE TABLE statements to identify CDC-enabled tables</li>
 *   <li>Extracting keyspace-specific UDTs and replication settings</li>
 *   <li>Building complete {@link CqlTable} objects with partitioner information</li>
 *   <li>Coordinating with the CDC bridge to maintain schema consistency</li>
 * </ol>
 *
 * @see SchemaSupplier
 * @see CqlTable
 * @see CdcUtil
 * @see CassandraBridge
 */
@Singleton
public class CdcSchemaSupplier implements SchemaSupplier
{
    private final InstanceMetadataFetcher instanceMetadataFetcher;
    private final CassandraBridgeFactory cassandraBridgeFactory;
    private final CdcDatabaseAccessor cdcDatabaseAccessor;
    private final ConcurrentHashMap<TableIdentifier, UUID> tableIdCache = new ConcurrentHashMap<>();

    public CdcSchemaSupplier(InstanceMetadataFetcher instanceMetadataFetcher, CassandraBridgeFactory cassandraBridgeFactory, CdcDatabaseAccessor cdcDatabaseAccessor)
    {
        this.instanceMetadataFetcher = instanceMetadataFetcher;
        this.cassandraBridgeFactory = cassandraBridgeFactory;
        this.cdcDatabaseAccessor = cdcDatabaseAccessor;
    }

    public CompletableFuture<Set<CqlTable>> getCdcEnabledTables()
    {
        String schema = instanceMetadataFetcher.callOnFirstAvailableInstance(instance-> instance.delegate().metadata().exportSchemaAsString());
        NodeSettings nodeSettings = instanceMetadataFetcher.callOnFirstAvailableInstance(instance-> instance.delegate().nodeSettings());
        CassandraBridge cassandraBridge = cassandraBridgeFactory.get(nodeSettings.releaseVersion());
        CdcBridge cdcBridge = CdcBridgeFactory.getCdcBridge(cassandraBridge);

        Set<CqlTable> cqlTables = buildCdcTables(schema,
                                                 cdcDatabaseAccessor.partitioner(),
                                                 tableIdCache,
                                                 cdcDatabaseAccessor::getTableId,
                                                 cassandraBridge);
        cdcBridge.updateCdcSchema(cqlTables, getPartitioner(nodeSettings),
                                  ((keyspace, table) -> tableIdCache.get(TableIdentifier.of(keyspace, table))));
        return CompletableFuture.completedFuture(cqlTables);
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

    private static Set<CqlTable> buildCdcTables(@NotNull String fullSchema,
                                                @NotNull Partitioner partitioner,
                                                @NotNull ConcurrentHashMap<TableIdentifier, UUID> tableIdCache,
                                                @NotNull Function<TableIdentifier, UUID> tableIdLoaderFunction,
                                                @NotNull CassandraBridge cassandraBridge)
    {
        Map<TableIdentifier, String> createStmts = CdcUtil.extractCdcTables(fullSchema);
        Map<String, Set<String>> udtsPerKeyspace = createStmts.keySet()
                                                              .stream()
                                                              .map(TableIdentifier::keyspace)
                                                              .distinct() // remove duplicated keyspace strings
                                                              .collect(Collectors.toMap(Function.identity(),
                                                                                        keyspace -> CqlUtils.extractUdts(fullSchema, keyspace)));

        Map<TableIdentifier, UUID> tableIds = createStmts.keySet()
                                                         .stream()
                                                         .collect(Collectors.toMap(Function.identity(),
                                                                                   id -> tableIdCache.computeIfAbsent(id, tableIdLoaderFunction)));

        return createStmts.entrySet().stream()
                          .map(e ->
                               {
                                   TableIdentifier id = e.getKey();
                                   ReplicationFactor rf = CqlUtils.extractReplicationFactor(fullSchema, id.keyspace());

                                   return cassandraBridge.buildSchema(e.getValue(), id.keyspace(), rf,
                                                                      partitioner, udtsPerKeyspace.get(id.keyspace()),
                                                                      tableIds.get(id), 0, true);
                               })
                          .collect(Collectors.toSet());
    }
}
