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

import org.apache.cassandra.bridge.CassandraBridge;
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
 * Supplies schema information for tables in a Cassandra cluster, for CDC (Change Data Capture) processing.
 *
 * <p>This class is responsible for:
 * <ul>
 *   <li>Retrieving and parsing the complete schema from Cassandra instances</li>
 *   <li>Building {@link CqlTable} representations, with each table's CDC-enabled flag, UDTs,
 *       replication factor, and partitioner</li>
 *   <li>Caching table IDs to optimize repeated lookups</li>
 * </ul>
 *
 * <p>{@link #getTables()} returns every table; the CDC-enabled subset
 * that callers actually care about is obtained via {@link SchemaSupplier#getCDCEnabledTables()}.
 *
 * @see SchemaSupplier
 * @see CqlTable
 * @see CdcUtil
 * @see CassandraBridge
 */
public class CdcSchemaSupplier implements SchemaSupplier
{
    private final InstanceMetadataFetcher instanceMetadataFetcher;
    private final CassandraBridgeFactory cassandraBridgeFactory;
    private final CdcDatabaseAccessor cdcDatabaseAccessor;
    private final ConcurrentHashMap<TableIdentifier, UUID> tableIdCache = new ConcurrentHashMap<>();

    public CdcSchemaSupplier(InstanceMetadataFetcher instanceMetadataFetcher,
                             CassandraBridgeFactory cassandraBridgeFactory,
                             CdcDatabaseAccessor cdcDatabaseAccessor)
    {
        this.instanceMetadataFetcher = instanceMetadataFetcher;
        this.cassandraBridgeFactory = cassandraBridgeFactory;
        this.cdcDatabaseAccessor = cdcDatabaseAccessor;
    }

    /**
     * The CDC library uses this to keep its bridge schema complete, so that deserializing a commit log
     * mutation for a CDC-disabled table co-located with CDC-enabled tables never throws
     * {@code UnknownTableException}. Callers that only want CDC-enabled tables should use
     * {@link SchemaSupplier#getCDCEnabledTables()} instead of filtering this result themselves.
     */
    @Override
    public CompletableFuture<Set<CqlTable>> getTables()
    {
        String schema = instanceMetadataFetcher.callOnFirstAvailableInstance(instance-> instance.delegate().metadata().exportSchemaAsString());
        NodeSettings nodeSettings = instanceMetadataFetcher.callOnFirstAvailableInstance(instance-> instance.delegate().nodeSettings());
        CassandraBridge cassandraBridge = cassandraBridgeFactory.get(nodeSettings.releaseVersion());

        Set<CqlTable> cqlTables = buildTables(schema,
                                              cdcDatabaseAccessor.partitioner(),
                                              tableIdCache,
                                              cdcDatabaseAccessor::getTableId,
                                              cassandraBridge);
        return CompletableFuture.completedFuture(cqlTables);
    }

    private static Set<CqlTable> buildTables(@NotNull String fullSchema,
                                              @NotNull Partitioner partitioner,
                                              @NotNull ConcurrentHashMap<TableIdentifier, UUID> tableIdCache,
                                              @NotNull Function<TableIdentifier, UUID> tableIdLoaderFunction,
                                              @NotNull CassandraBridge cassandraBridge)
    {
        Map<TableIdentifier, String> createStmts = CdcUtil.extractAllTables(fullSchema);
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
                                   String createStmt = e.getValue();
                                   ReplicationFactor rf = CqlUtils.extractReplicationFactor(fullSchema, id.keyspace());

                                   return cassandraBridge.buildSchema(createStmt, id.keyspace(), rf,
                                                                      partitioner, udtsPerKeyspace.get(id.keyspace()),
                                                                      tableIds.get(id), 0, CdcUtil.isCdcEnabled(createStmt));
                               })
                          .collect(Collectors.toSet());
    }
}
