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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.sidecar.utils.CdcUtil;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.CqlUtils;
import org.apache.cassandra.spark.utils.TableIdentifier;
import org.jetbrains.annotations.NotNull;

/**
 * Shared utilities for building {@link CqlTable} sets used by CDC schema loading.
 *
 * <p>Both {@link CdcSchemaSupplier} and
 * {@link org.apache.cassandra.sidecar.tasks.CassandraClusterSchemaMonitor} need to build
 * the set of user tables (CDC-enabled and, when batch statements are possible, tables that
 * share partition-key structure with a CDC-enabled table) so that the bridge's
 * {@code Schema.instance} is complete enough to deserialize commit log mutations without
 * throwing {@code UnknownTableException} when a {@code BEGIN BATCH} co-locates CDC-enabled and
 * CDC-disabled tables under the same partition key. See {@link CdcBatchRiskAnalyzer} for how
 * the final table set is decided.
 */
public final class CdcSchemaUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcSchemaUtils.class);

    private CdcSchemaUtils()
    {
    }

    /**
     * Builds {@link CqlTable} objects for the user tables that need to be registered in the
     * CDC bridge's schema. See {@link CdcBatchRiskAnalyzer} for how that set is decided.
     *
     * <p>System keyspaces (all starting with {@code "system"}) are excluded because the
     * bridge's internal {@code Schema.instance} cannot handle them, and Cassandra does not
     * allow batch writes mixing system and user keyspaces.
     *
     * <p>Each returned {@link CqlTable} has {@link CqlTable#cdc()} set correctly from the
     * CREATE TABLE statement, so callers can filter by {@code cdc()} to determine what to
     * publish to Kafka without any additional schema queries.
     *
     * @param fullSchema             the full cluster schema string from {@code exportSchemaAsString()}
     * @param partitioner            the cluster partitioner
     * @param tableIdCache           cache of already-resolved table UUIDs
     * @param tableIdLoader          function to resolve a table UUID from the driver metadata
     * @param cassandraBridge        the Cassandra bridge for building {@link CqlTable} objects
     * @param batchStatementsEnabled whether the workload may issue cross-table batch
     *                               statements; see {@link CdcBatchRiskAnalyzer}
     * @return set of user tables that need to be registered, with correct CDC flags
     */
    public static Set<CqlTable> buildTablesToRegisterForCDC(@NotNull String fullSchema,
                                                            @NotNull Partitioner partitioner,
                                                            @NotNull ConcurrentHashMap<TableIdentifier, UUID> tableIdCache,
                                                            @NotNull Function<TableIdentifier, UUID> tableIdLoader,
                                                            @NotNull CassandraBridge cassandraBridge,
                                                            boolean batchStatementsEnabled)
    {
        Map<TableIdentifier, CdcUtil.TableSchema> allTables = CdcUtil.extractAllTablesWithCdcFlag(fullSchema);

        // Filter out system keyspaces and sidecar_internal.
        // Cassandra naming convention requires all system keyspaces to start with "system".
        // sidecar_internal is an internal Cassandra Sidecar keyspace that should not be
        // registered in the bridge's Schema.instance.
        Map<TableIdentifier, CdcUtil.TableSchema> userTables = allTables.entrySet().stream()
                                                                        .filter(e -> !e.getKey().keyspace().startsWith("system")
                                                                                  && !e.getKey().keyspace().equals("sidecar_internal"))
                                                                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        userTables = CdcBatchRiskAnalyzer.computeTablesToRegister(userTables, batchStatementsEnabled);

        Map<String, Set<String>> udtsPerKeyspace = userTables.keySet().stream()
                                                             .map(TableIdentifier::keyspace)
                                                             .distinct()
                                                             .collect(Collectors.toMap(Function.identity(),
                                                                                       keyspace -> CqlUtils.extractUdts(fullSchema, keyspace)));

        // Resolve table UUIDs from driver metadata, skipping any that cannot be resolved
        // (e.g. tables that appear in exportSchemaAsString but are unknown to the driver).
        Map<TableIdentifier, UUID> tableIds = new HashMap<>();
        for (TableIdentifier id : userTables.keySet())
        {
            // computeIfAbsent makes the cache lookup-and-populate atomic, so concurrent callers
            // resolving the same uncached id cannot race and invoke tableIdLoader twice.
            UUID uuid = tableIdCache.computeIfAbsent(id, key -> {
                try
                {
                    UUID resolved = tableIdLoader.apply(key);
                    if (resolved == null)
                    {
                        LOGGER.debug("Skipping table with null UUID keyspace={} table={}", key.keyspace(), key.table());
                    }
                    return resolved;
                }
                catch (Exception e)
                {
                    LOGGER.debug("Skipping table not found in driver metadata keyspace={} table={} error={}",
                                 key.keyspace(), key.table(), e.getMessage());
                    return null;
                }
            });
            if (uuid != null)
            {
                tableIds.put(id, uuid);
            }
        }

        return userTables.entrySet().stream()
                         .filter(e -> tableIds.containsKey(e.getKey()))
                         .map(e -> {
                             TableIdentifier id = e.getKey();
                             CdcUtil.TableSchema tableSchema = e.getValue();
                             ReplicationFactor rf = CqlUtils.extractReplicationFactor(fullSchema, id.keyspace());
                             // tableSchema.cdc becomes CqlTable.cdc() — callers filter by it
                             return cassandraBridge.buildSchema(tableSchema.createStatement, id.keyspace(), rf,
                                                                partitioner, udtsPerKeyspace.get(id.keyspace()),
                                                                tableIds.get(id), 0, tableSchema.cdc);
                         })
                         .collect(Collectors.toSet());
    }
}
