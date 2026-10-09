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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.sidecar.utils.CdcUtil;
import org.apache.cassandra.spark.utils.TableIdentifier;
import org.jetbrains.annotations.NotNull;

/**
 * Decides which tables need to be registered in the CDC bridge's {@code Schema.instance} so
 * that commit log mutations can be deserialized without throwing {@code UnknownTableException}.
 *
 * <p>A {@code Mutation} read from the commit log is always scoped to one keyspace (Cassandra
 * splits a {@code BEGIN BATCH} statement spanning multiple keyspaces into one {@code Mutation}
 * per keyspace before writing to the commit log), and can reference multiple tables within that
 * keyspace at once when a batch writes to them under a shared partition key. Two tables can only
 * ever be co-located in the same {@code Mutation} this way if they share partition key
 * <em>structure</em> — the same ordered list of partition-key column types — since a batch
 * statement must supply matching partition key values across the statements it groups, and that
 * requires the same declared CQL type in most cases, but not always: some distinct types share
 * an identical fixed-width encoding (e.g. {@code bigint}/{@code timestamp}, both 8 bytes), which
 * {@link CdcUtil.PartitionKeySignature#structurallyMatches} also treats as matching (column
 * names are irrelevant to Cassandra's grouping either way).
 *
 * <p>This class registers every CDC-enabled table unconditionally (they must always be
 * registered, since we need to publish their events), plus any non-CDC table in the same
 * keyspace whose partition key structure matches a CDC-enabled table's — these are the only
 * non-CDC tables that could ever be silently dropped as collateral damage if a batch mixes them
 * with a CDC-enabled table. Non-CDC tables with no partition-key-structure overlap with any
 * CDC-enabled table in their keyspace are excluded entirely, saving the (otherwise wasted) cost
 * of fully deserializing their mutations only to discard them.
 *
 * <p>Ambiguous ({@link CdcUtil.PartitionKeySignature#indeterminate()}) signatures always match,
 * so a table we can't confidently analyze is included rather than silently excluded.
 */
public final class CdcBatchRiskAnalyzer
{
    private CdcBatchRiskAnalyzer()
    {
    }

    /**
     * @param userTables             every non-system user table in the cluster, with cdc flag
     *                               and partition-key signature (see
     *                               {@link CdcUtil#extractAllTablesWithCdcFlag})
     * @param batchStatementsEnabled if {@code false}, the operator has asserted that the
     *                               workload never issues cross-table batch statements; only
     *                               CDC-enabled tables are registered (cheapest option, no
     *                               partition-key analysis performed)
     * @return the subset of {@code userTables} that must be registered in the CDC bridge's
     * schema: every CDC-enabled table, plus (when {@code batchStatementsEnabled}) every non-CDC
     * table that shares partition-key structure with a CDC-enabled table in the same keyspace
     */
    @NotNull
    public static Map<TableIdentifier, CdcUtil.TableSchema> computeTablesToRegister(
        @NotNull Map<TableIdentifier, CdcUtil.TableSchema> userTables,
        boolean batchStatementsEnabled)
    {
        if (!batchStatementsEnabled)
        {
            return userTables.entrySet().stream()
                             .filter(e -> e.getValue().cdc)
                             .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        Map<String, List<Map.Entry<TableIdentifier, CdcUtil.TableSchema>>> byKeyspace =
            userTables.entrySet().stream()
                      .collect(Collectors.groupingBy(e -> e.getKey().keyspace()));

        Set<TableIdentifier> tablesToRegister = new HashSet<>();

        for (List<Map.Entry<TableIdentifier, CdcUtil.TableSchema>> tablesInKeyspace : byKeyspace.values())
        {
            List<Map.Entry<TableIdentifier, CdcUtil.TableSchema>> cdcTables = tablesInKeyspace.stream()
                                                                                              .filter(e -> e.getValue().cdc)
                                                                                              .collect(Collectors.toList());
            List<Map.Entry<TableIdentifier, CdcUtil.TableSchema>> nonCdcTables = tablesInKeyspace.stream()
                                                                                                 .filter(e -> !e.getValue().cdc)
                                                                                                 .collect(Collectors.toList());

            // CDC-enabled tables are always registered — we need to publish their events
            // regardless of any batch risk analysis.
            for (Map.Entry<TableIdentifier, CdcUtil.TableSchema> cdcTable : cdcTables)
            {
                tablesToRegister.add(cdcTable.getKey());
            }

            // Register every non-CDC table whose partition key structure matches ANY
            // CDC-enabled table in this keyspace — not just the first match found — since any
            // one of them could be the batch partner in a future mutation.
            for (Map.Entry<TableIdentifier, CdcUtil.TableSchema> cdcTable : cdcTables)
            {
                CdcUtil.PartitionKeySignature cdcSignature = cdcTable.getValue().partitionKeySignature;
                for (Map.Entry<TableIdentifier, CdcUtil.TableSchema> nonCdcTable : nonCdcTables)
                {
                    if (cdcSignature.structurallyMatches(nonCdcTable.getValue().partitionKeySignature))
                    {
                        tablesToRegister.add(nonCdcTable.getKey());
                    }
                }
            }
        }

        Map<TableIdentifier, CdcUtil.TableSchema> result = new HashMap<>();
        for (TableIdentifier id : tablesToRegister)
        {
            result.put(id, userTables.get(id));
        }
        return result;
    }
}
