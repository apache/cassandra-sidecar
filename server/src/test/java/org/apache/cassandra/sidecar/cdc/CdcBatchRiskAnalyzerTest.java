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
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.utils.CdcUtil;
import org.apache.cassandra.spark.utils.TableIdentifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link CdcBatchRiskAnalyzer}.
 */
class CdcBatchRiskAnalyzerTest
{
    private static TableIdentifier id(String keyspace, String table)
    {
        return TableIdentifier.of(keyspace, table);
    }

    private static CdcUtil.TableSchema schema(boolean cdc, String... pkTypes)
    {
        return new CdcUtil.TableSchema("irrelevant create statement", cdc,
                                       CdcUtil.PartitionKeySignature.of(List.of(pkTypes)));
    }

    private static CdcUtil.TableSchema indeterminateSchema(boolean cdc)
    {
        return new CdcUtil.TableSchema("irrelevant create statement", cdc,
                                       CdcUtil.PartitionKeySignature.indeterminate());
    }

    @Test
    void testBatchStatementsDisabledRegistersOnlyCdcTables()
    {
        Map<TableIdentifier, CdcUtil.TableSchema> userTables = new HashMap<>();
        userTables.put(id("ks1", "cdc_table"), schema(true, "uuid"));
        userTables.put(id("ks1", "non_cdc_table"), schema(false, "uuid"));

        Map<TableIdentifier, CdcUtil.TableSchema> result =
        CdcBatchRiskAnalyzer.computeTablesToRegister(userTables, false);

        assertThat(result).containsOnlyKeys(id("ks1", "cdc_table"));
    }

    @Test
    void testMatchingSingleColumnPartitionKeyIncludesNonCdcTable()
    {
        Map<TableIdentifier, CdcUtil.TableSchema> userTables = new HashMap<>();
        userTables.put(id("ks1", "cdc_table"), schema(true, "uuid"));
        userTables.put(id("ks1", "non_cdc_table"), schema(false, "uuid"));

        Map<TableIdentifier, CdcUtil.TableSchema> result =
        CdcBatchRiskAnalyzer.computeTablesToRegister(userTables, true);

        assertThat(result).containsOnlyKeys(id("ks1", "cdc_table"), id("ks1", "non_cdc_table"));
    }

    @Test
    void testMismatchedSingleColumnPartitionKeyExcludesNonCdcTable()
    {
        Map<TableIdentifier, CdcUtil.TableSchema> userTables = new HashMap<>();
        userTables.put(id("ks1", "cdc_table"), schema(true, "uuid"));
        userTables.put(id("ks1", "non_cdc_table"), schema(false, "text"));

        Map<TableIdentifier, CdcUtil.TableSchema> result =
        CdcBatchRiskAnalyzer.computeTablesToRegister(userTables, true);

        assertThat(result).containsOnlyKeys(id("ks1", "cdc_table"));
    }

    @Test
    void testMatchingCompositePartitionKeyIncludesNonCdcTable()
    {
        Map<TableIdentifier, CdcUtil.TableSchema> userTables = new HashMap<>();
        userTables.put(id("ks1", "cdc_table"), schema(true, "int", "text"));
        userTables.put(id("ks1", "non_cdc_table"), schema(false, "int", "text"));

        Map<TableIdentifier, CdcUtil.TableSchema> result =
        CdcBatchRiskAnalyzer.computeTablesToRegister(userTables, true);

        assertThat(result).containsOnlyKeys(id("ks1", "cdc_table"), id("ks1", "non_cdc_table"));
    }

    @Test
    void testMismatchedCompositePartitionKeyExcludesNonCdcTable()
    {
        Map<TableIdentifier, CdcUtil.TableSchema> userTables = new HashMap<>();
        userTables.put(id("ks1", "cdc_table"), schema(true, "int", "text"));
        // same number of columns, different types — must not match
        userTables.put(id("ks1", "non_cdc_table"), schema(false, "int", "int"));

        Map<TableIdentifier, CdcUtil.TableSchema> result =
        CdcBatchRiskAnalyzer.computeTablesToRegister(userTables, true);

        assertThat(result).containsOnlyKeys(id("ks1", "cdc_table"));
    }

    @Test
    void testFrozenAndUdtPartitionKeyTypesMatchAsOpaqueTokens()
    {
        Map<TableIdentifier, CdcUtil.TableSchema> userTables = new HashMap<>();
        userTables.put(id("ks1", "cdc_table"), schema(true, "frozen<my_udt>"));
        userTables.put(id("ks1", "non_cdc_table"), schema(false, "frozen<my_udt>"));
        userTables.put(id("ks1", "other_udt_table"), schema(false, "frozen<other_udt>"));

        Map<TableIdentifier, CdcUtil.TableSchema> result =
        CdcBatchRiskAnalyzer.computeTablesToRegister(userTables, true);

        assertThat(result).containsOnlyKeys(id("ks1", "cdc_table"), id("ks1", "non_cdc_table"));
    }

    @Test
    void testIndeterminateSignatureIsIncludedAsFailSafe()
    {
        Map<TableIdentifier, CdcUtil.TableSchema> userTables = new HashMap<>();
        userTables.put(id("ks1", "cdc_table"), schema(true, "uuid"));
        userTables.put(id("ks1", "unparseable_table"), indeterminateSchema(false));

        Map<TableIdentifier, CdcUtil.TableSchema> result =
        CdcBatchRiskAnalyzer.computeTablesToRegister(userTables, true);

        assertThat(result).containsOnlyKeys(id("ks1", "cdc_table"), id("ks1", "unparseable_table"));
    }

    @Test
    void testAnyAtRiskPairPullsInAllStructurallyMatchingTables()
    {
        Map<TableIdentifier, CdcUtil.TableSchema> userTables = new HashMap<>();
        userTables.put(id("ks1", "cdc_table"), schema(true, "uuid"));
        userTables.put(id("ks1", "match1"), schema(false, "uuid"));
        userTables.put(id("ks1", "match2"), schema(false, "uuid"));
        userTables.put(id("ks1", "no_match"), schema(false, "text"));

        Map<TableIdentifier, CdcUtil.TableSchema> result =
        CdcBatchRiskAnalyzer.computeTablesToRegister(userTables, true);

        assertThat(result).containsOnlyKeys(id("ks1", "cdc_table"), id("ks1", "match1"), id("ks1", "match2"));
    }

    @Test
    void testKeyspaceWithNoCdcTablesRegistersNothing()
    {
        Map<TableIdentifier, CdcUtil.TableSchema> userTables = new HashMap<>();
        userTables.put(id("ks1", "t1"), schema(false, "uuid"));
        userTables.put(id("ks1", "t2"), schema(false, "uuid"));

        Map<TableIdentifier, CdcUtil.TableSchema> result =
        CdcBatchRiskAnalyzer.computeTablesToRegister(userTables, true);

        assertThat(result).isEmpty();
    }

    @Test
    void testKeyspaceWithAllCdcTablesRegistersAllOfThem()
    {
        Map<TableIdentifier, CdcUtil.TableSchema> userTables = new HashMap<>();
        userTables.put(id("ks1", "t1"), schema(true, "uuid"));
        userTables.put(id("ks1", "t2"), schema(true, "text"));

        Map<TableIdentifier, CdcUtil.TableSchema> result =
        CdcBatchRiskAnalyzer.computeTablesToRegister(userTables, true);

        assertThat(result).containsOnlyKeys(id("ks1", "t1"), id("ks1", "t2"));
    }

    @Test
    void testDoesNotCrossKeyspaceBoundaries()
    {
        Map<TableIdentifier, CdcUtil.TableSchema> userTables = new HashMap<>();
        userTables.put(id("ks1", "cdc_table"), schema(true, "uuid"));
        // same partition key structure, but a DIFFERENT keyspace — must never be pulled in,
        // since a Mutation is always single-keyspace-scoped
        userTables.put(id("ks2", "non_cdc_table"), schema(false, "uuid"));

        Map<TableIdentifier, CdcUtil.TableSchema> result =
        CdcBatchRiskAnalyzer.computeTablesToRegister(userTables, true);

        assertThat(result).containsOnlyKeys(id("ks1", "cdc_table"));
    }

    @Test
    void testEmptyInputRegistersNothing()
    {
        Map<TableIdentifier, CdcUtil.TableSchema> result =
        CdcBatchRiskAnalyzer.computeTablesToRegister(Map.of(), true);

        assertThat(result).isEmpty();
    }

    @Test
    void testNonCdcTableMatchingMultipleCdcTablesIsRegisteredOnce()
    {
        // A single non-CDC table whose PK structure matches THREE different CDC tables in the
        // same keyspace must still be registered exactly once — this exercises the dedup
        // (Set-based) accumulation across multiple cdcTable x nonCdcTable matching pairs.
        Map<TableIdentifier, CdcUtil.TableSchema> userTables = new HashMap<>();
        userTables.put(id("ks1", "cdc_table1"), schema(true, "uuid"));
        userTables.put(id("ks1", "cdc_table2"), schema(true, "uuid"));
        userTables.put(id("ks1", "cdc_table3"), schema(true, "uuid"));
        userTables.put(id("ks1", "non_cdc_table"), schema(false, "uuid"));

        Map<TableIdentifier, CdcUtil.TableSchema> result =
        CdcBatchRiskAnalyzer.computeTablesToRegister(userTables, true);

        assertThat(result).containsOnlyKeys(id("ks1", "cdc_table1"), id("ks1", "cdc_table2"),
                                            id("ks1", "cdc_table3"), id("ks1", "non_cdc_table"));
    }

    @Test
    void testAllIndeterminateSignaturesIncludesAllTablesAsFailSafe()
    {
        // When every table in a keyspace (CDC and non-CDC alike) has an unparseable/ambiguous
        // signature, the fail-safe must still hold: nothing is silently excluded.
        Map<TableIdentifier, CdcUtil.TableSchema> userTables = new HashMap<>();
        userTables.put(id("ks1", "cdc_table"), indeterminateSchema(true));
        userTables.put(id("ks1", "non_cdc_table1"), indeterminateSchema(false));
        userTables.put(id("ks1", "non_cdc_table2"), indeterminateSchema(false));

        Map<TableIdentifier, CdcUtil.TableSchema> result =
        CdcBatchRiskAnalyzer.computeTablesToRegister(userTables, true);

        assertThat(result).containsOnlyKeys(id("ks1", "cdc_table"), id("ks1", "non_cdc_table1"),
                                            id("ks1", "non_cdc_table2"));
    }

    @Test
    void testBigintAndTimestampShareByteEncodingIncludesNonCdcTable()
    {
        // bigint and timestamp both serialize as an 8-byte big-endian long — a bigint value of 1
        // and a timestamp one millisecond after the epoch are byte-identical on the wire, so
        // Cassandra can co-locate them in the same batch Mutation even though the declared types
        // differ.
        Map<TableIdentifier, CdcUtil.TableSchema> userTables = new HashMap<>();
        userTables.put(id("ks1", "cdc_table"), schema(true, "bigint"));
        userTables.put(id("ks1", "non_cdc_table"), schema(false, "timestamp"));

        Map<TableIdentifier, CdcUtil.TableSchema> result =
        CdcBatchRiskAnalyzer.computeTablesToRegister(userTables, true);

        assertThat(result).containsOnlyKeys(id("ks1", "cdc_table"), id("ks1", "non_cdc_table"));
    }

    @Test
    void testDifferentFixedWidthTypesStillExcludeNonCdcTable()
    {
        // bigint (8 bytes) and int (4 bytes) can never share bytes at the same position — the
        // byte-width relaxation must not become a blanket "any numeric type matches" rule.
        Map<TableIdentifier, CdcUtil.TableSchema> userTables = new HashMap<>();
        userTables.put(id("ks1", "cdc_table"), schema(true, "bigint"));
        userTables.put(id("ks1", "non_cdc_table"), schema(false, "int"));

        Map<TableIdentifier, CdcUtil.TableSchema> result =
        CdcBatchRiskAnalyzer.computeTablesToRegister(userTables, true);

        assertThat(result).containsOnlyKeys(id("ks1", "cdc_table"));
    }

    @Test
    void testVariableLengthTypeStillRequiresExactNameMatch()
    {
        // blob has no fixed, type-bounded byte width, so it must fall back to exact-name
        // equality rather than being treated as an automatic match against everything (which
        // would defeat the point of the risk analysis for the common case of blob/text columns).
        Map<TableIdentifier, CdcUtil.TableSchema> userTables = new HashMap<>();
        userTables.put(id("ks1", "cdc_table"), schema(true, "bigint"));
        userTables.put(id("ks1", "non_cdc_table"), schema(false, "blob"));

        Map<TableIdentifier, CdcUtil.TableSchema> result =
        CdcBatchRiskAnalyzer.computeTablesToRegister(userTables, true);

        assertThat(result).containsOnlyKeys(id("ks1", "cdc_table"));
    }
}
