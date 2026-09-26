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

package org.apache.cassandra.sidecar.utils;

import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.utils.TableIdentifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the {@link CdcUtil}
 */
public class CdcUtilTest
{
    @Test
    public void testMatcher()
    {
        assertTrue(CdcUtil.SEGMENT_PATTERN.matcher("CommitLog-7-1689642717704.log").matches());
        assertTrue(CdcUtil.SEGMENT_PATTERN.matcher("CommitLog-12345.log").matches());
        assertTrue(CdcUtil.SEGMENT_PATTERN.matcher("CommitLog-2-1340512736956320000.log").matches());
        assertTrue(CdcUtil.SEGMENT_PATTERN.matcher("CommitLog-2-1340512736959990000.log").matches());
        assertTrue(CdcUtil.isValid("CommitLog-7-1689642717704.log"));
        assertTrue(CdcUtil.isValid("CommitLog-12345.log"));
        assertTrue(CdcUtil.isValid("CommitLog-2-1340512736956320000.log"));
        assertTrue(CdcUtil.isValid("CommitLog-2-1340512736959990000.log"));
        assertTrue(CdcUtil.isLogFile("CommitLog-7-1689642717704.log"));
        assertTrue(CdcUtil.isLogFile("CommitLog-12345.log"));
        assertTrue(CdcUtil.isLogFile("CommitLog-2-1340512736956320000.log"));
        assertTrue(CdcUtil.isLogFile("CommitLog-2-1340512736959990000.log"));
        assertTrue(CdcUtil.IDX_FILE_PATTERN.matcher("CommitLog-7-1689642717704_cdc.idx").matches());
        assertTrue(CdcUtil.IDX_FILE_PATTERN.matcher("CommitLog-12345_cdc.idx").matches());
        assertTrue(CdcUtil.IDX_FILE_PATTERN.matcher("CommitLog-2-1240512736956320000_cdc.idx").matches());
        assertTrue(CdcUtil.IDX_FILE_PATTERN.matcher("CommitLog-2-1340512736956320000_cdc.idx").matches());

        assertFalse(CdcUtil.isValid("CommitLog-abc.log"));
        assertFalse(CdcUtil.isValid("abc-7-1689642717704.log"));
        assertFalse(CdcUtil.isValid("abc-1689642717704.log"));
        assertFalse(CdcUtil.isLogFile("CommitLog-abc.log"));
        assertFalse(CdcUtil.isLogFile("abc-7-1689642717704.log"));
        assertFalse(CdcUtil.isLogFile("abc-1689642717704.log"));
        assertFalse(CdcUtil.isLogFile("CommitLog-7-1689642717704"));
        assertFalse(CdcUtil.isLogFile("CommitLog-12345"));
        assertFalse(CdcUtil.isLogFile("CommitLog-2-1340512736956320000"));
        assertFalse(CdcUtil.isLogFile("CommitLog-2-1340512736959990000"));
    }

    @Test
    public void testExtractSegmentIdMatcher()
    {
        assertEquals(12345L, CdcUtil.parseSegmentId("CommitLog-12345.log"));
        assertEquals(1689642717704L, CdcUtil.parseSegmentId("CommitLog-7-1689642717704.log"));
        assertEquals(1340512736956320000L, CdcUtil.parseSegmentId("CommitLog-2-1340512736956320000.log"));
        assertEquals(1340512736959990000L, CdcUtil.parseSegmentId("CommitLog-2-1340512736959990000.log"));
        assertEquals(12345L, CdcUtil.parseSegmentId("CommitLog-6-12345.log"));
        assertEquals(1646094405659L, CdcUtil.parseSegmentId("CommitLog-7-1646094405659.log"));
        assertEquals(1646094405659L, CdcUtil.parseSegmentId("CommitLog-1646094405659.log"));
    }

    @Test
    public void testIdxToLogFileName()
    {
        assertEquals("CommitLog-7-1689642717704.log", CdcUtil.idxToLogFileName("CommitLog-7-1689642717704_cdc.idx"));
        assertEquals("CommitLog-12345.log", CdcUtil.idxToLogFileName("CommitLog-12345_cdc.idx"));
        assertEquals("CommitLog-2-1240512736956320000.log", CdcUtil.idxToLogFileName("CommitLog-2-1240512736956320000_cdc.idx"));
        assertEquals("CommitLog-2-1340512736956320000.log", CdcUtil.idxToLogFileName("CommitLog-2-1340512736956320000_cdc.idx"));
    }

    @Test
    public void testExtractAllTablesWithCdcFlag()
    {
        // Single CDC-enabled table
        String singleTableSchema = "CREATE TABLE test_ks.cdc_table (id uuid PRIMARY KEY, data text) WITH cdc = true;";
        Map<TableIdentifier, CdcUtil.TableSchema> result = CdcUtil.extractAllTablesWithCdcFlag(singleTableSchema);
        assertEquals(1, result.size());
        TableIdentifier cdcTableId = TableIdentifier.of("test_ks", "cdc_table");
        assertTrue(result.containsKey(cdcTableId));
        assertTrue(result.get(cdcTableId).cdc);

        // Single CDC-disabled table (explicit cdc = false) — this method must still return the
        // table, just with cdc=false, since the whole point of this method is to return every
        // table so Schema.instance can be built completely.
        String singleNonCdcSchema = "CREATE TABLE test_ks.regular_table (id uuid PRIMARY KEY, data text) WITH cdc = false;";
        result = CdcUtil.extractAllTablesWithCdcFlag(singleNonCdcSchema);
        assertEquals(1, result.size());
        TableIdentifier regularTableId = TableIdentifier.of("test_ks", "regular_table");
        assertTrue(result.containsKey(regularTableId));
        assertFalse(result.get(regularTableId).cdc);

        // Table with no cdc option at all — must also be returned, with cdc=false.
        String noCdcOptionSchema = "CREATE TABLE test_ks.plain_table (id uuid PRIMARY KEY, data text);";
        result = CdcUtil.extractAllTablesWithCdcFlag(noCdcOptionSchema);
        assertEquals(1, result.size());
        TableIdentifier plainTableId = TableIdentifier.of("test_ks", "plain_table");
        assertTrue(result.containsKey(plainTableId));
        assertFalse(result.get(plainTableId).cdc);

        // Mixed CDC/non-CDC tables in the SAME keyspace — this is the exact schema shape
        // that BEGIN BATCH mixing CDC-enabled and CDC-disabled tables produces a single
        // Mutation for. Both tables must come back, with correct individual cdc flags.
        String mixedSameKeyspaceSchema =
            "CREATE TABLE shared_ks.cdc_table (id uuid PRIMARY KEY, data text) WITH cdc = true;" +
            "CREATE TABLE shared_ks.non_cdc_table (id uuid PRIMARY KEY, data text) WITH cdc = false;";
        result = CdcUtil.extractAllTablesWithCdcFlag(mixedSameKeyspaceSchema);
        assertEquals(2, result.size());
        TableIdentifier sharedCdcId = TableIdentifier.of("shared_ks", "cdc_table");
        TableIdentifier sharedNonCdcId = TableIdentifier.of("shared_ks", "non_cdc_table");
        assertTrue(result.get(sharedCdcId).cdc);
        assertFalse(result.get(sharedNonCdcId).cdc);

        // Mixed CDC/non-CDC tables across DIFFERENT keyspaces — every table across every
        // keyspace must be returned with its own correct flag.
        String mixedAcrossKeyspacesSchema =
            "CREATE TABLE ks1.table1 (id uuid PRIMARY KEY, data text) WITH cdc = true;" +
            "CREATE TABLE ks2.table2 (id uuid PRIMARY KEY, value int) WITH cdc = false;" +
            "CREATE TABLE ks3.table3 (id uuid PRIMARY KEY, info text);";
        result = CdcUtil.extractAllTablesWithCdcFlag(mixedAcrossKeyspacesSchema);
        assertEquals(3, result.size());
        assertTrue(result.get(TableIdentifier.of("ks1", "table1")).cdc);
        assertFalse(result.get(TableIdentifier.of("ks2", "table2")).cdc);
        assertFalse(result.get(TableIdentifier.of("ks3", "table3")).cdc);

        // Quoted keyspace and table names
        String quotedNamesSchema =
            "CREATE TABLE \"my_keyspace\".\"my_table\" (id uuid PRIMARY KEY, data text) WITH cdc = true;";
        result = CdcUtil.extractAllTablesWithCdcFlag(quotedNamesSchema);
        assertEquals(1, result.size());
        TableIdentifier quotedId = TableIdentifier.of("my_keyspace", "my_table");
        assertTrue(result.containsKey(quotedId));
        assertTrue(result.get(quotedId).cdc);

        // Complex schema including clustering and additional WITH options
        String complexSchema =
            "CREATE TABLE events.user_activity (" +
            "user_id uuid, " +
            "timestamp timestamp, " +
            "activity text, " +
            "PRIMARY KEY (user_id, timestamp)" +
            ") WITH CLUSTERING ORDER BY (timestamp DESC) " +
            "AND bloom_filter_fp_chance = 0.01 " +
            "AND cdc = true " +
            "AND comment = 'User activity tracking';";
        result = CdcUtil.extractAllTablesWithCdcFlag(complexSchema);
        assertEquals(1, result.size());
        TableIdentifier activityId = TableIdentifier.of("events", "user_activity");
        assertTrue(result.containsKey(activityId));
        assertTrue(result.get(activityId).cdc);

        // createStatement is populated (not just the cdc flag) for a downstream-usable schema
        assertTrue(result.get(activityId).createStatement.contains("user_activity"));

        // Empty schema
        result = CdcUtil.extractAllTablesWithCdcFlag("");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testExtractPartitionKeySignatureWithQuotedColumnNameContainingSpace()
    {
        // A quoted partition-key column name containing a space must not be misparsed as if
        // the space inside the quotes were the name/type separator — that would corrupt the
        // column name lookup and force the signature to indeterminate instead of "uuid".
        String schema =
            "CREATE TABLE test_ks.quoted_col_table (\"my col\" uuid PRIMARY KEY, data text) WITH cdc = true;";
        Map<TableIdentifier, CdcUtil.TableSchema> result = CdcUtil.extractAllTablesWithCdcFlag(schema);
        TableIdentifier id = TableIdentifier.of("test_ks", "quoted_col_table");
        assertTrue(result.containsKey(id));
        CdcUtil.PartitionKeySignature signature = result.get(id).partitionKeySignature;
        assertFalse(signature.indeterminate);
        assertEquals(java.util.List.of("uuid"), signature.columnTypes);
    }
}
