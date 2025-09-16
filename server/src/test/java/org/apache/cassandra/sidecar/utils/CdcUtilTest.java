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
    public void testExtractCdcTables()
    {
        // Test with single CDC-enabled table
        String singleTableSchema = "CREATE TABLE test_ks.cdc_table (id uuid PRIMARY KEY, data text) WITH cdc = true;";
        Map<TableIdentifier, String> result = CdcUtil.extractCdcTables(singleTableSchema);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(TableIdentifier.of("test_ks", "cdc_table")));

        // Test with multiple CDC-enabled tables
        String multipleTablesSchema = 
            "CREATE TABLE ks1.table1 (id uuid PRIMARY KEY, data text) WITH cdc = true;" +
            "CREATE TABLE ks2.table2 (id uuid PRIMARY KEY, value int) WITH cdc = true;" +
            "CREATE TABLE ks3.table3 (id uuid PRIMARY KEY, info text) WITH cdc = false;";
        result = CdcUtil.extractCdcTables(multipleTablesSchema);
        assertEquals(2, result.size());
        assertTrue(result.containsKey(TableIdentifier.of("ks1", "table1")));
        assertTrue(result.containsKey(TableIdentifier.of("ks2", "table2")));
        assertFalse(result.containsKey(TableIdentifier.of("ks3", "table3")));

        // Test with quoted keyspace and table names
        String quotedNamesSchema = 
            "CREATE TABLE \"my_keyspace\".\"my_table\" (id uuid PRIMARY KEY, data text) WITH cdc = true;";
        result = CdcUtil.extractCdcTables(quotedNamesSchema);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(TableIdentifier.of("my_keyspace", "my_table")));

        // Test with no CDC-enabled tables
        String noCdcSchema = 
            "CREATE TABLE test_ks.regular_table (id uuid PRIMARY KEY, data text);" +
            "CREATE TABLE test_ks.another_table (id uuid PRIMARY KEY, value int) WITH cdc = false;";
        result = CdcUtil.extractCdcTables(noCdcSchema);
        assertTrue(result.isEmpty());

        // Test with complex schema including clustering and additional options
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
        result = CdcUtil.extractCdcTables(complexSchema);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(TableIdentifier.of("events", "user_activity")));

        // Test with empty schema
        result = CdcUtil.extractCdcTables("");
        assertTrue(result.isEmpty());
    }
}
