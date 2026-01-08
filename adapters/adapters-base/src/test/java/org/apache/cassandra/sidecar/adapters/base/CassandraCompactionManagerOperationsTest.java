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

package org.apache.cassandra.sidecar.adapters.base;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.adapters.base.jmx.CompactionManagerJmxOperations;
import org.apache.cassandra.sidecar.common.server.JmxClient;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.apache.cassandra.sidecar.adapters.base.jmx.CompactionManagerJmxOperations.COMPACTION_MANAGER_OBJ_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Tests for {@link CassandraCompactionManagerOperations} class
 */
class CassandraCompactionManagerOperationsTest
{
    private CassandraCompactionManagerOperations compactionManagerOperations;
    private JmxClient mockJmxClient;
    private CompactionManagerJmxOperations mockJmxOperations;

    @BeforeEach
    void setUp()
    {
        mockJmxClient = mock(JmxClient.class);
        mockJmxOperations = mock(CompactionManagerJmxOperations.class);
        compactionManagerOperations = new CassandraCompactionManagerOperations(mockJmxClient);

        // Setup JMX proxy mock
        when(mockJmxClient.proxy(CompactionManagerJmxOperations.class, COMPACTION_MANAGER_OBJ_NAME))
            .thenReturn(mockJmxOperations);
    }

    @Test
    void testStopCompactionByIdOnly()
    {
        // Test stopCompactionById called when providing compactionId
        String compactionId = "abc-123";
        compactionManagerOperations.stopCompactionById(compactionId);

        verify(mockJmxOperations, times(1)).stopCompactionById(compactionId);
        verify(mockJmxOperations, times(0)).stopCompaction(org.mockito.ArgumentMatchers.anyString());
    }

    @Test
    void testStopCompactionByTypeOnly()
    {
        // Test stopCompaction called when no compactionId provided
        String compactionType = "COMPACTION";
        compactionManagerOperations.stopCompaction(compactionType);

        verify(mockJmxOperations, times(1)).stopCompaction(compactionType);
        verify(mockJmxOperations, times(0)).stopCompactionById(org.mockito.ArgumentMatchers.anyString());
    }

    @Test
    void testStopCompactionByIdWithWhitespace()
    {
        // Test trim does not result in empty string
        String compactionId = "  abc-123  ";
        compactionManagerOperations.stopCompactionById(compactionId);

        verify(mockJmxOperations, times(1)).stopCompactionById(compactionId);
        verify(mockJmxOperations, times(0)).stopCompaction(org.mockito.ArgumentMatchers.anyString());
    }

    @Test
    void testStopCompactionAllSupportedTypes()
    {
        // Test no failures upon any supported type being provided as param
        String[] supportedTypes = {
            "COMPACTION", "VALIDATION", "KEY_CACHE_SAVE", "ROW_CACHE_SAVE",
            "COUNTER_CACHE_SAVE", "CLEANUP", "SCRUB", "UPGRADE_SSTABLES",
            "INDEX_BUILD", "TOMBSTONE_COMPACTION", "ANTICOMPACTION",
            "VERIFY", "VIEW_BUILD", "INDEX_SUMMARY", "RELOCATE",
            "GARBAGE_COLLECT"
        };

        for (String type : supportedTypes)
        {
            compactionManagerOperations.stopCompaction(type);
            verify(mockJmxOperations, times(1)).stopCompaction(type);
        }
    }

    @Test
    void testStopCompactionCatchesUnsupportedType()
    {
        String compactionType = "MAJOR_COMPACTION";
        assertThrows(IllegalArgumentException.class,
                     () -> compactionManagerOperations.stopCompaction(compactionType));
    }

    @Test
    void testStopCompactionJmxProxyCalledOnce()
    {
        // Test JMX proxy obtained exactly once per call
        compactionManagerOperations.stopCompactionById("test-id");

        verify(mockJmxClient, times(1))
            .proxy(CompactionManagerJmxOperations.class, COMPACTION_MANAGER_OBJ_NAME);
    }
}
