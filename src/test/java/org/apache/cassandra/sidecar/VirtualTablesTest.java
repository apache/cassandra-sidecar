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
package org.apache.cassandra.sidecar;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.mapping.annotations.Table;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for virtual tables
 */
public class VirtualTablesTest
{
    @Test
    public void testNoManagerNeverConnected()
    {
        CQLSession session = mock(CQLSession.class);
        VirtualTables tables = new VirtualTables(session);
        assertNull(tables.manger());
    }

    @Test
    public void testExceptionNeverConnected()
    {
        CQLSession session = mock(CQLSession.class);
        VirtualTables tables = new VirtualTables(session);
        assertThrows(NoHostAvailableException.class, tables::settings);
        assertThrows(NoHostAvailableException.class, tables::sstableTasks);
        assertThrows(NoHostAvailableException.class, tables::threadPools);
    }

    @Table(keyspace = "system", name = "clients")
    private static class TestInvalidKeyspace
    {
    }

    @Test
    public void testInvalidClass()
    {
        CQLSession session = mock(CQLSession.class);
        VirtualTables tables = new VirtualTables(session);
        assertThrows(IllegalArgumentException.class, () -> tables.getTableResults(VirtualTablesTest.class));
        assertThrows(IllegalArgumentException.class, () -> tables.getTableResults(TestInvalidKeyspace.class));
    }
}
