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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterCdcSidecarIntegrationTestBase;
import org.apache.cassandra.sidecar.testing.TestCdcEventConsumer;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for CDC functionality.
 * Tests that mutations on CDC-enabled tables are captured and published to TestCdcEventConsumer.
 */
public class CdcIntegrationTest extends SharedClusterCdcSidecarIntegrationTestBase
{
    private static final QualifiedName CDC_TEST_TABLE = new QualifiedName("cdc_test_ks", "cdc_test_table");

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(CDC_TEST_TABLE, DC1_RF1);

        String createTableStatement = "CREATE TABLE IF NOT EXISTS %s " +
                "(id int PRIMARY KEY, value int) " +
                "WITH cdc = true";
        createTestTable(CDC_TEST_TABLE, createTableStatement);
    }

    @Override
    protected void beforeTestStart()
    {
        waitForSchemaReady(30, TimeUnit.SECONDS);
    }

    @Test
    void testCdcEventsPublishedToInMemory()
    {
        // Write mutations into the test table
        int mutationCount = 100;
        Map<Integer, Integer> expectedMutations = new HashMap<>();
        for (int i = 1; i <= mutationCount; i++)
        {
            String query = String.format("INSERT INTO %s (id, value) VALUES (%d, %d)", CDC_TEST_TABLE, i, i);
            cluster.getFirstRunningInstance()
                    .coordinator()
                    .execute(query, ConsistencyLevel.ONE);
            expectedMutations.put(i, i);
        }

        // Seal the active commit log segment so CDC can find the mutations in cdc_raw
        cluster.getFirstRunningInstance().flush(CDC_TEST_TABLE.keyspace());

        TestCdcEventConsumer consumer = getTestEventConsumer();
        waitUntil(() -> consumer.getEvents().size() >= mutationCount, 120, 1000);
        assertThat(consumer.getEvents().size())
                .as("All CDC events should be published to in-memory consumer")
                .isGreaterThanOrEqualTo(mutationCount);

        // Verify all the mutations with expected values
        List<CdcEvent> events = consumer.getEvents();
        for (CdcEvent cdcEvent : events)
        {
            assertThat(cdcEvent.keyspace).isEqualTo(CDC_TEST_TABLE.keyspace());
            assertThat(cdcEvent.table).isEqualTo(CDC_TEST_TABLE.table());
            assertThat(cdcEvent.getKind()).isEqualTo(CdcEvent.Kind.INSERT);
            assertThat(cdcEvent.getValueColumns().get(0).columnName).isEqualTo("value");

            int value = ByteBuffer.wrap(Objects.requireNonNull(cdcEvent.getValueColumns().get(0).getBytes())).getInt();
            expectedMutations.remove(value);
        }
        assertThat(expectedMutations).isEmpty();
    }
}
