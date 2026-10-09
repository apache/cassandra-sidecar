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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.sidecar.tasks.CassandraClusterSchemaMonitor;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterCdcSidecarIntegrationTestBase;
import org.apache.cassandra.sidecar.testing.TestCdcEventConsumer;
import org.apache.cassandra.spark.utils.TableIdentifier;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for CDC functionality.
 *
 * <p>Both scenarios share one CDC-enabled sidecar/cluster and live in a single test class so they run
 * in one fork — {@link SharedClusterCdcSidecarIntegrationTestBase}'s hardcoded sidecar port would
 * otherwise collide across parallel forks:
 * <ul>
 *   <li>basic capture — mutations on a CDC-enabled table are published to {@link TestCdcEventConsumer}; and</li>
 *   <li>the batch-write regression (CASSSIDECAR-499) — a {@code BEGIN BATCH} mixing a CDC-enabled and a
 *   CDC-disabled table under the same partition key produces one commit log {@code Mutation} spanning
 *   both. Before the fix, {@code Schema.instance} only knew CDC-enabled tables, so deserializing that
 *   Mutation threw {@code UnknownTableException} and dropped the whole thing — including the CDC table's
 *   update. Also covers the partition-key risk analysis ({@link CdcBatchRiskAnalyzer}): a non-CDC table
 *   whose partition-key type differs from every CDC-enabled table in its keyspace is never registered,
 *   since it could never share a Mutation with a CDC-enabled update.</li>
 * </ul>
 */
public class CdcIntegrationTest extends SharedClusterCdcSidecarIntegrationTestBase
{
    private static final QualifiedName CDC_TEST_TABLE = new QualifiedName("cdc_test_ks", "cdc_test_table");

    private static final QualifiedName CDC_TABLE = new QualifiedName("cdc_batch_test_ks", "cdc_table");
    private static final QualifiedName NON_CDC_TABLE = new QualifiedName("cdc_batch_test_ks", "non_cdc_table");
    private static final QualifiedName MISMATCHED_PK_TABLE = new QualifiedName("cdc_batch_test_ks", "mismatched_pk_table");

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(CDC_TEST_TABLE, DC1_RF1);
        createTestTable(CDC_TEST_TABLE, "CREATE TABLE IF NOT EXISTS %s "
                                        + "(id int PRIMARY KEY, value int) "
                                        + "WITH cdc = true");

        createTestKeyspace(CDC_TABLE, DC1_RF1);
        createTestTable(CDC_TABLE, "CREATE TABLE IF NOT EXISTS %s "
                                   + "(id int PRIMARY KEY, value int) "
                                   + "WITH cdc = true");
        createTestTable(NON_CDC_TABLE, "CREATE TABLE IF NOT EXISTS %s "
                                       + "(id int PRIMARY KEY, value int) "
                                       + "WITH cdc = false");
        // Partition key type (text) differs from CDC_TABLE's (int), so it can never be registered.
        createTestTable(MISMATCHED_PK_TABLE, "CREATE TABLE IF NOT EXISTS %s "
                                             + "(id text PRIMARY KEY, value int) "
                                             + "WITH cdc = false");
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

    @Test
    void testBatchMixingCdcAndNonCdcTableDoesNotDropCdcMutation()
    {
        // One BEGIN BATCH over both tables produces a single Mutation spanning both — the scenario
        // that used to drop the CDC table's update.
        String batch = String.format(
            "BEGIN BATCH " +
            "INSERT INTO %s (id, value) VALUES (1, 100); " +
            "INSERT INTO %s (id, value) VALUES (1, 200); " +
            "APPLY BATCH",
            CDC_TABLE, NON_CDC_TABLE);

        cluster.getFirstRunningInstance()
               .coordinator()
               .execute(batch, ConsistencyLevel.ONE);

        // Seal the active commit log segment so CDC can find the mutation in cdc_raw
        cluster.getFirstRunningInstance().flush(CDC_TABLE.keyspace());

        TestCdcEventConsumer consumer = getTestEventConsumer();
        waitUntil(() -> !consumer.getEvents().isEmpty(), 120, 1000);

        List<CdcEvent> events = consumer.getEvents();

        // The CDC table's mutation must survive despite sharing a Mutation with the non-CDC table.
        assertThat(events)
        .as("CDC-enabled table's mutation must be published even though it shared a batch "
          + "Mutation with a CDC-disabled table")
        .anySatisfy(event -> {
            assertThat(event.keyspace).isEqualTo(CDC_TABLE.keyspace());
            assertThat(event.table).isEqualTo(CDC_TABLE.table());
            assertThat(event.getKind()).isEqualTo(CdcEvent.Kind.INSERT);
        });

        // The non-CDC table is registered only so the shared Mutation deserializes; it's still gated
        // out at the per-PartitionUpdate CDC-flag check, so it must never be published.
        assertThat(events)
        .as("Non-CDC table's mutation must never be published, even though it's registered in "
          + "Schema.instance because it shares CDC_TABLE's partition-key structure")
        .noneMatch(event -> event.table.equals(NON_CDC_TABLE.table()));
    }

    @Test
    void testTableWithMismatchedPartitionKeyIsNeverRegistered()
    {
        CassandraClusterSchemaMonitor schemaMonitor = serverWrapper.injector.getInstance(CassandraClusterSchemaMonitor.class);
        waitUntil(() -> !schemaMonitor.getRegisteredTables().isEmpty(), 30, 1000);

        Set<TableIdentifier> registered = schemaMonitor.getRegisteredTables();

        assertThat(registered)
        .as("CDC-enabled table must always be registered")
        .contains(TableIdentifier.of(CDC_TABLE.keyspace(), CDC_TABLE.table()));
        assertThat(registered)
        .as("Non-CDC table sharing CDC_TABLE's partition-key structure must be registered — "
          + "it could be co-located with CDC_TABLE's update in the same batch Mutation")
        .contains(TableIdentifier.of(NON_CDC_TABLE.keyspace(), NON_CDC_TABLE.table()));
        assertThat(registered)
        .as("Table with a different partition-key type than any CDC-enabled table in its "
          + "keyspace could never be co-located with a CDC-enabled table's update in the same "
          + "Mutation, so it must never be registered")
        .doesNotContain(TableIdentifier.of(MISMATCHED_PK_TABLE.keyspace(), MISMATCHED_PK_TABLE.table()));
    }
}
