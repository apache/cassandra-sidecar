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

package org.apache.cassandra.sidecar.routes;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.ext.web.client.predicate.ResponsePredicate;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.sidecar.common.response.StreamStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.StreamsProgressStats;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.CassandraTestContext;

import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the stream stats endpoint with cassandra container.
 */
public class StreamStatsIntegrationTest extends IntegrationTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamStatsIntegrationTest.class);

    @CassandraIntegrationTest(nodesPerDc = 2, network = true)
    void streamStatsTest(CassandraTestContext cassandraTestContext)
    {
        UpgradeableCluster cluster = cassandraTestContext.cluster();

        createTestKeyspace(Map.of("datacenter1", 2));
        QualifiedTableName tableName = createTestTable(
        "CREATE TABLE %s ( \n" +
        "  race_year int, \n" +
        "  race_name text, \n" +
        "  cyclist_name text, \n" +
        "  rank int, \n" +
        "  PRIMARY KEY ((race_year, race_name), rank) \n" +
        ");");
        // craft inconsistency for repair
        populateDataAtNode2Only(cluster, tableName);

        // Poll stream stats while repair is running in the background.
        CountDownLatch testStart = new CountDownLatch(1);
        IUpgradeableInstance node = cluster.get(1);
        AtomicReference<RuntimeException> nodetoolError = new AtomicReference<>();
        startRepairAsync(node, testStart, tableName, nodetoolError);

        TestState testState = new TestState();
        testStart.countDown();
        loopAssert(20, 500, () -> {
            if (nodetoolError.get() != null)
            {
                throw nodetoolError.get();
            }
            streamStats(testState);
            testState.assertCompletion();
        });
    }

    private void startRepairAsync(IUpgradeableInstance node, CountDownLatch testStart, QualifiedTableName tableName, AtomicReference<RuntimeException> nodetoolError)
    {
        startAsync("Repairing node" + node.config().num(),
                   () -> {
                       Uninterruptibles.awaitUninterruptibly(testStart);
                       try
                       {
                           node.nodetoolResult("repair", tableName.keyspace(), tableName.tableName(), "--full").asserts().success();
                       }
                       catch (Throwable cause)
                       {
                           nodetoolError.set(new RuntimeException("Nodetool failed", cause));
                       }
                   });
    }

    private void streamStats(TestState testState)
    {
        String testRoute = "/api/v1/cassandra/stats/streams";
        StreamStatsResponse streamStatsResponse = getBlocking(client.get(server.actualPort(), "127.0.0.1", testRoute)
                                                                    .expect(ResponsePredicate.SC_OK)
                                                                    .send())
                                                  .bodyAsJson(StreamStatsResponse.class);
        assertThat(streamStatsResponse).isNotNull();
        StreamsProgressStats streamProgress = streamStatsResponse.streamsProgressStats();
        assertThat(streamProgress).isNotNull();
        LOGGER.info("Fetched {}", streamProgress);
        testState.update(streamProgress);
    }

    static class TestState
    {
        StreamsProgressStats lastStats;
        boolean streamStarted = false, streamCompleted = false;

        void update(StreamsProgressStats streamProgress)
        {
            lastStats = streamProgress;
            if (streamProgress.totalFilesToReceive() > 0)
            {
                streamStarted = true;
            }

            if (streamStarted && streamProgress.totalFilesReceived() == streamProgress.totalFilesToReceive())
            {
                streamCompleted = true;
            }
        }

        void assertCompletion()
        {
            assertThat(streamStarted)
            .describedAs("Expecting to have non-empty stream stats. last stats: " + lastStats)
            .isTrue();
            assertThat(streamCompleted)
            .describedAs("Expecting to complete. last stats: " + lastStats)
            .isTrue();
        }
    }

    void populateDataAtNode2Only(UpgradeableCluster cluster, QualifiedTableName tableName)
    {
        // disable compaction for the table to have more files to stream
        cluster.stream()
               .forEach(node -> node.nodetoolResult("disableautocompaction", tableName.keyspace(), tableName.tableName())
                                    .asserts().success());
        IInstance node = cluster.get(2);
        for (int i = 1; i <= 200; i++)
        {
            node.executeInternal("INSERT INTO " + tableName + " (race_year, race_name, rank, cyclist_name) " +
                                 "VALUES (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', " + i + ", 'Benjamin PRADES');");
            node.flush(TEST_KEYSPACE);
        }
    }
}
