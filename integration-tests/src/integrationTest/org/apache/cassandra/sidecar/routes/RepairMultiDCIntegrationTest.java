/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.routes;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpResponseExpectation;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.request.data.RepairPayload;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.TestUtils;

import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.RUNNING;
import static org.apache.cassandra.testing.TestUtils.DC1_RF2_DC2_RF2;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.TEST_TABLE_PREFIX;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Integration tests for repair operations in multi-datacenter environments.
 * This test class validates both cross-DC repair (default behavior when no datacenter is specified)
 * and single-DC repair (when a specific datacenter is specified) scenarios.
 */
@Tag("heavy")
class RepairMultiDCIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    private static final String INSERT_STMT = "INSERT INTO %s (race_year, race_name, rank, cyclist_name) " +
                                              "VALUES (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', %d, 'Benjamin PRADES')";

    private static final String SELECT_STMT = "SELECT * FROM %s";

    private static final String CREATE_STMT = "CREATE TABLE %s ( \n" +
                                              "  race_year int, \n" +
                                              "  race_name text, \n" +
                                              "  cyclist_name text, \n" +
                                              "  rank int, \n" +
                                              "  PRIMARY KEY ((race_year, race_name), rank) \n" +
                                              ")  WITH read_repair='NONE'";

    private static final int NUM_ROWS = 100;

    static final QualifiedName CROSS_DC_REPAIR_TABLE = TestUtils.uniqueTestTableFullName(TEST_KEYSPACE, TEST_TABLE_PREFIX);
    static final QualifiedName SINGLE_DC_REPAIR_TABLE = TestUtils.uniqueTestTableFullName(TEST_KEYSPACE, TEST_TABLE_PREFIX);
    static final QualifiedName CROSS_DC_IR_TABLE = TestUtils.uniqueTestTableFullName(TEST_KEYSPACE, TEST_TABLE_PREFIX);

    static final List<QualifiedName> ALL_TABLES = List.of(CROSS_DC_REPAIR_TABLE,
                                                          SINGLE_DC_REPAIR_TABLE,
                                                          CROSS_DC_IR_TABLE);

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .nodesPerDc(2)    // 2 nodes per datacenter
                    .dcCount(2)      // 2 datacenters (datacenter1 and datacenter2)
                    .requestFeature(Feature.NETWORK);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        // Create keyspace with replication across both datacenters
        createTestKeyspace(TEST_KEYSPACE, DC1_RF2_DC2_RF2);

        for (QualifiedName table : ALL_TABLES)
        {
            createTestTable(table, CREATE_STMT);

            // Disable auto-compaction to ensure data inconsistency persists until repair
            cluster.stream().forEach(node -> node.nodetoolResult("disableautocompaction", table.keyspace(), table.table())
                                                 .asserts().success());

            // Create data inconsistency by writing to only one node in datacenter1
            populateDataSingleNode(table, 1, NUM_ROWS); // Node 1 is in datacenter1

            // Verify initial data distribution - only datacenter1 node 1 should have data
            validateDataConsistency(table, List.of(1), NUM_ROWS);      // datacenter1 node 1 has data
            validateDataConsistency(table, List.of(2, 3, 4), 0);      // Other nodes have no data
        }
    }

    /**
     * Test cross-datacenter repair (default behavior when no datacenter is specified).
     */
    @Test
    void testCrossDCRepair()
    {
        // Perform cross-DC repair (no datacenter specified = repair across all DCs)
        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of(CROSS_DC_REPAIR_TABLE.table()))
                                             .build(); // No datacenter specified - should repair across all DCs

        String testRoute = "/api/v1/cassandra/keyspaces/" + TEST_KEYSPACE + "/repair";
        OperationalJobResponse response = getBlocking(trustedClient().put(serverWrapper.serverPort, "localhost", testRoute)
                                                                     .as(BodyCodec.json(OperationalJobResponse.class))
                                                                     .sendJson(JsonObject.mapFrom(payload))
                                                                     .expecting(HttpResponseExpectation.SC_ACCEPTED))
                                          .body();

        assertThat(response).isNotNull();
        assertThat(response.jobId()).isNotNull();
        assertThat(response.status()).isEqualTo(RUNNING);

        // Wait for repair to complete
        pollStatusForState(response.jobId().toString());

        // Verify data is now consistent across ALL nodes in BOTH datacenters
        validateDataConsistency(CROSS_DC_REPAIR_TABLE, List.of(1, 2, 3, 4), NUM_ROWS);
    }

    /**
     * Test single-datacenter repair (when a specific datacenter is specified).
     * This validates that repair can be limited to a specific datacenter.
     */
    @Test
    void testSingleDCRepair()
    {
        // Debug: Print datacenter information for each node
        for (int i = 1; i <= 4; i++)
        {
            IInstance node = cluster.get(i);
            String datacenter = node.config().localDatacenter();
            logger.info("Node {} is in datacenter: {}", i, datacenter);
        }

        // Perform single-DC repair (specify datacenter1 only)
        RepairPayload payload = RepairPayload.builder()
                                             .datacenter("datacenter1")  // Repair only datacenter1
                                             .isPrimaryRange(true)
                                             .tables(List.of(SINGLE_DC_REPAIR_TABLE.table()))
                                             .build();

        String testRoute = "/api/v1/cassandra/keyspaces/" + TEST_KEYSPACE + "/repair";
        OperationalJobResponse response = getBlocking(trustedClient().put(serverWrapper.serverPort, "localhost", testRoute)
                                                                     .as(BodyCodec.json(OperationalJobResponse.class))
                                                                     .sendJson(JsonObject.mapFrom(payload))
                                                                     .expecting(HttpResponseExpectation.SC_ACCEPTED))
                                          .body();

        assertThat(response).isNotNull();
        assertThat(response.jobId()).isNotNull();
        assertThat(response.status()).isEqualTo(RUNNING);

        // Wait for repair to complete
        pollStatusForState(response.jobId().toString());

        // Debug: Check data on all nodes after repair
        for (int i = 1; i <= 4; i++)
        {
            IInstance node = cluster.get(i);
            SimpleQueryResult rows = node.executeInternalWithResult(String.format(SELECT_STMT, SINGLE_DC_REPAIR_TABLE));
            long rowCount = StreamSupport.stream(rows.spliterator(), false).count();
            String datacenter = node.config().localDatacenter();
            logger.info("After repair - Node {} (DC: {}) has {} rows", i, datacenter, rowCount);
        }

        // Based on actual node distribution:
        // datacenter1: nodes 1, 3
        // datacenter2: nodes 2, 4
        validateDataConsistency(SINGLE_DC_REPAIR_TABLE, List.of(1, 3), NUM_ROWS);   // datacenter1 nodes should be consistent
        validateDataConsistency(SINGLE_DC_REPAIR_TABLE, List.of(2, 4), 0);         // datacenter2 nodes should still have no data
    }

    /**
     * Test incremental repair across datacenters.
     */
    @Test
    void testCrossDCIncrementalRepair()
    {
        // Perform incremental cross-DC repair
        RepairPayload payload = RepairPayload.builder()
                                             .repairType(RepairPayload.RepairType.INCREMENTAL)
                                             .tables(List.of(CROSS_DC_IR_TABLE.table()))
                                             .build(); // No datacenter = cross-DC repair

        String testRoute = "/api/v1/cassandra/keyspaces/" + TEST_KEYSPACE + "/repair";
        OperationalJobResponse response = getBlocking(trustedClient().put(serverWrapper.serverPort, "localhost", testRoute)
                                                                     .as(BodyCodec.json(OperationalJobResponse.class))
                                                                     .sendJson(JsonObject.mapFrom(payload))
                                                                     .expecting(HttpResponseExpectation.SC_ACCEPTED))
                                          .body();

        assertThat(response).isNotNull();
        assertThat(response.jobId()).isNotNull();
        assertThat(response.status()).isEqualTo(RUNNING);

        // Wait for repair to complete
        pollStatusForState(response.jobId().toString());

        // Verify data consistency across all nodes
        validateDataConsistency(CROSS_DC_IR_TABLE, List.of(1, 2, 3, 4), NUM_ROWS);
    }

    private void pollStatusForState(String uuid)
    {
        String status = "/api/v1/cassandra/operational-jobs/" + uuid;
        loopAssert(30, 500, () -> {
            HttpResponse<Buffer> resp = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", status)
                                                                   .send());
            logger.info("Success Status Response code: {}", resp.statusCode());
            logger.info("Status Response: {}", resp.bodyAsString());
            if (resp.statusCode() == HttpResponseStatus.OK.code())
            {
                OperationalJobResponse jobStatusResp = resp.bodyAsJson(OperationalJobResponse.class);
                assertThat(jobStatusResp.jobId()).isEqualTo(UUID.fromString(uuid));
                assertThat(jobStatusResp.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
                assertThat(jobStatusResp.reason()).isNull();
                assertThat(jobStatusResp.operation()).isEqualTo("repair");
            }
            else
            {
                assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                OperationalJobResponse jobStatusResp = resp.bodyAsJson(OperationalJobResponse.class);
                assertThat(jobStatusResp.jobId()).isEqualTo(UUID.fromString(uuid));
                fail("Repair is still in progress");
            }
        });
    }

    private void validateDataConsistency(QualifiedName tableName, List<Integer> nodes, long expectedNumRows)
    {
        nodes.forEach(nodeNumber -> {
            IInstance node = cluster.get(nodeNumber);
            SimpleQueryResult rows = node.executeInternalWithResult(String.format(SELECT_STMT, tableName));
            long rowCount = StreamSupport.stream(rows.spliterator(), false).count();
            assertThat(rowCount).as("Node %d should have %d rows", nodeNumber, expectedNumRows)
                                .isEqualTo(expectedNumRows);
        });
    }

    private void populateDataSingleNode(QualifiedName tableName, int nodeNumber, int numRows)
    {
        IInstance node = cluster.get(nodeNumber);
        IntStream.rangeClosed(1, numRows)
                 .forEach(i -> {
                     node.executeInternal(String.format(INSERT_STMT, tableName, i));
                     node.flush(TEST_KEYSPACE);
                 });
    }
}
