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
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.TEST_TABLE_PREFIX;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Integration tests for repair operations
 */
class RepairIntegrationTest extends SharedClusterSidecarIntegrationTestBase
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
                                              ")  WITH read_repair='NONE';";

    private static final int NUM_ROWS = 300;

    static final QualifiedName REPAIR_TEST_TABLE = TestUtils.uniqueTestTableFullName(TEST_KEYSPACE, TEST_TABLE_PREFIX);
    static final QualifiedName REPAIR_REPLICAS_WITH_DATA_TABLE = TestUtils.uniqueTestTableFullName(TEST_KEYSPACE, TEST_TABLE_PREFIX);
    static final QualifiedName REPAIR_REPLICAS_IR_WITH_DATA_TABLE = TestUtils.uniqueTestTableFullName(TEST_KEYSPACE, TEST_TABLE_PREFIX);
    static final QualifiedName REPAIR_REPLICAS_TIMEOUT_TABLE = TestUtils.uniqueTestTableFullName(TEST_KEYSPACE, TEST_TABLE_PREFIX);

    static final List<QualifiedName> ALL_TABLES = List.of(REPAIR_TEST_TABLE,
                                                          REPAIR_REPLICAS_WITH_DATA_TABLE,
                                                          REPAIR_REPLICAS_IR_WITH_DATA_TABLE,
                                                          REPAIR_REPLICAS_TIMEOUT_TABLE);

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .nodesPerDc(3)
                    .requestFeature(Feature.NETWORK);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);
        for (QualifiedName table : ALL_TABLES)
        {
            createTestTable(table, CREATE_STMT);
        }
        populateData(REPAIR_TEST_TABLE, 10);

        cluster.stream().forEach(node -> node.nodetoolResult("disableautocompaction",
                                                             REPAIR_REPLICAS_WITH_DATA_TABLE.keyspace(),
                                                             REPAIR_REPLICAS_WITH_DATA_TABLE.table())
                                             .asserts().success());
        populateDataSingleNode(REPAIR_REPLICAS_WITH_DATA_TABLE, 2, NUM_ROWS);
        validateDataConsistency(REPAIR_REPLICAS_WITH_DATA_TABLE, List.of(2), NUM_ROWS);
        validateDataConsistency(REPAIR_REPLICAS_WITH_DATA_TABLE, List.of(3, 1), 0);


        cluster.stream().forEach(node -> node.nodetoolResult("disableautocompaction",
                                                             REPAIR_REPLICAS_IR_WITH_DATA_TABLE.keyspace(),
                                                             REPAIR_REPLICAS_IR_WITH_DATA_TABLE.table())
                                             .asserts().success());
        populateDataSingleNode(REPAIR_REPLICAS_IR_WITH_DATA_TABLE, 2, NUM_ROWS);
        validateDataConsistency(REPAIR_REPLICAS_IR_WITH_DATA_TABLE, List.of(2), NUM_ROWS);
        validateDataConsistency(REPAIR_REPLICAS_IR_WITH_DATA_TABLE, List.of(3, 1), 0);

        cluster.stream().forEach(node -> node.nodetoolResult("disableautocompaction",
                                                             REPAIR_REPLICAS_TIMEOUT_TABLE.keyspace(),
                                                             REPAIR_REPLICAS_TIMEOUT_TABLE.table())
                                             .asserts().success());
        populateDataSingleNode(REPAIR_REPLICAS_TIMEOUT_TABLE, 2, NUM_ROWS);
        validateDataConsistency(REPAIR_REPLICAS_TIMEOUT_TABLE, List.of(2), NUM_ROWS);
        validateDataConsistency(REPAIR_REPLICAS_TIMEOUT_TABLE, List.of(3, 1), 0);
    }

    @Test
    void repairTest()
    {
        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of(REPAIR_TEST_TABLE.table()))
                                             .build();
        String testRoute = "/api/v1/cassandra/keyspaces/" + TEST_KEYSPACE + "/repair";
        OperationalJobResponse response = getBlocking(trustedClient().put(serverWrapper.serverPort, "localhost", testRoute)
                                                                     .as(BodyCodec.json(OperationalJobResponse.class))
                                                                     .sendJson(JsonObject.mapFrom(payload))
                                                                     .expecting(HttpResponseExpectation.SC_ACCEPTED))
                                          .body();

        assertThat(response).isNotNull();
        assertThat(response.jobId()).isNotNull();
        pollStatusForState(response.jobId().toString());
    }

    @Test
    void repairReplicasWithData()
    {
        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of(REPAIR_REPLICAS_WITH_DATA_TABLE.table()))
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

        pollStatusForState(response.jobId().toString());
        validateDataConsistency(REPAIR_REPLICAS_WITH_DATA_TABLE, List.of(2, 3, 1), NUM_ROWS);
    }

    @Test
    void repairReplicasIRWithData()
    {
        RepairPayload payload = RepairPayload.builder()
                                             .tables(List.of(REPAIR_REPLICAS_IR_WITH_DATA_TABLE.table()))
                                             .repairType(RepairPayload.RepairType.INCREMENTAL)
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

        pollStatusForState(response.jobId().toString());
        validateDataConsistency(REPAIR_REPLICAS_IR_WITH_DATA_TABLE, List.of(2, 3, 1), NUM_ROWS);
    }

    @Test
    void repairReplicasTimeout()
    {
        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of(REPAIR_REPLICAS_TIMEOUT_TABLE.table()))
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

        pollStatusForState(response.jobId().toString());
        validateDataConsistency(REPAIR_REPLICAS_TIMEOUT_TABLE, List.of(2, 3, 1), NUM_ROWS);
    }

    private void pollStatusForState(String uuid)
    {
        String status = "/api/v1/cassandra/operational-jobs/" + uuid;
        loopAssert(60, 500, () -> {
            HttpResponse<Buffer> resp = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", status)
                                                                   .send());
            logger.info("Success Status Response code: {}", resp.statusCode());
            logger.info("Status Response: {}", resp.bodyAsString());
            if (resp.statusCode() == HttpResponseStatus.OK.code())
            {
                OperationalJobResponse jobStatusResp = resp.bodyAsJson(OperationalJobResponse.class);
                assertThat(jobStatusResp.jobId()).isEqualTo(UUID.fromString(uuid));
                assertThat(jobStatusResp.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
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
            assertThat(expectedNumRows).isEqualTo(rowCount);
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

    private void populateData(QualifiedName tableName, int numRows)
    {
        IntStream.rangeClosed(1, numRows)
                 .mapToObj(i -> String.format(INSERT_STMT, tableName, i))
                 .forEach(cluster::schemaChangeIgnoringStoppedInstances);
    }
}
