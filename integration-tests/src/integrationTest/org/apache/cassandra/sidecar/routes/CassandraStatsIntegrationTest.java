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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import org.apache.cassandra.sidecar.common.response.ConnectedClientStatsResponse;
import org.apache.cassandra.sidecar.common.response.TableStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.ClientConnectionEntry;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.TEST_TABLE_PREFIX;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the stats endpoints (that require a single node cluster) with cassandra container.
 */
class CassandraStatsIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    private static final int DEFAULT_CONNECTION_COUNT = 2;
    private static final QualifiedName TEST_TABLE = new QualifiedName(TEST_KEYSPACE, TEST_TABLE_PREFIX);

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(TEST_TABLE,
                        "CREATE TABLE %s ( \n" +
                        "  race_year int, \n" +
                        "  race_name text, \n" +
                        "  cyclist_name text, \n" +
                        "  rank int, \n" +
                        "  PRIMARY KEY ((race_year, race_name), rank) \n" +
                        ");");
    }

    @Test
    void retrieveClientStatsDefault()
    {
        Map<String, Boolean> expectedParams = Map.of("summary", true);
        String testRoute = "/api/v1/cassandra/stats/connected-clients";
        HttpResponse<Buffer> response = getBlocking(trustedClient().get(server.actualPort(), "localhost", testRoute)
                                                                   .expect(ResponsePredicate.SC_OK)
                                                                   .send());
        assertClientStatsResponse(response, expectedParams);
    }

    @Test
    void retrieveClientStatsListConnections()
    {
        Map<String, Boolean> expectedParams = Map.of("summary", false);
        String testRoute = "/api/v1/cassandra/stats/connected-clients?summary=false";
        HttpResponse<Buffer> response = getBlocking(trustedClient().get(server.actualPort(), "localhost", testRoute)
                                                                   .expect(ResponsePredicate.SC_OK)
                                                                   .send());
        assertClientStatsResponse(response, expectedParams);
    }

    @Test
    void retrieveClientStatsListConnectionsWithKeyspace()
    {
        try (Cluster driverCluster = createDriverCluster(cluster.delegate()); Session session = driverCluster.connect())
        {
            session.execute("USE " + TEST_KEYSPACE);

            Map<String, Boolean> expectedParams = Map.of("summary", false);
            String testRoute = "/api/v1/cassandra/stats/connected-clients?summary=false";
            HttpResponse<Buffer> response = getBlocking(trustedClient().get(server.actualPort(), "localhost", testRoute)
                                                                       .expect(ResponsePredicate.SC_OK)
                                                                       .send());
            assertClientStatsResponse(response, expectedParams, 4, true);
        }
    }

    @Test
    void retrieveClientStatsMultipleConnections()
    {
        // Creates an additional connection pair
        try (Cluster driverCluster = createDriverCluster(cluster.delegate()); Session ignored = driverCluster.connect())
        {
            Map<String, Boolean> expectedParams = Map.of("summary", false);
            String testRoute = "/api/v1/cassandra/stats/connected-clients?summary=false";
            HttpResponse<Buffer> response = getBlocking(trustedClient().get(server.actualPort(), "localhost", testRoute)
                                                                       .expect(ResponsePredicate.SC_OK)
                                                                       .send());
            assertClientStatsResponse(response, expectedParams, 4);
        }
    }

    /**
     * Expects unrecognized params to be ignored and invalid value for the expected parameter to be defaulted to true
     * to prevent heavyweight query in the bad request case.
     */
    @Test
    void retrieveClientStatsInvalidParameterValue()
    {
        Map<String, Boolean> expectedParams = Map.of("summary", true);
        String testRoute = "/api/v1/cassandra/stats/connected-clients?summary=123&bad-arg=xyz";
        HttpResponse<Buffer> response = getBlocking(trustedClient().get(server.actualPort(), "localhost", testRoute)
                                                                   .expect(ResponsePredicate.SC_OK)
                                                                   .send());
        assertClientStatsResponse(response, expectedParams);
    }

    @Test
    void retrieveTableStats()
    {
        /*
         * "SnapshotSize" table stats metric reports the size of snapshot files which are not links for "live" SSTables.
         * In order to simulate non-zero data for this metric, we do the following:
         * 1. Insert data
         * 2. Create snapshot
         * 3. Truncate table to ensure snapshot references non-live sstables
         * 4. Insert more data (and flush) to ensure other metrics, have non-zero values
         */
        insertData(TEST_TABLE);
        createSnapshot(TEST_TABLE);
        cluster.schemaChangeIgnoringStoppedInstances("TRUNCATE TABLE " + TEST_TABLE);
        insertData(TEST_TABLE);
        cluster.stream().forEach(instance -> instance.flush(TEST_KEYSPACE));
        tableStats(TEST_TABLE);
    }

    private void insertData(QualifiedName tableName)
    {
        for (int i = 1; i <= 10; i++)
        {
            String statement = "INSERT INTO " + tableName + " (race_year, race_name, rank, cyclist_name) " +
                               "VALUES (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', " + i + ", 'Benjamin PRADES');";
            cluster.schemaChangeIgnoringStoppedInstances(statement);
        }
    }

    private void createSnapshot(QualifiedName tableName)
    {
        String testRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/" + tableName.table() + "-snapshot",
                                         tableName.keyspace(), tableName.table());
        HttpResponse<Buffer> resp;
        resp = getBlocking(trustedClient().put(server.actualPort(), "localhost", testRoute)
                                 .send());
        assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
    }

    private void tableStats(QualifiedName tableName)
    {
        String testRoute = "/api/v1/cassandra/keyspaces/" + tableName.keyspace() + "/tables/" + tableName.table() + "/stats";
        HttpResponse<Buffer> resp;
        resp = getBlocking(trustedClient().get(server.actualPort(), "localhost", testRoute)
                                 .send());
        assertTableStatsResponse(tableName, resp);
    }

    void assertTableStatsResponse(QualifiedName tableName, HttpResponse<Buffer> response)
    {
        TableStatsResponse stats = response.bodyAsJson(TableStatsResponse.class);
        assertThat(stats).isNotNull();
        assertThat(stats.table()).isEqualTo(tableName.table());
        assertThat(stats.keyspace()).isEqualTo(tableName.keyspace());
        assertThat(stats.snapshotsSizeBytes()).isGreaterThan(0);
        assertThat(stats.sstableCount()).isGreaterThan(0);
        assertThat(stats.diskSpaceUsedBytes()).isGreaterThan(0);
        assertThat(stats.totalDiskSpaceUsedBytes()).isGreaterThan(0);
    }


    void assertClientStatsResponse(HttpResponse<Buffer> response, Map<String, Boolean> params)
    {
        assertClientStatsResponse(response, params, DEFAULT_CONNECTION_COUNT);
    }

    void assertClientStatsResponse(HttpResponse<Buffer> response, Map<String, Boolean> params, int expectedConnections)
    {
        assertClientStatsResponse(response, params, expectedConnections, false);
    }

    void assertClientStatsResponse(HttpResponse<Buffer> response, Map<String, Boolean> params, int expectedConnections, boolean usingKeyspace)
    {
        boolean isSummary = params.get("summary");

        logger.info("Response: {}", response.bodyAsString());
        ConnectedClientStatsResponse clientStats = response.bodyAsJson(ConnectedClientStatsResponse.class);
        assertThat(clientStats).isNotNull();
        assertThat(clientStats.connectionsByUser()).isNotEmpty();
        assertThat(clientStats.connectionsByUser()).containsKey("anonymous");
        assertThat(clientStats.totalConnectedClients()).isEqualTo(expectedConnections);

        List<ClientConnectionEntry> stats = clientStats.clientConnections();
        if (isSummary)
        {
            assertThat(stats).isNull();
        }
        else
        {
            SimpleCassandraVersion releaseVersion = SimpleCassandraVersion.create(cluster.get(1).getReleaseVersionString());
            SimpleCassandraVersion majorVersion = SimpleCassandraVersion.create(releaseVersion.major, releaseVersion.minor, 0);
            SimpleCassandraVersion fourZero = SimpleCassandraVersion.create("4.0");
            assertThat(stats.size()).isEqualTo(expectedConnections);
            for (ClientConnectionEntry stat : stats)
            {
                assertThat(stat.address()).contains("127.0.0.1");
                assertThat(stat.sslEnabled()).isEqualTo(false);
                assertThat(stat.driverName()).isEqualTo("DataStax Java Driver");
                assertThat(stat.driverVersion()).isNotNull();
                assertThat(stat.username()).isEqualTo("anonymous");
                if (majorVersion.isGreaterThan(fourZero))
                {
                    assertThat(stat.clientOptions()).isNotNull();
                    assertThat(stat.clientOptions().containsKey("CQL_VERSION")).isTrue();
                }
            }

            // TODO: Add validations for fields in trunk once dtest jars can advance beyond TCM commit
            if (usingKeyspace
                && majorVersion.compareTo(SimpleCassandraVersion.create("5.0.0")) >= 0)
            {
                assertThat(stats.stream().map(ClientConnectionEntry::keyspaceName).collect(Collectors.toSet())).contains(TEST_KEYSPACE);
            }
        }
    }
}
