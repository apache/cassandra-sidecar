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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.Session;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxExtension;
import org.apache.cassandra.sidecar.common.response.TableStatsResponse;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.CassandraTestContext;

import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the table-stats endpoint with cassandra container.
 */
@ExtendWith(VertxExtension.class)
public class TableStatsIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest(numDataDirsPerInstance = 4, network = true)
    void retrieveTableStats(CassandraTestContext cassandraTestContext)
    {
        createTestKeyspace(ImmutableMap.of("datacenter1", 2));
        QualifiedTableName tableName = createTestTable(
        "CREATE TABLE %s ( \n" +
        "  race_year int, \n" +
        "  race_name text, \n" +
        "  cyclist_name text, \n" +
        "  rank int, \n" +
        "  PRIMARY KEY ((race_year, race_name), rank) \n" +
        ");");
        Session session = maybeGetSession();
        /*
         * "SnapshotSize" table stats metric reports the size of snapshot files which are not links for "live" SSTables.
         * In order to simulate non-zero data for this metric, we do the following:
         * 1. Insert data
         * 2. Create snapshot
         * 3. Truncate table to ensure snapshot references non-live sstables
         * 4. Insert more data (and flush) to ensure other metrics, have non-zero values
         */
        insertData(session, tableName);
        createSnapshot(tableName);
        session.execute("TRUNCATE TABLE " + tableName);
        insertData(session, tableName);
        cassandraTestContext.cluster().stream().forEach(instance -> instance.flush(TEST_KEYSPACE));
        tableStats(tableName);
    }

    private void insertData(Session session, QualifiedTableName tableName)
    {
        for (int i = 1; i <= 10; i++)
        {
            session.execute("INSERT INTO " + tableName + " (race_year, race_name, rank, cyclist_name) " +
                            "VALUES (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', " + i + ", 'Benjamin PRADES');");
        }
    }

    private void createSnapshot(QualifiedTableName table)
    {
        String testRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/" + table.tableName() + "-snapshot",
                                         table.keyspace(), table.tableName());
        HttpResponse<Buffer> resp;
        resp = getBlocking(client.put(server.actualPort(), "127.0.0.1", testRoute)
                                 .send());
        assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
    }

    private void tableStats(QualifiedTableName tableName)
    {
        String testRoute = "/api/v1/cassandra/keyspaces/" + tableName.keyspace() + "/tables/" + tableName.tableName() + "/stats";
        HttpResponse<Buffer> resp;
        resp = getBlocking(client.get(server.actualPort(), "127.0.0.1", testRoute)
                                 .send());
        assertTableStatsResponse(tableName, resp);
    }

    void assertTableStatsResponse(QualifiedTableName tableName, HttpResponse<Buffer> response)
    {
        TableStatsResponse stats = response.bodyAsJson(TableStatsResponse.class);
        assertThat(stats).isNotNull();
        assertThat(stats.table()).isEqualTo(tableName.tableName());
        assertThat(stats.keyspace()).isEqualTo(tableName.keyspace());
        assertThat(stats.snapshotsSizeBytes()).isGreaterThan(0);
        assertThat(stats.sstableCount()).isGreaterThan(0);
        assertThat(stats.diskSpaceUsedBytes()).isGreaterThan(0);
        assertThat(stats.totalDiskSpaceUsedBytes()).isGreaterThan(0);
    }
}
