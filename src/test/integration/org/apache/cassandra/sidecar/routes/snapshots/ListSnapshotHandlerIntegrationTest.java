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

package org.apache.cassandra.sidecar.routes.snapshots;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.Session;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.response.ListSnapshotFilesResponse;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class ListSnapshotHandlerIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest(numDataDirsPerInstance = 4)
    void testListSnapshot(VertxTestContext context)
    {
        createTestKeyspace();
        Session session = maybeGetSession();
        session.execute("CREATE TABLE " + TEST_KEYSPACE + ".rank_by_year_and_name ( \n" +
                        "  race_year int, \n" +
                        "  race_name text, \n" +
                        "  cyclist_name text, \n" +
                        "  rank int, \n" +
                        "  PRIMARY KEY ((race_year, race_name), rank) \n" +
                        ")" + WITH_COMPACTION_DISABLED + ";");

        // Insert a single record (only one data directory will be created from the configured 4 data dirs per instance)
        session.execute("INSERT INTO " + TEST_KEYSPACE + ".rank_by_year_and_name (race_year, race_name, cyclist_name, rank) " +
                        "VALUES (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Benjamin PRADES', 1);");

        // Create the snapshot
        WebClient client = WebClient.create(vertx);
        String testRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/rank_snapshot", TEST_KEYSPACE, "rank_by_year_and_name");
        createSnapshot(client, testRoute)
        .compose(response -> {
            assertThat(response.statusCode()).isEqualTo(OK.code());

            // validate that the snapshot is created
            List<Path> found = findChildFile(sidecarTestContext, "127.0.0.1",
                                             TEST_KEYSPACE, "rank_snapshot");
            assertThat(found).isNotEmpty()
                             .anyMatch(p -> p.toString().endsWith("manifest.json"))
                             .anyMatch(p -> p.toString().endsWith("schema.cql"))
                             .anyMatch(p -> p.toString().endsWith("-big-Data.db"));

            return listSnapshot(client, testRoute);
        })
        .compose(response -> {
            assertThat(response.statusCode()).isEqualTo(OK.code());

            ListSnapshotFilesResponse listResponse = transformAndValidateListSnapshotResponse(response);
            // The Data.db file can only be found in one data directory (because only one record was inserted),
            // let's make sure that's the case
            assertThat(listResponse.snapshotFilesInfo().stream().filter(f -> f.fileName.endsWith("-Data.db")).count()).isOne();

            // Let's create a secondary index, and make sure we list a snapshot with and another one
            // without secondary indexes
            session.execute("CREATE INDEX ryear ON " + TEST_KEYSPACE + ".rank_by_year_and_name (race_year);");

            // Populate the table with random data
            populateTable(session, "rank_by_year_and_name");

            // Now create a new snapshot
            return createSnapshot(client, testRoute + "_1");
        })
        // And then list the new snapshot without secondary indexes
        .compose(response -> listSnapshot(client, testRoute + "_1"))
        .compose(response -> {
            assertThat(response.statusCode()).isEqualTo(OK.code());

            ListSnapshotFilesResponse listResponse = transformAndValidateListSnapshotResponse(response);

            // The Data.db files should now live under all four data directories
            assertThat(listResponse.snapshotFilesInfo()
                                   .stream()
                                   .filter(f -> f.fileName.endsWith("-Data.db"))
                                   .map(f -> f.dataDirIndex)
                                   .collect(Collectors.toSet())
                                   .size()).isEqualTo(4);

            // ensure that none of the files in the response include the secondary index
            assertThat(listResponse.snapshotFilesInfo().stream()).noneMatch(f -> f.fileName.contains(".ryear"));

            // Create a new snapshot that includes index files
            return createSnapshot(client, testRoute + "_2");
        })
        // finally list the snapshot requesting secondary indexes to be listed
        .compose(response -> listSnapshot(client, testRoute + "_2?includeSecondaryIndexFiles=true"))
        .onSuccess(response -> {
            assertThat(response.statusCode()).isEqualTo(OK.code());

            ListSnapshotFilesResponse listResponse = transformAndValidateListSnapshotResponse(response);

            // we should have files that include the secondary index files
            assertThat(listResponse.snapshotFilesInfo().stream()).anyMatch(f -> f.fileName.startsWith(".ryear"));

            // This should still hold true: Data.db files should now live under all four data directories
            assertThat(listResponse.snapshotFilesInfo()
                                   .stream()
                                   .filter(f -> f.fileName.endsWith("-Data.db"))
                                   .map(f -> f.dataDirIndex)
                                   .collect(Collectors.toSet())
                                   .size()).isEqualTo(4);

            context.completeNow();
        })
        .onFailure(context::failNow);
    }

    private void populateTable(Session session, String tableName)
    {
        for (int i = 0; i < 1000; i++)
        {
            session.execute("INSERT INTO " + TEST_KEYSPACE + "." + tableName + " (race_year, race_name, cyclist_name, rank) " +
                            "VALUES (" + (1990 + i) + ", 'Tour_" + i + "', 'Benjamin_" + i + "', " + (i + 1) + ");");
        }
    }

    private Future<HttpResponse<String>> createSnapshot(WebClient client, String testRoute)
    {
        return client.put(server.actualPort(), "127.0.0.1", testRoute)
                     .expect(ResponsePredicate.SC_OK)
                     .as(BodyCodec.string()).send();
    }

    private Future<HttpResponse<Buffer>> listSnapshot(WebClient client, String testRoute)
    {
        return client.get(server.actualPort(), "127.0.0.1", testRoute).expect(ResponsePredicate.SC_OK).send();
    }

    private static ListSnapshotFilesResponse transformAndValidateListSnapshotResponse(HttpResponse<Buffer> response)
    {
        ListSnapshotFilesResponse listResponse = response.bodyAsJson(ListSnapshotFilesResponse.class);
        assertThat(listResponse).isNotNull();
        assertThat(listResponse.snapshotFilesInfo()).isNotEmpty();
        return listResponse;
    }
}
