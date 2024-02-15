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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.Session;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.data.ListSnapshotFilesResponse;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.testing.CassandraSidecarTestContext;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class StreamSSTableComponentHandlerIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void testStreamIncludingIndexFiles(VertxTestContext context) throws InterruptedException
    {
        runTestScenario(context);
    }

    private void runTestScenario(VertxTestContext context) throws InterruptedException
    {
        createTestKeyspace();
        QualifiedTableName table = createTestTableAndPopulate(sidecarTestContext);

        List<String> expectedFileList = Arrays.asList(".ryear/[a-z]{2}-1-big-Data.db",
                                                      ".ryear/[a-z]{2}-1-big-TOC.txt",
                                                      "[a-z]{2}-1-big-Data.db",
                                                      "[a-z]{2}-1-big-TOC.txt");

        WebClient client = WebClient.create(vertx);
        String testRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                         table.keyspace(), table.tableName());

        createSnapshot(client, testRoute)
        .compose(route -> listSnapshot(client, route))
        .onComplete(context.succeeding(response -> {

            List<ListSnapshotFilesResponse.FileInfo> filesToStream =
            response.snapshotFilesInfo()
                    .stream()
                    .filter(info -> info.fileName.endsWith("-Data.db") || info.fileName.endsWith("-TOC.txt"))
                    .sorted(Comparator.comparing(o -> o.fileName))
                    .collect(Collectors.toList());

            assertThat(filesToStream).hasSize(4);
            for (int i = 0; i < filesToStream.size(); i++)
            {
                ListSnapshotFilesResponse.FileInfo fileInfo = filesToStream.get(i);
                assertThat(fileInfo.fileName).matches(expectedFileList.get(i));
            }

            List<Future<HttpResponse<Buffer>>> futures = new ArrayList<>();
            // Stream all the files including index files
            for (ListSnapshotFilesResponse.FileInfo fileInfo : filesToStream)
            {
                futures.add(streamSSTableComponent(client, fileInfo));
            }

            Future.all(futures)
                  .onSuccess(s -> context.completeNow())
                  .onFailure(context::failNow);
        }));

        // wait until test completes
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    Future<String> createSnapshot(WebClient client, String route)
    {
        Promise<String> promise = Promise.promise();
        client.put(server.actualPort(), "127.0.0.1", route)
              .expect(ResponsePredicate.SC_OK)
              .send()
              .onSuccess(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  promise.complete(route);
              })
              .onFailure(promise::fail);
        return promise.future();
    }

    Future<ListSnapshotFilesResponse> listSnapshot(WebClient client, String route)
    {
        Promise<ListSnapshotFilesResponse> promise = Promise.promise();
        client.get(server.actualPort(), "127.0.0.1", route + "?includeSecondaryIndexFiles=true")
              .expect(ResponsePredicate.SC_OK)
              .send()
              .onSuccess(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  promise.complete(response.bodyAsJson(ListSnapshotFilesResponse.class));
              })
              .onFailure(promise::fail);
        return promise.future();
    }

    Future<HttpResponse<Buffer>> streamSSTableComponent(WebClient client,
                                                        ListSnapshotFilesResponse.FileInfo fileInfo)
    {
        String route = "/keyspaces/" + fileInfo.keySpaceName +
                       "/tables/" + fileInfo.tableName +
                       "/snapshots/" + fileInfo.snapshotName +
                       "/components/" + fileInfo.fileName;

        return client.get(server.actualPort(), "localhost", "/api/v1" + route)
                     .expect(ResponsePredicate.SC_OK)
                     .as(BodyCodec.buffer())
                     .send();
    }

    QualifiedTableName createTestTableAndPopulate(CassandraSidecarTestContext cassandraTestContext)
    {
        QualifiedTableName tableName = createTestTable(
        "CREATE TABLE %s ( \n" +
        "  race_year int, \n" +
        "  race_name text, \n" +
        "  cyclist_name text, \n" +
        "  rank int, \n" +
        "  PRIMARY KEY ((race_year, race_name), rank) \n" +
        ");");
        Session session = maybeGetSession();

        session.execute("CREATE INDEX ryear ON " + tableName + " (race_year);");
        session.execute("INSERT INTO " + tableName + " (race_year, race_name, rank, cyclist_name) " +
                        "VALUES (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 1, 'Benjamin PRADES');");
        session.execute("INSERT INTO " + tableName + " (race_year, race_name, rank, cyclist_name) " +
                        "VALUES (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 2, 'Adam PHELAN');");
        session.execute("INSERT INTO " + tableName + " (race_year, race_name, rank, cyclist_name) " +
                        "VALUES (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 3, 'Thomas LEBAS');");
        return tableName;
    }
}
