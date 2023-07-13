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

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.Session;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.IntegrationTestBase;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.testing.CassandraSidecarParameterResolver;
import org.apache.cassandra.sidecar.testing.CassandraSidecarTestContext;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class SnapshotsHandlerIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void createSnapshotEndpointFailsWhenKeyspaceDoesNotExist(VertxTestContext context) throws InterruptedException
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/keyspaces/non-existent/tables/testtable/snapshots/my-snapshot";
        client.put(config.getPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_NOT_FOUND)
              .send(context.succeedingThenComplete());
        // wait until the test completes
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @CassandraIntegrationTest
    void createSnapshotEndpointFailsWhenTableDoesNotExist(VertxTestContext context)
    throws InterruptedException
    {
        createTestKeyspace(cassandraTestContext);

        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/keyspaces/testkeyspace/tables/non-existent/snapshots/my-snapshot";
        client.put(config.getPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_NOT_FOUND)
              .send(context.succeedingThenComplete());
        // wait until the test completes
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @CassandraIntegrationTest
    void createSnapshotFailsWhenSnapshotAlreadyExists(VertxTestContext context)
    throws InterruptedException
    {
        createTestKeyspace(cassandraTestContext);
        String table = createTestTableAndPopulate(cassandraTestContext);

        WebClient client = WebClient.create(vertx);
        String testRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                         TEST_KEYSPACE, table);
        client.put(config.getPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response ->
                                       context.verify(() -> {
                                           assertThat(response.statusCode()).isEqualTo(OK.code());

                                           // creating the snapshot with the same name will return
                                           // a 409 (Conflict) status code
                                           client.put(config.getPort(), "127.0.0.1", testRoute)
                                                 .expect(ResponsePredicate.SC_CONFLICT)
                                                 .send(context.succeedingThenComplete());
                                       })));
        // wait until the test completes
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @CassandraIntegrationTest
    void testCreateSnapshotEndpoint(VertxTestContext context)
    throws InterruptedException
    {
        createTestKeyspace(cassandraTestContext);
        String table = createTestTableAndPopulate(cassandraTestContext);

        WebClient client = WebClient.create(vertx);
        String testRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                         TEST_KEYSPACE, table);
        client.put(config.getPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());

                  // validate that the snapshot is created
                  final List<Path> found = findChildFile(cassandraTestContext, "127.0.0.1",
                                                         "my-snapshot");
                  assertThat(found).isNotEmpty();

                  assertThat(found.stream().anyMatch(p -> p.endsWith("manifest.json")));
                  assertThat(found.stream().anyMatch(p -> p.endsWith("schema.cql")));
                  assertThat(found.stream().anyMatch(p -> p.endsWith("-big-Data.db")));


                  context.completeNow();
              })));
        // wait until test completes
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @CassandraIntegrationTest
    void deleteSnapshotFailsWhenKeyspaceDoesNotExist(VertxTestContext context) throws InterruptedException
    {
        String testRoute = "/api/v1/keyspaces/non-existent/tables/testtable/snapshots/my-snapshot";
        assertNotFoundOnDeleteSnapshot(context, testRoute);
    }

    @CassandraIntegrationTest
    void deleteSnapshotFailsWhenTableDoesNotExist(VertxTestContext context)
    throws InterruptedException
    {
        createTestKeyspace(cassandraTestContext);
        createTestTableAndPopulate(cassandraTestContext);

        String testRoute = "/api/v1/keyspaces/testkeyspace/tables/non-existent/snapshots/my-snapshot";
        assertNotFoundOnDeleteSnapshot(context, testRoute);
    }

    @CassandraIntegrationTest
    void deleteSnapshotFailsWhenSnapshotDoesNotExist(VertxTestContext context)
    throws InterruptedException
    {
        createTestKeyspace(cassandraTestContext);
        String table = createTestTableAndPopulate(cassandraTestContext);

        String testRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/non-existent",
                                         TEST_KEYSPACE, table);
        assertNotFoundOnDeleteSnapshot(context, testRoute);
    }

    @CassandraIntegrationTest(numDataDirsPerInstance = 1)
        // Set to > 1 to fail test
    void testDeleteSnapshotEndpoint(VertxTestContext context)
    throws InterruptedException
    {
        createTestKeyspace(cassandraTestContext);
        String table = createTestTableAndPopulate(cassandraTestContext);

        WebClient client = WebClient.create(vertx);
        String snapshotName = "my-snapshot" + UUID.randomUUID();
        String testRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/%s",
                                         TEST_KEYSPACE, table, snapshotName);

        // first create the snapshot
        client.put(config.getPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(
              createResponse ->
              context.verify(() -> {
                  assertThat(createResponse.statusCode()).isEqualTo(OK.code());
                  final List<Path> found =
                  findChildFile(cassandraTestContext, "127.0.0.1", snapshotName);
                  assertThat(found).isNotEmpty();

                  // snapshot directory exists inside data directory
                  assertThat(found).isNotEmpty();

                  // then delete the snapshot
                  client.delete(config.getPort(), "127.0.0.1", testRoute)
                        .expect(ResponsePredicate.SC_OK)
                        .send(context.succeeding(
                        deleteResponse ->
                        context.verify(() ->
                                       {
                                           assertThat(deleteResponse.statusCode()).isEqualTo(OK.code());
                                           final List<Path> found2 =
                                           findChildFile(cassandraTestContext,
                                                         "127.0.0.1", snapshotName);
                                           assertThat(found2).isEmpty();
                                           context.completeNow();
                                       })));
              })));
        // wait until the test completes
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    private String createTestTableAndPopulate(CassandraSidecarTestContext cassandraTestContext)
    {
        QualifiedTableName tableName = createTestTable(cassandraTestContext,
                                                       "CREATE TABLE %s (id text PRIMARY KEY, name text);");
        Session session = maybeGetSession(cassandraTestContext);

        session.execute("INSERT INTO " + tableName + " (id, name) VALUES ('1', 'Francisco');");
        session.execute("INSERT INTO " + tableName + " (id, name) VALUES ('2', 'Saranya');");
        session.execute("INSERT INTO " + tableName + " (id, name) VALUES ('3', 'Yifan');");
        return tableName.tableName();
    }

    private void assertNotFoundOnDeleteSnapshot(VertxTestContext context, String testRoute) throws InterruptedException
    {
        WebClient client = WebClient.create(vertx);
        client.delete(config.getPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_NOT_FOUND)
              .send(context.succeedingThenComplete());
        // wait until test completes
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }
}
