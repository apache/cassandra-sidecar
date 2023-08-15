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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.Session;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.CassandraTestContext;
import org.apache.cassandra.testing.SimpleCassandraVersion;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

@ExtendWith(VertxExtension.class)
class CreateSnapshotHandlerIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void createSnapshotEndpointFailsWhenKeyspaceDoesNotExist(VertxTestContext context) throws InterruptedException
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/keyspaces/non_existent/tables/testtable/snapshots/my-snapshot";
        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_NOT_FOUND)
              .send(context.succeedingThenComplete());
        // wait until the test completes
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @CassandraIntegrationTest
    void createSnapshotEndpointFailsWhenTableDoesNotExist(VertxTestContext context)
    throws InterruptedException
    {
        createTestKeyspace();

        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/keyspaces/testkeyspace/tables/non_existent/snapshots/my-snapshot";
        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_NOT_FOUND)
              .send(context.succeedingThenComplete());
        // wait until the test completes
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @CassandraIntegrationTest
    void createSnapshotWithTtlFailsWhenUnitsAreNotSpecified(VertxTestContext context,
                                                            CassandraTestContext cassandraTestContext)
    throws InterruptedException
    {
        assumeThat(cassandraTestContext.version).as("TTL is only supported in Cassandra 4.1")
                                                .isGreaterThanOrEqualTo(SimpleCassandraVersion.create(4, 1, 0));

        createTestKeyspace();
        QualifiedTableName tableName = createTestTableAndPopulate();

        WebClient client = WebClient.create(vertx);
        String expectedErrorMessage = "Invalid duration: 500 Accepted units:[SECONDS, MINUTES, HOURS, DAYS] " +
                                      "where case matters and only non-negative values.";
        String testRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot?ttl=500",
                                         tableName.maybeQuotedKeyspace(), tableName.maybeQuotedTableName());
        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.bodyAsJsonObject().getString("message"))
                  .isEqualTo(expectedErrorMessage);
                  context.completeNow();
              })));
        // wait until test completes
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @CassandraIntegrationTest
    void createSnapshotWithTtlFailsWhenTtlIsTooSmall(VertxTestContext context,
                                                     CassandraTestContext cassandraTestContext)
    throws InterruptedException
    {
        if (cassandraTestContext.version.compareTo(SimpleCassandraVersion.create(4, 1, 0)) < 0)
        {
            // TTL is only supported in Cassandra 4.1
            context.completeNow();
            return;
        }

        createTestKeyspace();
        QualifiedTableName tableName = createTestTableAndPopulate();

        WebClient client = WebClient.create(vertx);
        String expectedErrorMessage = "ttl for snapshot must be at least 60 seconds";
        String testRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot?ttl=1s",
                                         tableName.maybeQuotedKeyspace(), tableName.maybeQuotedTableName());
        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.bodyAsJsonObject().getString("message"))
                  .isEqualTo(expectedErrorMessage);
                  context.completeNow();
              })));
        // wait until test completes
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @CassandraIntegrationTest
    void createSnapshotFailsWhenSnapshotAlreadyExists(VertxTestContext context)
    throws InterruptedException
    {
        createTestKeyspace();
        QualifiedTableName tableName = createTestTableAndPopulate();

        WebClient client = WebClient.create(vertx);
        String testRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                         tableName.maybeQuotedKeyspace(), tableName.maybeQuotedTableName());
        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response ->
                                       context.verify(() -> {
                                           assertThat(response.statusCode()).isEqualTo(OK.code());

                                           // creating the snapshot with the same name will return
                                           // a 409 (Conflict) status code
                                           client.put(server.actualPort(), "127.0.0.1", testRoute)
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
        createTestKeyspace();
        QualifiedTableName tableName = createTestTableAndPopulate();

        WebClient client = WebClient.create(vertx);
        String testRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                         tableName.maybeQuotedKeyspace(), tableName.maybeQuotedTableName());
        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());

                  // validate that the snapshot is created
                  List<Path> found = findChildFile(sidecarTestContext, "127.0.0.1",
                                                   "my-snapshot");
                  assertThat(found).isNotEmpty()
                                   .anyMatch(p -> p.toString().endsWith("manifest.json"))
                                   .anyMatch(p -> p.toString().endsWith("schema.cql"))
                                   .anyMatch(p -> p.toString().endsWith("-big-Data.db"));

                  context.completeNow();
              })));
        // wait until test completes
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @CassandraIntegrationTest
    void testCreateSnapshotEndpointWithTtl(VertxTestContext context,
                                           CassandraTestContext cassandraTestContext) throws InterruptedException
    {
        // TTL is only supported in Cassandra 4.1
        boolean validateExpectedTtl = cassandraTestContext.version
                                      .compareTo(SimpleCassandraVersion.create("4.1.0")) >= 0;

        createTestKeyspace();
        QualifiedTableName tableName = createTestTableAndPopulate();

        long expectedTtlInSeconds = 61;
        WebClient client = WebClient.create(vertx);
        String testRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/ttl-snapshot?ttl=%ds",
                                         tableName.maybeQuotedKeyspace(), tableName.maybeQuotedTableName(),
                                         expectedTtlInSeconds);
        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());

                  // validate that the snapshot is created
                  List<Path> found = findChildFile(sidecarTestContext, "127.0.0.1",
                                                   "ttl-snapshot");
                  assertThat(found).isNotEmpty()
                                   .anyMatch(p -> p.toString().endsWith("manifest.json"))
                                   .anyMatch(p -> p.toString().endsWith("schema.cql"))
                                   .anyMatch(p -> p.toString().endsWith("-big-Data.db"));

                  // get manifest
                  Optional<Path> manifest = found.stream()
                                                 .filter(p -> p.toString().endsWith("manifest.json"))
                                                 .findFirst();

                  assertThat(manifest).isPresent();
                  assertThat(manifest.get()).exists();
                  validateManifestExpirationDate(manifest.get(), expectedTtlInSeconds, validateExpectedTtl);

                  context.completeNow();
              })));
        // wait until test completes
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    private void validateManifestExpirationDate(Path manifestPath, long expectedTtl, boolean validateExpectedTtl)
    {
        ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());
        jsonMapper.registerModule(new JavaTimeModule());
        jsonMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        try (InputStream in = Files.newInputStream(manifestPath))
        {
            SnapshotManifest manifest = jsonMapper.readValue(in, SnapshotManifest.class);

            if (validateExpectedTtl)
            {
                long actualTtl = ChronoUnit.SECONDS.between(manifest.createdAt, manifest.expiresAt);
                assertThat(actualTtl).isEqualTo(expectedTtl);
            }
            else
            {
                assertThat(manifest.createdAt).isNull();
                assertThat(manifest.expiresAt).isNull();
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @CassandraIntegrationTest
    void testCreateSnapshotEndpointWithMixedCaseTableName(VertxTestContext context)
    throws InterruptedException
    {
        createTestKeyspace();
        QualifiedTableName tableName = createTestTableAndPopulate("QuOtEdTaBlENaMe");

        WebClient client = WebClient.create(vertx);
        String testRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                         tableName.maybeQuotedKeyspace(), tableName.maybeQuotedTableName());
        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());

                  // validate that the snapshot is created
                  List<Path> found = findChildFile(sidecarTestContext, "127.0.0.1",
                                                   "my-snapshot");
                  assertThat(found).isNotEmpty()
                                   .anyMatch(p -> p.toString().endsWith("manifest.json"))
                                   .anyMatch(p -> p.toString().endsWith("schema.cql"))
                                   .anyMatch(p -> p.toString().endsWith("-big-Data.db"));

                  context.completeNow();
              })));
        // wait until test completes
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    private QualifiedTableName createTestTableAndPopulate(String tableNamePrefix)
    {
        QualifiedTableName tableName = createTestTable(tableNamePrefix,
                                                       "CREATE TABLE %s (id text PRIMARY KEY, name text);");
        Session session = maybeGetSession();

        session.execute("INSERT INTO " + tableName + " (id, name) VALUES ('1', 'Francisco');");
        session.execute("INSERT INTO " + tableName + " (id, name) VALUES ('2', 'Saranya');");
        session.execute("INSERT INTO " + tableName + " (id, name) VALUES ('3', 'Yifan');");
        return tableName;
    }

    private QualifiedTableName createTestTableAndPopulate()
    {
        QualifiedTableName tableName = createTestTable(
        "CREATE TABLE %s (id text PRIMARY KEY, name text);");
        Session session = maybeGetSession();

        session.execute("INSERT INTO " + tableName + " (id, name) VALUES ('1', 'Francisco');");
        session.execute("INSERT INTO " + tableName + " (id, name) VALUES ('2', 'Saranya');");
        session.execute("INSERT INTO " + tableName + " (id, name) VALUES ('3', 'Yifan');");
        return tableName;
    }

    static class SnapshotManifest
    {
        @JsonProperty("files")
        public List<String> files;

        @JsonProperty("created_at")
        public Instant createdAt;

        @JsonProperty("expires_at")
        public Instant expiresAt;
    }
}
