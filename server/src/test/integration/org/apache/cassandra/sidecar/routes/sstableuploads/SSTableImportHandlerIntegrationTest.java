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

package org.apache.cassandra.sidecar.routes.sstableuploads;

import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.handlers.sstableuploads.SSTableImportHandler;
import org.apache.cassandra.sidecar.testing.CassandraSidecarTestContext;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.IClusterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration tests for {@link SSTableImportHandler}
 */
@ExtendWith(VertxExtension.class)
public class SSTableImportHandlerIntegrationTest extends IntegrationTestBase
{
    public static final SimpleCassandraVersion MIN_VERSION_WITH_IMPORT = SimpleCassandraVersion.create("4.0.0");
    public static final SimpleCassandraVersion MIN_VERSION_WITH_SAI = SimpleCassandraVersion.create("5.0.0");

    @CassandraIntegrationTest
    void testSSTableImport(VertxTestContext vertxTestContext)
    throws Exception
    {
        // Cassandra before 4.0 does not have the necessary JMX endpoints,
        // so we skip if the cluster version is below 4.0
        assumeThat(sidecarTestContext.version)
        .withFailMessage("Import is only available in Cassandra 4.0 and later.")
        .isGreaterThanOrEqualTo(MIN_VERSION_WITH_IMPORT);

        // create a table. Insert some data, create a snapshot that we'll use for import.
        // Truncate the table, insert more data.
        // Test the import SSTable endpoint by importing data that was originally truncated.
        // Verify by querying the table contains all the results before truncation and after truncation.
        createTestKeyspace();
        Session session = maybeGetSession();
        QualifiedTableName tableName = createTestTableAndPopulate(sidecarTestContext, Arrays.asList("a", "b"));

        // create a snapshot called <tableName>-snapshot for tbl1
        IClusterExtension<? extends IInstance> cluster = sidecarTestContext.cluster();
        final String snapshotStdout = cluster.get(1).nodetoolResult("snapshot",
                                                                    "--tag", tableName.tableName() + "-snapshot",
                                                                    "--table", tableName.tableName(),
                                                                    "--", tableName.keyspace()).getStdout();
        assertThat(snapshotStdout).contains("Snapshot directory: " + tableName.tableName() + "-snapshot");
        // find the directory in the filesystem
        final List<Path> snapshotFiles = findChildFile(sidecarTestContext, "127.0.0.1",
                                                       tableName.keyspace(), tableName.tableName() + "-snapshot");

        assertThat(snapshotFiles).isNotEmpty();

        // copy the snapshot to the expected staging directory
        final UUID uploadId = UUID.randomUUID();
        // for the test, we need to have the same directory structure for the local data directory replicated
        // inside the cluster. The way the endpoint works is by verifying that the directory exists, this
        // verification happens in the host system. When calling import we use the same directory, but the
        // directory does not exist inside the cluster. For that reason we need to do the following to
        // ensure "import" finds the path inside the cluster
        String uploadStagingDir = sidecarTestContext.instancesMetadata()
                                                    .instanceFromHost("127.0.0.1").stagingDir();
        final String stagingPathInContainer = uploadStagingDir + File.separator + uploadId
                                              + File.separator + tableName.keyspace()
                                              + File.separator + tableName.tableName();
        boolean mkdirs = new File(stagingPathInContainer).mkdirs();
        assertThat(mkdirs)
        .withFailMessage("Could not create directory " + uploadStagingDir)
        .isTrue();

        // copy snapshot files into the staging path in the cluster
        for (Path path : snapshotFiles)
        {
            if (path.toFile().isFile())
            {
                Files.copy(path, Paths.get(stagingPathInContainer).resolve(path.getFileName()));
            }
        }

        // Now truncate the contents of the table
        truncateAndVerify(tableName);

        // Add new data (c, d) to table
        populateTable(session, tableName, Arrays.asList("c", "d"));

        WebClient client = mTLSClient();
        String testRoute = "/api/v1/uploads/" + uploadId + "/keyspaces/" + tableName.keyspace()
                           + "/tables/" + tableName.tableName() + "/import";
        sendRequest(vertxTestContext,
                    () -> client.put(server.actualPort(), "127.0.0.1", testRoute),
                    vertxTestContext.succeeding(response -> vertxTestContext.verify(() -> {
                        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                        assertThat(queryValues(tableName))
                        .containsAll(Arrays.asList("a", "b", "c", "d"));
                        vertxTestContext.completeNow();
                    })));
        // wait until test completes
        assertThat(vertxTestContext.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    private void sendRequest(VertxTestContext vertxTestContext, Supplier<HttpRequest<Buffer>> requestSupplier,
                             Handler<AsyncResult<HttpResponse<Buffer>>> handler)
    {
        requestSupplier.get()
                       .send(vertxTestContext.succeeding(r -> vertxTestContext.verify(() -> {
                           int statusCode = r.statusCode();
                           if (statusCode == HttpResponseStatus.ACCEPTED.code())
                           {
                               // retry the request every second when the request is accepted
                               vertx.setTimer(1000, tid -> sendRequest(vertxTestContext, requestSupplier, handler));
                           }
                           else
                           {
                               handler.handle(Future.succeededFuture(r));
                           }
                       })));
    }

    private void truncateAndVerify(QualifiedTableName qualifiedTableName)
    throws InterruptedException
    {
        Session session = maybeGetSession();
        session.execute("TRUNCATE TABLE " + qualifiedTableName);

        while (true)
        {
            TimeUnit.MILLISECONDS.sleep(100);
            ResultSet rs = session.execute("SELECT * FROM " + qualifiedTableName);
            if (rs.all().size() == 0)
                break; // truncate succeeded
        }
    }

    private List<String> queryValues(QualifiedTableName tableName)
    {
        Session session = maybeGetSession();
        return session.execute("SELECT id FROM " + tableName)
                      .all()
                      .stream()
                      .map(row -> row.getString("id"))
                      .collect(Collectors.toList());
    }

    private QualifiedTableName createTestTableAndPopulate(CassandraSidecarTestContext cassandraTestContext,
                                                          List<String> values)
    {
        QualifiedTableName tableName = createTestTable(
        "CREATE TABLE IF NOT EXISTS %s (id text, PRIMARY KEY(id))" + WITH_COMPACTION_DISABLED + ";");
        Session session = maybeGetSession();
        populateTable(session, tableName, values);
        return tableName;
    }

    private void populateTable(Session session, QualifiedTableName tableName, List<String> values)
    {
        for (String value : values)
        {
            session.execute(String.format("INSERT INTO %s (id) VALUES ('%s');", tableName, value));
        }
    }

    @CassandraIntegrationTest
    void testSSTableImportWithSaiIndexParams(VertxTestContext vertxTestContext)
    throws Exception
    {
        assumeThat(sidecarTestContext.version)
        .withFailMessage("SAI indexes are only available in Cassandra 5.0 and later.")
        .isGreaterThanOrEqualTo(MIN_VERSION_WITH_SAI);

        createTestKeyspace();
        Session session = maybeGetSession();

        // Create table with SAI index and populate initial data
        QualifiedTableName tableName = createTestTableWithSaiIndex(Arrays.asList(new String[]{ "a", "val_a" },
                                                                                 new String[]{ "b", "val_b" }));

        // Snapshot the table
        IClusterExtension<? extends IInstance> cluster = sidecarTestContext.cluster();
        String snapshotStdout = cluster.get(1).nodetoolResult("snapshot",
                                                              "--tag", tableName.tableName() + "-snapshot",
                                                              "--table", tableName.tableName(),
                                                              "--", tableName.keyspace()).getStdout();
        assertThat(snapshotStdout).contains("Snapshot directory: " + tableName.tableName() + "-snapshot");

        List<Path> snapshotFiles = findChildFile(sidecarTestContext, "127.0.0.1",
                                                 tableName.keyspace(), tableName.tableName() + "-snapshot");
        assertThat(snapshotFiles).isNotEmpty();

        // Upload snapshot files via REST endpoint
        UUID uploadId = UUID.randomUUID();
        WebClient client = mTLSClient();
        List<Path> filesToUpload = snapshotFiles.stream()
                                                .filter(p -> p.toFile().isFile())
                                                .collect(Collectors.toList());
        assertThat(filesToUpload).isNotEmpty();

        // Verify at least one file contains '+' in its name (SAI index files)
        assertThat(filesToUpload.stream().anyMatch(p -> p.getFileName().toString().contains("+")))
        .withFailMessage("Expected at least one snapshot file with '+' in its name (SAI index file)")
        .isTrue();

        uploadSnapshotFiles(client, uploadId, tableName, filesToUpload)
        .toCompletionStage().toCompletableFuture().get(60, TimeUnit.SECONDS);

        // Truncate table (no new data added before import)
        truncateAndVerify(tableName);

        // Import with SAI index query params
        String testRoute = "/api/v1/uploads/" + uploadId + "/keyspaces/" + tableName.keyspace()
                           + "/tables/" + tableName.tableName() + "/import";
        sendRequest(vertxTestContext,
                    () -> client.put(server.actualPort(), "127.0.0.1", testRoute)
                                .addQueryParam("failOnMissingIndex", "true")
                                .addQueryParam("validateIndexChecksum", "true"),
                    vertxTestContext.succeeding(response -> vertxTestContext.verify(() -> {
                        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());

                        // Verify SAI index component files exist on the filesystem
                        // This assertion is meaningful because no new data was written after truncate,
                        // so any SAI files must have come from the imported snapshot
                        String dataDir = sidecarTestContext.instancesMetadata()
                                                           .instanceFromHost("127.0.0.1").dataDirs().get(0);
                        Path keyspacePath = Paths.get(dataDir, tableName.keyspace());
                        Path tableDir;
                        try (Stream<Path> dirs = Files.list(keyspacePath))
                        {
                            tableDir = dirs.filter(dir -> dir.getFileName().toString()
                                                             .startsWith(tableName.tableName()))
                                           .findFirst()
                                           .orElseThrow(() -> new AssertionError("Table directory not found"));
                        }

                        List<Path> saiFiles;
                        try (Stream<Path> files = Files.list(tableDir))
                        {
                            saiFiles = files.filter(f -> f.getFileName().toString().toUpperCase()
                                                          .contains("SAI"))
                                            .collect(Collectors.toList());
                        }

                        assertThat(saiFiles)
                            .withFailMessage("Expected SAI index component files on the filesystem after import")
                            .isNotEmpty();

                        // Verify imported data is present
                        assertThat(queryValues(tableName)).containsExactlyInAnyOrder("a", "b");

                        // Add new data after import and SAI verification
                        populateTableWithValue(session, tableName, Arrays.asList(new String[]{ "c", "val_c" },
                                                                                 new String[]{ "d", "val_d" }));

                        // Verify all data (imported + new) is present
                        assertThat(queryValues(tableName)).containsExactlyInAnyOrder("a", "b", "c", "d");

                        vertxTestContext.completeNow();
                    })));
        assertThat(vertxTestContext.awaitCompletion(300, TimeUnit.SECONDS)).isTrue();
    }

    private QualifiedTableName createTestTableWithSaiIndex(List<String[]> rows)
    {
        QualifiedTableName tableName = createTestTable(
        "CREATE TABLE IF NOT EXISTS %s (id text, value text, PRIMARY KEY(id))" + WITH_COMPACTION_DISABLED + ";");
        Session session = maybeGetSession();
        session.execute(String.format("CREATE CUSTOM INDEX IF NOT EXISTS %s_sai_idx ON %s (value) USING " +
                                      "'org.apache.cassandra.index.sai.StorageAttachedIndex';",
                                      tableName.tableName(), tableName));
        populateTableWithValue(session, tableName, rows);
        return tableName;
    }

    private void populateTableWithValue(Session session, QualifiedTableName tableName, List<String[]> rows)
    {
        for (String[] row : rows)
        {
            session.execute(String.format("INSERT INTO %s (id, value) VALUES ('%s', '%s');",
                                          tableName, row[0], row[1]));
        }
    }

    private Future<Void> uploadSnapshotFiles(WebClient client, UUID uploadId,
                                              QualifiedTableName tableName, List<Path> snapshotFiles)
    {
        Future<Void> future = Future.succeededFuture();
        for (Path path : snapshotFiles)
        {
            future = future.compose(v -> {
                String fileName = path.getFileName().toString();
                // URLEncoder.encode encodes space as '+', which is correct for query params but not path segments.
                // For path segments, '+' is a literal character, so we must encode it as %2B.
                String encodedFileName = URLEncoder.encode(fileName, StandardCharsets.UTF_8)
                                                   .replace("+", "%2B");
                String uploadRoute = "/api/v1/uploads/" + uploadId + "/keyspaces/" + tableName.keyspace()
                                     + "/tables/" + tableName.tableName() + "/components/" + encodedFileName;
                Buffer fileContent = vertx.fileSystem().readFileBlocking(path.toString());
                return client.put(server.actualPort(), "127.0.0.1", uploadRoute)
                             .sendBuffer(fileContent)
                             .compose(response -> {
                                 if (response.statusCode() != HttpResponseStatus.OK.code())
                                 {
                                     return Future.failedFuture("Upload failed for " + fileName
                                                                + " with status " + response.statusCode()
                                                                + ": " + response.bodyAsString());
                                 }
                                 return Future.succeededFuture();
                             });
            });
        }
        return future;
    }
}
