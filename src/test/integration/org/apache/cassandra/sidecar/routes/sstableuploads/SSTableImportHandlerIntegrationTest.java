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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.sidecar.IntegrationTestBase;
import org.apache.cassandra.sidecar.common.SimpleCassandraVersion;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.testing.CassandraSidecarTestContext;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration tests for {@link SSTableImportHandler}
 */
@ExtendWith(VertxExtension.class)
public class SSTableImportHandlerIntegrationTest extends IntegrationTestBase
{
    public static final SimpleCassandraVersion MIN_VERSION_WITH_IMPORT = SimpleCassandraVersion.create("4.0.0");

    @CassandraIntegrationTest
    void testSSTableImport(VertxTestContext vertxTestContext)
    throws IOException, InterruptedException
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

        Session session = maybeGetSession();
        createTestKeyspace();
        QualifiedTableName tableName = createTestTableAndPopulate(sidecarTestContext, Arrays.asList("a", "b"));

        // create a snapshot called <tableName>-snapshot for tbl1
        UpgradeableCluster cluster = sidecarTestContext.cluster();
        final String snapshotStdout = cluster.get(1).nodetoolResult("snapshot",
                                                                    "--tag", tableName.tableName() + "-snapshot",
                                                                    "--table", tableName.tableName(),
                                                                    "--", tableName.keyspace()).getStdout();
        assertThat(snapshotStdout).contains("Snapshot directory: " + tableName.tableName() + "-snapshot");
        // find the directory in the filesystem
        final List<Path> snapshotFiles = findChildFile(sidecarTestContext,
                                                       "127.0.0.1", tableName.tableName() + "-snapshot");

        assertThat(snapshotFiles).isNotEmpty();

        // copy the snapshot to the expected staging directory
        final UUID uploadId = UUID.randomUUID();
        // for the test, we need to have the same directory structure for the local data directory replicated
        // inside the cluster. The way the endpoint works is by verifying that the directory exists, this
        // verification happens in the host system. When calling import we use the same directory, but the
        // directory does not exist inside the cluster. For that reason we need to do the following to
        // ensure "import" finds the path inside the cluster
        String uploadStagingDir = sidecarTestContext.instancesConfig()
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

        WebClient client = WebClient.create(vertx);
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
        "CREATE TABLE IF NOT EXISTS %s (id text, PRIMARY KEY(id));");
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
}
