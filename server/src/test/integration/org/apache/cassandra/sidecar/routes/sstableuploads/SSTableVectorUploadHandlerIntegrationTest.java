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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.Session;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.handlers.sstableuploads.SSTableImportHandler;
import org.apache.cassandra.sidecar.handlers.sstableuploads.SSTableUploadHandler;
import org.apache.cassandra.sidecar.testing.CassandraSidecarTestContext;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.IClusterExtension;
import org.apache.cassandra.testing.utils.AssertionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration tests for {@link SSTableUploadHandler}, {@link SSTableImportHandler} and vector type
 * TODO: Consider removing after upgrade to Java driver 4.x (CASSSIDECAR-421).
 */
@ExtendWith(VertxExtension.class)
public class SSTableVectorUploadHandlerIntegrationTest extends IntegrationTestBase
{
    public static final SimpleCassandraVersion MIN_VERSION_WITH_VECTOR = SimpleCassandraVersion.create("5.0.0");

    @CassandraIntegrationTest
    void testSSTableImport(VertxTestContext vertxTestContext)
    throws Exception
    {
        assumeThat(sidecarTestContext.version)
        .as("Vector type is supported since Cassandra 5.0")
        .isGreaterThanOrEqualTo(MIN_VERSION_WITH_VECTOR);

        // Create a table. Insert some data, create a snapshot that we'll use for import.
        // Move snapshot files to a temporary destination.
        // Truncate the table and insert more data.
        // Upload the SSTables.
        // Test the import SSTable endpoint by importing data that was originally truncated.
        // Verify by querying the table contains all the results before truncation and after truncation.

        createTestKeyspace();
        Session session = maybeGetSession();
        QualifiedTableName tableName = createTestTableAndPopulate(sidecarTestContext, Arrays.asList("a", "b"));

        // wait for schema to be present
        // periodic refresh of Java driver unsupported schemas is configured to 3 seconds
        AssertionUtils.loopAssert(15, 1000, () -> {
            String testSchemaRoute = "/api/v1/cassandra/schema";
            HttpResponse<Buffer> response = client.get(server.actualPort(), "127.0.0.1", testSchemaRoute).send()
                                                  .toCompletionStage().toCompletableFuture().join();
            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
            assertThat(response.bodyAsString()).contains(tableName.tableName());
        });

        // create a snapshot called <tableName>-snapshot for tbl1
        IClusterExtension<? extends IInstance> cluster = sidecarTestContext.cluster();
        String snapshotStdout = cluster.get(1).nodetoolResult("snapshot",
                                                              "--tag", tableName.tableName() + "-snapshot",
                                                              "--table", tableName.tableName(),
                                                              "--", tableName.keyspace()).getStdout();
        assertThat(snapshotStdout).contains("Snapshot directory: " + tableName.tableName() + "-snapshot");
        // find the directory in the filesystem
        final List<Path> snapshotFiles = findChildFile(sidecarTestContext, "127.0.0.1",
                                                       tableName.keyspace(), tableName.tableName() + "-snapshot");

        assertThat(snapshotFiles).isNotEmpty();

        // copy snapshot files into the temporary directory
        File tmpDir = new File("/tmp", UUID.randomUUID().toString());
        boolean mkdirs = tmpDir.mkdirs();
        assertThat(mkdirs)
        .withFailMessage("Could not create directory " + tmpDir)
        .isTrue();
        for (Path path : snapshotFiles)
        {
            if (path.toFile().isFile())
            {
                Files.copy(path, tmpDir.toPath().resolve(path.getFileName()));
            }
        }

        // Now truncate the contents of the table
        truncateAndVerify(tableName);

        // Add new data (c, d) to table
        populateTable(session, tableName, Arrays.asList("c", "d"));

        WebClient client = mTLSClient();

        // Upload sstables form the snapshot
        final UUID uploadId = UUID.randomUUID();
        String testUploadRoute = "/api/v1/uploads/" + uploadId + "/keyspaces/" + tableName.keyspace()
                                 + "/tables/" + tableName.tableName() + "/components/";
        for (Path path : snapshotFiles)
        {
            if (path.toFile().isFile())
            {
                Path sstableComponent = tmpDir.toPath().resolve(path.getFileName());
                client.put(server.actualPort(), "127.0.0.1", testUploadRoute + path.getFileName())
                      .sendStream(vertx.fileSystem().openBlocking(sstableComponent.toFile().getAbsolutePath(), new OpenOptions().setRead(true)),
                                  vertxTestContext.succeeding(response -> vertxTestContext.verify(() -> {
                                      assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                                      vertxTestContext.completeNow();
                                  })));
            }
        }
        assertThat(vertxTestContext.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();

        String testImportRoute = "/api/v1/uploads/" + uploadId + "/keyspaces/" + tableName.keyspace()
                                 + "/tables/" + tableName.tableName() + "/import";
        sendRequest(vertxTestContext,
                    () -> client.put(server.actualPort(), "127.0.0.1", testImportRoute),
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
            // use count, because Java driver may fail to deserialize unknown types
            long count = session.execute("SELECT COUNT(*) FROM " + qualifiedTableName).one().getLong(0);
            if (count == 0)
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

    protected QualifiedTableName createTestTableAndPopulate(CassandraSidecarTestContext cassandraTestContext,
                                                            List<String> values)
    {
        QualifiedTableName tableName = createTestTable(
        "CREATE TABLE IF NOT EXISTS %s (id text, value vector<float, 3>, PRIMARY KEY(id))" + WITH_COMPACTION_DISABLED + ";");
        Session session = maybeGetSession();
        populateTable(session, tableName, values);
        return tableName;
    }

    private void populateTable(Session session, QualifiedTableName tableName, List<String> values)
    {
        float index = 1;
        for (String value : values)
        {
            session.execute(String.format("INSERT INTO %s (id, value) VALUES ('%s', [%f, %f, %f]);", tableName, value, index, index, index));
            index++;
        }
    }
}
