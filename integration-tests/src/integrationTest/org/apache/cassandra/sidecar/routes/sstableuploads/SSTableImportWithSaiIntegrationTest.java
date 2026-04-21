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

package org.apache.cassandra.sidecar.routes.sstableuploads;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;
import org.apache.cassandra.testing.TestUtils;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.TEST_TABLE_PREFIX;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration tests for SSTable import with SAI (Storage Attached Index) parameters.
 * Validates that SSTables with SAI index metadata can be uploaded and imported via the Sidecar REST API,
 * including proper handling of SAI index files and query parameters ({@code failOnMissingIndex},
 * {@code validateIndexChecksum}).
 */
class SSTableImportWithSaiIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    private static final SimpleCassandraVersion MIN_VERSION_WITH_SAI = SimpleCassandraVersion.create("5.0.0");
    private static final String WITH_COMPACTION_DISABLED = " WITH COMPACTION = {\n" +
                                                           "   'class': 'SizeTieredCompactionStrategy', \n" +
                                                           "   'enabled': 'false' }";

    static final QualifiedName SAI_TABLE = TestUtils.uniqueTestTableFullName(TEST_KEYSPACE, TEST_TABLE_PREFIX);

    @Override
    protected void beforeClusterProvisioning()
    {
        SimpleCassandraVersion version = SimpleCassandraVersion.create(testVersion.version());
        assumeThat(version)
        .as("SAI indexes are only available in Cassandra 5.0 and later")
        .isGreaterThanOrEqualTo(MIN_VERSION_WITH_SAI);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(SAI_TABLE,
                        "CREATE TABLE IF NOT EXISTS %s (id text, value text, PRIMARY KEY(id))"
                        + WITH_COMPACTION_DISABLED + ";");

        cluster.schemaChangeIgnoringStoppedInstances(
        String.format("CREATE CUSTOM INDEX IF NOT EXISTS %s_sai_idx ON %s (value) " +
                       "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';",
                       SAI_TABLE.table(), SAI_TABLE));

        cluster.schemaChangeIgnoringStoppedInstances(
        String.format("INSERT INTO %s (id, value) VALUES ('a', 'val_a');", SAI_TABLE));
        cluster.schemaChangeIgnoringStoppedInstances(
        String.format("INSERT INTO %s (id, value) VALUES ('b', 'val_b');", SAI_TABLE));
    }

    @Test
    void testSSTableImportWithSaiIndexParams() throws Exception
    {
        String snapshotName = SAI_TABLE.table() + "-snapshot";

        // Take snapshot
        cluster.get(1).nodetoolResult("snapshot",
                                       "--tag", snapshotName,
                                       "--table", SAI_TABLE.table(),
                                       "--", SAI_TABLE.keyspace())
               .asserts().success();

        // Find snapshot files on the filesystem
        List<Path> snapshotFiles = findChildFile("127.0.0.1", SAI_TABLE.keyspace(), snapshotName);
        assertThat(snapshotFiles).isNotEmpty();

        List<Path> filesToUpload = snapshotFiles.stream()
                                                .filter(p -> p.toFile().isFile())
                                                .collect(Collectors.toList());
        assertThat(filesToUpload).isNotEmpty();

        // SAI index files have '+' in their filename
        assertThat(filesToUpload.stream().anyMatch(p -> p.getFileName().toString().contains("+")))
        .withFailMessage("Expected at least one snapshot file with '+' in its name (SAI index file)")
        .isTrue();

        // Upload snapshot files via the REST upload endpoint
        UUID uploadId = UUID.randomUUID();
        for (Path path : filesToUpload)
        {
            String fileName = path.getFileName().toString();
            // URLEncoder.encode encodes space as '+', which is correct for query params but not path segments.
            // For path segments, '+' is a literal character, so we must encode it as %2B.
            String encodedFileName = URLEncoder.encode(fileName, StandardCharsets.UTF_8)
                                               .replace("+", "%2B");

            String uploadRoute = "/api/v1/uploads/" + uploadId + "/keyspaces/" + SAI_TABLE.keyspace()
                                 + "/tables/" + SAI_TABLE.table() + "/components/" + encodedFileName;
            Buffer fileContent = Buffer.buffer(Files.readAllBytes(path));
            HttpResponse<Buffer> uploadResponse = getBlocking(
            trustedClient().put(serverWrapper.serverPort, "127.0.0.1", uploadRoute)
                           .sendBuffer(fileContent));
            assertThat(uploadResponse.statusCode())
            .withFailMessage("Upload failed for " + fileName + " with status " + uploadResponse.statusCode()
                             + ": " + uploadResponse.bodyAsString())
            .isEqualTo(HttpResponseStatus.OK.code());
        }

        // Truncate table and wait until empty
        cluster.schemaChangeIgnoringStoppedInstances(String.format("TRUNCATE TABLE %s", SAI_TABLE));
        loopAssert(30, () -> {
            Object[][] rows = cluster.getFirstRunningInstance()
                                     .coordinator()
                                     .execute(String.format("SELECT * FROM %s", SAI_TABLE),
                                              ConsistencyLevel.LOCAL_QUORUM);
            assertThat(rows == null || rows.length == 0)
            .withFailMessage("Table should be empty after truncation")
            .isTrue();
        });

        // Import with SAI index query params, poll until completed
        String importRoute = "/api/v1/uploads/" + uploadId + "/keyspaces/" + SAI_TABLE.keyspace()
                             + "/tables/" + SAI_TABLE.table() + "/import";
        loopAssert(300, 1000, () -> {
            HttpResponse<Buffer> importResponse = getBlocking(
            trustedClient().put(serverWrapper.serverPort, "127.0.0.1", importRoute)
                           .addQueryParam("failOnMissingIndex", "true")
                           .addQueryParam("validateIndexChecksum", "true")
                           .send());
            assertThat(importResponse.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
        });

        // Verify SAI index component files exist on the filesystem.
        // No new data was written after truncate, so any SAI files must have come from the imported snapshot.
        InstanceMetadata instance = serverWrapper.injector.getInstance(InstancesMetadata.class)
                                                          .instanceFromHost("127.0.0.1");
        String dataDir = instance.dataDirs().get(0);
        Path keyspacePath = Paths.get(dataDir, SAI_TABLE.keyspace());
        Path tableDir;
        try (Stream<Path> dirs = Files.list(keyspacePath))
        {
            tableDir = dirs.filter(dir -> dir.getFileName().toString().startsWith(SAI_TABLE.table()))
                           .findFirst()
                           .orElseThrow(() -> new AssertionError("Table directory not found"));
        }

        List<Path> saiFiles;
        try (Stream<Path> files = Files.list(tableDir))
        {
            saiFiles = files.filter(f -> f.getFileName().toString().toUpperCase().contains("SAI"))
                            .collect(Collectors.toList());
        }
        assertThat(saiFiles)
        .withFailMessage("Expected SAI index component files on the filesystem after import")
        .isNotEmpty();

        // Verify imported data is present
        assertThat(queryIds()).containsExactlyInAnyOrder("a", "b");

        // Add new data after import
        cluster.schemaChangeIgnoringStoppedInstances(
        String.format("INSERT INTO %s (id, value) VALUES ('c', 'val_c');", SAI_TABLE));
        cluster.schemaChangeIgnoringStoppedInstances(
        String.format("INSERT INTO %s (id, value) VALUES ('d', 'val_d');", SAI_TABLE));

        // Verify all data (imported + new) is present
        assertThat(queryIds()).containsExactlyInAnyOrder("a", "b", "c", "d");
    }

    private List<String> queryIds()
    {
        Object[][] rows = cluster.getFirstRunningInstance()
                                  .coordinator()
                                  .execute(String.format("SELECT id FROM %s", SAI_TABLE),
                                           ConsistencyLevel.LOCAL_QUORUM);
        return Stream.of(rows)
                     .map(row -> (String) row[0])
                     .collect(Collectors.toList());
    }
}
