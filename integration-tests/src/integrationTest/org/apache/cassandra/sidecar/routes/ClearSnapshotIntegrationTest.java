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

package org.apache.cassandra.sidecar.routes;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpResponseExpectation;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for clear snapshot endpoint
 */
class ClearSnapshotIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    private static final String SNAPSHOT_ROUTE_TEMPLATE = "/api/v1/keyspaces/%s/tables/%s/snapshots/%s";

    @Test
    void deleteSnapshotFailsWhenKeyspaceDoesNotExist()
    {
        String testRoute = String.format(SNAPSHOT_ROUTE_TEMPLATE, "non_existent", "testtable", "my-snapshot");
        assertNotFoundOnDeleteSnapshot(testRoute);
    }

    @Test
    void deleteSnapshotFailsWhenTableDoesNotExist()
    {
        String testRoute = String.format(SNAPSHOT_ROUTE_TEMPLATE, "testkeyspace", "non_existent", "my-snapshot");
        assertNotFoundOnDeleteSnapshot(testRoute);
    }

    @Test
    void testDeleteSnapshotEndpoint()
    {
        testSnapshotCreateAndDelete(new QualifiedName("testkeyspace", "testtable"));
    }

    @Test
    void testDeleteSnapshotWhenQuotedTableExists()
    {
        testSnapshotCreateAndDelete(new QualifiedName("testkeyspace", "QuotedTable", false, true));
    }

    @Test
    void testDeleteSnapshotWhenQuotedKeyspaceExists()
    {
        testSnapshotCreateAndDelete(new QualifiedName("QuotedKeyspace", "testtable", true, false));
    }

    @Test
    void testDeleteSnapshotWhenBothKeyspaceAndTableAreQuoted()
    {
        testSnapshotCreateAndDelete(new QualifiedName("QuotedKeyspace", "QuotedTable", true, true));
    }

    private void testSnapshotCreateAndDelete(QualifiedName tableName)
    {
        String snapshotName = "my-snapshot-" + UUID.randomUUID();
        String testRoute = String.format(SNAPSHOT_ROUTE_TEMPLATE,
                                         tableName.maybeQuotedKeyspace(),
                                         tableName.maybeQuotedTable(),
                                         snapshotName);

        // Create the snapshot
        HttpResponse<Buffer> createResponse = getBlocking(
        trustedClient().put(serverWrapper.serverPort, "127.0.0.1", testRoute)
                       .send());
        assertThat(createResponse.statusCode()).isEqualTo(OK.code());

        // Verify snapshot files exist
        List<Path> snapshotFiles = findChildFile("127.0.0.1", tableName.keyspace(), snapshotName);
        assertThat(snapshotFiles).isNotEmpty();

        // Delete the snapshot
        HttpResponse<Buffer> deleteResponse = getBlocking(
        trustedClient().delete(serverWrapper.serverPort, "127.0.0.1", testRoute)
                       .send());
        assertThat(deleteResponse.statusCode()).isEqualTo(OK.code());

        // Verify snapshot files were deleted
        List<Path> snapshotFilesAfterDelete = findChildFile("127.0.0.1", tableName.keyspace(), snapshotName);
        assertThat(snapshotFilesAfterDelete).isEmpty();
    }

    private void assertNotFoundOnDeleteSnapshot(String testRoute)
    {
        HttpResponse<Buffer> response = getBlocking(trustedClient().delete(serverWrapper.serverPort, "127.0.0.1", testRoute)
                                                                   .send()
                                                                   .expecting(HttpResponseExpectation.SC_NOT_FOUND));
        assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace("testkeyspace", DC1_RF1);
        createTestKeyspace("\"QuotedKeyspace\"", DC1_RF1);

        createTestTable(new QualifiedName("testkeyspace", "testtable"),
                        "CREATE TABLE %s (id text PRIMARY KEY, name text)");
        createTestTable(new QualifiedName("testkeyspace", "QuotedTable", false, true),
                        "CREATE TABLE %s (id text PRIMARY KEY, name text)");

        createTestTable(new QualifiedName("QuotedKeyspace", "testtable", true, false),
                        "CREATE TABLE %s (id text PRIMARY KEY, name text)");
        createTestTable(new QualifiedName("QuotedKeyspace", "QuotedTable", true, true),
                        "CREATE TABLE %s (id text PRIMARY KEY, name text)");

        cluster.schemaChangeIgnoringStoppedInstances(
        "INSERT INTO testkeyspace.testtable (id, name) VALUES ('1', 'Francisco')");
        cluster.schemaChangeIgnoringStoppedInstances(
        "INSERT INTO testkeyspace.testtable (id, name) VALUES ('2', 'Saranya')");
        cluster.schemaChangeIgnoringStoppedInstances(
        "INSERT INTO testkeyspace.testtable (id, name) VALUES ('3', 'Yifan')");

        cluster.schemaChangeIgnoringStoppedInstances(
        "INSERT INTO testkeyspace.\"QuotedTable\" (id, name) VALUES ('1', 'Francisco')");
        cluster.schemaChangeIgnoringStoppedInstances(
        "INSERT INTO testkeyspace.\"QuotedTable\" (id, name) VALUES ('2', 'Saranya')");
        cluster.schemaChangeIgnoringStoppedInstances(
        "INSERT INTO testkeyspace.\"QuotedTable\" (id, name) VALUES ('3', 'Yifan')");

        cluster.schemaChangeIgnoringStoppedInstances(
        "INSERT INTO \"QuotedKeyspace\".testtable (id, name) VALUES ('1', 'Francisco')");
        cluster.schemaChangeIgnoringStoppedInstances(
        "INSERT INTO \"QuotedKeyspace\".testtable (id, name) VALUES ('2', 'Saranya')");
        cluster.schemaChangeIgnoringStoppedInstances(
        "INSERT INTO \"QuotedKeyspace\".testtable (id, name) VALUES ('3', 'Yifan')");

        cluster.schemaChangeIgnoringStoppedInstances(
        "INSERT INTO \"QuotedKeyspace\".\"QuotedTable\" (id, name) VALUES ('1', 'Francisco')");
        cluster.schemaChangeIgnoringStoppedInstances(
        "INSERT INTO \"QuotedKeyspace\".\"QuotedTable\" (id, name) VALUES ('2', 'Saranya')");
        cluster.schemaChangeIgnoringStoppedInstances(
        "INSERT INTO \"QuotedKeyspace\".\"QuotedTable\" (id, name) VALUES ('3', 'Yifan')");
    }
}
