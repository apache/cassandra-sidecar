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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.common.data.ListSnapshotFilesResponse;
import org.apache.cassandra.sidecar.snapshots.SnapshotUtils;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.cassandra.sidecar.snapshots.SnapshotUtils.mockInstancesConfig;
import static org.assertj.core.api.Assertions.assertThat;


/**
 * Tests for the {@link ListSnapshotFilesHandler}
 */
@ExtendWith(VertxExtension.class)
class ListSnapshotFilesHandlerTest extends AbstractHandlerTest
{
    @TempDir
    File temporaryFolder;

    @Override
    protected Module initializeCustomModule()
    {
        return new ListSnapshotTestModule();
    }

    @Override
    protected void afterInitialized() throws IOException
    {
        SnapshotUtils.initializeTmpDirectory(temporaryFolder);
    }

    @Test
    public void testRouteSucceedsWithKeyspaceAndTableName(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/keyspace/keyspace1/table/table1-1234/snapshots/snapshot1";
        ListSnapshotFilesResponse.FileInfo fileInfoExpected =
        new ListSnapshotFilesResponse.FileInfo(11,
                                               "localhost",
                                               6475,
                                               0,
                                               "snapshot1",
                                               "keyspace1",
                                               "table1-1234",
                                               "1.db");
        ListSnapshotFilesResponse.FileInfo fileInfoNotExpected =
        new ListSnapshotFilesResponse.FileInfo(11,
                                               "localhost",
                                               6475,
                                               0,
                                               "snapshot1",
                                               "keyspace1",
                                               "table1-1234",
                                               "2.db");

        client.get(config.getPort(), "localhost", testRoute)
              .send(context.succeeding(response -> context.verify(() ->
              {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  ListSnapshotFilesResponse resp = response.bodyAsJson(ListSnapshotFilesResponse.class);
                  assertThat(resp.getSnapshotFilesInfo().size()).isEqualTo(1);
                  assertThat(resp.getSnapshotFilesInfo()).contains(fileInfoExpected);
                  assertThat(resp.getSnapshotFilesInfo()).doesNotContain(fileInfoNotExpected);
                  context.completeNow();
              })));
    }

    @Test
    public void testRouteSucceedsIncludeSecondaryIndexes(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/keyspace/keyspace1/table/table1-1234" +
                           "/snapshots/snapshot1?includeSecondaryIndexFiles=true";
        List<ListSnapshotFilesResponse.FileInfo> fileInfoExpected = Arrays.asList(
        new ListSnapshotFilesResponse.FileInfo(11,
                                               "localhost",
                                               6475,
                                               0,
                                               "snapshot1",
                                               "keyspace1",
                                               "table1-1234",
                                               "1.db"),
        new ListSnapshotFilesResponse.FileInfo(0,
                                               "localhost",
                                               6475,
                                               0,
                                               "snapshot1",
                                               "keyspace1",
                                               "table1-1234",
                                               ".index/secondary.db")
        );
        ListSnapshotFilesResponse.FileInfo fileInfoNotExpected =
        new ListSnapshotFilesResponse.FileInfo(11,
                                               "localhost",
                                               6475,
                                               0,
                                               "snapshot1",
                                               "keyspace1",
                                               "table1-1234",
                                               "2.db");

        client.get(config.getPort(), "localhost", testRoute)
              .send(context.succeeding(response -> context.verify(() ->
              {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  ListSnapshotFilesResponse resp = response.bodyAsJson(ListSnapshotFilesResponse.class);
                  assertThat(resp.getSnapshotFilesInfo()).containsAll(fileInfoExpected);
                  assertThat(resp.getSnapshotFilesInfo()).doesNotContain(fileInfoNotExpected);
                  context.completeNow();
              })));
    }

    @Test
    public void testRouteInvalidSnapshot(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/keyspace/keyspace1/table/table1-1234/snapshots/snapshotInvalid";
        client.get(config.getPort(), "localhost", testRoute)
              .send(context.succeeding(response -> context.verify(() ->
              {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  assertThat(response.statusMessage()).isEqualTo(NOT_FOUND.reasonPhrase());
                  context.completeNow();
              })));
    }

    class ListSnapshotTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public InstancesConfig getInstancesConfig() throws IOException
        {
            return mockInstancesConfig(temporaryFolder.getCanonicalPath());
        }
    }
}
