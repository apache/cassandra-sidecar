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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.Configuration;
import org.apache.cassandra.sidecar.MainModule;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.common.data.ListSnapshotFilesResponse;
import org.apache.cassandra.sidecar.snapshots.SnapshotUtils;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.cassandra.sidecar.snapshots.SnapshotUtils.mockInstancesConfig;
import static org.assertj.core.api.Assertions.assertThat;


/**
 * Tests for the {@link SnapshotsHandler}
 */
@ExtendWith(VertxExtension.class)
public class SnapshotsHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(SnapshotsHandlerTest.class);
    private Vertx vertx;
    private HttpServer server;
    private Configuration config;
    @TempDir
    File temporaryFolder;

    @BeforeEach
    public void setup() throws InterruptedException, IOException
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule())
                                                        .with(Modules.override(new TestModule())
                                                                     .with(new ListSnapshotTestModule())));
        server = injector.getInstance(HttpServer.class);
        vertx = injector.getInstance(Vertx.class);
        config = injector.getInstance(Configuration.class);

        VertxTestContext context = new VertxTestContext();
        server.listen(config.getPort(), config.getHost(), context.succeedingThenComplete());

        context.awaitCompletion(5, TimeUnit.SECONDS);
        SnapshotUtils.initializeTmpDirectory(temporaryFolder);
    }

    @AfterEach
    void tearDown() throws InterruptedException
    {
        final CountDownLatch closeLatch = new CountDownLatch(1);
        server.close(res -> closeLatch.countDown());
        vertx.close();
        if (closeLatch.await(60, TimeUnit.SECONDS))
            logger.info("Close event received before timeout.");
        else
            logger.error("Close event timed out.");
    }

    @Test
    public void testRouteSucceedsWithKeyspaceAndTableName(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/keyspaces/keyspace1/tables/table1-1234/snapshots/snapshot1";
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
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  ListSnapshotFilesResponse resp = response.bodyAsJson(ListSnapshotFilesResponse.class);
                  assertThat(resp.snapshotFilesInfo().size()).isEqualTo(1);
                  assertThat(resp.snapshotFilesInfo()).contains(fileInfoExpected);
                  assertThat(resp.snapshotFilesInfo()).doesNotContain(fileInfoNotExpected);
                  context.completeNow();
              })));
    }

    @Test
    public void testRouteSucceedsIncludeSecondaryIndexes(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/keyspaces/keyspace1/tables/table1-1234" +
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
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  ListSnapshotFilesResponse resp = response.bodyAsJson(ListSnapshotFilesResponse.class);
                  assertThat(resp.snapshotFilesInfo()).containsAll(fileInfoExpected);
                  assertThat(resp.snapshotFilesInfo()).doesNotContain(fileInfoNotExpected);
                  context.completeNow();
              })));
    }

    @Test
    public void testRouteInvalidSnapshot(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/keyspaces/keyspace1/tables/table1-1234/snapshots/snapshotInvalid";
        client.get(config.getPort(), "localhost", testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  assertThat(response.statusMessage()).isEqualTo(NOT_FOUND.reasonPhrase());
                  context.completeNow();
              })));
    }

    @Test
    void failsWhenKeyspaceContainsInvalidCharacters(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/i_❤_u/tables/table/snapshots/snapshot";
        client.get(config.getPort(), "localhost", "/api/v1" + testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.statusMessage()).isEqualTo(BAD_REQUEST.reasonPhrase());
                  context.completeNow();
              })));
    }

    @ParameterizedTest
    @ValueSource(strings = { "system_schema", "system_traces", "system_distributed", "system", "system_auth",
                             "system_views", "system_virtual_schema" })
    void failsWhenKeyspaceIsForbidden(String forbiddenKeyspace)
    {
        VertxTestContext context = new VertxTestContext();
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + forbiddenKeyspace + "/tables/table/snapshots/snapshot";
        client.get(config.getPort(), "localhost", "/api/v1" + testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.statusMessage()).isEqualTo(BAD_REQUEST.reasonPhrase());
                  context.completeNow();
              })));
    }

    @Test
    void failsWhenTableNameContainsInvalidCharacters(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/ks/tables/i_❤_u/snapshots/snapshot";
        client.get(config.getPort(), "localhost", "/api/v1" + testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.statusMessage()).isEqualTo(BAD_REQUEST.reasonPhrase());
                  context.completeNow();
              })));
    }

    class ListSnapshotTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public InstancesConfig instancesConfig() throws IOException
        {
            return mockInstancesConfig(temporaryFolder.getCanonicalPath());
        }
    }
}
