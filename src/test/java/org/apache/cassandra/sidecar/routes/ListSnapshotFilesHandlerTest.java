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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
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

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.cassandra.sidecar.snapshots.SnapshotUtils.mockInstancesConfig;
import static org.assertj.core.api.Assertions.assertThat;


@ExtendWith(VertxExtension.class)
public class ListSnapshotFilesHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(ListSnapshotFilesHandlerTest.class);
    private static Vertx vertx;
    private static HttpServer server;
    private static Configuration config;
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
    void testRouteSucceeds(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/snapshots/snapshot1";
        ListSnapshotFilesResponse.FileInfo fileInfoExpected =
        new ListSnapshotFilesResponse.FileInfo(11,
                                               "localhost",
                                               9043,
                                               0,
                                               "snapshot1",
                                               "keyspace1",
                                               "table1-1234",
                                               "1.db");
        ListSnapshotFilesResponse.FileInfo fileInfoNotExpected =
        new ListSnapshotFilesResponse.FileInfo(11,
                                               "localhost",
                                               9043,
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
                  assertThat(resp.getSnapshotFilesInfo()).contains(fileInfoExpected);
                  assertThat(resp.getSnapshotFilesInfo()).doesNotContain(fileInfoNotExpected);
                  context.completeNow();
              })));
    }

    @Test
    public void testRouteSucceedsWithKeyspaceAndTableName(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/keyspace/keyspace1/table/table1-1234/snapshots/snapshot1";
        ListSnapshotFilesResponse.FileInfo fileInfoExpected =
        new ListSnapshotFilesResponse.FileInfo(11,
                                               "localhost",
                                               9043,
                                               0,
                                               "snapshot1",
                                               "keyspace1",
                                               "table1-1234",
                                               "1.db");
        ListSnapshotFilesResponse.FileInfo fileInfoNotExpected =
        new ListSnapshotFilesResponse.FileInfo(11,
                                               "localhost",
                                               9043,
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
    public void testRouteInvalidSnapshot(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/snapshots/snapshotInvalid";
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
        @Override
        protected void configure()
        {
            try
            {
                bind(InstancesConfig.class).toInstance(mockInstancesConfig(temporaryFolder.getCanonicalPath()));
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}