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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.PARTIAL_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for StreamSSTableComponent
 */
@ExtendWith(VertxExtension.class)
public class StreamSSTableComponentHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(StreamSSTableComponentHandlerTest.class);
    static final String TEST_KEYSPACE = "TestKeyspace";
    static final String TEST_TABLE = "TestTable";

    private Vertx vertx;
    private Server server;

    @BeforeEach
    void setUp() throws InterruptedException
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(new TestModule()));
        server = injector.getInstance(Server.class);
        vertx = injector.getInstance(Vertx.class);

        VertxTestContext context = new VertxTestContext();
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);

        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() throws InterruptedException
    {
        final CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            logger.info("Close event received before timeout.");
        else
            logger.error("Close event timed out.");
    }

    @Test
    void testRoute(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "/snapshots" +
                           "/TestSnapshot/components/nb-1-big-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .as(BodyCodec.buffer())
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  assertThat(response.bodyAsString()).isEqualTo("data");
                  context.completeNow();
              })));
    }

    @Test
    void failsWhenKeyspaceContainsInvalidCharacters(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/i_❤_u/tables/table/snapshots/snapshot/components/component-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
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
        String testRoute = "/keyspaces/" + forbiddenKeyspace + "/tables/table/snapshots/snapshot" +
                           "/components/component-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(FORBIDDEN.code());
                  assertThat(response.statusMessage()).isEqualTo(FORBIDDEN.reasonPhrase());
                  context.completeNow();
              })));
    }

    @Test
    void testKeyspaceNotFound(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/random/tables/" + TEST_TABLE + "/snapshots" +
                           "/TestSnapshot/components/nb-1-big-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  context.completeNow();
              })));
    }

    @Test
    void testSnapshotNotFound(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "/snapshots" +
                           "/random/components/nb-1-big-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  context.completeNow();
              })));
    }

    @Test
    void testForbiddenKeyspace(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/system/tables/" + TEST_TABLE + "/snapshots" +
                           "/TestSnapshot/components/nb-1-big-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(FORBIDDEN.code());
                  assertThat(response.statusMessage()).isEqualTo(FORBIDDEN.reasonPhrase());
                  context.completeNow();
              })));
    }

    @Test
    void testIncorrectKeyspaceFormat(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/k*s/tables/" + TEST_TABLE + "/snapshots" +
                           "/TestSnapshot/components/nb-1-big-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.statusMessage()).isEqualTo(BAD_REQUEST.reasonPhrase());
                  context.completeNow();
              })));
    }

    @Test
    void testIncorrectComponentFormat(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "/snapshots" +
                           "/TestSnapshot/components/" + TEST_KEYSPACE + "-" + TEST_TABLE + "-Data...db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.statusMessage()).isEqualTo(BAD_REQUEST.reasonPhrase());
                  context.completeNow();
              })));
    }

    @Test
    void testAccessDeniedToCertainComponents(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "/snapshots" +
                           "/TestSnapshot/components/" + TEST_KEYSPACE + "-" + TEST_TABLE + "-Digest.crc32d";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
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
        String testRoute = "/keyspaces/" + TEST_KEYSPACE +
                           "/tables/i_❤_u/snapshots/snap/components/component-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.statusMessage()).isEqualTo(BAD_REQUEST.reasonPhrase());
                  context.completeNow();
              })));
    }

    @ParameterizedTest
    @ValueSource(strings = { "slash/is-not-allowed", "null-char\0-is-not-allowed", "../../../etc/passwd" })
    void failsWhenSnapshotNameContainsInvalidCharacters(String invalidFileName)
    {
        VertxTestContext context = new VertxTestContext();
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/TestTable/snapshots/" +
                           invalidFileName + "/components/component-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.statusMessage()).isEqualTo(BAD_REQUEST.reasonPhrase());
                  context.completeNow();
              })));
    }

    @ParameterizedTest
    @ValueSource(strings = { "i_❤_u.db", "this-is-not-allowed.jar", "../../../etc/passwd.db",
                             "../not-an-index-file-Data.db" })
    void failsWhenComponentNameContainsInvalidCharacters(String invalidComponentName)
    {
        VertxTestContext context = new VertxTestContext();
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "/snapshots/snap/components/" +
                           invalidComponentName;
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(FORBIDDEN.code());
                  assertThat(response.statusMessage()).isEqualTo(FORBIDDEN.reasonPhrase());
                  context.completeNow();
              })));
    }

    @Test
    void testPartialTableName(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/TestTable/snapshots/TestSnapshot/components" +
                           "/nb-1-big-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=0-")
              .as(BodyCodec.buffer())
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  assertThat(response.bodyAsString()).isEqualTo("data");
                  context.completeNow();
              })));
    }

    @Test
    void testInvalidRange(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "/snapshots" +
                           "/TestSnapshot/components/nb-1-big-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=4-3")
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(REQUESTED_RANGE_NOT_SATISFIABLE.code());
                  context.completeNow();
              })));
    }

    @Test
    void testRangeExceeds(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "/snapshots" +
                           "/TestSnapshot/components/nb-1-big-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=5-9")
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(REQUESTED_RANGE_NOT_SATISFIABLE.code());
                  context.completeNow();
              })));
    }

    @Test
    void testPartialRangeExceeds(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "/snapshots" +
                           "/TestSnapshot/components/nb-1-big-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=5-")
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(REQUESTED_RANGE_NOT_SATISFIABLE.code());
                  context.completeNow();
              })));
    }

    @Test
    void testRangeBoundaryExceeds(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "/snapshots" +
                           "/TestSnapshot/components/nb-1-big-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=0-999999")
              .as(BodyCodec.buffer())
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  assertThat(response.bodyAsString()).isEqualTo("data");
                  context.completeNow();
              })));
    }

    @Test
    void testPartialRangeStreamed(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "/snapshots" +
                           "/TestSnapshot/components/nb-1-big-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=0-2") // 3 bytes streamed
              .as(BodyCodec.buffer())
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(PARTIAL_CONTENT.code());
                  assertThat(response.bodyAsString()).isEqualTo("dat");
                  context.completeNow();
              })));
    }

    @Test
    void testSuffixRange(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "/snapshots" +
                           "/TestSnapshot/components/nb-1-big-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=-2") // last 2 bytes streamed
              .as(BodyCodec.buffer())
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(PARTIAL_CONTENT.code());
                  assertThat(response.bodyAsString()).isEqualTo("ta");
                  context.completeNow();
              })));
    }

    @Test
    void testSuffixRangeExceeds(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "/snapshots" +
                           "/TestSnapshot/components/nb-1-big-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=-5")
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  assertThat(response.getHeader(HttpHeaderNames.CONTENT_LENGTH.toString()))
                  .describedAs("Server should shrink the range to the file length")
                  .isEqualTo("4");
                  context.completeNow();
              })));
    }

    @Test
    void testInvalidRangeUnit(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "/snapshots" +
                           "/TestSnapshot/components/nb-1-big-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bits=0-2")
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(REQUESTED_RANGE_NOT_SATISFIABLE.code());
                  context.completeNow();
              })));
    }

    @Test
    void testStreamingFromSpecificInstance(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "/" +
                           "snapshots/TestSnapshot/components/nb-1-big-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute + "?instanceId=2")
              .as(BodyCodec.buffer())
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  assertThat(response.bodyAsString()).isEqualTo("data");
                  context.completeNow();
              })));
    }
}
