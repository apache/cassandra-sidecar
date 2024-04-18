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

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.management.InstanceNotFoundException;

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
import org.apache.cassandra.sidecar.common.TableOperations;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetricsImpl;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.PARTIAL_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for StreamSSTableComponent
 */
@ExtendWith(VertxExtension.class)
class StreamSSTableComponentHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(StreamSSTableComponentHandlerTest.class);
    static final String TEST_KEYSPACE = "TestKeyspace";
    static final String TEST_TABLE = "TestTable";
    static final String TEST_TABLE_ID = "54ea95cebba24e0aa9bee428e5d7160b";

    private Vertx vertx;
    private Server server;
    TestModule testModule = new TestModule();

    @BeforeEach
    void setUp() throws InterruptedException
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(testModule));
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
        registry().removeMatching((name, metric) -> true);
        registry(1).removeMatching((name, metric) -> true);
        registry(2).removeMatching((name, metric) -> true);
        if (closeLatch.await(60, TimeUnit.SECONDS))
            logger.info("Close event received before timeout.");
        else
            logger.error("Close event timed out.");
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testRoute(boolean useLegacyEndpoint, VertxTestContext context) throws IOException
    {
        configureTableDirectoryLocations();
        WebClient client = WebClient.create(vertx);
        String testRoute = testRoute(useLegacyEndpoint);
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .as(BodyCodec.buffer())
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  assertThat(response.bodyAsString()).isEqualTo("data");
                  vertx.setTimer(100, v -> {
                      assertThat(instanceMetrics(1).streamSSTable()
                                                   .forComponent("Data.db").bytesStreamedRate.metric.getCount()).isEqualTo(4);
                      assertThat(instanceMetrics(1).streamSSTable().totalBytesStreamedRate.metric.getCount()).isEqualTo(4);
                      context.completeNow();
                  });
              })));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void failsWhenKeyspaceContainsInvalidCharacters(boolean useLegacyEndpoint, VertxTestContext context) throws URISyntaxException
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = testRoute(useLegacyEndpoint, "i_❤_u", TEST_TABLE);
        client.get(server.actualPort(), "localhost", "/api/v1" + new URI(testRoute).toASCIIString())
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.statusMessage()).isEqualTo(BAD_REQUEST.reasonPhrase());
                  context.completeNow();
              })));
    }

    @ParameterizedTest
    @ValueSource(strings = { "system_schema", "system_traces", "system_distributed", "system", "system_auth",
                             "system_views", "system_virtual_schema" })
    void failsWhenKeyspaceIsForbidden(String forbiddenKeyspace) throws InterruptedException
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
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void testKeyspaceNotFound(VertxTestContext context) throws IOException
    {
        TableOperations mockTableOperations = mock(TableOperations.class);
        when(mockTableOperations.getDataPaths("random", TEST_TABLE))
        .thenThrow(new UndeclaredThrowableException(new InstanceNotFoundException("keyspace not found")));
        testModule.delegate.setTableOperations(mockTableOperations);

        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/random/tables/" + TEST_TABLE + "/snapshots" +
                           "/TestSnapshot/components/nb-1-big-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  assertThat(response.bodyAsJsonObject().getString("message")).isEqualTo("keyspace/table combination not found");
                  context.completeNow();
              })));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testSnapshotNotFound(boolean useLegacyEndpoint, VertxTestContext context) throws IOException
    {
        configureTableDirectoryLocations();
        WebClient client = WebClient.create(vertx);
        String testRoute = testRoute(useLegacyEndpoint, TEST_KEYSPACE, TEST_TABLE, TEST_TABLE_ID,
                                     "random", "nb-1-big-Data.db");
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  context.completeNow();
              })));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testForbiddenKeyspace(boolean useLegacyEndpoint, VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = testRoute(useLegacyEndpoint, "system", TEST_TABLE);
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(FORBIDDEN.code());
                  assertThat(response.statusMessage()).isEqualTo(FORBIDDEN.reasonPhrase());
                  context.completeNow();
              })));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testIncorrectKeyspaceFormat(boolean useLegacyEndpoint, VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = testRoute(useLegacyEndpoint, "k*s", TEST_TABLE);
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.statusMessage()).isEqualTo(BAD_REQUEST.reasonPhrase());
                  context.completeNow();
              })));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testIncorrectComponentFormat(boolean useLegacyEndpoint, VertxTestContext context) throws IOException, URISyntaxException
    {
        configureTableDirectoryLocations();
        WebClient client = WebClient.create(vertx);
        String testRoute = testRoute(useLegacyEndpoint, TEST_KEYSPACE, TEST_TABLE, TEST_TABLE_ID,
                                     "TestSnapshot", TEST_KEYSPACE + "-" + TEST_TABLE + "-Data...db");
        client.get(server.actualPort(), "localhost", "/api/v1" + new URI(testRoute).toASCIIString())
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.statusMessage()).isEqualTo(BAD_REQUEST.reasonPhrase());
                  context.completeNow();
              })));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testAccessDeniedToCertainComponents(boolean useLegacyEndpoint, VertxTestContext context) throws IOException
    {
        configureTableDirectoryLocations();
        WebClient client = WebClient.create(vertx);
        String testRoute = testRoute(useLegacyEndpoint, TEST_KEYSPACE, TEST_TABLE, TEST_TABLE_ID,
                                     "TestSnapshot", TEST_KEYSPACE + "-" + TEST_TABLE + "-Digest.crc32d");
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.statusMessage()).isEqualTo(BAD_REQUEST.reasonPhrase());
                  context.completeNow();
              })));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void failsWhenTableNameContainsInvalidCharacters(boolean useLegacyEndpoint, VertxTestContext context) throws URISyntaxException
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = testRoute(useLegacyEndpoint, TEST_KEYSPACE, "i_❤_u");
        client.get(server.actualPort(), "localhost", "/api/v1" + new URI(testRoute).toASCIIString())
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.statusMessage()).isEqualTo(BAD_REQUEST.reasonPhrase());
                  context.completeNow();
              })));
    }

    @ParameterizedTest
    @ValueSource(strings = { "null-char\0-is-not-allowed" })
    void failsWhenSnapshotNameContainsInvalidCharacters(String invalidSnapshotName) throws InterruptedException, IOException
    {
        configureTableDirectoryLocations();
        VertxTestContext context = new VertxTestContext();
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/TestTable/snapshots/" +
                           invalidSnapshotName + "/components/component-Data.db";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .send()
              .onSuccess(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.statusMessage()).isEqualTo(BAD_REQUEST.reasonPhrase());
                  assertThat(response.bodyAsJsonObject().getString("message"))
                  .isEqualTo("Invalid characters in snapshot name: " + invalidSnapshotName);
                  context.completeNow();
              })
              .onFailure(context::failNow);
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = { "slash/is-not-allowed", "../../../etc/passwd" })
    void failsWhenSnapshotNameHasPathTraversalAttack(String invalidSnapshotName) throws InterruptedException, URISyntaxException
    {
        VertxTestContext context = new VertxTestContext();
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/TestTable/snapshots/" +
                           invalidSnapshotName + "/components/component-Data.db";
        String url = new URI("/api/v1" + testRoute).toASCIIString();
        client.get(server.actualPort(), "localhost", url)
              .send()
              .onSuccess(response -> {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  assertThat(response.statusMessage()).isEqualTo(NOT_FOUND.reasonPhrase());
                  context.completeNow();
              })
              .onFailure(context::failNow);
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = { "i_❤_u.db", "this-is-not-allowed.jar" })
    void failsWhenComponentNameContainsInvalidCharacters(String invalidComponentName) throws InterruptedException, URISyntaxException, IOException
    {
        configureTableDirectoryLocations();
        VertxTestContext context = new VertxTestContext();
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "/snapshots/snap/components/" +
                           invalidComponentName;
        String url = new URI("/api/v1" + testRoute).toASCIIString();
        client.get(server.actualPort(), "localhost", url)
              .send()
              .onSuccess(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.statusMessage()).isEqualTo(BAD_REQUEST.reasonPhrase());
                  assertThat(response.bodyAsJsonObject().getString("message")).isEqualTo("Invalid component name: " + invalidComponentName);
                  context.completeNow();
              })
              .onFailure(context::failNow);
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = { "../../../etc/passwd.db", "../not-an-index-file-Data.db" })
    void failsWhenComponentNameHasPathTraversalAttack(String invalidComponentName) throws InterruptedException, URISyntaxException
    {
        // 404 is expected here as adding `/` means that a different route is created
        VertxTestContext context = new VertxTestContext();
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "/snapshots/snap/components/" +
                           invalidComponentName;
        String url = new URI("/api/v1" + testRoute).toASCIIString();
        client.get(server.actualPort(), "localhost", url)
              .send()
              .onSuccess(response -> {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  assertThat(response.statusMessage()).isEqualTo(NOT_FOUND.reasonPhrase());
                  context.completeNow();
              })
              .onFailure(context::failNow);
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = { "i_❤_u.db", "this_is_not_allowed.jar", "not_a_valid_hex", "aag" })
    void failsWhenTableIdContainsInvalidCharacters(String invalidTableId) throws InterruptedException, URISyntaxException
    {
        VertxTestContext context = new VertxTestContext();
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "-" + invalidTableId +
                           "/snapshots/snap/components/nb-1-big-Data.db?dataDirectoryIndex=0";
        String url = new URI("/api/v1" + testRoute).toASCIIString();
        client.get(server.actualPort(), "localhost", url)
              .send()
              .onSuccess(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.statusMessage()).isEqualTo(BAD_REQUEST.reasonPhrase());
                  assertThat(response.bodyAsJsonObject().getString("message")).isEqualTo("Invalid characters in table id: " + invalidTableId);
                  context.completeNow();
              })
              .onFailure(context::failNow);
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = { "53464aa75e6b3d8a84c4e87abbdcbbefa", "53464aa75e6b3d8a84c4e87abbdcbbefa5e6b3d8a84" })
    void failsWhenTableIdExceedsLength(String invalidTableId) throws InterruptedException, URISyntaxException
    {
        VertxTestContext context = new VertxTestContext();
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "-" + invalidTableId +
                           "/snapshots/snap/components/nb-1-big-Data.db?dataDirectoryIndex=0";
        String url = new URI("/api/v1" + testRoute).toASCIIString();
        client.get(server.actualPort(), "localhost", url)
              .send()
              .onSuccess(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.statusMessage()).isEqualTo(BAD_REQUEST.reasonPhrase());
                  assertThat(response.bodyAsJsonObject().getString("message")).isEqualTo("tableId cannot be longer than 32 characters");
                  context.completeNow();
              })
              .onFailure(context::failNow);
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 2, 100 })
    void failsWhenDataDirectoryIndexIsOutOfRange(int dataDirectoryIndex) throws InterruptedException, URISyntaxException
    {
        VertxTestContext context = new VertxTestContext();
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "-abc123" +
                           "/snapshots/snap/components/nb-1-big-Data.db?dataDirectoryIndex=" + dataDirectoryIndex;
        String url = new URI("/api/v1" + testRoute).toASCIIString();
        client.get(server.actualPort(), "localhost", url)
              .send()
              .onSuccess(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.statusMessage()).isEqualTo(BAD_REQUEST.reasonPhrase());
                  assertThat(response.bodyAsJsonObject().getString("message")).isEqualTo("Invalid data directory index: " + dataDirectoryIndex);
                  context.completeNow();
              })
              .onFailure(context::failNow);
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testPartialTableName(boolean useLegacyEndpoint, VertxTestContext context) throws IOException
    {
        configureTableDirectoryLocations();
        WebClient client = WebClient.create(vertx);
        String testRoute = testRoute(useLegacyEndpoint);
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=0-")
              .as(BodyCodec.buffer())
              .send(context.succeeding(response -> context.verify(() -> {
                  vertx.setTimer(100, v -> {
                      assertThat(response.statusCode()).isEqualTo(OK.code());
                      assertThat(response.bodyAsString()).isEqualTo("data");
                      assertThat(instanceMetrics(1).streamSSTable()
                                                   .forComponent("Data.db").bytesStreamedRate.metric.getCount()).isEqualTo(4);
                      assertThat(instanceMetrics(1).streamSSTable().totalBytesStreamedRate.metric.getCount()).isEqualTo(4);
                      context.completeNow();
                  });
              })));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testInvalidRange(boolean useLegacyEndpoint, VertxTestContext context) throws IOException
    {
        configureTableDirectoryLocations();
        WebClient client = WebClient.create(vertx);
        String testRoute = testRoute(useLegacyEndpoint);
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=4-3")
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(REQUESTED_RANGE_NOT_SATISFIABLE.code());
                  context.completeNow();
              })));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testRangeExceeds(boolean useLegacyEndpoint, VertxTestContext context) throws IOException
    {
        configureTableDirectoryLocations();
        WebClient client = WebClient.create(vertx);
        String testRoute = testRoute(useLegacyEndpoint);
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=5-9")
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(REQUESTED_RANGE_NOT_SATISFIABLE.code());
                  context.completeNow();
              })));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testPartialRangeExceeds(boolean useLegacyEndpoint, VertxTestContext context) throws IOException
    {
        configureTableDirectoryLocations();
        WebClient client = WebClient.create(vertx);
        String testRoute = testRoute(useLegacyEndpoint);
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=5-")
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(REQUESTED_RANGE_NOT_SATISFIABLE.code());
                  context.completeNow();
              })));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testRangeBoundaryExceeds(boolean useLegacyEndpoint, VertxTestContext context) throws IOException
    {
        configureTableDirectoryLocations();
        WebClient client = WebClient.create(vertx);
        String testRoute = testRoute(useLegacyEndpoint);
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=0-999999")
              .as(BodyCodec.buffer())
              .send(context.succeeding(response -> context.verify(() -> {
                  vertx.setTimer(100, v -> {
                      assertThat(response.statusCode()).isEqualTo(OK.code());
                      assertThat(response.bodyAsString()).isEqualTo("data");
                      assertThat(instanceMetrics(1).streamSSTable()
                                                   .forComponent("Data.db").bytesStreamedRate.metric.getCount()).isEqualTo(4);
                      assertThat(instanceMetrics(1).streamSSTable().totalBytesStreamedRate.metric.getCount()).isEqualTo(4);
                      context.completeNow();
                  });
              })));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testPartialRangeStreamed(boolean useLegacyEndpoint, VertxTestContext context) throws IOException
    {
        configureTableDirectoryLocations();
        WebClient client = WebClient.create(vertx);
        String testRoute = testRoute(useLegacyEndpoint);
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=0-2") // 3 bytes streamed
              .as(BodyCodec.buffer())
              .send(context.succeeding(response -> context.verify(() -> {
                  vertx.setTimer(100, v -> {
                      assertThat(response.statusCode()).isEqualTo(PARTIAL_CONTENT.code());
                      assertThat(response.bodyAsString()).isEqualTo("dat");
                      assertThat(instanceMetrics(1).streamSSTable()
                                                   .forComponent("Data.db").bytesStreamedRate.metric.getCount()).isEqualTo(3);
                      assertThat(instanceMetrics(1).streamSSTable().totalBytesStreamedRate.metric.getCount()).isEqualTo(3);
                      context.completeNow();
                  });
              })));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testSuffixRange(boolean useLegacyEndpoint, VertxTestContext context) throws IOException
    {
        configureTableDirectoryLocations();
        WebClient client = WebClient.create(vertx);
        String testRoute = testRoute(useLegacyEndpoint);
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=-2") // last 2 bytes streamed
              .as(BodyCodec.buffer())
              .send(context.succeeding(response -> context.verify(() -> {
                  vertx.setTimer(100, v -> {
                      assertThat(response.statusCode()).isEqualTo(PARTIAL_CONTENT.code());
                      assertThat(response.bodyAsString()).isEqualTo("ta");
                      assertThat(instanceMetrics(1).streamSSTable()
                                                   .forComponent("Data.db").bytesStreamedRate.metric.getCount()).isEqualTo(2);
                      assertThat(instanceMetrics(1).streamSSTable().totalBytesStreamedRate.metric.getCount()).isEqualTo(2);
                      context.completeNow();
                  });
              })));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testSuffixRangeExceeds(boolean useLegacyEndpoint, VertxTestContext context) throws IOException
    {
        configureTableDirectoryLocations();
        WebClient client = WebClient.create(vertx);
        String testRoute = testRoute(useLegacyEndpoint);
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=-5")
              .send(context.succeeding(response -> context.verify(() -> {
                  vertx.setTimer(100, v -> {
                      assertThat(response.statusCode()).isEqualTo(OK.code());
                      assertThat(response.getHeader(HttpHeaderNames.CONTENT_LENGTH.toString()))
                      .describedAs("Server should shrink the range to the file length")
                      .isEqualTo("4");
                      assertThat(instanceMetrics(1).streamSSTable()
                                                   .forComponent("Data.db").bytesStreamedRate.metric.getCount()).isEqualTo(4);
                      assertThat(instanceMetrics(1).streamSSTable().totalBytesStreamedRate.metric.getCount()).isEqualTo(4);
                      context.completeNow();
                  });
              })));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testInvalidRangeUnit(boolean useLegacyEndpoint, VertxTestContext context) throws IOException
    {
        configureTableDirectoryLocations();
        WebClient client = WebClient.create(vertx);
        String testRoute = testRoute(useLegacyEndpoint);
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bits=0-2")
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(REQUESTED_RANGE_NOT_SATISFIABLE.code());
                  context.completeNow();
              })));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testStreamingFromSpecificInstance(boolean useLegacyEndpoint, VertxTestContext context) throws IOException
    {
        configureTableDirectoryLocations();
        WebClient client = WebClient.create(vertx);
        String testRoute;
        if (useLegacyEndpoint)
        {
            testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "/" +
                        "snapshots/TestSnapshot/components/nb-1-big-Data.db?instanceId=2";
        }
        else
        {
            testRoute = "/keyspaces/" + TEST_KEYSPACE + "/tables/" + TEST_TABLE + "-" + TEST_TABLE_ID + "/" +
                        "snapshots/TestSnapshot/components/nb-1-big-Data.db?dataDirectoryIndex=0&instanceId=2";
        }

        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .as(BodyCodec.buffer())
              .send(context.succeeding(response -> context.verify(() -> {
                  vertx.setTimer(100, v -> {
                      assertThat(response.statusCode()).isEqualTo(OK.code());
                      assertThat(response.bodyAsString()).isEqualTo("data");
                      assertThat(instanceMetrics(2).streamSSTable()
                                                   .forComponent("Data.db").bytesStreamedRate.metric.getCount()).isEqualTo(4);
                      assertThat(instanceMetrics(2).streamSSTable().totalBytesStreamedRate.metric.getCount()).isEqualTo(4);
                      context.completeNow();
                  });
              })));
    }

    private InstanceMetrics instanceMetrics(int id)
    {
        return new InstanceMetricsImpl(registry(id));
    }

    void configureTableDirectoryLocations() throws IOException
    {
        List<String> tableDirectoryLocations = testModule.delegate
                                               .storageOperations()
                                               .dataFileLocations()
                                               .stream()
                                               .map(dataDir -> dataDir + "/" + TEST_KEYSPACE + "/" + TEST_TABLE + "-" + TEST_TABLE_ID)
                                               .collect(Collectors.toList());

        TableOperations mockTableOperations = mock(TableOperations.class);
        when(mockTableOperations.getDataPaths(TEST_KEYSPACE, TEST_TABLE)).thenReturn(tableDirectoryLocations);
        testModule.delegate.setTableOperations(mockTableOperations);
    }

    static String testRoute(boolean useLegacyEndpoint)
    {
        return testRoute(useLegacyEndpoint, TEST_KEYSPACE, TEST_TABLE);
    }

    static String testRoute(boolean useLegacyEndpoint, String keyspace, String table)
    {
        return testRoute(useLegacyEndpoint, keyspace, table, TEST_TABLE_ID, "TestSnapshot", "nb-1-big-Data.db");
    }

    static String testRoute(boolean useLegacyEndpoint, String keyspace, String table, String tableId,
                            String snapshot, String component)
    {
        if (useLegacyEndpoint)
        {
            return "/keyspaces/" + keyspace + "/tables/" + table + "/snapshots" +
                   "/" + snapshot + "/components/" + component;
        }
        else
        {
            return "/keyspaces/" + keyspace + "/tables/" + table + "-" + tableId + "/snapshots" +
                   "/" + snapshot + "/components/" + component + "?dataDirectoryIndex=0";
        }
    }
}
