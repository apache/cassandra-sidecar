package org.apache.cassandra.sidecar;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static org.junit.Assert.assertEquals;

/**
 * Test for StreamSSTableComponent
 */
@ExtendWith(VertxExtension.class)
public class StreamSSTableComponentTest
{
    private static final Logger logger = LoggerFactory.getLogger(StreamSSTableComponentTest.class);
    private Vertx vertx;
    private HttpServer server;
    private Configuration config;

    @BeforeEach
    void setUp() throws InterruptedException
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(new TestModule()));
        server = injector.getInstance(HttpServer.class);
        vertx = injector.getInstance(Vertx.class);
        config = injector.getInstance(Configuration.class);

        VertxTestContext context = new VertxTestContext();
        server.listen(config.getPort(), context.completing());

        context.awaitCompletion(5, TimeUnit.SECONDS);
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
    void testRoute(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspace/TestKeyspace/table/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshot" +
                "/TestSnapshot/component/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        client.get(config.getPort(), "localhost", "/api/v1/stream" + testRoute)
                .as(BodyCodec.buffer())
                .send(context.succeeding(response -> context.verify(() ->
                {
                    assertEquals(200, response.statusCode());
                    assertEquals("data", response.bodyAsString());
                    context.completeNow();
                })));
    }

    @Test
    void testKeyspaceNotFound(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspace/random/table/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshot" +
                "/TestSnapshot/component/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        client.get(config.getPort(), "localhost", "/api/v1/stream" + testRoute)
                .send(context.succeeding(response -> context.verify(() ->
                {
                    assertEquals(404, response.statusCode());
                    context.completeNow();
                })));
    }

    @Test
    void testSnapshotNotFound(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspace/TestKeyspace/table/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshot" +
                "/random/component/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        client.get(config.getPort(), "localhost", "/api/v1/stream" + testRoute)
                .send(context.succeeding(response -> context.verify(() ->
                {
                    assertEquals(404, response.statusCode());
                    context.completeNow();
                })));
    }

    @Test
    void testForbiddenKeyspace(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspace/system/table/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshot" +
                "/TestSnapshot/component/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        client.get(config.getPort(), "localhost", "/api/v1/stream" + testRoute)
                .send(context.succeeding(response -> context.verify(() ->
                {
                    assertEquals(403, response.statusCode());
                    assertEquals("system keyspace is forbidden", response.statusMessage());
                    context.completeNow();
                })));
    }

    @Test
    void testIncorrectKeyspaceFormat(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspace/k*s/table/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshot" +
                "/TestSnapshot/component/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        client.get(config.getPort(), "localhost", "/api/v1/stream" + testRoute)
                .send(context.succeeding(response -> context.verify(() ->
                {
                    assertEquals(400, response.statusCode());
                    assertEquals("Invalid path params found", response.statusMessage());
                    context.completeNow();
                })));
    }

    @Test
    void testIncorrectComponentFormat(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspace/TestKeyspace/table/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshot" +
                "/TestSnapshot/component/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data...db";
        client.get(config.getPort(), "localhost", "/api/v1/stream" + testRoute)
                .send(context.succeeding(response -> context.verify(() ->
                {
                    assertEquals(400, response.statusCode());
                    assertEquals("Invalid path params found", response.statusMessage());
                    context.completeNow();
                })));
    }

    @Test
    void testAccessDeniedToCertainComponents(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspace/TestKeyspace/table/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshot" +
                "/TestSnapshot/component/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Digest.crc32d";
        client.get(config.getPort(), "localhost", "/api/v1/stream" + testRoute)
                .send(context.succeeding(response -> context.verify(() ->
                {
                    assertEquals(400, response.statusCode());
                    assertEquals("Invalid path params found", response.statusMessage());
                    context.completeNow();
                })));
    }

    @Test
    void testPartialTableName(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspace/TestKeyspace/table/TestTable/snapshot/TestSnapshot/component" +
                "/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        client.get(config.getPort(), "localhost", "/api/v1/stream" + testRoute)
                .putHeader("Range", "bytes=0-")
                .as(BodyCodec.buffer())
                .send(context.succeeding(response -> context.verify(() ->
                {
                    assertEquals(200, response.statusCode());
                    assertEquals("data", response.bodyAsString());
                    context.completeNow();
                })));
    }

    @Test
    void testInvalidRange(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspace/TestKeyspace/table/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshot" +
                "/TestSnapshot/component/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        client.get(config.getPort(), "localhost", "/api/v1/stream" + testRoute)
                .putHeader("Range", "bytes=4-3")
                .send(context.succeeding(response -> context.verify(() ->
                {
                    assertEquals(416, response.statusCode());
                    context.completeNow();
                })));
    }

    @Test
    void testRangeExceeds(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspace/TestKeyspace/table/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshot" +
                "/TestSnapshot/component/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        client.get(config.getPort(), "localhost", "/api/v1/stream" + testRoute)
                .putHeader("Range", "bytes=5-9")
                .send(context.succeeding(response -> context.verify(() ->
                {
                    assertEquals(416, response.statusCode());
                    context.completeNow();
                })));
    }

    @Test
    void testPartialRangeExceeds(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspace/TestKeyspace/table/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshot" +
                "/TestSnapshot/component/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        client.get(config.getPort(), "localhost", "/api/v1/stream" + testRoute)
                .putHeader("Range", "bytes=5-")
                .send(context.succeeding(response -> context.verify(() ->
                {
                    assertEquals(416, response.statusCode());
                    context.completeNow();
                })));
    }

    @Test
    void testRangeBoundaryExceeds(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspace/TestKeyspace/table/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshot" +
                "/TestSnapshot/component/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        client.get(config.getPort(), "localhost", "/api/v1/stream" + testRoute)
                .putHeader("Range", "bytes=0-999999")
                .as(BodyCodec.buffer())
                .send(context.succeeding(response -> context.verify(() ->
                {
                    assertEquals(200, response.statusCode());
                    assertEquals("data", response.bodyAsString());
                    context.completeNow();
                })));
    }

    @Test
    void testPartialRangeStreamed(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspace/TestKeyspace/table/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshot" +
                "/TestSnapshot/component/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        client.get(config.getPort(), "localhost", "/api/v1/stream" + testRoute)
                .putHeader("Range", "bytes=0-2") // 3 bytes streamed
                .as(BodyCodec.buffer())
                .send(context.succeeding(response -> context.verify(() ->
                {
                    assertEquals(206, response.statusCode());
                    assertEquals("dat", response.bodyAsString());
                    context.completeNow();
                })));
    }

    @Test
    void testSuffixRange(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspace/TestKeyspace/table/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshot" +
                "/TestSnapshot/component/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        client.get(config.getPort(), "localhost", "/api/v1/stream" + testRoute)
                .putHeader("Range", "bytes=-2") // last 2 bytes streamed
                .as(BodyCodec.buffer())
                .send(context.succeeding(response -> context.verify(() ->
                {
                    assertEquals(206, response.statusCode());
                    assertEquals("ta", response.bodyAsString());
                    context.completeNow();
                })));
    }

    @Test
    void testSuffixRangeExceeds(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspace/TestKeyspace/table/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshot" +
                "/TestSnapshot/component/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        client.get(config.getPort(), "localhost", "/api/v1/stream" + testRoute)
                .putHeader("Range", "bytes=-5")
                .send(context.succeeding(response -> context.verify(() ->
                {
                    assertEquals(416, response.statusCode());
                    context.completeNow();
                })));
    }

    @Test
    void testInvalidRangeUnit(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspace/TestKeyspace/table/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshot" +
                "/TestSnapshot/component/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        client.get(config.getPort(), "localhost", "/api/v1/stream" + testRoute)
                .putHeader("Range", "bits=0-2")
                .send(context.succeeding(response -> context.verify(() ->
                {
                    assertEquals(416, response.statusCode());
                    context.completeNow();
                })));
    }

    @Test
    void testStreamingFromSpecificInstance(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/keyspace/TestKeyspace/table/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/" +
                           "snapshot/TestSnapshot/component/" +
                           "TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        client.get(config.getPort(), "localhost", "/api/v1/stream/instance/2" + testRoute)
              .as(BodyCodec.buffer())
              .send(context.succeeding(response -> context.verify(() ->
              {
                  assertEquals(200, response.statusCode());
                  assertEquals("data", response.bodyAsString());
                  context.completeNow();
              })));
    }
}
