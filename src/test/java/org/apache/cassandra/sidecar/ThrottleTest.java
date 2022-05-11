package org.apache.cassandra.sidecar;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test rate limiting stream requests
 */
@ExtendWith(VertxExtension.class)
public class ThrottleTest
{
    private static final Logger logger = LoggerFactory.getLogger(ThrottleTest.class);
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

        context.awaitCompletion(5, SECONDS);
    }

    @AfterEach
    void tearDown() throws InterruptedException
    {
        final CountDownLatch closeLatch = new CountDownLatch(1);
        server.close(res -> closeLatch.countDown());
        vertx.close();
        if (closeLatch.await(60, SECONDS))
            logger.info("Close event received before timeout.");
        else
            logger.error("Close event timed out.");
    }

    @Test
    void testStreamRequestsThrottled() throws Exception
    {
        String testRoute = "/keyspace/TestKeyspace/table/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshots" +
                "/TestSnapshot/component/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";

        for (int i = 0; i < 20; i++)
        {
            unblockingClientRequest(testRoute);
        }

        HttpResponse response = blockingClientRequest(testRoute);
        assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS.code(), response.statusCode());

        long secsToWait = Long.parseLong(response.getHeader("Retry-After"));
        Thread.sleep(SECONDS.toMillis(secsToWait));

        HttpResponse finalResp = blockingClientRequest(testRoute);
        assertEquals(HttpResponseStatus.OK.code(), finalResp.statusCode());
        assertEquals("data", finalResp.bodyAsString());
    }

    private void unblockingClientRequest(String route)
    {
        WebClient client = WebClient.create(vertx);
        client.get(config.getPort(), "localhost", "/api/v1" + route)
                .as(BodyCodec.buffer())
                .send(resp ->
                {
                    // do nothing
                });
    }

    private HttpResponse blockingClientRequest(String route) throws ExecutionException, InterruptedException
    {
        WebClient client = WebClient.create(vertx);
        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
        client.get(config.getPort(), "localhost", "/api/v1" + route)
                .as(BodyCodec.buffer())
                .send(resp -> future.complete(resp.result()));
        return future.get();
    }
}
