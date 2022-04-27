package org.apache.cassandra.sidecar.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.handler.TimeoutHandler;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.Configuration;
import org.apache.cassandra.sidecar.MainModule;
import org.apache.cassandra.sidecar.TestModule;
import org.jboss.resteasy.plugins.server.vertx.VertxRequestHandler;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link JsonErrorHandler} class
 */
@ExtendWith(VertxExtension.class)
class JsonErrorHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(JsonErrorHandlerTest.class);
    private Vertx vertx;
    private Configuration config;
    private HttpServer server;

    @BeforeEach
    void before() throws InterruptedException
    {
        VertxTestContext context = new VertxTestContext();

        MainModule mainModule = new MainModule();
        Injector injector = Guice.createInjector(Modules.override(mainModule).with(new TestModule()));
        vertx = injector.getInstance(Vertx.class);

        Router router = getRouter(vertx);
        VertxRequestHandler vertxRequestHandler = injector.getInstance(VertxRequestHandler.class);
        config = injector.getInstance(Configuration.class);
        server = mainModule.vertxServer(vertx, config, router, vertxRequestHandler);
        server.listen(config.getPort(), context.succeedingThenComplete());

        assertThat(context.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
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
    public void testHttpExceptionHandling() throws InterruptedException
    {
        testHelper("/http-exception", result ->
        {
            assertThat(result.statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
            JsonObject response = result.bodyAsJsonObject();
            assertThat(response.getString("status")).isEqualTo("Fail");
            assertThat(response.getString("message")).isEqualTo("Payload is written to JSON");
        }, true);
    }

    @Test
    public void testRequestTimeoutHandling() throws InterruptedException
    {
        testHelper("/timeout", result ->
        {
            assertThat(result.statusCode()).isEqualTo(HttpResponseStatus.REQUEST_TIMEOUT.code());
            assertThat(result.bodyAsJsonObject().getString("status")).isEqualTo("Request Timeout");
        }, true);
    }

    @Test
    public void testUnhandledThrowable() throws InterruptedException
    {
        testHelper("/RuntimeException", result ->
        {
            assertThat(result.statusCode()).isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
            assertThat(result.bodyAsString()).isEqualTo("Internal Server Error");
        }, false);
    }

    @Test
    public void testUnhandledNotFound() throws InterruptedException
    {
        testHelper("/does-not-exist", result ->
                                      assertThat(result.statusCode())
                                      .isEqualTo(HttpResponseStatus.NOT_FOUND.code()), false);
    }

    private void testHelper(String requestURI, Consumer<HttpResponse<Buffer>> consumer, boolean expectJsonContentType)
    throws InterruptedException
    {
        VertxTestContext testContext = new VertxTestContext();
        WebClient client = WebClient.create(vertx);
        client.get(config.getPort(), config.getHost(), requestURI)
              .as(BodyCodec.buffer())
              .send(resp ->
                    {
                        client.close();
                        if (expectJsonContentType)
                        {
                            assertThat(resp.result().getHeader(HttpHeaders.CONTENT_TYPE.toString()))
                            .isEqualTo("application/json");
                        }
                        consumer.accept(resp.result());
                        testContext.completeNow();
                    });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    private Router getRouter(Vertx vertx)
    {
        Router router = Router.router(vertx);
        router.route().failureHandler(new JsonErrorHandler())
              .handler(TimeoutHandler.create(250, HttpResponseStatus.REQUEST_TIMEOUT.code()));
        router.get("/http-exception").handler(ctx ->
                                              {
                                                  throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                                                          "Payload is written to JSON");
                                              });
        router.get("/timeout").handler(ctx ->
                                       {
                                           // wait for the timeout
                                       });
        router.get("/RuntimeException").handler(ctx ->
                                                {
                                                    throw new RuntimeException("oops");
                                                });
        return router;
    }
}
