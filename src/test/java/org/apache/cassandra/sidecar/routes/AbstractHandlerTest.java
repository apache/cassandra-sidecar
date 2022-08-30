package org.apache.cassandra.sidecar.routes;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.Configuration;
import org.apache.cassandra.sidecar.MainModule;
import org.apache.cassandra.sidecar.TestModule;

/**
 * Provides common functionality for HandlerTests
 */
abstract class AbstractHandlerTest
{
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    Vertx vertx;
    Configuration config;
    HttpServer server;

    protected abstract Module initializeCustomModule();

    protected void afterInitialized() throws IOException
    {
        // no-op
    }

    @BeforeEach
    void before() throws InterruptedException, IOException
    {
        Module customModule = initializeCustomModule();
        Injector injector;
        if (customModule != null)
        {
            injector = Guice.createInjector(Modules.override(new MainModule())
                                                   .with(Modules.override(new TestModule())
                                                                .with(customModule)));
        }
        else
        {
            injector = Guice.createInjector(Modules.override(new MainModule()).with(new TestModule()));
        }
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(HttpServer.class);
        config = injector.getInstance(Configuration.class);
        VertxTestContext context = new VertxTestContext();
        server.listen(config.getPort(), config.getHost(), context.succeedingThenComplete());
        context.awaitCompletion(5, TimeUnit.SECONDS);
        afterInitialized();
    }

    @AfterEach
    void after() throws InterruptedException
    {
        final CountDownLatch closeLatch = new CountDownLatch(1);
        server.close(res -> closeLatch.countDown());
        vertx.close();
        if (closeLatch.await(60, TimeUnit.SECONDS))
            logger.info("Close event received before timeout.");
        else
            logger.error("Close event timed out.");
    }
}
