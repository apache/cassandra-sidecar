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

package org.apache.cassandra.sidecar.handlers.cdc;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cdc.CdcLogCache;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Test class for StreamCDCSegmentHandler.
 */
@ExtendWith(VertxExtension.class)
class StreamCdcSegmentHandlerTest
{
    public static final String CDC_RAW_DIR = "./src/test/resources/instance1/cdc_raw";
    public static final String CDC_RAW_TEMP_DIR = CDC_RAW_DIR + CdcLogCache.TEMP_DIR_SUFFIX;
    static final Logger LOGGER = LoggerFactory.getLogger(StreamCdcSegmentHandlerTest.class);
    private Vertx vertx;
    private Server server;

    @BeforeEach
    void setUp() throws InterruptedException, IOException
    {
        Module testOverride = new TestModule();
        Injector injector = Guice.createInjector(Modules.override(SidecarModules.all())
                                                        .with(testOverride));
        server = injector.getInstance(Server.class);
        vertx = injector.getInstance(Vertx.class);

        VertxTestContext context = new VertxTestContext();
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void after() throws InterruptedException
    {
        CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    @Test
    void testRouteSucceeds(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/cdc/segments/CommitLog-1-1.log";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .as(BodyCodec.buffer())
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(resp -> {
                  context.verify(() -> assertThat(resp.bodyAsString()).isEqualTo("x"));
                  client.close();
                  context.completeNow();
              }));
        spinAssertCdcRawTempEmpty();
    }

    @Test  // A variant of testRouteSucceeds. It sends concurrent requests for the same segment and assert the all requests should be satisfied.
    void testConcurrentRequestsForSegmentSucceeds(VertxTestContext context)
    {
        int requests = 5;
        WebClient client = WebClient.create(vertx);
        CountDownLatch latch = new CountDownLatch(requests);
        String testRoute = "/cdc/segments/CommitLog-1-1.log";
        for (int i = 0; i < requests; i++)
        {
            client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
                  .as(BodyCodec.buffer())
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(resp -> {
                      context.verify(() -> assertThat(resp.bodyAsString()).isEqualTo("x"));
                      latch.countDown();
                      if (latch.getCount() == 0)
                      {
                          context.completeNow();
                      }
                  }));
        }
        spinAssertCdcRawTempEmpty();
    }

    @Test // The server internal should re-create hardlinks for each request in this scenario.
    void testSequentialRequestsForSegmentSucceeds(VertxTestContext context)
    {
        for (int i = 0; i < 3; i++)
        {
            testRouteSucceeds(context);
        }
    }

    @Test
    void testSegmentNotFoundSucceeds(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/cdc/segments/CommitLog-1-123456.log";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .expect(ResponsePredicate.SC_NOT_FOUND)
              .send(context.succeeding(resp -> {
                  context.verify(() -> {
                      assertThat(resp.statusCode()).isEqualTo(404);
                      assertThat(resp.statusMessage()).isEqualTo("Not Found");
                      assertThat(resp.bodyAsJsonObject().getString("message"))
                      .isEqualTo("CDC segment not found: CommitLog-1-123456.log");
                  });
                  client.close();
                  context.completeNow();
              }));
    }

    @Test
    void testIncorrectSegmentExtensionSucceeds(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/cdc/segments/CommitLog-1-1"; // not a valid file name, missing `.log` extension
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .send(context.succeeding(resp -> {
                  context.verify(() -> {
                      assertThat(resp.statusCode()).isEqualTo(400);
                      assertThat(resp.statusMessage()).isEqualTo("Bad Request");
                      assertThat(resp.bodyAsJsonObject().getString("message"))
                      .isEqualTo("Invalid path param for CDC segment: CommitLog-1-1");
                  });
                  context.completeNow();
                  client.close();
              }));
    }

    @Test
    void testInvalidRangeFails(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/cdc/segments/CommitLog-1-1.log";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=4-3")
              .expect(ResponsePredicate.SC_REQUESTED_RANGE_NOT_SATISFIABLE)
              .send(context.succeeding(resp -> {
                  context.verify(() -> {
                      assertThat(resp.statusCode()).isEqualTo(416);
                      assertThat(resp.statusMessage()).isEqualTo("Requested Range Not Satisfiable");
                      assertThat(resp.bodyAsJsonObject().getString("message"))
                      .isEqualTo("Range does not satisfy boundary requirements. range=[4, 3]");
                  });
                  client.close();
                  context.completeNow();
              }));
        spinAssertCdcRawTempEmpty();
    }

    @Test
    void testPartialRangeStreamedSucceeds(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/cdc/segments/CommitLog-1-2.log";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .putHeader("Range", "bytes=0-2")
              .as(BodyCodec.buffer())
              .expect(ResponsePredicate.SC_PARTIAL_CONTENT)
              .send(context.succeeding(resp -> {
                  context.verify(() -> {
                      assertThat(resp.getHeader(HttpHeaderNames.CONTENT_LENGTH.toString()))
                      .withFailMessage("It should only stream to the last flushed position: 1")
                      .isEqualTo("1");
                      // see src/test/resources/cdc_raw/CommitLog-1-2_cdc.idx
                      assertThat(resp.getHeader(HttpHeaderNames.CONTENT_RANGE.toString()))
                      .withFailMessage("It should only stream to the last flushed position: 1")
                      .isEqualTo("bytes 0-0/1");
                      assertThat(resp.bodyAsString()).isEqualTo("x");
                  });
                  client.close();
                  context.completeNow();
              }));
        spinAssertCdcRawTempEmpty();
    }

    @Test
    void testPartialRangeStreamedForIncompleteLogFileWithoutRangeHeader(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/cdc/segments/CommitLog-1-2.log";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .as(BodyCodec.buffer())
              .expect(ResponsePredicate.SC_PARTIAL_CONTENT)
              .send(context.succeeding(resp -> {
                  context.verify(() -> assertThat(resp.bodyAsString()).isEqualTo("x"));
                  client.close();
                  context.completeNow();
              }));
        spinAssertCdcRawTempEmpty();
    }

    @Test
    void testIdxFileIsNotStreamed(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/cdc/segments/CommitLog-1-2_cdc.idx";
        client.get(server.actualPort(), "localhost", "/api/v1" + testRoute)
              .as(BodyCodec.buffer())
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .send(context.succeeding(resp -> {
                  context.verify(() -> {
                      assertThat(resp.statusCode()).isEqualTo(400);
                      assertThat(resp.statusMessage()).isEqualTo("Bad Request");
                      assertThat(resp.bodyAsJsonObject().getString("message"))
                      .isEqualTo("Invalid path param for CDC segment: CommitLog-1-2_cdc.idx");
                  });
                  client.close();
                  context.completeNow();
              }));
    }

    private void spinAssertCdcRawTempEmpty()
    {
        File cdcTempDir = new File(CDC_RAW_TEMP_DIR);
        assertThat(cdcTempDir.exists()).isTrue();
        int attempts = 10;
        while (attempts > 0)
        {
            File[] files = cdcTempDir.listFiles();
            assertThat(files).isNotNull();
            if (files.length > 0)
            {
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                attempts--;
                if (attempts == 0)
                {
                    assertThat(files.length)
                    .withFailMessage("Expect empty directory. But found those files: " + Arrays.toString(files))
                    .isEqualTo(0);
                }
            }
            else
            {
                break;
            }
        }
    }
}
