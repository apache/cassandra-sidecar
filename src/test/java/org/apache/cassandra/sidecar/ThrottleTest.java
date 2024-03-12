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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.getMetric;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test rate limiting stream requests
 */
@ExtendWith(VertxExtension.class)
public class ThrottleTest
{
    private static final Logger logger = LoggerFactory.getLogger(ThrottleTest.class);
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
        context.awaitCompletion(5, SECONDS);
    }

    @AfterEach
    void tearDown() throws InterruptedException
    {
        CountDownLatch closeLatch = new CountDownLatch(1);
        registry().removeMatching((name, metric) -> true);
        registry(1).removeMatching((name, metric) -> true);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, SECONDS))
            logger.info("Close event received before timeout.");
        else
            logger.error("Close event timed out.");
    }

    @Test
    void testStreamRequestsThrottled() throws Exception
    {
        String testRoute = "/keyspaces/TestKeyspace/tables/TestTable/snapshots/TestSnapshot" +
                           "/components/nb-1-big-Data.db";

        for (int i = 0; i < 20; i++)
        {
            unblockingClientRequest(testRoute);
        }

        HttpResponse response = blockingClientRequest(testRoute);
        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.TOO_MANY_REQUESTS.code());
        assertRateLimitedMetricsRecorded();

        long secsToWait = Long.parseLong(response.getHeader("Retry-After"));
        Thread.sleep(SECONDS.toMillis(secsToWait));

        HttpResponse finalResp = blockingClientRequest(testRoute);
        assertThat(finalResp.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
        assertThat(finalResp.bodyAsString()).isEqualTo("data");
    }

    private void unblockingClientRequest(String route)
    {
        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "localhost", "/api/v1" + route)
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
        client.get(server.actualPort(), "localhost", "/api/v1" + route)
              .as(BodyCodec.buffer())
              .send(resp -> future.complete(resp.result()));
        return future.get();
    }

    private void assertRateLimitedMetricsRecorded()
    {
        assertThat(getMetric(1, "sidecar.instance.stream.component=db.rate_limited_calls_429", Meter.class)
                   .getCount()).isGreaterThanOrEqualTo(1);
        assertThat(getMetric(1, "sidecar.instance.stream.component=db.429_wait_time", Timer.class)
                   .getSnapshot()
                   .getValues()
                   .length).isGreaterThanOrEqualTo(1);
        assertThat(getMetric(1, "sidecar.instance.stream.component=db.429_wait_time", Timer.class)
                   .getSnapshot()
                   .getValues()[0]).isPositive();
    }
}
