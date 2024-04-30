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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.SharedMetricRegistries;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetricsImpl;
import org.apache.cassandra.sidecar.metrics.instance.StreamSSTableMetrics;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
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
        SharedMetricRegistries.clear();
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, SECONDS))
            logger.info("Close event received before timeout.");
        else
            logger.error("Close event timed out.");
    }

    @Test
    void testStreamRequestsThrottled(VertxTestContext context) throws Exception
    {
        String testRoute = "/keyspaces/TestKeyspace/tables/TestTable-54ea95cebba24e0aa9bee428e5d7160b/snapshots" +
                           "/TestSnapshot/components/nb-1-big-Data.db?dataDirectoryIndex=0";

        long startTime = System.nanoTime();
        List<Future<HttpResponse<Buffer>>> responseFutures = new ArrayList<>();
        for (int i = 0; i < 50; i++)
        {
            responseFutures.add(sendRequest(testRoute));
        }

        Future.all(responseFutures)
              .onComplete(context.succeeding(combinedResp -> {
                  if (combinedResp.cause() != null)
                  {
                      context.failNow(combinedResp.cause());
                      return;
                  }

                  try
                  {
                      long timeTakenInSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
                      int okResponse = 0;
                      for (Future<HttpResponse<Buffer>> resp : responseFutures)
                      {
                          if (resp.result() != null && resp.result().statusCode() == HttpResponseStatus.OK.code())
                          {
                              okResponse++;
                          }
                      }
                      // remove burst permits from calculation. In this scenario of 5qps and 1 sec burst second.
                      // Stored permits are 5 (5 * 1).
                      double rate = (okResponse - 5) / (double) timeTakenInSeconds;
                      assertThat(rate).isGreaterThanOrEqualTo(3);
                      assertThat(rate).isLessThanOrEqualTo(7);
                      StreamSSTableMetrics streamSSTableMetrics = new InstanceMetricsImpl(registry(1)).streamSSTable();
                      assertThat(streamSSTableMetrics.throttled.metric.getValue()).isGreaterThanOrEqualTo(5);
                      context.completeNow();
                  }
                  catch (Exception e)
                  {
                      context.failNow(e);
                  }
              }));
        context.awaitCompletion(1, MINUTES);
    }

    private Future<HttpResponse<Buffer>> sendRequest(String route)
    {
        WebClient client = WebClient.create(vertx);
        return client.get(server.actualPort(), "localhost", "/api/v1" + route)
                     .as(BodyCodec.buffer())
                     .send();
    }
}
