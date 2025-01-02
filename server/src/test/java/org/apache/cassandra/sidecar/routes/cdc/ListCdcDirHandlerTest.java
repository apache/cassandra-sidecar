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

package org.apache.cassandra.sidecar.routes.cdc;

import java.io.IOException;
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
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.common.response.ListCdcSegmentsResponse;
import org.apache.cassandra.sidecar.common.response.data.CdcSegmentInfo;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Test class for testing ListCdcDirHandler class.
 */
@ExtendWith(VertxExtension.class)
class ListCdcDirHandlerTest
{
    static final Logger LOGGER = LoggerFactory.getLogger(ListCdcDirHandlerTest.class);
    private static final String ROUTE = "/api/v1/cdc/segments";
    private Vertx vertx;
    private Server server;

    @BeforeEach
    public void setUp() throws InterruptedException, IOException
    {
        Module testOverride = new TestModule();
        Injector injector = Guice.createInjector(Modules.override(new MainModule())
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
        client.get(server.actualPort(), "localhost", ROUTE)
              .as(BodyCodec.buffer())
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(resp -> {
                  context.verify(() -> {
                      ListCdcSegmentsResponse listCDCSegmentsResponse = resp.bodyAsJson(ListCdcSegmentsResponse.class);
                      assertThat(listCDCSegmentsResponse.segmentsInfo().size()).isEqualTo(2);
                      for (CdcSegmentInfo segmentInfo : listCDCSegmentsResponse.segmentsInfo())
                      {
                          if (segmentInfo.name.equals("CommitLog-1-1.log"))
                          {
                              assertThat(segmentInfo.name).isEqualTo("CommitLog-1-1.log");
                              assertThat(segmentInfo.idx).isEqualTo(1);
                              assertThat(segmentInfo.size).isEqualTo(1);
                              assertThat(segmentInfo.completed).isTrue();
                          }
                      }
                  });
                  client.close();
                  context.completeNow();
              }));
    }

    @Test
    void testLogFilesWithoutIdxFilesNotStreamed(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "localhost", ROUTE)
              .as(BodyCodec.buffer())
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(resp -> {
                  context.verify(() -> {
                      ListCdcSegmentsResponse listCDCSegmentsResponse = resp.bodyAsJson(ListCdcSegmentsResponse.class);
                      for (CdcSegmentInfo segmentInfo : listCDCSegmentsResponse.segmentsInfo())
                      {
                          assertThat(segmentInfo.name).isNotEqualTo("CommitLog-1-3.log");
                      }
                  });
                  client.close();
                  context.completeNow();
              }));
    }
}
