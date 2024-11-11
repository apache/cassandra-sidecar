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

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.common.response.OperationsJobsResponse;
import org.apache.cassandra.sidecar.common.utils.OperationsJobResult;
import org.apache.cassandra.sidecar.job.OperationsJob;
import org.apache.cassandra.sidecar.job.OperationsJobManager;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link OperationsJobsHandler}
 */
@ExtendWith(VertxExtension.class)
public class OperationsJobsHandlerTest
{
    static final Logger LOGGER = LoggerFactory.getLogger(GossipInfoHandlerTest.class);
    Vertx vertx;
    Server server;

    static UUID runningUuid = UUID.randomUUID();
    static UUID completedUuid = UUID.randomUUID();
    static UUID failedUuid = UUID.randomUUID();

    @BeforeEach
    void before() throws InterruptedException
    {
        Injector injector;
        Module testOverride = Modules.override(new TestModule())
                                     .with(new OperationsJobsHandlerTestModule());
        injector = Guice.createInjector(Modules.override(new MainModule())
                                               .with(testOverride));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
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
    void testGetJobStatusNonExistentJob(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String uuid = UUID.randomUUID().toString();
        String testRoute = "/api/v1/cassandra/operations/jobs/" + uuid;
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_NOT_FOUND)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  context.completeNow();
              }));
    }

    @Test
    void testGetJobStatusRunningJob(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/operations/jobs/" + runningUuid;
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_ACCEPTED)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(ACCEPTED.code());
                  context.completeNow();
              }));
    }

    @Test
    void testGetJobStatusCompletedJob(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/operations/jobs/" + completedUuid;
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  OperationsJobsResponse jobStatus = response.bodyAsJson(OperationsJobsResponse.class);
                  assertThat(jobStatus.jobId()).isEqualTo(completedUuid);
                  assertThat(jobStatus.status()).isEqualTo(OperationsJobResult.OperationsJobStatus.COMPLETED);
                  assertThat(jobStatus.operation()).isEqualTo("testCompleted");
                  context.completeNow();
              }));
    }

    @Test
    void testGetJobStatusFailedJob(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/operations/jobs/" + failedUuid;
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  OperationsJobsResponse jobStatus = response.bodyAsJson(OperationsJobsResponse.class);
                  assertThat(jobStatus.jobId()).isEqualTo(failedUuid);
                  assertThat(jobStatus.status()).isEqualTo(OperationsJobResult.OperationsJobStatus.FAILED);
                  assertThat(jobStatus.operation()).isEqualTo("testFailed");
                  assertThat(jobStatus.reason()).isEqualTo("Simulated failure");

                  context.completeNow();
              }));
    }

    static class OperationsJobsHandlerTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public OperationsJobManager jobManager()
        {
            OperationsJobManager mockManager = mock(OperationsJobManager.class);
            OperationsJob runningMock = mock(OperationsJob.class);
            when(runningMock.status()).thenReturn(OperationsJobResult.OperationsJobStatus.RUNNING);
            OperationsJob completedMock = mock(OperationsJob.class);
            when(completedMock.status()).thenReturn(OperationsJobResult.OperationsJobStatus.COMPLETED);
            when(completedMock.operation()).thenReturn("testCompleted");
            OperationsJob failedMock = mock(OperationsJob.class);
            when(failedMock.status()).thenReturn(OperationsJobResult.OperationsJobStatus.FAILED);
            when(failedMock.operation()).thenReturn("testFailed");
            when(failedMock.failureReason()).thenReturn("Simulated failure");

            when(mockManager.getJobIfExists(runningUuid)).thenReturn(runningMock);
            when(mockManager.getJobIfExists(completedUuid)).thenReturn(completedMock);
            when(mockManager.getJobIfExists(failedUuid)).thenReturn(failedMock);
            return mockManager;
        }
    }
}
