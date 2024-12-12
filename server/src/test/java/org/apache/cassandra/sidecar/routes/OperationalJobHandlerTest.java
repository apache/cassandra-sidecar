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

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.job.OperationalJob;
import org.apache.cassandra.sidecar.job.OperationalJobManager;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link OperationalJobHandler}
 */
@ExtendWith(VertxExtension.class)
class OperationalJobHandlerTest
{
    static final Logger LOGGER = LoggerFactory.getLogger(GossipInfoHandlerTest.class);
    Vertx vertx;
    Server server;

    static UUID runningUuid = UUIDs.timeBased();
    static UUID completedUuid = UUIDs.timeBased();
    static UUID failedUuid = UUIDs.timeBased();

    @BeforeEach
    void before() throws InterruptedException
    {
        Injector injector;
        Module testOverride = Modules.override(new TestModule())
                                     .with(new OperationalJobsHandlerTestModule());
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
        String uuid = UUIDs.timeBased().toString();
        String testRoute = "/api/v1/cassandra/operational-jobs/" + uuid;
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
        String testRoute = "/api/v1/cassandra/operational-jobs/" + runningUuid;
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
        String testRoute = "/api/v1/cassandra/operational-jobs/" + completedUuid;
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  OperationalJobResponse jobStatus = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(jobStatus.jobId()).isEqualTo(completedUuid);
                  assertThat(jobStatus.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
                  assertThat(jobStatus.operation()).isEqualTo("testCompleted");
                  context.completeNow();
              }));
    }

    @Test
    void testGetJobStatusFailedJob(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/operational-jobs/" + failedUuid;
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  OperationalJobResponse jobStatus = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(jobStatus.jobId()).isEqualTo(failedUuid);
                  assertThat(jobStatus.status()).isEqualTo(OperationalJobStatus.FAILED);
                  assertThat(jobStatus.operation()).isEqualTo("testFailed");
                  assertThat(jobStatus.reason()).isEqualTo("Test failed");

                  context.completeNow();
              }));
    }

    static class OperationalJobsHandlerTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public OperationalJobManager jobManager()
        {
            OperationalJobManager mockManager = mock(OperationalJobManager.class);
            OperationalJob runningMock = mock(OperationalJob.class);
            Promise<Void> p = Promise.promise();
            when(runningMock.status()).thenReturn(OperationalJobStatus.RUNNING);
            when(runningMock.asyncResult()).thenReturn(p.future());
            OperationalJob completedMock = mock(OperationalJob.class);
            when(completedMock.status()).thenReturn(OperationalJobStatus.SUCCEEDED);
            when(completedMock.name()).thenReturn("testCompleted");
            OperationalJob failedMock = mock(OperationalJob.class);
            when(failedMock.status()).thenReturn(OperationalJobStatus.FAILED);
            when(failedMock.asyncResult()).thenReturn(Future.failedFuture("Test failed"));
            when(failedMock.name()).thenReturn("testFailed");

            when(mockManager.getJobIfExists(runningUuid)).thenReturn(runningMock);
            when(mockManager.getJobIfExists(completedUuid)).thenReturn(completedMock);
            when(mockManager.getJobIfExists(failedUuid)).thenReturn(failedMock);
            return mockManager;
        }
    }
}
