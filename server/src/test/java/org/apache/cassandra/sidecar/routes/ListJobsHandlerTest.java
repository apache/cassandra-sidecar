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

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
import org.apache.cassandra.sidecar.common.response.ListJobsResponse;
import org.apache.cassandra.sidecar.common.utils.JobResult;
import org.apache.cassandra.sidecar.job.Job;
import org.apache.cassandra.sidecar.job.JobManager;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link ListJobsHandler}
 */
@ExtendWith(VertxExtension.class)
public class ListJobsHandlerTest
{
    static final Logger LOGGER = LoggerFactory.getLogger(ListJobsHandlerTest.class);
    Vertx vertx;
    Server server;

    static UUID runningUuid = UUID.randomUUID();
    static UUID pendingUuid = UUID.randomUUID();
    static SampleJob running = new SampleJob(runningUuid, JobResult.JobStatus.Running);
    static SampleJob pending = new SampleJob(pendingUuid, JobResult.JobStatus.Pending);

    @BeforeEach
    void before() throws InterruptedException
    {
        Injector injector;
        Module testOverride = Modules.override(new TestModule())
                                     .with(new ListJobsHandlerTest.ListJobsTestModule());
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
    void testListJobs(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/jobs";
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  ListJobsResponse listJobs = response.bodyAsJson(ListJobsResponse.class);
                  assertThat(listJobs).isNotNull();
                  assertThat(listJobs.jobs()).isNotNull();
                  assertThat(listJobs.jobs().size()).isEqualTo(2);
                  assertThat(listJobs.jobs().get(0).jobId).isIn(runningUuid, pendingUuid);
                  assertThat(listJobs.jobs().get(1).jobId).isIn(runningUuid, pendingUuid);
                  context.completeNow();
              }));
    }

    static class ListJobsTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public JobManager jobManager()
        {
            List<Job> testJobs = Arrays.asList(running, pending);
            JobManager mockManager = mock(JobManager.class);
            when(mockManager.allInflightJobs()).thenReturn(testJobs);
            return mockManager;
        }
    }

    static class SampleJob extends Job
    {
        public SampleJob()
        {
            super();
        }

        protected SampleJob(UUID jobId, JobResult.JobStatus status)
        {
            super(jobId, status);
        }

        public Supplier<JobResult> jobOperationSupplier()
        {
            return null;
        }

        public String operation()
        {
            return "test";
        }

        public boolean jobInProgress()
        {
            return false;
        }
    }
}
