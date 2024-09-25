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

package org.apache.cassandra.sidecar.routes.restore;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.common.request.data.CreateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.request.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobDatabaseAccessor;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreRangeDatabaseAccessor;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.db.RestoreSliceDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.foundation.RestoreJobSecretsGen;
import org.apache.cassandra.sidecar.restore.RestoreJobDiscoverer;
import org.apache.cassandra.sidecar.restore.RestoreJobManagerGroup;
import org.apache.cassandra.sidecar.restore.RestoreJobProgressTracker;
import org.apache.cassandra.sidecar.restore.RestoreProcessor;
import org.apache.cassandra.sidecar.restore.RingTopologyRefresher;
import org.apache.cassandra.sidecar.routes.restore.BaseRestoreJobTests.TestModuleOverride.TestRestoreJobDatabaseAccessor;
import org.apache.cassandra.sidecar.routes.restore.BaseRestoreJobTests.TestModuleOverride.TestRestoreJobManagerGroup;
import org.apache.cassandra.sidecar.routes.restore.BaseRestoreJobTests.TestModuleOverride.TestRestoreRangeDatabaseAccessor;
import org.apache.cassandra.sidecar.routes.restore.BaseRestoreJobTests.TestModuleOverride.TestRestoreSliceDatabaseAccessor;
import org.apache.cassandra.sidecar.routes.restore.BaseRestoreJobTests.TestModuleOverride.TestRingTopologyRefresher;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Common functionality for restore jobs {@link CreateRestoreJobHandler} tests.
 */
@ExtendWith(VertxExtension.class)
public abstract class BaseRestoreJobTests
{
    protected static final RestoreJobSecrets SECRETS = RestoreJobSecretsGen.genRestoreJobSecrets();
    protected Vertx vertx;
    protected Server server;
    protected WebClient client;
    protected TestRestoreJobDatabaseAccessor testRestoreJobs;
    protected TestRestoreSliceDatabaseAccessor testRestoreSlices;
    protected TestRestoreRangeDatabaseAccessor testRestoreRanges;
    protected TestRestoreJobManagerGroup testRestoreJobManagerGroup;
    protected TestRingTopologyRefresher testRingTopologyRefresher;

    @BeforeEach
    public void setup(VertxTestContext context) throws Exception
    {
        TestModule testModule = new TestModule();
        configureTestModule(testModule);
        Injector injector = Guice.createInjector(Modules.override(new MainModule())
                                                        .with(Modules.override(testModule)
                                                                     .with(new TestModuleOverride())));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        client = WebClient.create(vertx, new WebClientOptions());
        testRestoreJobs = (TestRestoreJobDatabaseAccessor) injector.getInstance(RestoreJobDatabaseAccessor.class);
        testRestoreSlices = (TestRestoreSliceDatabaseAccessor) injector.getInstance(RestoreSliceDatabaseAccessor.class);
        testRestoreRanges = (TestRestoreRangeDatabaseAccessor) injector.getInstance(RestoreRangeDatabaseAccessor.class);
        testRestoreJobManagerGroup = (TestRestoreJobManagerGroup) injector.getInstance(RestoreJobManagerGroup.class);
        testRingTopologyRefresher = (TestRingTopologyRefresher) injector.getInstance(RingTopologyRefresher.class);
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);
        context.awaitCompletion(10, TimeUnit.SECONDS);
    }

    protected void configureTestModule(TestModule testInternalModule)
    {

    }

    protected void postAndVerify(String endpoint,
                                 JsonObject requestPayload,
                                 Handler<AsyncResult<HttpResponse<Buffer>>> responseVerifier)
    {
        sendRequestAndVerify(HttpMethod.POST, endpoint, requestPayload, responseVerifier);
    }

    protected void postThenComplete(VertxTestContext context, String endpoint,
                                    JsonObject requestPayload, Consumer<AsyncResult<HttpResponse<Buffer>>> assertions)
    {
        sendRequestAndVerify(HttpMethod.POST, endpoint, requestPayload, asyncResult -> {
            context.verify(() -> assertions.accept(asyncResult));
            context.completeNow();
        });
    }

    protected void getThenComplete(VertxTestContext context, String endpoint, Consumer<AsyncResult<HttpResponse<Buffer>>> assertions)
    {
        sendRequestAndVerify(HttpMethod.GET, endpoint, null, asyncResult -> {
            context.verify(() -> assertions.accept(asyncResult));
            context.completeNow();
        });
    }

    private void sendRequestAndVerify(HttpMethod httpMethod,
                                      String endpoint,
                                      JsonObject requestPayload,
                                      Handler<AsyncResult<HttpResponse<Buffer>>> responseVerifier)
    {
        HttpRequest<Buffer> request = client.request(httpMethod, server.actualPort(), "localhost", endpoint);
        assertThat(request).isNotNull();
        request.as(BodyCodec.buffer());
        if (requestPayload == null)
        {
            request.send(responseVerifier);
        }
        else
        {
            request.sendJsonObject(requestPayload, responseVerifier);
        }
    }

    @AfterEach
    public void tearDown(VertxTestContext context) throws Throwable
    {
        client.close();
        vertx.close(result -> context.completeNow());
        assertThat(context.awaitCompletion(10, TimeUnit.SECONDS)).isTrue();
        assertThat(vertx.deploymentIDs()).isEmpty();
        if (context.failed())
        {
            throw context.causeOfFailure();
        }
    }

    protected void mockCreateRestoreJob(Function<CreateRestoreJobRequestPayload, RestoreJob> func)
    {
        testRestoreJobs.createFunc = func;
    }

    protected void mockUpdateRestoreJob(Function<UpdateRestoreJobRequestPayload, RestoreJob> func)
    {
        testRestoreJobs.updateFunc = func;
    }

    protected void mockLookupRestoreJob(Function<UUID, RestoreJob> func)
    {
        testRestoreJobs.lookupFunc = func;
    }

    // The input of the func is the sliceId. The func returns RestoreSliceTracker.Status
    // The func is only evaluated when the restore job is managed by Spark.
    protected void mockSubmitRestoreSlice(ThrowableFunction<String, RestoreJobProgressTracker.Status, RestoreJobFatalException> func)
    {
        testRestoreJobManagerGroup.submitFunc = func;
    }

    protected void mockCreateRestoreSlice(Function<RestoreSlice, RestoreSlice> func)
    {
        testRestoreSlices.createFunc = func;
    }

    protected void mockCreateRestoreRange(UnaryOperator<RestoreRange> func)
    {
        testRestoreRanges.createFunc = func;
    }

    protected void mockUpdateRestoreRangeStatus(UnaryOperator<RestoreRange> func)
    {
        testRestoreRanges.updateStatusFunc = func;
    }

    protected void mockFindAllRestoreRanges(Function<UUID, List<RestoreRange>> func)
    {
        testRestoreRanges.findAllFunc = func;
    }

    protected void mockTopologyInRefresher(Supplier<TokenRangeReplicasResponse> topologySupplier)
    {
        testRingTopologyRefresher.topologySupplier = topologySupplier;
    }

    interface ThrowableFunction<A, B, E extends Exception>
    {

        B apply(A a) throws E;
    }

    static class TestModuleOverride extends AbstractModule
    {
        protected static class TestRestoreJobDatabaseAccessor extends RestoreJobDatabaseAccessor
        {
            Function<CreateRestoreJobRequestPayload, RestoreJob> createFunc;
            Function<UpdateRestoreJobRequestPayload, RestoreJob> updateFunc;
            Function<UUID, RestoreJob> lookupFunc;

            TestRestoreJobDatabaseAccessor(SidecarSchema sidecarSchema)
            {
                super(sidecarSchema, null, null);
            }

            @Override
            public RestoreJob create(CreateRestoreJobRequestPayload payload, QualifiedTableName qualifiedTableName)
            {
                return createFunc.apply(payload);
            }

            @Override
            public RestoreJob update(UpdateRestoreJobRequestPayload payload,
                                     UUID jobId)
            {
                return updateFunc.apply(payload);
            }

            @Override
            public void abort(UUID jobId, String reason)
            {
                // do nothing
            }

            @Override
            public RestoreJob find(@NotNull UUID jobId)
            {
                return lookupFunc.apply(jobId);
            }
        }

        static class TestRestoreSliceDatabaseAccessor extends RestoreSliceDatabaseAccessor
        {
            Function<RestoreSlice, RestoreSlice> createFunc;

            TestRestoreSliceDatabaseAccessor(SidecarSchema sidecarSchema)
            {
                super(sidecarSchema, null, null);
            }

            @Override
            public RestoreSlice create(RestoreSlice restoreSlice)
            {
                return createFunc.apply(restoreSlice);
            }

            @Override
            public List<RestoreSlice> selectByJobByBucketByTokenRange(RestoreJob restoreJob, short bucketId, TokenRange range)
            {
                throw new UnsupportedOperationException("Not being tested here");
            }
        }

        protected static class TestRestoreRangeDatabaseAccessor extends RestoreRangeDatabaseAccessor
        {
            UnaryOperator<RestoreRange> createFunc;
            UnaryOperator<RestoreRange> updateStatusFunc;
            Function<UUID, List<RestoreRange>> findAllFunc;

            TestRestoreRangeDatabaseAccessor(SidecarSchema sidecarSchema)
            {
                super(sidecarSchema, null, null);
            }

            @Override
            public RestoreRange create(RestoreRange range)
            {
                return createFunc.apply(range);
            }

            @Override
            public RestoreRange updateStatus(RestoreRange range)
            {
                return updateStatusFunc.apply(range);
            }

            @Override
            public List<RestoreRange> findAll(UUID jobId, short bucketId)
            {
                return findAllFunc.apply(jobId);
            }
        }

        static class TestRestoreJobManagerGroup extends RestoreJobManagerGroup
        {
            ThrowableFunction<String, RestoreJobProgressTracker.Status, RestoreJobFatalException> submitFunc;

            public TestRestoreJobManagerGroup(SidecarConfiguration configuration,
                                              InstancesConfig instancesConfig,
                                              ExecutorPools executorPools,
                                              PeriodicTaskExecutor periodicTaskExecutor,
                                              RestoreProcessor restoreProcessor,
                                              RestoreJobDiscoverer jobDiscoverer,
                                              RingTopologyRefresher ringTopologyRefresher)
            {
                super(configuration, instancesConfig, executorPools, periodicTaskExecutor, restoreProcessor,
                      jobDiscoverer, ringTopologyRefresher);
            }

            @Override
            public RestoreJobProgressTracker.Status trySubmit(InstanceMetadata instance, RestoreRange range,
                                                              RestoreJob restoreJob) throws RestoreJobFatalException
            {
                return submitFunc.apply(null);
            }
        }

        static class TestRingTopologyRefresher extends RingTopologyRefresher
        {
            Supplier<TokenRangeReplicasResponse> topologySupplier;

            public TestRingTopologyRefresher(InstanceMetadataFetcher metadataFetcher, SidecarConfiguration config)
            {
                super(metadataFetcher, config);
            }

            @Override
            @Nullable
            public TokenRangeReplicasResponse cachedReplicaByTokenRange(RestoreJob restoreJob)
            {
                return topologySupplier.get();
            }
        }

        @Provides
        @Singleton
        public RestoreJobDatabaseAccessor restoreJobs(SidecarSchema sidecarSchema)
        {
            return new TestRestoreJobDatabaseAccessor(sidecarSchema);
        }

        @Provides
        @Singleton
        public RestoreSliceDatabaseAccessor restoreSlices(SidecarSchema sidecarSchema)
        {
            return new TestRestoreSliceDatabaseAccessor(sidecarSchema);
        }

        @Provides
        @Singleton
        public RestoreRangeDatabaseAccessor restoreRanges(SidecarSchema sidecarSchema)
        {
            return new TestRestoreRangeDatabaseAccessor(sidecarSchema);
        }

        @Provides
        @Singleton
        public RestoreJobManagerGroup restoreJobManagerGroup(SidecarConfiguration configuration,
                                                             InstancesConfig instancesConfig,
                                                             ExecutorPools executorPools,
                                                             PeriodicTaskExecutor loopExecutor,
                                                             RestoreProcessor restoreProcessor,
                                                             RestoreJobDiscoverer jobDiscoverer,
                                                             RingTopologyRefresher ringTopologyRefresher)
        {
            return new TestRestoreJobManagerGroup(configuration,
                                                  instancesConfig,
                                                  executorPools,
                                                  loopExecutor,
                                                  restoreProcessor,
                                                  jobDiscoverer,
                                                  ringTopologyRefresher);
        }

        @Provides
        @Singleton
        public RingTopologyRefresher ringTopologyRefresher(InstanceMetadataFetcher metadataFetcher, SidecarConfiguration configuration)
        {
            return new TestRingTopologyRefresher(metadataFetcher, configuration);
        }
    }
}
