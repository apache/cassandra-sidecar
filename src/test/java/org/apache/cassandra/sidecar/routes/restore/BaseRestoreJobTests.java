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

import java.math.BigInteger;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.collect.Range;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.CreateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.common.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobDatabaseAccessor;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.db.RestoreSliceDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.foundation.RestoreJobSecretsGen;
import org.apache.cassandra.sidecar.restore.RestoreJobDiscoverer;
import org.apache.cassandra.sidecar.restore.RestoreJobManagerGroup;
import org.apache.cassandra.sidecar.restore.RestoreProcessor;
import org.apache.cassandra.sidecar.restore.RestoreSliceTracker;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.jetbrains.annotations.NotNull;

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
    protected TestModuleOverride.TestRestoreJobDatabaseAccessor testRestoreJobs;
    protected TestModuleOverride.TestRestoreSliceDatabaseAccessor testRestoreSlices;
    protected TestModuleOverride.TestRestoreJobManagerGroup testRestoreJobManagerGroup;

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
        testRestoreJobs
        = (TestModuleOverride.TestRestoreJobDatabaseAccessor) injector.getInstance(RestoreJobDatabaseAccessor.class);
        testRestoreSlices
        = (TestModuleOverride.TestRestoreSliceDatabaseAccessor)
          injector.getInstance(RestoreSliceDatabaseAccessor.class);
        testRestoreJobManagerGroup
        = (TestModuleOverride.TestRestoreJobManagerGroup) injector.getInstance(RestoreJobManagerGroup.class);
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);
        context.awaitCompletion(10, TimeUnit.SECONDS);
    }

    protected void configureTestModule(TestModule testInternalModule)
    {

    }

    @AfterEach
    public void tearDown(VertxTestContext context) throws Throwable
    {
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

    // The input of the func is the sliceId, and the func returns RestoreSliceTracker.Status
    // The implementation is used in the phase 1 of SBW-on-s3
    protected void mockSubmitRestoreSlice(ThrowableFunction<String,
                                                           RestoreSliceTracker.Status,
                                                           RestoreJobFatalException> func)
    {
        testRestoreJobManagerGroup.submitFunc = func;
    }

    protected void mockCreateRestoreSlice(Function<RestoreSlice, RestoreSlice> func)
    {
        testRestoreSlices.createFunc = func;
    }

    protected void mockUpdateRestoreSliceStatus(Function<RestoreSlice, RestoreSlice> func)
    {
        testRestoreSlices.updateStatusFunc = func;
    }

    protected void mockLookupRestoreSlices(BiFunction<UUID, Range<BigInteger>, List<RestoreSlice>> func)
    {
        testRestoreSlices.selectByJobByRangeFunc = func;
    }

    interface ThrowableFunction<A, B, E extends Exception>
    {
        B apply(A a) throws E;
    }

    static class TestModuleOverride extends AbstractModule
    {
        static class TestRestoreJobDatabaseAccessor extends RestoreJobDatabaseAccessor
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
                                     QualifiedTableName qualifiedTableName,
                                     UUID jobId)
            {
                return updateFunc.apply(payload);
            }

            @Override
            public void abort(UUID jobId)
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
            Function<RestoreSlice, RestoreSlice> updateStatusFunc;
            BiFunction<UUID, Range<BigInteger>, List<RestoreSlice>> selectByJobByRangeFunc;

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
            public RestoreSlice updateStatus(RestoreSlice slice)
            {
                return updateStatusFunc.apply(slice);
            }

            @Override
            public List<RestoreSlice> selectByJobByBucketByTokenRange(UUID jobId, short bucketId,
                                                                      BigInteger startToken, BigInteger endToken)
            {
                return selectByJobByRangeFunc.apply(jobId, Range.closed(startToken, endToken));
            }
        }

        static class TestRestoreJobManagerGroup extends RestoreJobManagerGroup
        {
            ThrowableFunction<String, RestoreSliceTracker.Status, RestoreJobFatalException> submitFunc;

            public TestRestoreJobManagerGroup(SidecarConfiguration configuration,
                                              InstancesConfig instancesConfig,
                                              ExecutorPools executorPools,
                                              PeriodicTaskExecutor periodicTaskExecutor,
                                              RestoreProcessor restoreProcessor,
                                              RestoreJobDiscoverer jobDiscoverer)
            {
                super(configuration, instancesConfig, executorPools, periodicTaskExecutor, restoreProcessor,
                      jobDiscoverer);
            }

            @Override
            public RestoreSliceTracker.Status trySubmit(InstanceMetadata instance, RestoreSlice slice,
                                                        RestoreJob restoreJob) throws RestoreJobFatalException
            {
                return submitFunc.apply(slice.sliceId());
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
        public RestoreJobManagerGroup restoreJobManagerGroup(SidecarConfiguration configuration,
                                                             InstancesConfig instancesConfig,
                                                             ExecutorPools executorPools,
                                                             PeriodicTaskExecutor loopExecutor,
                                                             RestoreProcessor restoreProcessor,
                                                             RestoreJobDiscoverer jobDiscoverer)
        {
            return new TestRestoreJobManagerGroup(configuration,
                                                  instancesConfig,
                                                  executorPools,
                                                  loopExecutor,
                                                  restoreProcessor,
                                                  jobDiscoverer);
        }
    }
}
