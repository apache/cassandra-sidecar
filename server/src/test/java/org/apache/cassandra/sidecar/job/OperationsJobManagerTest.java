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

package org.apache.cassandra.sidecar.job;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationsJobException;
import org.apache.cassandra.sidecar.common.utils.OperationsJobResult;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.apache.cassandra.sidecar.common.utils.OperationsJobResult.OperationsJobStatus.COMPLETED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests to validate the Job submission behavior for scenarios which are a combination of values for
 * 1) Downstream job existence,
 * 2) Cached job (null (not in cache), Completed/Failed job, Running job), and
 * 3) Request UUID (null (no header), UUID)
 */
public class OperationsJobManagerTest
{
    @Mock
    OperationsJob mockJob;

    @Mock
    SidecarConfiguration mockConfig;

    protected Vertx vertx;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        MockitoAnnotations.openMocks(this);
        ServiceConfiguration mockServiceConfig = mock(ServiceConfiguration.class);
        when(mockConfig.serviceConfiguration()).thenReturn(mockServiceConfig);
        when(mockServiceConfig.operationsJobSyncResponseTimeout()).thenReturn(5000);
    }

    @ParameterizedTest(name = "{index} => HeaderJobId {0}")
    @MethodSource("inputParams")
    void testWithNoDownstreamJob(String jobId)
    {
        UUID headerJobId = (jobId.isEmpty()) ? null :  UUID.fromString(jobId);
        OperationsJobTracker tracker = new OperationsJobTracker(4);
        ExecutorPools executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());
        OperationsJobManager manager = new OperationsJobManager(vertx, executorPools, mockConfig, tracker);

        OperationsJob testJob = new OperationsJob(vertx, headerJobId)
        {
            public String operation()
            {
                return "test";
            }

            public boolean isRunningDownstream()
            {
                return false;
            }

            public long creationTime()
            {
                return System.nanoTime();
            }

            protected OperationsJobResult.OperationsJobStatus executeInternal() throws OperationsJobException
            {
                return COMPLETED;
            }
        };

//        Function<UUID, OperationsJob> creator = (id) -> testJob;
//        OperationsJob createdJob = creator.apply(headerJobId);
        manager.trySubmitJob(headerJobId, testJob);
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        assertThat(testJob.status().isComplete()).isTrue();
        assertThat(testJob.status().result()).isEqualTo(COMPLETED);
        if (headerJobId != null)
        {
            assertThat(tracker.get(headerJobId)).isNotNull();
        }

    }

    @ParameterizedTest(name = "{index} => HeaderJobId {0}")
    @MethodSource("inputParams")
    void testWithRunningDownstreamJob(String jobId)
    {
        UUID headerJobId = (jobId.isEmpty()) ? null :  UUID.fromString(jobId);
        OperationsJobTracker tracker = new OperationsJobTracker(4);
        ExecutorPools mockPools = mock(ExecutorPools.class);
        TaskExecutorPool mockExecPool = mock(TaskExecutorPool.class);
        when(mockPools.internal()).thenReturn(mockExecPool);
        when(mockExecPool.runBlocking(any())).thenReturn(null);
        OperationsJobManager manager = new OperationsJobManager(vertx, mockPools, mockConfig, tracker);

        when(mockJob.isRunningDownstream()).thenReturn(true);
        Promise unresolved = Promise.promise();
        when(mockJob.status()).thenReturn(unresolved.future());
        if (headerJobId != null)
        {
            tracker.put(headerJobId, mockJob);
        }

        doAnswer(invocation -> {
            Promise<OperationsJobResult.OperationsJobStatus> capturedPromise = invocation.getArgument(0);
            capturedPromise.complete(COMPLETED);
            return null;  // void return
        }).when(mockJob).execute(any(Promise.class));

//        Function<UUID, OperationsJob> creator = (id) -> mockJob;
        OperationsJobException ex = Assertions.assertThrows(OperationsJobException.class,
                                                            () -> manager.trySubmitJob(headerJobId, mockJob));
        assertThat(ex.getMessage()).isEqualTo("Conflicting job running downstream");

        if (headerJobId == null)
        {
            assertThat(ex.getHeaderValue()).isEqualTo(null);
        }
        else
        {
            assertThat(ex.getHeaderValue()).isEqualTo(headerJobId);
        }
        unresolved.complete();
    }

    @ParameterizedTest(name = "{index} => HeaderJobId {0}")
    @MethodSource("inputParams")
    void testWithLongRunningJob(String jobId)
    {
        UUID headerJobId = (jobId.isEmpty()) ? null :  UUID.fromString(jobId);

        OperationsJobTracker tracker = new OperationsJobTracker(4);
        ExecutorPools executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());
        OperationsJobManager manager = new OperationsJobManager(vertx, executorPools, mockConfig, tracker);

        OperationsJob testJob = new OperationsJob(vertx, headerJobId)
        {
            public String operation()
            {
                return "test";
            }

            public boolean isRunningDownstream()
            {
                return false;
            }

            public long creationTime()
            {
                return System.nanoTime();
            }

            protected OperationsJobResult.OperationsJobStatus executeInternal() throws OperationsJobException
            {
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
                return COMPLETED;
            }
        };

        manager.trySubmitJob(headerJobId, testJob);
        assertThat(testJob.status().isComplete()).isFalse();
        if (headerJobId != null)
        {
            assertThat(tracker.get(headerJobId)).isNotNull();
        }
    }

    @ParameterizedTest(name = "{index} => HeaderJobId {0}")
    @MethodSource("inputParams")
    void testWithFailingJob(String jobId)
    {
        UUID headerJobId = (jobId.isEmpty()) ? null :  UUID.fromString(jobId);

        OperationsJobTracker tracker = new OperationsJobTracker(4);
        ExecutorPools executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());
        OperationsJobManager manager = new OperationsJobManager(vertx, executorPools, mockConfig, tracker);

        String msg = "Test Job failed";
        OperationsJob failingJob = new OperationsJob(vertx, UUID.randomUUID())
        {
            protected OperationsJobResult.OperationsJobStatus executeInternal() throws OperationsJobException
            {
                throw new OperationsJobException(msg);
            }

            public String operation()
            {
                return "test";
            }

            public boolean isRunningDownstream()
            {
                return false;
            }
        };

//        Function<UUID, OperationsJob> creator = (id) -> failingJob;
        manager.trySubmitJob(headerJobId, failingJob);
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        assertThat(failingJob.status().isComplete()).isTrue();
        assertThat(failingJob.status().failed()).isTrue();
        assertThat(tracker.get(headerJobId)).isNull();
    }

    static Stream<Arguments> inputParams()
    {
        List<String> uuidList = Arrays.asList("", UUID.randomUUID().toString());
        return uuidList.stream().map(Arguments::of);
    }
}
