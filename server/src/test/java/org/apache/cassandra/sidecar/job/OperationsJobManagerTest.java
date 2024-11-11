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
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.sidecar.common.server.exceptions.OperationsJobException;
import org.apache.cassandra.sidecar.common.utils.OperationsJobResult;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
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
    ExecutorPools mockPools;

    @Mock
    OperationsJob mockJob;

    @BeforeEach
    void setup()
    {
        MockitoAnnotations.openMocks(this);
        TaskExecutorPool mockExecPool = mock(TaskExecutorPool.class);
        when(mockPools.internal()).thenReturn(mockExecPool);
        when(mockExecPool.runBlocking(any())).thenReturn(null);
    }

    @ParameterizedTest(name = "UUID: {0}, status: {1}")
    @MethodSource("inputParams")
    void testWithNoDownstreamJob(String stringJobId, String stringStatus)
    {
        UUID headerJobId = (stringJobId.isEmpty()) ? null :  UUID.fromString(stringJobId);
        OperationsJobResult.OperationsJobStatus status = stringStatus.isEmpty() ? null : OperationsJobResult.OperationsJobStatus.valueOf(stringStatus);

        OperationsJobTracker tracker = new OperationsJobTracker(4);
        ExecutorPools mockPools = mock(ExecutorPools.class);
        TaskExecutorPool mockExecPool = mock(TaskExecutorPool.class);
        when(mockPools.internal()).thenReturn(mockExecPool);
        when(mockExecPool.runBlocking(any())).thenReturn(null);
        OperationsJobManager manager = new OperationsJobManager(mockPools, tracker);

        when(mockJob.isRunningDownstream()).thenReturn(false);
        when(mockJob.status()).thenReturn(status);
        if (status != null)
        {
            tracker.put(headerJobId, mockJob);
        }

        doNothing().when(mockJob).execute();
        Function<UUID, OperationsJob> creator = (id) -> mockJob;
        OperationsJob createdJob = creator.apply(headerJobId);
        OperationsJob job = manager.trySubmitJob(headerJobId, creator);
        assertThat(job).isEqualTo(createdJob);
        if (status != null)
        {
            assertThat(tracker).containsKey(headerJobId);
        }
        else
        {
            assertThat(tracker).isNotEmpty();
        }
    }

    @ParameterizedTest(name = "UUID: {0}, status: {1}")
    @MethodSource("inputParams")
    void testWithRunningDownstreamJob(String stringJobId, String stringStatus)
    {
        UUID headerJobId = (stringJobId.isEmpty()) ? null :  UUID.fromString(stringJobId);
        OperationsJobResult.OperationsJobStatus status = stringStatus.isEmpty() ? null : OperationsJobResult.OperationsJobStatus.valueOf(stringStatus);
        OperationsJobTracker tracker = new OperationsJobTracker(4);
        ExecutorPools mockPools = mock(ExecutorPools.class);
        TaskExecutorPool mockExecPool = mock(TaskExecutorPool.class);
        when(mockPools.internal()).thenReturn(mockExecPool);
        when(mockExecPool.runBlocking(any())).thenReturn(null);
        OperationsJobManager manager = new OperationsJobManager(mockPools, tracker);

        when(mockJob.isRunningDownstream()).thenReturn(true);
        when(mockJob.status()).thenReturn(status);
        if (headerJobId != null && status != null)
        {
            tracker.put(headerJobId, mockJob);
        }

        doNothing().when(mockJob).execute();
        Function<UUID, OperationsJob> creator = (id) -> mockJob;
        OperationsJobException ex = Assertions.assertThrows(OperationsJobException.class,
                                                            () -> manager.trySubmitJob(headerJobId, creator));
        assertThat(ex.getMessage()).isEqualTo("Conflicting job running downstream");

        if (headerJobId == null || status == null || status != OperationsJobResult.OperationsJobStatus.RUNNING)
        {
            assertThat(ex.getHeaderValue()).isEqualTo(null);
        }
        else
        {
            assertThat(ex.getHeaderValue()).isEqualTo(headerJobId);
        }
    }

    static Collection<Object[]> inputParams()
    {
        List<String> uuidList = Arrays.asList("", UUID.randomUUID().toString());
        List<String> statuses = Arrays.asList("",
                                              OperationsJobResult.OperationsJobStatus.RUNNING.toString(),
                                              OperationsJobResult.OperationsJobStatus.COMPLETED.toString());
        List<List<String>> uuidsStatuses = Lists.cartesianProduct(uuidList, statuses);
        return uuidsStatuses.stream().map(List::toArray).collect(Collectors.toList());

    }
}
