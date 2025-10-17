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

package org.apache.cassandra.sidecar.lifecycle;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.Lifecycle.CassandraState;
import org.apache.cassandra.sidecar.common.data.Lifecycle.OperationStatus;
import org.apache.cassandra.sidecar.common.response.LifecycleInfoResponse;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.exceptions.LifecycleTaskConflictException;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link LifecycleManager} to validate lifecycle management behavior
 */
class LifecycleManagerTest
{
    private static final String TEST_HOST = "127.0.0.1";
    private static final InstanceMetadata TEST_HOST_META = mock(InstanceMetadata.class);
    private static final String TEST_HOST_2 = "127.0.0.2";
    private static final InstanceMetadata TEST_HOST_2_META = mock(InstanceMetadata.class);

    protected Vertx vertx;
    protected ExecutorPools executorPools;
    protected LifecycleProvider mockLifecycleProvider;
    protected InstanceMetadataFetcher metadataFetcher = mock(InstanceMetadataFetcher.class);

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());
        mockLifecycleProvider = mock(LifecycleProvider.class);
        when(metadataFetcher.instance(TEST_HOST)).thenReturn(TEST_HOST_META);
        when(metadataFetcher.instance(TEST_HOST_2)).thenReturn(TEST_HOST_2_META);
    }

    @AfterEach
    void cleanup()
    {
        TestResourceReaper.create().with(vertx).with(executorPools).close();
    }

    @Test
    void testGetLifecycleInfoWithNoTaskSubmitted()
    {
        LifecycleManager lifecycleManager = new LifecycleManager(metadataFetcher, mockLifecycleProvider, executorPools);
        when(mockLifecycleProvider.isRunning(TEST_HOST_META)).thenReturn(false);

        LifecycleInfoResponse response = lifecycleManager.getLifecycleInfo(TEST_HOST);
        assertThat(response.currentState()).isEqualTo(CassandraState.STOPPED);
        assertThat(response.desiredState()).isEqualTo(CassandraState.UNKNOWN);
        assertThat(response.status()).isEqualTo(OperationStatus.UNDEFINED);
        assertThat(response.lastUpdate()).isEqualTo("No lifecycle task submitted for this instance yet.");
    }

    @Test
    void testSubmittedTaskSucceeds() throws LifecycleTaskConflictException
    {
        // Submit slow start task
        when(mockLifecycleProvider.isRunning(TEST_HOST_META)).thenReturn(false);
        CountDownLatch startLatch = slowCassandraStart();
        LifecycleManager lifecycleManager = new LifecycleManager(metadataFetcher, mockLifecycleProvider, executorPools);

        LifecycleInfoResponse actualResponse = lifecycleManager.updateDesiredState(TEST_HOST, CassandraState.RUNNING);
        LifecycleInfoResponse expectedResponse = new LifecycleInfoResponse(CassandraState.STOPPED, CassandraState.RUNNING,
                                                                           OperationStatus.CONVERGING,
                                                                           "Submitting start task for instance");
        assertThat(actualResponse).isEqualTo(expectedResponse);

        // Wait for the task to complete
        startLatch.countDown();
        when(mockLifecycleProvider.isRunning(TEST_HOST_META)).thenReturn(true);
        
        // Check status was updated
        LifecycleInfoResponse expectedResponseAfterStart = new LifecycleInfoResponse(CassandraState.RUNNING, CassandraState.RUNNING,
                                                     OperationStatus.CONVERGED,
                                                     "Instance has started");
        loopAssert(1, 200, () -> {
            LifecycleInfoResponse actualResponseAfterStart = lifecycleManager.getLifecycleInfo(TEST_HOST);
            assertThat(actualResponseAfterStart).isEqualTo(expectedResponseAfterStart);
        });

        // Attempt to start the instance again, should be no-op since instance is already running
        LifecycleInfoResponse responseAfterStartAgain = lifecycleManager.updateDesiredState(TEST_HOST, CassandraState.RUNNING);
        assertThat(responseAfterStartAgain).isEqualTo(expectedResponseAfterStart);
    }

    @Test
    void testSubmittedTaskFails() throws LifecycleTaskConflictException
    {
        LifecycleManager lifecycleManager = new LifecycleManager(metadataFetcher, mockLifecycleProvider, executorPools);
        when(mockLifecycleProvider.isRunning(TEST_HOST_META)).thenReturn(false);

        String errorMessage = "Cannot find Cassandra executable to start instance.";
        doThrow(new RuntimeException(errorMessage)).when(mockLifecycleProvider).start(TEST_HOST_META);

        lifecycleManager.updateDesiredState(TEST_HOST, CassandraState.RUNNING);

        loopAssert(1, 200, () -> {
            LifecycleInfoResponse response = lifecycleManager.getLifecycleInfo(TEST_HOST);
            assertThat(response.currentState()).isEqualTo(CassandraState.STOPPED);
            assertThat(response.desiredState()).isEqualTo(CassandraState.RUNNING);
            assertThat(response.status()).isEqualTo(OperationStatus.DIVERGED);
            assertThat(response.lastUpdate()).isEqualTo(String.format("Failed to start instance 127.0.0.1: %s", errorMessage));
        });

        lifecycleManager.updateDesiredState(TEST_HOST, CassandraState.RUNNING);
        reset(mockLifecycleProvider);

        when(mockLifecycleProvider.isRunning(TEST_HOST_META)).thenReturn(true);

        loopAssert(1, 200, () -> {
            LifecycleInfoResponse newResponse = lifecycleManager.getLifecycleInfo(TEST_HOST);
            assertThat(newResponse.currentState()).isEqualTo(CassandraState.RUNNING);
            assertThat(newResponse.desiredState()).isEqualTo(CassandraState.RUNNING);
            assertThat(newResponse.status()).isEqualTo(OperationStatus.CONVERGED);
            assertThat(newResponse.lastUpdate()).isEqualTo("Instance has started");
        });
    }

    @Test
    void testSubmitTaskWhenInProgressThrowsException() throws LifecycleTaskConflictException
    {
        LifecycleManager lifecycleManager = new LifecycleManager(metadataFetcher, mockLifecycleProvider, executorPools);
        // Mock a slow start operation
        slowCassandraStart();

        lifecycleManager.updateDesiredState(TEST_HOST, CassandraState.RUNNING);

        assertThatThrownBy(() -> lifecycleManager.updateDesiredState(TEST_HOST, CassandraState.RUNNING))
        .isExactlyInstanceOf(LifecycleTaskConflictException.class)
        .hasMessage("Cannot update lifecycle state of instance " + TEST_HOST + " to RUNNING. Task already in progress for this host.");
    }

    @Test
    void testSubmitNewTaskSucceedsAfterOldTaskFinishes() throws LifecycleTaskConflictException
    {
        LifecycleManager lifecycleManager = new LifecycleManager(metadataFetcher, mockLifecycleProvider, executorPools);
        when(mockLifecycleProvider.isRunning(TEST_HOST_META)).thenReturn(true);

        lifecycleManager.updateDesiredState(TEST_HOST, CassandraState.RUNNING);

        loopAssert(1, 200, () -> {
            LifecycleInfoResponse firstResponse = lifecycleManager.getLifecycleInfo(TEST_HOST);
            assertThat(firstResponse.currentState()).isEqualTo(CassandraState.RUNNING);
            assertThat(firstResponse.desiredState()).isEqualTo(CassandraState.RUNNING);
            assertThat(firstResponse.status()).isEqualTo(OperationStatus.CONVERGED);
            assertThat(firstResponse.lastUpdate()).isEqualTo("Instance has started");
        });
        when(mockLifecycleProvider.isRunning(TEST_HOST_META)).thenReturn(false);

        loopAssert(1, 200, () -> {
            try
            {
                lifecycleManager.updateDesiredState(TEST_HOST, CassandraState.STOPPED);
            }
            catch (LifecycleTaskConflictException e)
            {
                // Ignore and retry in case the previous task is still in progress
            }
            LifecycleInfoResponse finalResponse = lifecycleManager.getLifecycleInfo(TEST_HOST);
            assertThat(finalResponse.currentState()).isEqualTo(CassandraState.STOPPED);
            assertThat(finalResponse.desiredState()).isEqualTo(CassandraState.STOPPED);
            assertThat(finalResponse.status()).isEqualTo(OperationStatus.CONVERGED);
            assertThat(finalResponse.lastUpdate()).isEqualTo("Instance has stopped");
        });
    }

    @Test
    void testStateChangesUnexpectedlyFlapping() throws LifecycleTaskConflictException
    {
        // Update state to RUNNING
        LifecycleManager lifecycleManager = new LifecycleManager(metadataFetcher, mockLifecycleProvider, executorPools);
        when(mockLifecycleProvider.isRunning(TEST_HOST_META)).thenReturn(true);
        lifecycleManager.updateDesiredState(TEST_HOST, CassandraState.RUNNING);

        loopAssert(1, 200, () -> {
            LifecycleInfoResponse response = lifecycleManager.getLifecycleInfo(TEST_HOST);
            assertThat(response.currentState()).isEqualTo(CassandraState.RUNNING);
            assertThat(response.desiredState()).isEqualTo(CassandraState.RUNNING);
            assertThat(response.status()).isEqualTo(OperationStatus.CONVERGED);
            assertThat(response.lastUpdate()).isEqualTo("Instance has started");
        });

        // Now simulate instance getting stopped without a STOP lifecycle task being submitted
        when(mockLifecycleProvider.isRunning(TEST_HOST_META)).thenReturn(false);

        loopAssert(1, 200, () -> {
            LifecycleInfoResponse divergedResponse = lifecycleManager.getLifecycleInfo(TEST_HOST);
            assertThat(divergedResponse.currentState()).isEqualTo(CassandraState.STOPPED);
            assertThat(divergedResponse.desiredState()).isEqualTo(CassandraState.RUNNING);
            assertThat(divergedResponse.status()).isEqualTo(OperationStatus.DIVERGED);
            assertThat(divergedResponse.lastUpdate()).isEqualTo("Instance 127.0.0.1 has unexpectedly diverged from the desired state RUNNING to STOPPED.");
        });

        // Now simulate instance getting running without a START lifecycle task being submitted
        when(mockLifecycleProvider.isRunning(TEST_HOST_META)).thenReturn(true);
        LifecycleInfoResponse convergedResponse = lifecycleManager.getLifecycleInfo(TEST_HOST);
        assertThat(convergedResponse.currentState()).isEqualTo(CassandraState.RUNNING);
        assertThat(convergedResponse.desiredState()).isEqualTo(CassandraState.RUNNING);
        assertThat(convergedResponse.status()).isEqualTo(OperationStatus.CONVERGED);
        assertThat(convergedResponse.lastUpdate()).isEqualTo("Instance 127.0.0.1 has converged back to the desired state RUNNING.");
    }

    @Test
    void testCanSubmitTasksForIndependentHosts() throws LifecycleTaskConflictException
    {
        when(mockLifecycleProvider.isRunning(TEST_HOST_META)).thenReturn(false);
        when(mockLifecycleProvider.isRunning(TEST_HOST_2_META)).thenReturn(true);

        // Mock a slow start operation
        CountDownLatch startLatch = slowCassandraStart();
        // Mock a slow stop operation
        CountDownLatch stopLatch = slowCassandraStop();

        LifecycleManager lifecycleManager = new LifecycleManager(metadataFetcher, mockLifecycleProvider, executorPools);
        lifecycleManager.updateDesiredState(TEST_HOST, CassandraState.RUNNING);
        lifecycleManager.updateDesiredState(TEST_HOST_2, CassandraState.STOPPED);


        loopAssert(1, 200, () -> {
            LifecycleInfoResponse response1 = lifecycleManager.getLifecycleInfo(TEST_HOST);
            LifecycleInfoResponse response2 = lifecycleManager.getLifecycleInfo(TEST_HOST_2);

            assertThat(response1.currentState()).isEqualTo(CassandraState.STOPPED);
            assertThat(response1.desiredState()).isEqualTo(CassandraState.RUNNING);
            assertThat(response1.status()).isEqualTo(OperationStatus.CONVERGING);
            assertThat(response1.lastUpdate()).isEqualTo("Starting instance");

            assertThat(response2.currentState()).isEqualTo(CassandraState.RUNNING);
            assertThat(response2.desiredState()).isEqualTo(CassandraState.STOPPED);
            assertThat(response2.status()).isEqualTo(OperationStatus.CONVERGING);
            assertThat(response2.lastUpdate()).isEqualTo("Stopping instance");
        });

        startLatch.countDown();
        stopLatch.countDown();
    }

    private @NotNull CountDownLatch slowCassandraStop()
    {
        CountDownLatch stopLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            try
            {
                stopLatch.await(5, TimeUnit.SECONDS);
                return null;
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }).when(mockLifecycleProvider).stop(TEST_HOST_2_META);
        return stopLatch;
    }

    private @NotNull CountDownLatch slowCassandraStart()
    {
        CountDownLatch startLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            try
            {
                startLatch.await(5, TimeUnit.SECONDS);
                return null;
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }).when(mockLifecycleProvider).start(TEST_HOST_META);
        return startLatch;
    }
}
