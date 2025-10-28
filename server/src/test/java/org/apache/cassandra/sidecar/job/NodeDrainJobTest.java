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

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link NodeDrainJob}
 */
class NodeDrainJobTest
{
    private static final String OPERATION_MODE_DRAINING = "DRAINING";
    private static final String OPERATION_MODE_DRAINED = "DRAINED";
    private static final String OPERATION_MODE_NORMAL = "NORMAL";
    private static final String OPERATION_MODE_UNKNOWN = "UNKNOWN";
    private static final String JOB_NAME_DRAIN = "drain";

    private StorageOperations mockStorageOperations;
    private NodeDrainJob nodeDrainJob;
    private UUID jobId;

    @BeforeEach
    void setup()
    {
        mockStorageOperations = mock(StorageOperations.class);
        jobId = UUIDs.timeBased();
        nodeDrainJob = new NodeDrainJob(jobId, mockStorageOperations);
    }

    @Test
    void testJobIdAndName()
    {
        assertThat(nodeDrainJob.jobId()).isEqualTo(jobId);
        assertThat(nodeDrainJob.name()).isEqualTo(JOB_NAME_DRAIN);
    }

    @Test
    void testIsRunningOnCassandra_WhenDraining()
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_DRAINING);

        assertThat(nodeDrainJob.isRunningOnCassandra()).isTrue();
    }

    @Test
    void testIsRunningOnCassandra_WhenDrained()
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_DRAINED);

        assertThat(nodeDrainJob.isRunningOnCassandra()).isFalse();
    }

    @Test
    void testIsRunningOnCassandra_WhenNormal()
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);

        assertThat(nodeDrainJob.isRunningOnCassandra()).isFalse();
    }

    @Test
    void testIsRunningOnCassandra_WhenUnknownState()
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_UNKNOWN);

        assertThat(nodeDrainJob.isRunningOnCassandra()).isFalse();
    }

    @Test
    void testIsRunningOnCassandra_WhenNull()
    {
        when(mockStorageOperations.operationMode()).thenReturn(null);

        assertThat(nodeDrainJob.isRunningOnCassandra()).isFalse();
    }

    @Test
    void testStatus_WhenDraining()
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_DRAINING);

        assertThat(nodeDrainJob.status()).isEqualTo(OperationalJobStatus.RUNNING);
    }

    @Test
    void testStatus_WhenDrained()
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_DRAINED);

        assertThat(nodeDrainJob.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
    }

    @Test
    void testStatus_WhenNormal()
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);

        assertThat(nodeDrainJob.status()).isEqualTo(OperationalJobStatus.CREATED);
    }

    @Test
    void testStatus_WhenUnknownState()
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_UNKNOWN);

        assertThat(nodeDrainJob.status()).isEqualTo(OperationalJobStatus.CREATED);
    }

    @Test
    void testStatus_WhenNull()
    {
        when(mockStorageOperations.operationMode()).thenReturn(null);

        assertThat(nodeDrainJob.status()).isEqualTo(OperationalJobStatus.CREATED);
    }

    @Test
    void testExecuteInternal_WhenNotRunning() throws IOException, ExecutionException, InterruptedException
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);

        nodeDrainJob.executeInternal();

        verify(mockStorageOperations).drain();
    }

    @Test
    void testExecuteInternal_WhenAlreadyDraining() throws IOException, ExecutionException, InterruptedException
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_DRAINING);

        nodeDrainJob.executeInternal();

        verify(mockStorageOperations, never()).drain();
    }

    @Test
    void testExecuteInternal_WhenDrainThrowsIOException() throws IOException, ExecutionException, InterruptedException
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);
        IOException ioException = new IOException("Drain failed due to IO error");
        doThrow(ioException).when(mockStorageOperations).drain();

        assertThatThrownBy(() -> nodeDrainJob.executeInternal())
        .isInstanceOf(OperationalJobException.class)
        .hasCause(ioException);

        verify(mockStorageOperations).drain();
    }

    @Test
    void testExecuteInternal_WhenDrainThrowsExecutionException() throws IOException, ExecutionException, InterruptedException
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);
        ExecutionException executionException = new ExecutionException("Drain failed during execution", new RuntimeException());
        doThrow(executionException).when(mockStorageOperations).drain();

        assertThatThrownBy(() -> nodeDrainJob.executeInternal())
        .isInstanceOf(OperationalJobException.class)
        .hasCause(executionException);

        verify(mockStorageOperations).drain();
    }

    @Test
    void testExecuteInternal_WhenDrainThrowsInterruptedException() throws IOException, ExecutionException, InterruptedException
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);
        InterruptedException interruptedException = new InterruptedException("Drain was interrupted");
        doThrow(interruptedException).when(mockStorageOperations).drain();

        assertThatThrownBy(() -> nodeDrainJob.executeInternal())
        .isInstanceOf(OperationalJobException.class)
        .hasCause(interruptedException);

        verify(mockStorageOperations).drain();
    }

    @Test
    void testNodeDrainStateEnum_FromOperationMode()
    {
        assertThat(NodeDrainJob.NodeDrainStateEnum.fromOperationMode(OPERATION_MODE_DRAINING))
        .isEqualTo(NodeDrainJob.NodeDrainStateEnum.DRAINING);

        assertThat(NodeDrainJob.NodeDrainStateEnum.fromOperationMode(OPERATION_MODE_DRAINED))
        .isEqualTo(NodeDrainJob.NodeDrainStateEnum.DRAINED);

        assertThat(NodeDrainJob.NodeDrainStateEnum.fromOperationMode(OPERATION_MODE_NORMAL))
        .isNull();

        assertThat(NodeDrainJob.NodeDrainStateEnum.fromOperationMode(OPERATION_MODE_UNKNOWN))
        .isNull();

        assertThat(NodeDrainJob.NodeDrainStateEnum.fromOperationMode(null))
        .isNull();

        assertThat(NodeDrainJob.NodeDrainStateEnum.fromOperationMode(""))
        .isNull();
    }

    @Test
    void testNodeDrainStateEnum_JobStatusMapping()
    {
        assertThat(NodeDrainJob.NodeDrainStateEnum.DRAINING.jobStatus)
        .isEqualTo(OperationalJobStatus.RUNNING);

        assertThat(NodeDrainJob.NodeDrainStateEnum.DRAINED.jobStatus)
        .isEqualTo(OperationalJobStatus.SUCCEEDED);
    }

    @Test
    void testNodeDrainStateEnum_Values()
    {
        NodeDrainJob.NodeDrainStateEnum[] expectedValues = {
        NodeDrainJob.NodeDrainStateEnum.DRAINING,
        NodeDrainJob.NodeDrainStateEnum.DRAINED
        };

        assertThat(NodeDrainJob.NodeDrainStateEnum.values()).containsExactly(expectedValues);
    }

    @Test
    void testNodeDrainStateEnum_ValueOf()
    {
        assertThat(NodeDrainJob.NodeDrainStateEnum.valueOf(OPERATION_MODE_DRAINING))
        .isEqualTo(NodeDrainJob.NodeDrainStateEnum.DRAINING);

        assertThat(NodeDrainJob.NodeDrainStateEnum.valueOf(OPERATION_MODE_DRAINED))
        .isEqualTo(NodeDrainJob.NodeDrainStateEnum.DRAINED);

        assertThatThrownBy(() -> NodeDrainJob.NodeDrainStateEnum.valueOf(OPERATION_MODE_NORMAL))
        .isInstanceOf(IllegalArgumentException.class);
    }
}
