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
import java.util.Collections;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link NodeMoveJob}
 */
class NodeMoveJobTest
{
    public static final String OPERATION_MOVE = "move";
    public static final String OPERATION_MODE_MOVING = "MOVING";
    public static final String OPERATION_MODE_NORMAL = "NORMAL";
    public static final String OPERATION_MODE_JOINING = "JOINING";
    private StorageOperations mockStorageOperations;
    private UUID jobId;
    private String newToken;

    @BeforeEach
    void setUp()
    {
        mockStorageOperations = mock(StorageOperations.class);
        jobId = UUIDs.timeBased();
        newToken = "123456789";
    }

    @Test
    void testJobName()
    {
        NodeMoveJob job = new NodeMoveJob(jobId, newToken, mockStorageOperations);
        assertThat(job.name()).isEqualTo(OPERATION_MOVE);
    }

    @Test
    void testJobId()
    {
        NodeMoveJob job = new NodeMoveJob(jobId, newToken, mockStorageOperations);
        assertThat(job.jobId()).isEqualTo(jobId);
    }

    @Test
    void testIsRunningOnCassandraWhenMoving()
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_MOVING);
        NodeMoveJob job = new NodeMoveJob(jobId, newToken, mockStorageOperations);
        assertThat(job.hasConflict(Collections.emptyList())).isTrue();
    }

    @Test
    void testIsRunningOnCassandraWhenNormal()
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);
        NodeMoveJob job = new NodeMoveJob(jobId, newToken, mockStorageOperations);
        assertThat(job.hasConflict(Collections.emptyList())).isFalse();
    }

    @Test
    void testIsRunningOnCassandraWhenOtherMode()
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_JOINING);
        NodeMoveJob job = new NodeMoveJob(jobId, newToken, mockStorageOperations);
        assertThat(job.hasConflict(Collections.emptyList())).isFalse();
    }

    @Test
    void testStatusWhenNormal()
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);
        NodeMoveJob job = new NodeMoveJob(jobId, newToken, mockStorageOperations);
        assertThat(job.status()).isEqualTo(OperationalJobStatus.CREATED);
    }

    @Test
    void testStatusWhenFailed() throws IOException
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);
        RuntimeException testException = new RuntimeException("Test failure");
        doThrow(testException).when(mockStorageOperations).move(newToken);

        NodeMoveJob job = new NodeMoveJob(jobId, newToken, mockStorageOperations);

        Promise<Void> promise = Promise.promise();
        job.execute(promise);

        assertThat(promise.future().failed()).isTrue();
        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
    }

    @Test
    void testExecuteInternalCallsMove() throws IOException
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);
        NodeMoveJob job = new NodeMoveJob(jobId, newToken, mockStorageOperations);

        Promise<Void> promise = Promise.promise();
        job.execute(promise);

        verify(mockStorageOperations).move(newToken);
        assertThat(promise.future().succeeded()).isTrue();
    }

    @Test
    void testExecuteInternalHandlesException() throws IOException
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);
        RuntimeException testException = new RuntimeException("Test exception");
        doThrow(testException).when(mockStorageOperations).move(newToken);

        NodeMoveJob job = new NodeMoveJob(jobId, newToken, mockStorageOperations);

        Promise<Void> promise = Promise.promise();
        job.execute(promise);

        verify(mockStorageOperations).move(newToken);
        assertThat(promise.future().failed()).isTrue();
        assertThat(promise.future().cause()).isInstanceOf(OperationalJobException.class);
        assertThat(promise.future().cause().getCause()).isEqualTo(testException);
        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
    }

    @Test
    void testJobWithNegativeToken()
    {
        String negativeToken = "-9223372036854775808";
        NodeMoveJob job = new NodeMoveJob(jobId, negativeToken, mockStorageOperations);
        assertThat(job.name()).isEqualTo(OPERATION_MOVE);
        assertThat(job.jobId()).isEqualTo(jobId);
    }
}
