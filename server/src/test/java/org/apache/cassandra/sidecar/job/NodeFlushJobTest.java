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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link NodeFlushJob}
 */
class NodeFlushJobTest
{
    @Test
    void testConstructor()
    {
        UUID jobId = UUIDs.timeBased();
        StorageOperations storageOps = mock(StorageOperations.class);
        String keyspace = "testkeyspace";
        List<String> tableNames = Arrays.asList("table1", "table2");

        NodeFlushJob job = new NodeFlushJob(jobId, storageOps, keyspace, tableNames);

        assertThat(job.jobId()).isEqualTo(jobId);
        assertThat(job.name()).isEqualTo("flush");
        assertThat(job.isRunningOnCassandra()).isFalse();
    }

    @Test
    void testConstructorWithNullTableNames()
    {
        UUID jobId = UUIDs.timeBased();
        StorageOperations storageOps = mock(StorageOperations.class);
        String keyspace = "testkeyspace";

        NodeFlushJob job = new NodeFlushJob(jobId, storageOps, keyspace, null);

        assertThat(job.jobId()).isEqualTo(jobId);
        assertThat(job.name()).isEqualTo("flush");
        assertThat(job.isRunningOnCassandra()).isFalse();
    }

    @Test
    void testExecuteInternalWithMultipleTables() throws IOException
    {
        UUID jobId = UUIDs.timeBased();
        StorageOperations storageOps = mock(StorageOperations.class);
        String keyspace = "testkeyspace";
        List<String> tableNames = Arrays.asList("table1", "table2");

        NodeFlushJob job = new NodeFlushJob(jobId, storageOps, keyspace, tableNames);
        Promise<Void> promise = Promise.promise();

        job.execute(promise);

        assertThat(promise.future().succeeded()).isTrue();
        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        verify(storageOps).flush(keyspace, "table1", "table2");
    }

    @Test
    void testExecuteInternalWithSingleTable() throws IOException
    {
        UUID jobId = UUIDs.timeBased();
        StorageOperations storageOps = mock(StorageOperations.class);
        String keyspace = "testkeyspace";
        List<String> tableNames = Collections.singletonList("table1");

        NodeFlushJob job = new NodeFlushJob(jobId, storageOps, keyspace, tableNames);
        Promise<Void> promise = Promise.promise();

        job.execute(promise);

        assertThat(promise.future().succeeded()).isTrue();
        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        verify(storageOps).flush(keyspace, "table1");
    }

    @Test
    void testExecuteInternalWithEmptyTableList() throws IOException
    {
        UUID jobId = UUIDs.timeBased();
        StorageOperations storageOps = mock(StorageOperations.class);
        String keyspace = "testkeyspace";
        List<String> tableNames = Collections.emptyList();

        NodeFlushJob job = new NodeFlushJob(jobId, storageOps, keyspace, tableNames);
        Promise<Void> promise = Promise.promise();

        job.execute(promise);

        assertThat(promise.future().succeeded()).isTrue();
        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        verify(storageOps).flush(keyspace);
    }

    @Test
    void testExecuteInternalWithNullTableList() throws IOException
    {
        UUID jobId = UUIDs.timeBased();
        StorageOperations storageOps = mock(StorageOperations.class);
        String keyspace = "testkeyspace";

        NodeFlushJob job = new NodeFlushJob(jobId, storageOps, keyspace, null);
        Promise<Void> promise = Promise.promise();

        job.execute(promise);

        assertThat(promise.future().succeeded()).isTrue();
        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        verify(storageOps).flush(keyspace);
    }

    @Test
    void testExecuteInternalFailure() throws IOException
    {
        UUID jobId = UUIDs.timeBased();
        StorageOperations storageOps = mock(StorageOperations.class);
        String keyspace = "testkeyspace";
        List<String> tableNames = Arrays.asList("table1", "table2");
        String errorMessage = "Flush operation failed";

        doThrow(new IOException(errorMessage)).when(storageOps).flush(keyspace, "table1", "table2");

        NodeFlushJob job = new NodeFlushJob(jobId, storageOps, keyspace, tableNames);
        Promise<Void> promise = Promise.promise();

        job.execute(promise);

        assertThat(promise.future().failed()).isTrue();
        assertThat(promise.future().cause())
        .isInstanceOf(OperationalJobException.class)
        .hasCauseInstanceOf(IOException.class);
        assertThat(promise.future().cause().getCause().getMessage()).isEqualTo(errorMessage);
        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
        verify(storageOps).flush(keyspace, "table1", "table2");
    }

    @Test
    void testExecuteInternalRuntimeException() throws IOException
    {
        UUID jobId = UUIDs.timeBased();
        StorageOperations storageOps = mock(StorageOperations.class);
        String keyspace = "testkeyspace";
        List<String> tableNames = Arrays.asList("table1", "table2");
        String errorMessage = "Runtime error during flush";

        doThrow(new RuntimeException(errorMessage)).when(storageOps).flush(keyspace, "table1", "table2");

        NodeFlushJob job = new NodeFlushJob(jobId, storageOps, keyspace, tableNames);
        Promise<Void> promise = Promise.promise();

        job.execute(promise);

        assertThat(promise.future().failed()).isTrue();
        assertThat(promise.future().cause())
        .isInstanceOf(OperationalJobException.class)
        .hasCauseInstanceOf(RuntimeException.class);
        assertThat(promise.future().cause().getCause().getMessage()).isEqualTo(errorMessage);
        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
        verify(storageOps).flush(keyspace, "table1", "table2");
    }

    @Test
    void testJobIdValidation()
    {
        StorageOperations storageOps = mock(StorageOperations.class);
        String keyspace = "testkeyspace";
        List<String> tableNames = Arrays.asList("table1", "table2");

        // Test with non-time-based UUID should fail
        UUID randomUuid = UUID.randomUUID();
        assertThatThrownBy(() -> new NodeFlushJob(randomUuid, storageOps, keyspace, tableNames))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("OperationalJob accepts only time-based UUID");
    }

    @Test
    void testJobStatusLifecycle()
    {
        UUID jobId = UUIDs.timeBased();
        StorageOperations storageOps = mock(StorageOperations.class);
        String keyspace = "testkeyspace";
        List<String> tableNames = Arrays.asList("table1", "table2");

        NodeFlushJob job = new NodeFlushJob(jobId, storageOps, keyspace, tableNames);

        // Initial status should be CREATED
        assertThat(job.status()).isEqualTo(OperationalJobStatus.CREATED);
        assertThat(job.isExecuting()).isFalse();

        Promise<Void> promise = Promise.promise();
        job.execute(promise);

        // After successful execution, status should be SUCCEEDED
        assertThat(promise.future().succeeded()).isTrue();
        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
    }

    @Test
    void testCreationTime()
    {
        UUID jobId = UUIDs.timeBased();
        StorageOperations storageOps = mock(StorageOperations.class);
        String keyspace = "testkeyspace";
        List<String> tableNames = Arrays.asList("table1", "table2");

        long beforeCreation = System.currentTimeMillis();
        NodeFlushJob job = new NodeFlushJob(jobId, storageOps, keyspace, tableNames);
        long afterCreation = System.currentTimeMillis();

        long creationTime = job.creationTime();
        assertThat(creationTime).isBetween(beforeCreation - 1000, afterCreation + 1000); // Allow 1s tolerance
    }

    @Test
    void testIsStale()
    {
        UUID jobId = UUIDs.timeBased();
        StorageOperations storageOps = mock(StorageOperations.class);
        String keyspace = "testkeyspace";
        List<String> tableNames = Arrays.asList("table1", "table2");

        NodeFlushJob job = new NodeFlushJob(jobId, storageOps, keyspace, tableNames);

        long currentTime = System.currentTimeMillis();
        long ttl = 5000; // 5 seconds

        // Job should not be stale immediately
        assertThat(job.isStale(currentTime, ttl)).isFalse();

        // Job should be stale if reference time is beyond TTL
        assertThat(job.isStale(currentTime + ttl + 1000, ttl)).isTrue();
    }

    @Test
    void testAsyncResult()
    {
        UUID jobId = UUIDs.timeBased();
        StorageOperations storageOps = mock(StorageOperations.class);
        String keyspace = "testkeyspace";
        List<String> tableNames = Arrays.asList("table1", "table2");

        NodeFlushJob job = new NodeFlushJob(jobId, storageOps, keyspace, tableNames);

        // Initially, async result should not be complete
        assertThat(job.asyncResult().isComplete()).isFalse();

        Promise<Void> promise = Promise.promise();
        job.execute(promise);

        // After execution, async result should be complete
        assertThat(job.asyncResult().isComplete()).isTrue();
        assertThat(job.asyncResult().succeeded()).isTrue();
    }
}
