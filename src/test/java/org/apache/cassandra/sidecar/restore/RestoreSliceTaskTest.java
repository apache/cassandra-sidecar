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

package org.apache.cassandra.sidecar.restore;

import java.io.File;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools.TaskExecutorPool;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.exceptions.RestoreJobException;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.stats.RestoreJobStats;
import org.apache.cassandra.sidecar.stats.TestRestoreJobStats;
import org.apache.cassandra.sidecar.utils.SSTableImporter;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import static org.apache.cassandra.sidecar.AssertionUtils.getBlocking;
import static org.apache.cassandra.sidecar.db.RestoreJob.toLocalDate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RestoreSliceTaskTest
{
    private RestoreJob restoreJob;
    private RestoreSlice restoreSlice;
    private StorageClient storageClient;
    private TaskExecutorPool executorPool;
    private SSTableImporter importer;
    private TestRestoreJobStats stats;
    private RestoreSliceTask task;

    @BeforeEach
    void setup()
    {
        UUID jobId = UUIDs.timeBased();
        restoreJob = RestoreJob.builder()
                               .jobId(jobId)
                               .createdAt(toLocalDate(jobId))
                               .jobStatus(RestoreJobStatus.CREATED)
                               .build();
        restoreSlice = mock(RestoreSlice.class, Mockito.RETURNS_DEEP_STUBS);
        when(restoreSlice.targetPathInStaging()).thenReturn(Paths.get("."));
        when(restoreSlice.sliceId()).thenReturn("testing-slice");
        when(restoreSlice.key()).thenReturn("storage-key");
        when(restoreSlice.owner().id()).thenReturn(1);
        storageClient = mock(StorageClient.class);
        importer = mock(SSTableImporter.class);
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(new TestModule()));
        executorPool = injector.getInstance(ExecutorPools.class).internal();
        stats = new TestRestoreJobStats();
        task = new TestRestoreSliceTask(restoreJob, restoreSlice, storageClient,
                                        executorPool, importer, 0, stats);
    }

    @Test
    void testRestoreSucceeds()
    {
        when(storageClient.objectExists(restoreSlice)).thenReturn(CompletableFuture.completedFuture(null));
        when(storageClient.downloadObjectIfAbsent(restoreSlice))
        .thenReturn(CompletableFuture.completedFuture(new File(".")));
        Promise<RestoreSlice> promise = Promise.promise();
        task.handle(promise);
        getBlocking(promise.future()); // no error is thrown

        // assert on the stats collected
        assertThat(stats.sliceReplicationTimes).hasSize(1);
        assertThat(stats.sliceReplicationTimes.get(0)).isPositive();
        assertThat(stats.sliceDownloadTimes).hasSize(1);
        assertThat(stats.sliceDownloadTimes.get(0)).isPositive();
        assertThat(stats.sliceUnzipTimes).hasSize(1);
        assertThat(stats.sliceUnzipTimes.get(0)).isPositive();
        assertThat(stats.sliceValidationTimes).hasSize(1);
        assertThat(stats.sliceValidationTimes.get(0)).isPositive();
        assertThat(stats.sliceImportTimes).hasSize(1);
        assertThat(stats.sliceImportTimes.get(0)).isPositive();
    }

    @Test
    void testCaptureSliceReplicationTimeOnlyOnce()
    {
        // the existence of the slice is already confirmed by the s3 client
        when(restoreSlice.existsOnS3()).thenReturn(true);
        when(storageClient.downloadObjectIfAbsent(restoreSlice))
        .thenReturn(CompletableFuture.completedFuture(new File(".")));

        Promise<RestoreSlice> promise = Promise.promise();
        task.handle(promise);
        getBlocking(promise.future()); // no error is thrown

        assertThat(stats.sliceReplicationTimes)
        .describedAs("The replication time of the slice has been captured when confirming the existence." +
                     "It should not be captured again in this run.")
        .isEmpty();
    }

    @Test
    void testStopProcessingCancelledSlice()
    {
        when(restoreSlice.isCancelled()).thenReturn(true);

        Promise<RestoreSlice> promise = Promise.promise();
        task.handle(promise);

        assertThatThrownBy(() -> getBlocking(promise.future()))
        .hasRootCauseExactlyInstanceOf(RestoreJobFatalException.class)
        .hasMessageContaining("Restore slice is cancelled");
    }

    @Test
    void testThrowRetryableExceptionOnS3ObjectNotFound()
    {
        CompletableFuture<HeadObjectResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(mock(NoSuchKeyException.class));
        when(storageClient.objectExists(restoreSlice)).thenReturn(failedFuture);

        Promise<RestoreSlice> promise = Promise.promise();
        task.handle(promise);
        assertThatThrownBy(() -> getBlocking(promise.future()))
        .hasRootCauseExactlyInstanceOf(RestoreJobException.class) // NOT a fatal exception
        .hasMessageContaining("Object not found");
    }

    static class TestRestoreSliceTask extends RestoreSliceTask
    {
        private final RestoreSlice slice;
        private final RestoreJobStats stats;

        public TestRestoreSliceTask(RestoreJob job, RestoreSlice slice, StorageClient s3Client,
                                    TaskExecutorPool executorPool, SSTableImporter importer,
                                    double requiredUsableSpacePercentage, RestoreJobStats stats)
        {
            super(job, slice, s3Client, executorPool, importer, requiredUsableSpacePercentage, stats);
            this.slice = slice;
            this.stats = stats;
        }

        void unzipAndImport(Promise<RestoreSlice> event, File file)
        {
            stats.captureSliceUnzipTime(1, 123L);
            stats.captureSliceValidationTime(1, 123L);
            stats.captureSliceImportTime(1, 123L);
            event.tryComplete(slice);
        }
    }
}
