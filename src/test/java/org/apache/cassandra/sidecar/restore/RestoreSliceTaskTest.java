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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobTest;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.db.RestoreSliceDatabaseAccessor;
import org.apache.cassandra.sidecar.exceptions.RestoreJobException;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.stats.RestoreJobStats;
import org.apache.cassandra.sidecar.stats.TestRestoreJobStats;
import org.apache.cassandra.sidecar.utils.SSTableImporter;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import static org.apache.cassandra.sidecar.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RestoreSliceTaskTest
{
    private RestoreSlice mockSlice;
    private StorageClient mockStorageClient;
    private SSTableImporter mockSSTableImporter;
    private TaskExecutorPool executorPool;
    private TestRestoreJobStats stats;
    private TestRestoreSliceAccessor sliceDatabaseAccessor;

    @BeforeEach
    void setup()
    {
        mockSlice = mock(RestoreSlice.class);
        when(mockSlice.stageDirectory()).thenReturn(Paths.get("."));
        when(mockSlice.sliceId()).thenReturn("testing-slice");
        when(mockSlice.key()).thenReturn("storage-key");
        InstanceMetadata instance = mock(InstanceMetadata.class);
        when(instance.id()).thenReturn(1);
        when(mockSlice.owner()).thenReturn(instance);
        mockStorageClient = mock(StorageClient.class);
        mockSSTableImporter = mock(SSTableImporter.class);
        executorPool = new ExecutorPools(Vertx.vertx(), new ServiceConfigurationImpl()).internal();
        stats = new TestRestoreJobStats();
        sliceDatabaseAccessor = new TestRestoreSliceAccessor();
    }

    @Test
    void testRestoreSucceeds()
    {
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED);
        when(mockStorageClient.objectExists(mockSlice)).thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorageClient.downloadObjectIfAbsent(mockSlice))
        .thenReturn(CompletableFuture.completedFuture(new File(".")));

        Promise<RestoreSlice> promise = Promise.promise();
        RestoreSliceTask task = createTask(mockSlice, job);
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
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED);
        // the existence of the slice is already confirmed by the s3 client
        when(mockSlice.existsOnS3()).thenReturn(true);
        when(mockStorageClient.downloadObjectIfAbsent(mockSlice))
        .thenReturn(CompletableFuture.completedFuture(new File(".")));

        Promise<RestoreSlice> promise = Promise.promise();
        RestoreSliceTask task = createTask(mockSlice, job);
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
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED);
        when(mockSlice.isCancelled()).thenReturn(true);

        Promise<RestoreSlice> promise = Promise.promise();
        RestoreSliceTask task = createTask(mockSlice, job);
        task.handle(promise);

        assertThatThrownBy(() -> getBlocking(promise.future()))
        .hasRootCauseExactlyInstanceOf(RestoreJobFatalException.class)
        .hasMessageContaining("Restore slice is cancelled");
    }

    @Test
    void testThrowRetryableExceptionOnS3ObjectNotFound()
    {
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED);
        CompletableFuture<HeadObjectResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(mock(NoSuchKeyException.class));
        when(mockStorageClient.objectExists(mockSlice)).thenReturn(failedFuture);

        Promise<RestoreSlice> promise = Promise.promise();
        RestoreSliceTask task = createTask(mockSlice, job);
        task.handle(promise);
        assertThatThrownBy(() -> getBlocking(promise.future()))
        .hasRootCauseExactlyInstanceOf(RestoreJobException.class) // NOT a fatal exception
        .hasMessageContaining("Object not found");
    }

    @Test
    void testSliceStaging()
    {
        // test specific setup
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED, "QUORUM");
        assertThat(job.isManagedBySidecar()).isTrue();
        when(mockSlice.stagedObjectPath()).thenReturn(Paths.get("nonexist"));
        when(mockStorageClient.objectExists(mockSlice)).thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorageClient.downloadObjectIfAbsent(mockSlice))
        .thenReturn(CompletableFuture.completedFuture(new File(".")));

        Promise<RestoreSlice> promise = Promise.promise();
        RestoreSliceTask task = createTask(mockSlice, job);
        task.handle(promise);
        getBlocking(promise.future()); // no error is thrown

        verify(mockSlice, times(1)).completeStagePhase();
        verify(mockSlice, times(0)).completeImportPhase(); // should not be called in this phase
        assertThat(sliceDatabaseAccessor.updateInvokedTimes.get()).isOne();
    }

    @Test
    void testSliceStagingWithExistingObject(@TempDir Path testFolder) throws IOException
    {
        // test specific setup
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED, "QUORUM");
        assertThat(job.isManagedBySidecar()).isTrue();
        Path stagedPath = testFolder.resolve("slice.zip");
        Files.createFile(stagedPath);
        when(mockSlice.stagedObjectPath()).thenReturn(stagedPath);
        when(mockStorageClient.objectExists(mockSlice))
        .thenThrow(new RuntimeException("Should not call this method"));
        when(mockStorageClient.downloadObjectIfAbsent(mockSlice))
        .thenThrow(new RuntimeException("Should not call this method"));

        Promise<RestoreSlice> promise = Promise.promise();
        RestoreSliceTask task = createTask(mockSlice, job);
        task.handle(promise);
        getBlocking(promise.future()); // no error is thrown

        verify(mockSlice, times(1)).completeStagePhase();
        verify(mockSlice, times(0)).completeImportPhase(); // should not be called in this phase
        assertThat(sliceDatabaseAccessor.updateInvokedTimes.get()).isOne();
    }

    @Test
    void testSliceImport(@TempDir Path testFolder) throws IOException
    {
        // test specific setup
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.STAGED, "QUORUM");
        assertThat(job.isManagedBySidecar()).isTrue();
        Path stagedPath = testFolder.resolve("slice.zip");
        Files.createFile(stagedPath);
        when(mockSlice.stagedObjectPath()).thenReturn(stagedPath);

        Promise<RestoreSlice> promise = Promise.promise();
        RestoreSliceTask task = createTask(mockSlice, job);
        task.handle(promise);
        getBlocking(promise.future()); // no error is thrown

        verify(mockSlice, times(0)).completeStagePhase(); // should not be called in the phase
        verify(mockSlice, times(1)).completeImportPhase();
        assertThat(sliceDatabaseAccessor.updateInvokedTimes.get()).isOne();
    }

    @Test
    void testHandlingUnexpectedExceptionInObjectExistsCheck(@TempDir Path testFolder)
    {
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED, "QUORUM");
        when(mockStorageClient.objectExists(mockSlice)).thenThrow(new RuntimeException("Random exception"));
        Path stagedPath = testFolder.resolve("slice.zip");
        when(mockSlice.stagedObjectPath()).thenReturn(stagedPath);

        Promise<RestoreSlice> promise = Promise.promise();

        RestoreSliceTask task = createTask(mockSlice, job);
        task.handle(promise);

        assertThatThrownBy(() -> getBlocking(promise.future()))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Random exception");
    }

    @Test
    void testHandlingUnexpectedExceptionDuringDownloadSliceCheck(@TempDir Path testFolder)
    {
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED, "QUORUM");
        Path stagedPath = testFolder.resolve("slice.zip");
        when(mockSlice.stagedObjectPath()).thenReturn(stagedPath);
        when(mockSlice.isCancelled()).thenReturn(false);
        when(mockStorageClient.objectExists(mockSlice)).thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorageClient.downloadObjectIfAbsent(mockSlice)).thenThrow(new RuntimeException("Random exception"));

        Promise<RestoreSlice> promise = Promise.promise();

        RestoreSliceTask task = createTask(mockSlice, job);
        task.handle(promise);

        assertThatThrownBy(() -> getBlocking(promise.future()))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Random exception");
    }

    @Test
    void testHandlingUnexpectedExceptionDuringUnzip(@TempDir Path testFolder) throws IOException
    {

        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.STAGED, "QUORUM");
        Path stagedPath = testFolder.resolve("slice.zip");
        Files.createFile(stagedPath);
        when(mockSlice.stagedObjectPath()).thenReturn(stagedPath);
        Promise<RestoreSlice> promise = Promise.promise();
        RestoreSliceTask task = createTaskWithExceptions(mockSlice, job);
        task.handle(promise);

        assertThatThrownBy(() -> getBlocking(promise.future()))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Random exception");
    }

    @Test
    void testHandlingUnexpectedExceptionDuringDownloadAndImport(@TempDir Path testFolder)
    {
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED, null);
        Path stagedPath = testFolder.resolve("slice.zip");
        when(mockSlice.stagedObjectPath()).thenReturn(stagedPath);
        when(mockSlice.isCancelled()).thenReturn(false);
        when(mockStorageClient.objectExists(mockSlice)).thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorageClient.downloadObjectIfAbsent(mockSlice)).thenReturn(CompletableFuture.completedFuture(null));

        Promise<RestoreSlice> promise = Promise.promise();

        RestoreSliceTask task = createTaskWithExceptions(mockSlice, job);
        task.handle(promise);

        assertThatThrownBy(() -> getBlocking(promise.future()))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Random exception");
    }

    @Test
    void testSliceDuration()
    {
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.STAGED, "QUORUM");
        AtomicLong currentNanos = new AtomicLong(0);
        RestoreSliceTask task = createTask(mockSlice, job, currentNanos::get);
        currentNanos.set(123L);
        assertThat(task.getDuration()).isEqualTo(123L);
    }

    private RestoreSliceTask createTask(RestoreSlice slice, RestoreJob job)
    {
        return createTask(slice, job, System::nanoTime);
    }

    private RestoreSliceTask createTask(RestoreSlice slice, RestoreJob job, Supplier<Long> currentNanoTimeSupplier)
    {
        when(slice.job()).thenReturn(job);
        assertThat(slice.job()).isSameAs(job);
        assertThat(slice.job().isManagedBySidecar()).isEqualTo(job.isManagedBySidecar());
        assertThat(slice.job().status).isEqualTo(job.status);
        return new TestRestoreSliceTask(slice, mockStorageClient, executorPool, mockSSTableImporter,
                                        0, sliceDatabaseAccessor, stats, currentNanoTimeSupplier);
    }

    private RestoreSliceTask createTaskWithExceptions(RestoreSlice slice, RestoreJob job)
    {
        when(slice.job()).thenReturn(job);
        assertThat(slice.job()).isSameAs(job);
        assertThat(slice.job().isManagedBySidecar()).isEqualTo(job.isManagedBySidecar());
        assertThat(slice.job().status).isEqualTo(job.status);
        return new TestUnexpectedExceptionInRestoreSliceTask(slice, mockStorageClient, executorPool,
                                                             mockSSTableImporter, 0, sliceDatabaseAccessor, stats);
    }

    static class TestRestoreSliceAccessor extends RestoreSliceDatabaseAccessor
    {
        public final AtomicInteger updateInvokedTimes = new AtomicInteger(0);

        protected TestRestoreSliceAccessor()
        {
            super(null, null, null);
        }

        @Override
        public RestoreSlice updateStatus(RestoreSlice slice)
        {
            updateInvokedTimes.getAndIncrement();
            return slice;
        }
    }

    static class TestRestoreSliceTask extends RestoreSliceTask
    {
        private final RestoreSlice slice;
        private final RestoreJobStats stats;

        public TestRestoreSliceTask(RestoreSlice slice, StorageClient s3Client, TaskExecutorPool executorPool,
                                    SSTableImporter importer, double requiredUsableSpacePercentage,
                                    RestoreSliceDatabaseAccessor sliceDatabaseAccessor, RestoreJobStats stats,
                                    Supplier<Long> currentNanoTimeSupplier)
        {
            super(slice, s3Client, executorPool, importer, requiredUsableSpacePercentage,
                  sliceDatabaseAccessor, stats, null, currentNanoTimeSupplier);
            this.slice = slice;
            this.stats = stats;
        }

        @Override
        Future<Void> unzipAndImport(Promise<RestoreSlice> event, File file, Runnable onSuccessCommit)
        {
            stats.captureSliceUnzipTime(1, 123L);
            stats.captureSliceValidationTime(1, 123L);
            stats.captureSliceImportTime(1, 123L);
            slice.completeImportPhase();
            if (onSuccessCommit != null)
            {
                onSuccessCommit.run();
            }
            event.tryComplete(slice);
            return Future.succeededFuture();
        }

        @Override
        Future<Void> unzipAndImport(Promise<RestoreSlice> event, File file)
        {
            return unzipAndImport(event, file, null);
        }
    }

    static class TestUnexpectedExceptionInRestoreSliceTask extends RestoreSliceTask
    {
        public TestUnexpectedExceptionInRestoreSliceTask(RestoreSlice slice, StorageClient s3Client,
                                                         TaskExecutorPool executorPool, SSTableImporter importer,
                                                         double requiredUsableSpacePercentage,
                                                         RestoreSliceDatabaseAccessor sliceDatabaseAccessor,
                                                         RestoreJobStats stats)
        {
            super(slice, s3Client, executorPool, importer, requiredUsableSpacePercentage, sliceDatabaseAccessor, stats,
                  null);
        }

        @Override
        Future<Void> unzipAndImport(Promise<RestoreSlice> event, File file, Runnable onSuccessCommit)
        {
            throw new RuntimeException("Random exception");
        }
    }
}
