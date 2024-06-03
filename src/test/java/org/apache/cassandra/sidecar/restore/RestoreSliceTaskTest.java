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
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobTest;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.db.RestoreSliceDatabaseAccessor;
import org.apache.cassandra.sidecar.exceptions.RestoreJobException;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.exceptions.ThrowableUtils;
import org.apache.cassandra.sidecar.locator.LocalTokenRangesProvider;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetricsImpl;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetricsImpl;
import org.apache.cassandra.sidecar.metrics.instance.InstanceRestoreMetrics;
import org.apache.cassandra.sidecar.restore.RestoreSliceManifest.ManifestEntry;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.SSTableImporter;
import org.apache.cassandra.sidecar.utils.XXHash32Provider;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import static org.apache.cassandra.sidecar.AssertionUtils.getBlocking;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RestoreSliceTaskTest
{
    private RestoreSlice mockSlice;
    private StorageClient mockStorageClient;
    private SSTableImporter mockSSTableImporter;
    private TaskExecutorPool executorPool;
    private SidecarMetrics metrics;
    private TestRestoreSliceAccessor sliceDatabaseAccessor;
    private LocalTokenRangesProvider localTokenRangesProvider;
    private RestoreJobUtil util;

    @BeforeEach
    void setup()
    {
        mockSlice = mock(RestoreSlice.class);
        when(mockSlice.stageDirectory()).thenReturn(Paths.get("."));
        when(mockSlice.sliceId()).thenReturn("testing-slice");
        when(mockSlice.key()).thenReturn("storage-key");
        InstanceMetadata instance = mock(InstanceMetadata.class);
        when(instance.id()).thenReturn(1);
        when(instance.metrics()).thenReturn(new InstanceMetricsImpl(registry(1)));
        InstanceMetadataFetcher mockInstanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        when(mockInstanceMetadataFetcher.instance(1)).thenReturn(instance);
        when(mockSlice.owner()).thenReturn(instance);
        mockStorageClient = mock(StorageClient.class);
        mockSSTableImporter = mock(SSTableImporter.class);
        executorPool = new ExecutorPools(Vertx.vertx(), new ServiceConfigurationImpl()).internal();
        MetricRegistryFactory mockRegistryFactory = mock(MetricRegistryFactory.class);
        when(mockRegistryFactory.getOrCreate()).thenReturn(registry());
        when(mockRegistryFactory.getOrCreate(1)).thenReturn(registry(1));
        metrics = new SidecarMetricsImpl(mockRegistryFactory, mockInstanceMetadataFetcher);
        localTokenRangesProvider = mock(LocalTokenRangesProvider.class);
        util = new RestoreJobUtil(new XXHash32Provider());
        sliceDatabaseAccessor = new TestRestoreSliceAccessor();
    }

    @AfterEach
    void clear()
    {
        registry().removeMatching((name, metric) -> true);
        registry(1).removeMatching((name, metric) -> true);
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
        InstanceRestoreMetrics instanceRestoreMetrics = metrics.instance(1).restore();
        assertThat(metrics.server().restore().sliceReplicationTime.metric.getSnapshot().getValues()).hasSize(1);
        assertThat(metrics.server().restore().sliceReplicationTime.metric.getSnapshot().getValues()[0]).isPositive();
        assertThat(instanceRestoreMetrics.sliceDownloadTime.metric.getSnapshot().getValues()).hasSize(1);
        assertThat(instanceRestoreMetrics.sliceDownloadTime.metric.getSnapshot().getValues()[0]).isPositive();
        assertThat(instanceRestoreMetrics.sliceUnzipTime.metric.getSnapshot().getValues()).hasSize(1);
        assertThat(instanceRestoreMetrics.sliceUnzipTime.metric.getSnapshot().getValues()[0]).isPositive();
        assertThat(instanceRestoreMetrics.sliceValidationTime.metric.getSnapshot().getValues()).hasSize(1);
        assertThat(instanceRestoreMetrics.sliceValidationTime.metric.getSnapshot().getValues()[0]).isPositive();
        assertThat(instanceRestoreMetrics.sliceImportTime.metric.getSnapshot().getValues()).hasSize(1);
        assertThat(instanceRestoreMetrics.sliceImportTime.metric.getSnapshot().getValues()[0]).isPositive();
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

        assertThat(metrics.server().restore().sliceReplicationTime.metric.getSnapshot().getValues())
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
        Promise<RestoreSlice> promise = Promise.promise();
        task.handle(promise); // Task isn't considered started until it `handle` is called
        currentNanos.set(123L);
        assertThat(task.elapsedInNanos()).isEqualTo(123L);
    }

    @Test
    void testRemoveOutOfRangeSSTables(@TempDir Path tempDir) throws RestoreJobException, IOException
    {
        // TODO: update test to use replica ranges implementation
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.STAGED, "QUORUM");
        RestoreSliceTask task = createTask(mockSlice, job);

        // the mocked localTokenRangesProvider returns null, so retry later
        RestoreSliceManifest manifest = new RestoreSliceManifest();
        assertThatThrownBy(() -> task.removeOutOfRangeSSTablesUnsafe(tempDir.toFile(), manifest))
        .isExactlyInstanceOf(RestoreJobException.class)
        .hasMessageContaining("Unable to fetch local range, retry later");

        // enclosed in the node's owning range: [1, 10] is fully enclosed in (0, 100]
        // it should not remove the manifest entry, and no cleanup is needed
        Map<Integer, Set<TokenRange>> localRanges = new HashMap<>(1);
        Set<TokenRange> nodeRanges = new HashSet<>();
        nodeRanges.add(new TokenRange(0, 100)); // not using vnode, so a single range
        localRanges.put(1, nodeRanges); // instance id is 1. See setup()
        when(localTokenRangesProvider.localTokenRanges(any())).thenReturn(localRanges);
        ManifestEntry rangeEnclosed = new ManifestEntry(Collections.emptyMap(),
                                                        BigInteger.valueOf(1), // start
                                                        BigInteger.valueOf(10)); // end
        manifest.put("foo-", rangeEnclosed);
        task.removeOutOfRangeSSTablesUnsafe(tempDir.toFile(), manifest);
        assertThat(manifest).hasSize(1);
        verify(mockSlice, times(0)).requestOutOfRangeDataCleanup();

        // fully out of range: [-10, 0] is fully out of range of (0, 100]
        // it should remove the manifest entry entirely; no clean up required
        manifest.clear();
        ManifestEntry outOfRange = new ManifestEntry(Collections.emptyMap(),
                                                     BigInteger.valueOf(-10), // start
                                                     BigInteger.valueOf(0)); // end
        manifest.put("foo-", outOfRange);
        task.removeOutOfRangeSSTablesUnsafe(tempDir.toFile(), manifest);
        assertThat(manifest).isEmpty();
        verify(mockSlice, times(0)).requestOutOfRangeDataCleanup();

        // partially out of range: [-10, 10] is partially out of range of (0, 100]
        // it should not remove the manifest entry, but it should signal to request out of range data cleanup
        manifest.clear();
        ManifestEntry partiallyOutOfRange = new ManifestEntry(Collections.emptyMap(),
                                                              BigInteger.valueOf(-10), // start
                                                              BigInteger.valueOf(10)); // end
        manifest.put("foo-", partiallyOutOfRange);
        task.removeOutOfRangeSSTablesUnsafe(tempDir.toFile(), manifest);
        assertThat(manifest).hasSize(1);
        verify(mockSlice, times(1)).requestOutOfRangeDataCleanup();
    }

    @Test
    void testCompareChecksum(@TempDir Path tempDir) throws RestoreJobFatalException, IOException
    {
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED);
        RestoreSliceTask task = createTask(mockSlice, job);

        byte[] bytes = "Hello".getBytes(StandardCharsets.UTF_8);
        File[] testFiles = IntStream.range(0, 10).mapToObj(i -> new File(tempDir.toFile(), "f" + i))
                                    .map(f -> ThrowableUtils.propagate(() -> Files.write(f.toPath(), bytes)).toFile())
                                    .toArray(File[]::new);
        Map<String, String> expectedChecksums = new HashMap<>(10);
        for (File f : testFiles)
        {
            expectedChecksums.put(f.getName(), util.checksum(f));
        }

        assertThat(expectedChecksums)
        .hasSize(10)
        .containsEntry("f0", "f206d28f"); // hash value for "Hello"

        // it should not throw
        task.compareChecksumsUnsafe(expectedChecksums, testFiles);

        // test check with file that does not exist
        Map<String, String> nonexistFileChecksums = new HashMap<>(10);
        nonexistFileChecksums.put("non-exist-file", "hash");
        assertThatThrownBy(() -> task.compareChecksumsUnsafe(nonexistFileChecksums, testFiles))
        .isInstanceOf(RestoreJobFatalException.class)
        .hasMessageContaining("File not found in manifest");

        // test check with invalid checksum value
        Map<String, String> invalidChecksums = new HashMap<>(expectedChecksums);
        invalidChecksums.put("f0", "invalid_hash"); // modify the hash of the file
        assertThatThrownBy(() -> task.compareChecksumsUnsafe(invalidChecksums, testFiles))
        .isInstanceOf(RestoreJobFatalException.class)
        .hasMessageContaining("Checksum does not match. Expected: invalid_hash; actual: f206d28f");
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
        RestoreJobUtil spiedUtil = spy(util);
        when(spiedUtil.currentTimeNanos()).thenAnswer(invok -> currentNanoTimeSupplier.get());
        return new TestRestoreSliceTask(slice, mockStorageClient, executorPool, mockSSTableImporter,
                                        0, sliceDatabaseAccessor, spiedUtil, localTokenRangesProvider, metrics);
    }

    private RestoreSliceTask createTaskWithExceptions(RestoreSlice slice, RestoreJob job)
    {
        when(slice.job()).thenReturn(job);
        assertThat(slice.job()).isSameAs(job);
        assertThat(slice.job().isManagedBySidecar()).isEqualTo(job.isManagedBySidecar());
        assertThat(slice.job().status).isEqualTo(job.status);
        return new TestUnexpectedExceptionInRestoreSliceTask(slice, mockStorageClient, executorPool,
                                                             mockSSTableImporter, 0, sliceDatabaseAccessor,
                                                             util, localTokenRangesProvider, metrics);
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
        private final InstanceMetrics instanceMetrics;

        public TestRestoreSliceTask(RestoreSlice slice, StorageClient s3Client, TaskExecutorPool executorPool,
                                    SSTableImporter importer, double requiredUsableSpacePercentage,
                                    RestoreSliceDatabaseAccessor sliceDatabaseAccessor, RestoreJobUtil restoreJobUtil,
                                    LocalTokenRangesProvider localTokenRangesProvider,
                                    SidecarMetrics metrics)
        {
            super(slice, s3Client, executorPool, importer, requiredUsableSpacePercentage,
                  sliceDatabaseAccessor, restoreJobUtil, localTokenRangesProvider, metrics);
            this.slice = slice;
            this.instanceMetrics = metrics.instance(slice.owner().id());
        }

        @Override
        Future<Void> unzipAndImport(Promise<RestoreSlice> event, File file, Runnable onSuccessCommit)
        {
            instanceMetrics.restore().sliceUnzipTime.metric.update(123L, TimeUnit.NANOSECONDS);
            instanceMetrics.restore().sliceValidationTime.metric.update(123L, TimeUnit.NANOSECONDS);
            instanceMetrics.restore().sliceImportTime.metric.update(123L, TimeUnit.NANOSECONDS);
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
                                                         RestoreJobUtil util,
                                                         LocalTokenRangesProvider localTokenRangesProvider,
                                                         SidecarMetrics metrics)
        {
            super(slice, s3Client, executorPool, importer, requiredUsableSpacePercentage,
                  sliceDatabaseAccessor, util, localTokenRangesProvider, metrics);
        }

        @Override
        Future<Void> unzipAndImport(Promise<RestoreSlice> event, File file, Runnable onSuccessCommit)
        {
            throw new RuntimeException("Random exception");
        }
    }
}
