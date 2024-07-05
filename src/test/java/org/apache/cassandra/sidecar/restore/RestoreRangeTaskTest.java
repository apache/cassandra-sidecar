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
import org.apache.cassandra.sidecar.cluster.locator.LocalTokenRangesProvider;
import org.apache.cassandra.sidecar.common.ResourceUtils;
import org.apache.cassandra.sidecar.common.data.ConsistencyLevel;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.utils.ThrowableUtils;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobTest;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreRangeDatabaseAccessor;
import org.apache.cassandra.sidecar.exceptions.RestoreJobException;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
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
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import static org.apache.cassandra.sidecar.AssertionUtils.getBlocking;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RestoreRangeTaskTest
{
    private RestoreRange mockRange;
    private StorageClient mockStorageClient;
    private SSTableImporter mockSSTableImporter;
    private TaskExecutorPool executorPool;
    private SidecarMetrics metrics;
    private RestoreRangeDatabaseAccessor mockRangeAccessor;
    private LocalTokenRangesProvider localTokenRangesProvider;
    private RestoreJobUtil util;

    @BeforeEach
    void setup()
    {
        mockRange = mock(RestoreRange.class, Mockito.RETURNS_DEEP_STUBS);
        when(mockRange.stageDirectory()).thenReturn(Paths.get("."));
        when(mockRange.sliceId()).thenReturn("testing-slice");
        when(mockRange.sliceKey()).thenReturn("storage-key");
        when(mockRange.keyspace()).thenReturn("test_ks");
        when(mockRange.table()).thenReturn("test_tbl");
        when(mockRange.uploadId()).thenReturn("upload-id");
        InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
        when(instanceMetadata.id()).thenReturn(1);
        when(instanceMetadata.host()).thenReturn("host-1");
        when(instanceMetadata.metrics()).thenReturn(new InstanceMetricsImpl(registry(1)));
        InstanceMetadataFetcher mockInstanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        when(mockInstanceMetadataFetcher.instance(1)).thenReturn(instanceMetadata);
        when(mockRange.owner()).thenReturn(instanceMetadata);
        mockStorageClient = mock(StorageClient.class);
        mockSSTableImporter = mock(SSTableImporter.class);
        executorPool = new ExecutorPools(Vertx.vertx(), new ServiceConfigurationImpl()).internal();
        MetricRegistryFactory mockRegistryFactory = mock(MetricRegistryFactory.class);
        when(mockRegistryFactory.getOrCreate()).thenReturn(registry());
        when(mockRegistryFactory.getOrCreate(1)).thenReturn(registry(1));
        metrics = new SidecarMetricsImpl(mockRegistryFactory, mockInstanceMetadataFetcher);
        localTokenRangesProvider = mock(LocalTokenRangesProvider.class);
        util = new RestoreJobUtil(new XXHash32Provider());
        mockRangeAccessor = mock(RestoreRangeDatabaseAccessor.class);
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
        when(mockStorageClient.objectExists(mockRange)).thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorageClient.downloadObjectIfAbsent(mockRange))
        .thenReturn(CompletableFuture.completedFuture(new File(".")));
        RestoreRangeTask task = createTask(mockRange, job);

        Promise<RestoreRange> promise = Promise.promise();
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
    void testCaptureReplicationTimeOnlyOnce()
    {
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED);
        // the existence of the slice is already confirmed by the s3 client
        when(mockRange.existsOnS3()).thenReturn(true);
        when(mockStorageClient.downloadObjectIfAbsent(mockRange))
        .thenReturn(CompletableFuture.completedFuture(new File(".")));
        RestoreRangeTask task = createTask(mockRange, job);

        Promise<RestoreRange> promise = Promise.promise();
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
        when(mockRange.isCancelled()).thenReturn(true);
        RestoreRangeTask task = createTask(mockRange, job);

        Promise<RestoreRange> promise = Promise.promise();
        task.handle(promise);

        assertThatThrownBy(() -> getBlocking(promise.future()))
        .hasRootCauseExactlyInstanceOf(RestoreJobFatalException.class)
        .hasMessageContaining("Restore range is cancelled");
    }

    @Test
    void testThrowRetryableExceptionOnS3ObjectNotFound()
    {
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED);
        CompletableFuture<HeadObjectResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(mock(NoSuchKeyException.class));
        when(mockStorageClient.objectExists(mockRange)).thenReturn(failedFuture);
        RestoreRangeTask task = createTask(mockRange, job);

        Promise<RestoreRange> promise = Promise.promise();
        task.handle(promise);
        assertThatThrownBy(() -> getBlocking(promise.future()))
        .hasRootCauseExactlyInstanceOf(RestoreJobException.class) // NOT a fatal exception
        .hasMessageContaining("Object not found");
    }

    @Test
    void testStaging()
    {
        // test specific setup
        RestoreJob job = spy(RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED));
        doReturn(true).when(job).isManagedBySidecar();
        when(mockRange.job()).thenReturn(job);
        when(mockRange.stagedObjectPath()).thenReturn(Paths.get("nonexist"));
        when(mockStorageClient.objectExists(mockRange)).thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorageClient.downloadObjectIfAbsent(mockRange))
        .thenReturn(CompletableFuture.completedFuture(new File(".")));
        RestoreRangeTask task = createTask(mockRange, job);

        Promise<RestoreRange> promise = Promise.promise();
        task.handle(promise);
        getBlocking(promise.future()); // no error is thrown

        verify(mockRange, times(1)).completeStagePhase();
        verify(mockRange, times(0)).completeImportPhase(); // should not be called in this phase
        verify(mockRangeAccessor, times(1)).updateStatus(mockRange);
    }

    @Test
    void testStagingWithExistingObject(@TempDir Path testFolder) throws IOException
    {
        // test specific setup
        RestoreJob job = spy(RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED));
        doReturn(true).when(job).isManagedBySidecar();
        when(mockRange.job()).thenReturn(job);
        Path stagedPath = testFolder.resolve("slice.zip");
        Files.createFile(stagedPath);
        when(mockRange.stagedObjectPath()).thenReturn(stagedPath);
        when(mockStorageClient.objectExists(mockRange))
        .thenThrow(new RuntimeException("Should not call this method"));
        when(mockStorageClient.downloadObjectIfAbsent(mockRange))
        .thenThrow(new RuntimeException("Should not call this method"));
        RestoreRangeTask task = createTask(mockRange, job);

        Promise<RestoreRange> promise = Promise.promise();
        task.handle(promise);
        getBlocking(promise.future()); // no error is thrown

        verify(mockRange, times(1)).completeStagePhase();
        verify(mockRange, times(0)).completeImportPhase(); // should not be called in this phase
        verify(mockRangeAccessor, times(1)).updateStatus(mockRange);
    }

    @Test
    void testImportPhase()
    {
        // test specific setup
        RestoreJob job = spy(RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.STAGED));
        doReturn(true).when(job).isManagedBySidecar();
        when(mockRange.job()).thenReturn(job);
        RestoreRangeTask task = createTask(mockRange, job);

        Promise<RestoreRange> promise = Promise.promise();
        task.handle(promise);
        getBlocking(promise.future()); // no error is thrown

        verify(mockRange, times(0)).completeStagePhase(); // should not be called in the phase
        verify(mockRange, times(1)).completeImportPhase();
        verify(mockRangeAccessor, times(1)).updateStatus(mockRange);
    }

    @Test
    void testImportSSTables(@TempDir Path testFolder)
    {
        // import is successful
        when(mockSSTableImporter.scheduleImport(any())).thenReturn(Future.succeededFuture());
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED, ConsistencyLevel.QUORUM);
        RestoreRangeTask task = createTask(mockRange, job);
        Future<?> success = task.commit(testFolder.toFile());
        assertThat(success.failed()).isFalse();

        // import is failed
        when(mockSSTableImporter.scheduleImport(any())).thenReturn(Future.failedFuture("Import failed"));
        Future<?> failure = task.commit(testFolder.toFile());
        assertThat(failure.failed()).isTrue();
        assertThat(failure.cause()).hasMessageContaining("Import failed");
    }

    @Test
    void testHandlingUnexpectedExceptionInObjectExistsCheck(@TempDir Path testFolder)
    {
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED, ConsistencyLevel.QUORUM);
        when(mockStorageClient.objectExists(mockRange)).thenThrow(new RuntimeException("Random exception"));
        Path stagedPath = testFolder.resolve("slice.zip");
        when(mockRange.stagedObjectPath()).thenReturn(stagedPath);

        Promise<RestoreRange> promise = Promise.promise();

        RestoreRangeTask task = createTask(mockRange, job);
        task.handle(promise);

        assertThatThrownBy(() -> getBlocking(promise.future()))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Random exception");
    }

    @Test
    void testHandlingUnexpectedExceptionDuringDownloadSliceCheck(@TempDir Path testFolder)
    {
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED, ConsistencyLevel.QUORUM);
        Path stagedPath = testFolder.resolve("slice.zip");
        when(mockRange.stagedObjectPath()).thenReturn(stagedPath);
        when(mockRange.isCancelled()).thenReturn(false);
        when(mockStorageClient.objectExists(mockRange)).thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorageClient.downloadObjectIfAbsent(mockRange)).thenThrow(new RuntimeException("Random exception"));

        Promise<RestoreRange> promise = Promise.promise();

        RestoreRangeTask task = createTask(mockRange, job);
        task.handle(promise);

        assertThatThrownBy(() -> getBlocking(promise.future()))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Random exception");
    }

    @Test
    void testHandlingUnexpectedExceptionDuringUnzip(@TempDir Path testFolder) throws IOException
    {

        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.STAGED, ConsistencyLevel.QUORUM);
        Path stagedPath = testFolder.resolve("slice.zip");
        Files.createFile(stagedPath);
        when(mockRange.stagedObjectPath()).thenReturn(stagedPath);
        Promise<RestoreRange> promise = Promise.promise();
        RestoreRangeTask task = createTaskWithExceptions(mockRange, job);
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
        when(mockRange.stagedObjectPath()).thenReturn(stagedPath);
        when(mockRange.isCancelled()).thenReturn(false);
        when(mockStorageClient.objectExists(mockRange)).thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorageClient.downloadObjectIfAbsent(mockRange)).thenReturn(CompletableFuture.completedFuture(null));

        Promise<RestoreRange> promise = Promise.promise();

        RestoreRangeTask task = createTaskWithExceptions(mockRange, job);
        task.handle(promise);

        assertThatThrownBy(() -> getBlocking(promise.future()))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Random exception");
    }

    @Test
    void testSliceDuration()
    {
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.STAGED, ConsistencyLevel.QUORUM);
        AtomicLong currentNanos = new AtomicLong(0);
        RestoreRangeTask task = createTask(mockRange, job, currentNanos::get);
        Promise<RestoreRange> promise = Promise.promise();
        task.handle(promise); // Task isn't considered started until it `handle` is called
        currentNanos.set(123L);
        assertThat(task.elapsedInNanos()).isEqualTo(123L);
    }

    @Test
    void testRemoveOutOfRangeSSTables(@TempDir Path tempDir) throws RestoreJobException, IOException
    {
        // TODO: update test to use replica ranges implementation
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.STAGED, ConsistencyLevel.QUORUM);
        RestoreRangeTask task = createTask(mockRange, job);

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
        verify(mockRange, times(0)).requestOutOfRangeDataCleanup();

        // fully out of range: [-10, 0] is fully out of range of (0, 100]
        // it should remove the manifest entry entirely; no clean up required
        manifest.clear();
        ManifestEntry outOfRange = new ManifestEntry(Collections.emptyMap(),
                                                     BigInteger.valueOf(-10), // start
                                                     BigInteger.valueOf(0)); // end
        manifest.put("foo-", outOfRange);
        task.removeOutOfRangeSSTablesUnsafe(tempDir.toFile(), manifest);
        assertThat(manifest).isEmpty();
        verify(mockRange, times(0)).requestOutOfRangeDataCleanup();

        // partially out of range: [-10, 10] is partially out of range of (0, 100]
        // it should not remove the manifest entry, but it should signal to request out of range data cleanup
        manifest.clear();
        ManifestEntry partiallyOutOfRange = new ManifestEntry(Collections.emptyMap(),
                                                              BigInteger.valueOf(-10), // start
                                                              BigInteger.valueOf(10)); // end
        manifest.put("foo-", partiallyOutOfRange);
        task.removeOutOfRangeSSTablesUnsafe(tempDir.toFile(), manifest);
        assertThat(manifest).hasSize(1);
        verify(mockRange, times(1)).requestOutOfRangeDataCleanup();
    }

    @Test
    void testCompareChecksum(@TempDir Path tempDir) throws RestoreJobFatalException, IOException
    {
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED);
        RestoreRangeTask task = createTask(mockRange, job);

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

    @Test
    void testFailOnCancelledCheck()
    {
        when(mockRange.isCancelled()).thenReturn(false);
        Future<String> success = RestoreRangeTask.failOnCancelled(mockRange, "passed");
        assertThat(success.failed()).isFalse();
        assertThat(success.result()).isEqualTo("passed");

        when(mockRange.isCancelled()).thenReturn(true);
        Future<?> failure = RestoreRangeTask.failOnCancelled(mockRange, null);
        assertThat(failure.failed()).isTrue();
        assertThat(failure.cause()).isExactlyInstanceOf(RestoreJobFatalException.class)
                                   .hasMessageContaining("Restore range is cancelled");
    }

    @Test
    void testUnzipAction(@TempDir Path stagingPath) throws IOException, RestoreJobException
    {
        // when zip file is not on disk
        Path absentFile = stagingPath.resolve("none-exist");
        RestoreRange range = RestoreRangeTest.createTestRange(stagingPath, false);
        RestoreRangeTask task = new TestRestoreRangeTask(range, mockStorageClient, executorPool,
                                                         mockSSTableImporter, 0, mockRangeAccessor,
                                                         util, localTokenRangesProvider, metrics);
        assertThatThrownBy(() -> task.unzipAction(absentFile.toFile()))
        .isExactlyInstanceOf(RestoreJobException.class)
        .hasMessage("Object not found from disk. File: " + absentFile);

        // when the (unzipped) target directory already exists, but zip file does not exist.
        // We consider the zip file has been extracted and deleted
        Path unzipped = range.stageDirectory().resolve(range.keyspace()).resolve(range.table());
        Files.createDirectories(unzipped);
        assertThat(task.unzipAction(absentFile.toFile())).isEqualTo(unzipped.toFile());
        Files.deleteIfExists(unzipped);

        // unzip a valid zip
        Path zipFile = ResourceUtils.writeResourceToPath(RestoreRangeTaskTest.class.getClassLoader(),
                                                         stagingPath,
                                                         "test_unzip.zip");
        assertThat(task.unzipAction(zipFile.toFile())).isEqualTo(unzipped.toFile());
        assertThat(Files.exists(unzipped)).isTrue();
        assertThat(Files.exists(zipFile))
        .describedAs("zip file should be deleted on completion of the method")
        .isFalse();

        Path malformedZipFile = ResourceUtils.writeResourceToPath(RestoreRangeTaskTest.class.getClassLoader(),
                                                                  stagingPath,
                                                                  "test_unzip_malformed.zip");
        assertThatThrownBy(() -> task.unzipAction(malformedZipFile.toFile()))
        .isExactlyInstanceOf(RestoreJobFatalException.class)
        .hasMessageContaining("Failed to unzip")
        .hasMessageContaining("Unexpected directory in slice zip file");
    }

    @Test
    void testValidateFilesAction(@TempDir Path testDir) throws IOException, RestoreJobException
    {
        RestoreRange range = RestoreRangeTest.createTestRange();
        RestoreRangeTask task = new TestRestoreRangeTask(range, mockStorageClient, executorPool,
                                                         mockSSTableImporter, 0, mockRangeAccessor,
                                                         util, localTokenRangesProvider, metrics);
        // empty manifest file
        Path manifestFile = testDir.resolve(RestoreSliceManifest.MANIFEST_FILE_NAME);
        Files.createFile(manifestFile);
        assertThatThrownBy(() -> task.validateFilesAction(testDir.toFile()))
        .isExactlyInstanceOf(RestoreJobFatalException.class)
        .hasMessageContaining("Unable to read manifest");

        // manifest has empty json
        Files.write(manifestFile, "{}".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> task.validateFilesAction(testDir.toFile()))
        .isExactlyInstanceOf(RestoreJobFatalException.class)
        .hasMessageContaining("The downloaded slice has no data.");

        // manifest json has more entries than files under the directory
        String json = "{\"sstable1\":{\"components_checksum\":{\"file1\":\"checksum1\"},\"start_token\":1,\"end_token\":2}," +
                      "\"sstable2\":{\"components_checksum\":{\"file2\":\"checksum1\"},\"start_token\":1,\"end_token\":2}}";
        Files.write(manifestFile, json.getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> task.validateFilesAction(testDir.toFile()))
        .isExactlyInstanceOf(RestoreJobFatalException.class)
        .hasMessageContaining("Number of files does not match");

        // checksum does not match
        Path file1 = Files.createFile(testDir.resolve("file1"));
        Path file2 = Files.createFile(testDir.resolve("file2"));
        assertThatThrownBy(() -> task.validateFilesAction(testDir.toFile()))
        .isExactlyInstanceOf(RestoreJobFatalException.class)
        .hasMessageContaining("Checksum does not match");

        // valid case
        json = "{\"sstable1\":{\"components_checksum\":{\"file1\":\"" + util.checksum(file1.toFile()) + "\"},\"start_token\":1,\"end_token\":2}," +
               "\"sstable2\":{\"components_checksum\":{\"file2\":\"" + util.checksum(file2.toFile()) + "\"},\"start_token\":1,\"end_token\":2}}";
        Files.write(manifestFile, json.getBytes(StandardCharsets.UTF_8));
        assertThat(task.validateFilesAction(testDir.toFile())).isEqualTo(testDir.toFile());
    }

    private RestoreRangeTask createTask(RestoreRange range, RestoreJob job)
    {
        return createTask(range, job, System::nanoTime);
    }

    private RestoreRangeTask createTask(RestoreRange range, RestoreJob job, Supplier<Long> currentNanoTimeSupplier)
    {
        when(range.job()).thenReturn(job);
        assertThat(range.job()).isSameAs(job);
        assertThat(range.job().isManagedBySidecar()).isEqualTo(job.isManagedBySidecar());
        assertThat(range.job().status).isEqualTo(job.status);
        RestoreJobUtil spiedUtil = spy(util);
        when(spiedUtil.currentTimeNanos()).thenAnswer(invok -> currentNanoTimeSupplier.get());
        return new TestRestoreRangeTask(range, mockStorageClient, executorPool, mockSSTableImporter,
                                        0, mockRangeAccessor, spiedUtil, localTokenRangesProvider, metrics);
    }

    private RestoreRangeTask createTaskWithExceptions(RestoreRange range, RestoreJob job)
    {
        when(range.job()).thenReturn(job);
        assertThat(range.job()).isSameAs(job);
        assertThat(range.job().isManagedBySidecar()).isEqualTo(job.isManagedBySidecar());
        assertThat(range.job().status).isEqualTo(job.status);
        return new TestUnexpectedExceptionInRestoreSliceTask(range, mockStorageClient, executorPool,
                                                             mockSSTableImporter, 0, mockRangeAccessor,
                                                             util, localTokenRangesProvider, metrics);
    }

    static class TestRestoreRangeTask extends RestoreRangeTask
    {
        private final RestoreRange range;
        private final InstanceMetrics instanceMetrics;

        public TestRestoreRangeTask(RestoreRange range, StorageClient s3Client, TaskExecutorPool executorPool,
                                    SSTableImporter importer, double requiredUsableSpacePercentage,
                                    RestoreRangeDatabaseAccessor rangeDatabaseAccessor,
                                    RestoreJobUtil restoreJobUtil,
                                    LocalTokenRangesProvider localTokenRangesProvider,
                                    SidecarMetrics metrics)
        {
            super(range, s3Client, executorPool, importer, requiredUsableSpacePercentage,
                  rangeDatabaseAccessor, restoreJobUtil, localTokenRangesProvider, metrics);
            this.range = range;
            this.instanceMetrics = metrics.instance(range.owner().id());
        }

        @Override
        Future<Void> unzipAndImport(File file, Runnable onSuccessCommit)
        {
            instanceMetrics.restore().sliceUnzipTime.metric.update(123L, TimeUnit.NANOSECONDS);
            instanceMetrics.restore().sliceValidationTime.metric.update(123L, TimeUnit.NANOSECONDS);
            instanceMetrics.restore().sliceImportTime.metric.update(123L, TimeUnit.NANOSECONDS);
            if (onSuccessCommit != null)
            {
                onSuccessCommit.run();
            }
            range.completeImportPhase();
            return Future.succeededFuture();
        }

        @Override
        Future<Void> unzipAndImport(File file)
        {
            return unzipAndImport(file, null);
        }
    }

    static class TestUnexpectedExceptionInRestoreSliceTask extends RestoreRangeTask
    {
        public TestUnexpectedExceptionInRestoreSliceTask(RestoreRange range, StorageClient s3Client,
                                                         TaskExecutorPool executorPool, SSTableImporter importer,
                                                         double requiredUsableSpacePercentage,
                                                         RestoreRangeDatabaseAccessor rangeDatabaseAccessor,
                                                         RestoreJobUtil util,
                                                         LocalTokenRangesProvider localTokenRangesProvider,
                                                         SidecarMetrics metrics)
        {
            super(range, s3Client, executorPool, importer, requiredUsableSpacePercentage,
                  rangeDatabaseAccessor, util, localTokenRangesProvider, metrics);
        }

        @Override
        Future<Void> unzipAndImport(File file, Runnable onSuccessCommit)
        {
            throw new RuntimeException("Random exception");
        }
    }
}
