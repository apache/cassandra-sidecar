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

package org.apache.cassandra.sidecar.livemigration;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystemException;
import io.vertx.junit5.VertxExtension;
import org.apache.cassandra.sidecar.ExecutorPoolsHelper;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.common.request.LiveMigrationDataCopyRequest;
import org.apache.cassandra.sidecar.common.response.InstanceFileInfo;
import org.apache.cassandra.sidecar.common.response.InstanceFileInfo.FileType;
import org.apache.cassandra.sidecar.common.response.InstanceFilesListResponse;
import org.apache.cassandra.sidecar.common.response.LiveMigrationStatus;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType;
import org.apache.cassandra.sidecar.utils.SidecarClientProvider;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_FILES_ROUTE;
import static org.apache.cassandra.sidecar.common.response.LiveMigrationStatus.MigrationState.COMPLETED;
import static org.apache.cassandra.sidecar.common.response.LiveMigrationStatus.MigrationState.NOT_COMPLETED;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.DATA_FILE_DIR;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.HINTS_DIR;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.localPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({ "unchecked", "SameParameterValue" })
@ExtendWith(VertxExtension.class)
class LiveMigrationFileDownloaderTest
{
    private static final int MAX_ITERATIONS = 3;
    private static final String SOURCE = "source";
    private static final int FILE_DOWNLOAD_MAX_CONCURRENCY = 10;
    private static final int PORT = 9043;
    private static final LiveMigrationDataCopyRequest dummyRequest100pThreshold =
    new LiveMigrationDataCopyRequest(MAX_ITERATIONS, 1.0, FILE_DOWNLOAD_MAX_CONCURRENCY);
    final Vertx vertx = Vertx.vertx();

    private final List<String> dataDirsOne = Collections.singletonList("/tmp/data0");

    @Test
    void testDownloadListingFilesFailed() throws InterruptedException
    {
        final Consumer<OperationStatus> statusUpdater = mock(Consumer.class);
        Injector injector = getInjector();
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(eq(new SidecarInstanceImpl(SOURCE, PORT))))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")));

        LiveMigrationFileDownloader downloader = getDownloader(injector, dummyRequest100pThreshold, 0, statusUpdater, dataDirsOne);
        Future<OperationStatus> statusFuture = downloader.downloadFiles();
        awaitForFuture(statusFuture);

        assertThat(statusFuture.isComplete()).isTrue();
        OperationStatus operationStatus = statusFuture.result();
        assertThat(operationStatus).isNotNull();
        assertThat(operationStatus.state()).isEqualTo(OperationStatus.State.FAILED);
        verify(statusUpdater, times(1)).accept(any(OperationStatus.class));
    }

    @Test
    void testDownloadWhenDeletingUnwantedFilesFailed(@TempDir Path tempDir) throws InterruptedException, IOException
    {
        String storageDir = tempDir.resolve("testDownloadWhenDeletingUnwantedFilesFailed")
                                   .toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        final Consumer<OperationStatus> statusUpdater = mock(Consumer.class);
        int fileSize = 64;
        long lastModifiedTime = System.currentTimeMillis();

        List<TestFile> filesToDownload = List.of(
        new TestFile(DATA_FILE_DIR, 0, "ks1/t1/data.db", fileSize, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 0, "ks1/t2/data.db", fileSize, lastModifiedTime)
        );

        Injector injector = getInjector();
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(eq(new SidecarInstanceImpl(SOURCE, PORT))))
        .thenReturn(CompletableFuture.completedFuture(getInstanceFilesListResponse(filesToDownload)));

        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 0, statusUpdater, storageDir, dataDirs);
        doThrow(new RuntimeException("Cleanup failed"))
        .when(downloaderSpy).deleteUnnecessaryFilesAndDirectories(any(InstanceFilesListResponse.class));

        Future<OperationStatus> operationStatus = downloaderSpy.downloadFiles();
        awaitForFuture(operationStatus);

        assertThat(operationStatus.isComplete()).isTrue();
        assertThat(operationStatus.result().state()).isEqualTo(OperationStatus.State.FAILED);
        verify(sidecarClient, times(1)).liveMigrationListInstanceFilesAsync(any(SidecarInstance.class));
        verify(downloaderSpy, times(0))
        .shortlistDownloadFiles(any(InstanceFilesListResponse.class), anyDouble());
    }

    @Test
    void testDownloadSuccessCriteriaMeets(@TempDir Path tmpDir) throws InterruptedException, IOException
    {
        String storageDir = tmpDir.resolve("testDownloadSuccessCriteriaMeets").toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        final Consumer<OperationStatus> statusUpdater = mock(Consumer.class);
        int fileSize = 64;
        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> filesToDownload = List.of(
        new TestFile(DATA_FILE_DIR, 0, "ks1/t1/data.db", fileSize, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 0, "ks1/t2/data.db", fileSize, lastModifiedTime));

        Injector injector = getInjector();
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(eq(new SidecarInstanceImpl(SOURCE, PORT))))
        .thenReturn(CompletableFuture.completedFuture(getInstanceFilesListResponse(filesToDownload)));

        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 0, statusUpdater, storageDir, dataDirs);
        doAnswer(invocation -> invocation.getArguments()[0])
        .when(downloaderSpy).deleteUnnecessaryFilesAndDirectories(any(InstanceFilesListResponse.class));
        doReturn(Future.succeededFuture(Collections.emptyList()))
        .when(downloaderSpy).shortlistDownloadFiles(any(InstanceFilesListResponse.class), anyDouble());

        Future<OperationStatus> statusFuture = downloaderSpy.downloadFiles();
        awaitForFuture(statusFuture);

        assertThat(statusFuture.isComplete()).isTrue();
        assertThat(statusFuture.result().state()).isEqualTo(OperationStatus.State.SUCCESS);
    }

    @Test
    void testDownloadFilesEmptyDirsAndEmptyFiles(@TempDir Path tmpDir) throws InterruptedException
    {
        // Testing a corner case where there are only empty directories and empty files remaining to download.

        String storageDir = tmpDir.resolve("testDownloadFilesEmptyDirsAndEmptyFiles").toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        final Consumer<OperationStatus> statusUpdater = mock(Consumer.class);
        long lastModifiedTime = System.currentTimeMillis();
        TestFile emptyDir = new TestFile(DATA_FILE_DIR, 0, "ks1/", -1, lastModifiedTime);
        TestFile emptyFile = new TestFile(DATA_FILE_DIR, 0, "ks2/t2/data.db", 0, lastModifiedTime);
        List<TestFile> filesToDownload = List.of(emptyDir, emptyFile);

        Injector injector = getInjector();
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(eq(new SidecarInstanceImpl(SOURCE, PORT))))
        .thenReturn(CompletableFuture.completedFuture(getInstanceFilesListResponse(filesToDownload)));

        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 0, statusUpdater, storageDir, dataDirs);

        Future<OperationStatus> statusFuture = downloaderSpy.downloadFiles();
        awaitForFuture(statusFuture);

        assertThat(statusFuture.isComplete()).isTrue();
        assertThat(statusFuture.result().state()).isEqualTo(OperationStatus.State.DOWNLOAD_COMPLETE);
        assertThat(vertx.fileSystem().existsBlocking(emptyDir.getFilePath(storageDir))).isTrue();
        assertThat(vertx.fileSystem().existsBlocking(emptyFile.getFilePath(storageDir))).isTrue();
    }

    @Test
    void testDownloadFilesEmptyDirsAndEmptyFilesRetriesExhausted(@TempDir Path tmpDir) throws InterruptedException
    {
        // Testing a corner case where there are only empty directories and empty files remaining to download.

        String storageDir = tmpDir.resolve("testDownloadFilesEmptyDirsAndEmptyFiles").toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        final Consumer<OperationStatus> statusUpdater = mock(Consumer.class);
        long lastModifiedTime = System.currentTimeMillis();
        TestFile emptyDir = new TestFile(DATA_FILE_DIR, 0, "ks1/", -1, lastModifiedTime);
        TestFile emptyFile = new TestFile(DATA_FILE_DIR, 0, "ks2/t2/data.db", 0, lastModifiedTime);
        List<TestFile> filesToDownload = List.of(emptyDir, emptyFile);

        Injector injector = getInjector();
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(eq(new SidecarInstanceImpl(SOURCE, PORT))))
        .thenReturn(CompletableFuture.completedFuture(getInstanceFilesListResponse(filesToDownload)));

        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold,
                         dummyRequest100pThreshold.maxIterations, // setting current iteration as maxIteration
                         statusUpdater, storageDir, dataDirs);

        Future<OperationStatus> statusFuture = downloaderSpy.downloadFiles();
        awaitForFuture(statusFuture);

        assertThat(statusFuture.isComplete()).isTrue();
        assertThat(statusFuture.result().state()).isEqualTo(OperationStatus.State.FAILED);
        assertThat(vertx.fileSystem().existsBlocking(emptyDir.getFilePath(storageDir))).isFalse();
        assertThat(vertx.fileSystem().existsBlocking(emptyFile.getFilePath(storageDir))).isFalse();
    }

    @Test
    void testDownloadSuccessfullyDownloadedFiles(@TempDir Path tmpDir) throws InterruptedException, IOException
    {
        String storageDir = tmpDir.resolve("testDownloadSuccessfullyDownloadedFiles")
                                  .toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        final Consumer<OperationStatus> statusUpdater = mock(Consumer.class);
        int fileSize = 64;
        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> filesToDownload = List.of(
        new TestFile(DATA_FILE_DIR, 0, "ks1/t1/data.db", fileSize, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 0, "ks1/t2/data.db", fileSize, lastModifiedTime)
        );

        Injector injector = getInjector();
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(eq(new SidecarInstanceImpl(SOURCE, PORT))))
        .thenReturn(CompletableFuture.completedFuture(getInstanceFilesListResponse(filesToDownload)));

        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 0, statusUpdater, storageDir, dataDirs);

        doAnswer(invocation -> invocation.getArguments()[0])
        .when(downloaderSpy).deleteUnnecessaryFilesAndDirectories(any(InstanceFilesListResponse.class));

        doReturn(Future.succeededFuture(getInstanceFileInfo(filesToDownload)))
        .when(downloaderSpy).shortlistDownloadFiles(any(InstanceFilesListResponse.class), anyDouble());

        doReturn(Future.succeededFuture())
        .when(downloaderSpy).updateFileTimestampAsync(any(Path.class), anyLong());

        when(sidecarClient.liveMigrationStreamFileAsync(any(SidecarInstance.class), anyString(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));

        Future<OperationStatus> statusFuture = downloaderSpy.downloadFiles();
        awaitForFuture(statusFuture);

        assertThat(statusFuture.isComplete()).isTrue();
        assertThat(statusFuture.result().state()).isEqualTo(OperationStatus.State.DOWNLOAD_COMPLETE);
        verify(statusUpdater, times(4)).accept(any(OperationStatus.class));
    }

    @Test
    void testDownloadSuccessfullyDownloadedFilesSourceHasMoreDataDirs(@TempDir Path tmpDir) throws InterruptedException, IOException
    {
        // In this case source has more data dirs than destination.
        // Download should fail as the destination does not have sufficient data directories.
        String storageDir = tmpDir.resolve("testDownloadSuccessfullyDownloadedFiles")
                                  .toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        final Consumer<OperationStatus> statusUpdater = mock(Consumer.class);
        int fileSize = 64;
        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> filesToDownload = List.of(
        new TestFile(DATA_FILE_DIR, 0, "ks1/t1/data.db", fileSize, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 0, "ks1/t2/data.db", fileSize, lastModifiedTime),
        // Second data directory file specified with dirIndex as 1
        new TestFile(DATA_FILE_DIR, 1, "ks1/t2/data2.db", fileSize, lastModifiedTime)
        );

        Injector injector = getInjector();
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(eq(new SidecarInstanceImpl(SOURCE, PORT))))
        .thenReturn(CompletableFuture.completedFuture(getInstanceFilesListResponse(filesToDownload)));

        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 0, statusUpdater, storageDir, dataDirs);

        doAnswer(invocation -> invocation.getArguments()[0])
        .when(downloaderSpy).deleteUnnecessaryFilesAndDirectories(any(InstanceFilesListResponse.class));

        doReturn(Future.succeededFuture(getInstanceFileInfo(filesToDownload)))
        .when(downloaderSpy).shortlistDownloadFiles(any(InstanceFilesListResponse.class), anyDouble());

        doReturn(Future.succeededFuture())
        .when(downloaderSpy).updateFileTimestampAsync(any(Path.class), anyLong());

        when(sidecarClient.liveMigrationStreamFileAsync(any(SidecarInstance.class), anyString(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));

        Future<OperationStatus> statusFuture = downloaderSpy.downloadFiles();
        awaitForFuture(statusFuture);

        assertThat(statusFuture.isComplete()).isTrue();
        assertThat(statusFuture.result().state()).isEqualTo(OperationStatus.State.FAILED);
        verify(statusUpdater, times(4)).accept(any(OperationStatus.class));
    }

    @Test
    void testDownloadFilesWhenSourceHasStatusCompleted(@TempDir Path tmpDir) throws InterruptedException
    {
        String storageDir = tmpDir.resolve("testDownloadFilesWhenSourceAlreadyCompleted")
                                  .toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        final Consumer<OperationStatus> statusUpdater = mock(Consumer.class);
        int fileSize = 64;
        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> filesToDownload = List.of(
        new TestFile(DATA_FILE_DIR, 0, "ks1/t1/data.db", fileSize, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 0, "ks1/t2/data.db", fileSize, lastModifiedTime)
        );

        Injector injector = getInjector();
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        // Mocking source Live Migration status as "COMPLETED".
        when(sidecarClient.liveMigrationStatus(eq(new SidecarInstanceImpl(SOURCE, PORT))))
        .thenReturn(CompletableFuture.completedFuture(new LiveMigrationStatus(COMPLETED, 1L)));
        when(sidecarClient.liveMigrationListInstanceFilesAsync(eq(new SidecarInstanceImpl(SOURCE, PORT))))
        .thenReturn(CompletableFuture.completedFuture(getInstanceFilesListResponse(filesToDownload)));

        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 0, statusUpdater, storageDir, dataDirs);

        Future<OperationStatus> statusFuture = downloaderSpy.downloadFiles();
        awaitForFuture(statusFuture);

        assertThat(statusFuture.isComplete()).isTrue();
        assertThat(statusFuture.result().state()).isEqualTo(OperationStatus.State.FAILED);

        verify(statusUpdater, times(1)).accept(any(OperationStatus.class));
        verify(sidecarClient, times(0)).liveMigrationListInstanceFilesAsync(any());
    }

    @Test
    void testDownloadZeroSizedFiles(@TempDir Path tmpDir) throws InterruptedException
    {
        String storageDir = tmpDir.resolve("testDownloadZeroSizedFiles")
                                  .toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        final Consumer<OperationStatus> statusUpdater = mock(Consumer.class);
        int fileSize = 0;

        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> filesToDownload = List.of(
        new TestFile(DATA_FILE_DIR, 0, "ks1/t1/data.db", fileSize, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 0, "ks1/t2/data.db", fileSize, lastModifiedTime)
        );

        Injector injector = getInjector();
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(eq(new SidecarInstanceImpl(SOURCE, PORT))))
        .thenReturn(CompletableFuture.completedFuture(getInstanceFilesListResponse(filesToDownload)));

        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 0, statusUpdater, storageDir, dataDirs);

        doReturn(Future.succeededFuture(getInstanceFileInfo(filesToDownload)))
        .when(downloaderSpy).shortlistDownloadFiles(any(InstanceFilesListResponse.class), anyDouble());

        doReturn(Future.succeededFuture())
        .when(downloaderSpy).updateFileTimestampAsync(any(Path.class), anyLong());

        when(sidecarClient.liveMigrationStreamFileAsync(any(SidecarInstance.class), anyString(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));

        Future<OperationStatus> statusFuture = downloaderSpy.downloadFiles();
        awaitForFuture(statusFuture);

        assertThat(statusFuture.isComplete()).isTrue();
        assertThat(statusFuture.result().state()).isEqualTo(OperationStatus.State.DOWNLOAD_COMPLETE);
        verify(statusUpdater, times(4)).accept(any(OperationStatus.class));
    }

    @Test
    void testDownloadFewFilesRetriesExhausted(@TempDir Path tmpDir) throws InterruptedException
    {
        String storageDir = tmpDir.resolve("testDownloadFewFilesRetriesExhausted")
                                  .toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        final Consumer<OperationStatus> statusUpdater = mock(Consumer.class);
        final int fileSize = 64;
        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> filesToDownload = List.of(
        new TestFile(DATA_FILE_DIR, 0, "ks1/t1/data.db", fileSize, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 0, "ks1/t2/data.db", fileSize, lastModifiedTime)
        );

        Injector injector = getInjector();
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(eq(new SidecarInstanceImpl(SOURCE, PORT))))
        .thenReturn(CompletableFuture.completedFuture(getInstanceFilesListResponse(filesToDownload)));

        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, MAX_ITERATIONS + 1, statusUpdater, storageDir, dataDirs);

        doReturn(Future.succeededFuture(getInstanceFileInfo(filesToDownload)))
        .when(downloaderSpy).shortlistDownloadFiles(any(InstanceFilesListResponse.class), anyDouble());

        Future<OperationStatus> statusFuture = downloaderSpy.downloadFiles();
        awaitForFuture(statusFuture);

        assertThat(statusFuture.isComplete()).isTrue();
        assertThat(statusFuture.result().state()).isEqualTo(OperationStatus.State.FAILED);
    }


    @Test
    void testDownloadFailedForFewFiles(@TempDir Path tmpDir) throws InterruptedException
    {
        String storageDir = tmpDir.resolve("testDownloadFailedForFewFiles")
                                  .toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        final Consumer<OperationStatus> statusUpdater = mock(Consumer.class);
        int fileSize = 64;
        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> filesToDownload = List.of(
        new TestFile(DATA_FILE_DIR, 0, "ks1/t1/data.db", fileSize, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 0, "ks1/t2/data.db", fileSize, lastModifiedTime)
        );

        Injector injector = getInjector();
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(eq(new SidecarInstanceImpl(SOURCE, PORT))))
        .thenReturn(CompletableFuture.completedFuture(getInstanceFilesListResponse(filesToDownload)));

        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 0, statusUpdater, storageDir, dataDirs);

        doReturn(Future.succeededFuture(getInstanceFileInfo(filesToDownload)))
        .when(downloaderSpy).shortlistDownloadFiles(any(InstanceFilesListResponse.class), anyDouble());

        doReturn(Future.succeededFuture())
        .when(downloaderSpy).updateFileTimestampAsync(any(Path.class), anyLong());

        // First file download completes successfully
        when(sidecarClient.liveMigrationStreamFileAsync(any(SidecarInstance.class), eq(filesToDownload.get(0).getFileUrl()), anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));

        // Second file download fails
        when(sidecarClient.liveMigrationStreamFileAsync(any(SidecarInstance.class), eq(filesToDownload.get(1).getFileUrl()), anyString()))
        .thenReturn(CompletableFuture.failedFuture(new IOException("File download failed")));

        Future<OperationStatus> statusFuture = downloaderSpy.downloadFiles();

        awaitForFuture(statusFuture);

        assertThat(statusFuture.isComplete()).isTrue();
        assertThat(statusFuture.result().state()).isEqualTo(OperationStatus.State.FAILED);
        verify(downloaderSpy, times(1)).updateFileTimestampAsync(any(), anyLong());
    }

    @Test
    void testDownloadAbnormalExceptionWhileDownloading(@TempDir Path tmpDir) throws InterruptedException, IOException
    {
        String storageDir = tmpDir.resolve("testDownloadAbnormalExceptionWhileDownloading")
                                  .toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        final Consumer<OperationStatus> statusUpdater = mock(Consumer.class);
        final int fileSize = 64;
        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> filesToDownload = List.of(
        new TestFile(DATA_FILE_DIR, 0, "ks1/t1/data.db", fileSize, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 0, "ks1/t2/data.db", fileSize, lastModifiedTime)
        );

        Injector injector = getInjector();
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(eq(new SidecarInstanceImpl(SOURCE, PORT))))
        .thenReturn(CompletableFuture.completedFuture(getInstanceFilesListResponse(filesToDownload)));

        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, MAX_ITERATIONS + 1, statusUpdater, storageDir, dataDirs);

        doThrow(new RuntimeException("Dummy exception to break things"))
        .when(downloaderSpy).deleteUnnecessaryFilesAndDirectories(any(InstanceFilesListResponse.class));

        Future<OperationStatus> statusFuture = downloaderSpy.downloadFiles();
        awaitForFuture(statusFuture);

        assertThat(statusFuture.isComplete()).isTrue();
        assertThat(statusFuture.result().state()).isEqualTo(OperationStatus.State.FAILED);
        verify(statusUpdater, times(2)).accept(any(OperationStatus.class));
    }

    @Test
    void testDownloadServerDownloadFailuresWhileDownloading(@TempDir Path tempDir) throws InterruptedException
    {
        String storageDir = tempDir.resolve("testDownloadServerDownloadFailuresWhileDownloading")
                                   .toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        final Consumer<OperationStatus> statusUpdater = mock(Consumer.class);
        int fileSize = 64;
        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> filesToDownload = List.of(
        new TestFile(DATA_FILE_DIR, 0, "ks1/t1/data.db", fileSize, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 0, "ks1/t2/data.db", fileSize, lastModifiedTime)
        );
        Injector injector = getInjector();
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(eq(new SidecarInstanceImpl(SOURCE, PORT))))
        .thenReturn(CompletableFuture.completedFuture(getInstanceFilesListResponse(filesToDownload)));

        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 0, statusUpdater, storageDir, dataDirs);

        doReturn(Future.succeededFuture(getInstanceFileInfo(filesToDownload)))
        .when(downloaderSpy).shortlistDownloadFiles(any(InstanceFilesListResponse.class), anyDouble());

        when(sidecarClient.liveMigrationStreamFileAsync(any(SidecarInstance.class), eq(filesToDownload.get(0).getFileUrl()), anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));

        when(sidecarClient.liveMigrationStreamFileAsync(any(SidecarInstance.class), eq(filesToDownload.get(1).getFileUrl()), anyString()))
        .thenReturn(CompletableFuture.failedFuture(new IOException("Connection refused.")));

        Future<OperationStatus> statusFuture = downloaderSpy.downloadFiles();
        awaitForFuture(statusFuture);

        assertThat(statusFuture.isComplete()).isTrue();
        assertThat(statusFuture.result().state()).isEqualTo(OperationStatus.State.FAILED);
        verify(statusUpdater, times(4)).accept(any(OperationStatus.class));
    }

    @Test
    void testDownloadServerFailuresWhileDownloading(@TempDir Path tempDir) throws InterruptedException
    {
        String storageDir = tempDir.resolve("testDownloadServerFailuresWhileDownloading").toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        final Consumer<OperationStatus> statusUpdater = mock(Consumer.class);
        int fileSize = 64;
        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> filesToDownload = List.of(
        new TestFile(DATA_FILE_DIR, 0, "ks1/t1/data.db", fileSize, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 0, "ks1/t2/data.db", fileSize, lastModifiedTime));

        Injector injector = getInjector();
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(eq(new SidecarInstanceImpl(SOURCE, PORT))))
        .thenReturn(CompletableFuture.completedFuture(getInstanceFilesListResponse(filesToDownload)));

        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 0, statusUpdater, storageDir, dataDirs);

        doReturn(Future.succeededFuture(getInstanceFileInfo(filesToDownload)))
        .when(downloaderSpy).shortlistDownloadFiles(any(InstanceFilesListResponse.class), anyDouble());

        when(sidecarClient.liveMigrationStreamFileAsync(any(SidecarInstance.class), eq(filesToDownload.get(0).getFileUrl()), anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));

        // streamFileAsync future(false) on server error.
        when(sidecarClient.liveMigrationStreamFileAsync(any(SidecarInstance.class), eq(filesToDownload.get(1).getFileUrl()), anyString()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("some exception")));

        Future<OperationStatus> statusFuture = downloaderSpy.downloadFiles();
        awaitForFuture(statusFuture);

        assertThat(statusFuture.isComplete()).isTrue();
        assertThat(statusFuture.result().state()).isEqualTo(OperationStatus.State.FAILED);
        verify(statusUpdater, times(4)).accept(any(OperationStatus.class));
    }

    @Test
    void testCancel()
    {
        final Consumer<OperationStatus> statusUpdater = mock(Consumer.class);
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloader = getDownloader(injector, dummyRequest100pThreshold, 1, statusUpdater, dataDirsOne);

        downloader.cancel();
        verify(statusUpdater, times(1)).accept(any(OperationStatus.class));
    }

    @Test
    void testCancelWhenDownloadsAreInProgress(@TempDir Path tempDir) throws Exception
    {
        String storageDir = tempDir.resolve("testCreateDirectoryFailure").toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);

        final Consumer<OperationStatus> statusUpdater = mock(Consumer.class);
        Injector injector = getInjector();
        LiveMigrationDataCopyRequest maxConcurrency1Request =
        new LiveMigrationDataCopyRequest(1, 1.0, 2);
        LiveMigrationFileDownloader downloaderSpy =
        spy(getDownloader(injector, maxConcurrency1Request, 0, statusUpdater, storageDir, dataDirs));

        int fileSize = 64;
        long lastModifiedTime = System.currentTimeMillis();

        List<TestFile> filesToDownload = List.of(
        new TestFile(DATA_FILE_DIR, 0, "ks1/t1/data.db", fileSize, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 0, "ks1/t2/data.db", fileSize, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 0, "ks1/t3/data.db", fileSize, lastModifiedTime));

        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(eq(new SidecarInstanceImpl(SOURCE, PORT))))
        .thenReturn(CompletableFuture.completedFuture(getInstanceFilesListResponse(filesToDownload)));

        Promise<Void> t1DownloadPromise = Promise.promise();
        Promise<Void> t2DownloadPromise = Promise.promise();
        // promise is not resolved to mimic download in progress.
        when(sidecarClient.liveMigrationStreamFileAsync(any(), eq(filesToDownload.get(0).getFileUrl()), anyString()))
        .thenReturn(t1DownloadPromise.future().toCompletionStage().toCompletableFuture());
        when(sidecarClient.liveMigrationStreamFileAsync(any(), eq(filesToDownload.get(1).getFileUrl()), anyString()))
        .thenReturn(t2DownloadPromise.future().toCompletionStage().toCompletableFuture());

        doReturn(Future.succeededFuture())
        .when(downloaderSpy).updateFileTimestampAsync(any(Path.class), anyLong());

        // Start downloading files
        Future<OperationStatus> operationStatusFuture = downloaderSpy.downloadFiles();

        // Ensure that the current state of operation is not in final state
        assertThat(operationStatusFuture.isComplete()).isFalse();
        assertThat(downloaderSpy.operationStatus().state()).isNotEqualTo(OperationStatus.State.SUCCESS)
                                                         .isNotEqualTo(OperationStatus.State.FAILED)
                                                         .isNotEqualTo(OperationStatus.State.CANCELLED)
                                                         .isNotEqualTo(OperationStatus.State.CANCELLED);

        awaitForFuture(operationStatusFuture, 100);

        // Cancel the operation now
        downloaderSpy.cancel();

        // Since two downloads are in progress, future should not be completed
        assertThat(operationStatusFuture.isComplete()).isFalse();
        assertThat(downloaderSpy.operationStatus().state()).isEqualTo(OperationStatus.State.CANCELLED);

        // Now resolve downloadPromise to ensure that operation state remains in CANCELLED state
        t1DownloadPromise.complete();
        awaitForFuture(operationStatusFuture, 100);
        assertThat(operationStatusFuture.isComplete()).isFalse();
        assertThat(downloaderSpy.operationStatus().state()).isEqualTo(OperationStatus.State.CANCELLED);

        t2DownloadPromise.tryFail(new IOException("Failed to download file"));
        awaitForFuture(operationStatusFuture, 100);

        assertThat(operationStatusFuture.isComplete()).isTrue();
        assertThat(downloaderSpy.operationStatus().state()).isEqualTo(OperationStatus.State.CANCELLED);
    }

    @Test
    void testDeleteUnnecessary(@TempDir Path tempDir) throws IOException
    {
        int fileSize = 32;
        final long timeStamp = System.currentTimeMillis() - 1000;
        String storageDir = tempDir.resolve("testDeleteUnnecessaryFiles").toAbsolutePath().toString();
        String normalFile = "1.db";

        createFile(storageDir + "/" + normalFile, fileSize, timeStamp);
        TestFile droppedKeyspaceFile = new TestFile(DATA_FILE_DIR, 0, "ks1/t1/3.db", fileSize, timeStamp);
        TestFile partiallyDownloadedFile = new TestFile(DATA_FILE_DIR, 0, "ks2/t1/partial.db", fileSize / 2, timeStamp);
        TestFile fullyDownloadedFile = new TestFile(DATA_FILE_DIR, 0, "ks2/t1/2.db", fileSize, timeStamp);
        TestFile fileDoesNotExistInLocal = new TestFile(DATA_FILE_DIR, 0, "ks3/t2/5.db", fileSize, timeStamp);
        TestFile dirDoesNotExistInRemote = new TestFile(DATA_FILE_DIR, 0, "dirDoesNotExistInRemote", -1, timeStamp);
        TestFile wrongTimestampFile = new TestFile(DATA_FILE_DIR, 0, "ks2/t1/5.db", fileSize, timeStamp - 1000);
        TestFile emptyKeyspace = new TestFile(DATA_FILE_DIR, 0, "emptykeyspace", -1, timeStamp);

        long timeStamp2 = System.currentTimeMillis();
        List<TestFile> localFiles = List.of(droppedKeyspaceFile, partiallyDownloadedFile, fullyDownloadedFile, wrongTimestampFile, dirDoesNotExistInRemote, emptyKeyspace);
        prepareDataHomeDir(storageDir, localFiles);

        Consumer<OperationStatus> mockStatusUpdater = mock(Consumer.class);
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 1, mockStatusUpdater, storageDir,
                         Collections.singletonList(storageDir + "/data"));

        // instance files list from remote host
        List<TestFile> remoteFiles = List.of(
        new TestFile(DATA_FILE_DIR, 0, "ks2/t1/partial.db", fileSize, timeStamp),
        fullyDownloadedFile,
        new TestFile(DATA_FILE_DIR, 0, "ks3/t2/5.db", fileSize, timeStamp2),
        new TestFile(DATA_FILE_DIR, 0, "ks2/t1/5.db", fileSize, timeStamp2),
        new TestFile(DATA_FILE_DIR, 0, "ks2/t1", -1, timeStamp2),
        new TestFile(DATA_FILE_DIR, 0, "ks3/t2", -1, timeStamp2),
        new TestFile(DATA_FILE_DIR, 0, "ks2", -1, timeStamp2),
        new TestFile(DATA_FILE_DIR, 0, "ks3", -1, timeStamp2),
        emptyKeyspace
        );

        InstanceFilesListResponse instanceFilesListResponse =
        getInstanceFilesListResponse(remoteFiles);

        InstanceFilesListResponse responseFuture = downloaderSpy.deleteUnnecessaryFilesAndDirectories(instanceFilesListResponse);

        assertThat(responseFuture).isEqualTo(instanceFilesListResponse);
        assertFileExists(storageDir, normalFile, fileSize); // it should not touch already downloaded file matches with remote
        assertFileExists(fullyDownloadedFile.getFilePath(storageDir), fullyDownloadedFile.size);
        assertFileDoesNotExists(droppedKeyspaceFile.getFilePath(storageDir)); // if the file doesn't exist in remote, it should clear local file
        assertFileDoesNotExists(partiallyDownloadedFile.getFilePath(storageDir)); // partially downloaded file should be deleted
        assertFileDoesNotExists(fileDoesNotExistInLocal.getFilePath(storageDir)); // it should not create new/empty file
        assertFileDoesNotExists(dirDoesNotExistInRemote.getFilePath(storageDir)); // it should delete directories which doesn't exist in remote
        assertDirExists(emptyKeyspace.getFilePath(storageDir));
        assertFileDoesNotExists(wrongTimestampFile.getFilePath(storageDir)); // it should delete the file with wrong timestamp
    }

    @Test
    void testDataHomeDirShouldNotBeDeleted(@TempDir Path tempDir) throws IOException
    {
        String storageDir = tempDir.resolve("testDataHomeDirShouldNotBeDeleted").toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        File dataHomeDir = new File(storageDir);
        Files.createDirectories(dataHomeDir.toPath());
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 1, mock(Consumer.class), storageDir, dataDirs);
        assertThat(downloaderSpy.canDelete(new HashMap<>(), dataHomeDir.toPath(), dataHomeDir.toPath())).isFalse();
    }

    @Test
    void testFileNotPresentInRemoteShouldBeDeleted(@TempDir Path tempDir) throws IOException
    {
        String storageDir = tempDir.resolve("testFileNotPresentInRemoteShouldBeDeleted")
                                   .toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 1, mock(Consumer.class), storageDir, dataDirs);
        final String fileDoesNotExistInRemote = "fileDoesNotExistInRemote";

        List<TestFile> localFiles = List.of(
        new TestFile(DATA_FILE_DIR, 0, fileDoesNotExistInRemote, 32, System.currentTimeMillis())
        );

        prepareDataHomeDir(storageDir, localFiles);
        Path dataDir = Path.of(dataDirs.get(0));

        assertThat(downloaderSpy.canDelete(Map.of(), dataDir.resolve(fileDoesNotExistInRemote), dataDir)).isTrue();
    }

    @Test
    void testCreateEmptyFile(@TempDir Path tempDir) throws IOException, InterruptedException
    {
        String storageDir = tempDir.resolve("testCreateEmptyFile").toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 1, mock(Consumer.class), storageDir, dataDirs);

        String dummyFile = LIVE_MIGRATION_FILES_ROUTE + "/data/0/ks1/t1/dummy.txt";
        long lastModifiedTime = new Date().getTime() - 24 * 60 * 60 * 1000; // a day before
        InstanceFileInfo zeroSizedFileInfo = getInstanceFileInfo(new String[]{ dummyFile }, 0, lastModifiedTime).get(0);
        Path localFilePath = localPath(zeroSizedFileInfo.fileUrl, downloaderSpy.instanceMetadata());

        assertThat(Files.exists(localFilePath)).isFalse();

        Future<Void> future = downloaderSpy.createEmptyFile(zeroSizedFileInfo);
        awaitForFuture(future);

        assertThat(Files.exists(localFilePath)).isTrue();
        assertThat(future.succeeded()).isTrue();
        assertThat(Files.getLastModifiedTime(localFilePath).toMillis()).isEqualTo(lastModifiedTime);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private <T> void awaitForFuture(Future<T> future) throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(1);
        future.onComplete(res -> latch.countDown());

        latch.await(2, TimeUnit.SECONDS); // Change to latch.await() for debugging
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private <T> void awaitForFuture(Future<T> future, int millis) throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(1);
        future.onComplete(res -> latch.countDown());

        latch.await(millis, TimeUnit.MILLISECONDS);
    }

    @Test
    void testCreateEmptyFileFailedToUpdateLastModifiedTime(@TempDir Path tmpDir) throws InterruptedException
    {
        String storageDir = tmpDir.resolve("testCreateEmptyFileFailedToUpdateLastModifiedTime")
                                  .toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 1, mock(Consumer.class), storageDir, dataDirs);

        String remoteFilePath = LIVE_MIGRATION_FILES_ROUTE + "/data/0/dummy.txt";
        InstanceFileInfo zeroSizedFileInfo = getInstanceFileInfo(new String[]{ remoteFilePath }, 0).get(0);
        Path localFilePath = localPath(zeroSizedFileInfo.fileUrl, downloaderSpy.instanceMetadata());


        doReturn(Future.failedFuture(new NoSuchFileException("File does not exist.")))
        .when(downloaderSpy).updateFileTimestampAsync(any(Path.class), anyLong());
        assertThat(Files.exists(localFilePath)).isFalse();

        Future<Void> future = downloaderSpy.createEmptyFile(zeroSizedFileInfo);
        awaitForFuture(future);

        assertThat(Files.exists(localFilePath)).isTrue();
        assertThat(future.cause()).isNotNull();
    }

    @Test
    void testGetDownloadTask(@TempDir Path tmpDir) throws IOException
    {
        String storageDir = tmpDir.resolve("testGetDownloadTask").toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        int fileSize = 32;
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 1, mock(Consumer.class), storageDir, dataDirs);
        downloaderSpy.updateState(operationStatus -> operationStatus.toCleaningState(100, 10)
                                                                    .toPreparingState().toDownloadingState(100, 10));

        String fileExists = LIVE_MIGRATION_FILES_ROUTE + "/data/0/dummy.txt";
        String fileDoesNotExist = LIVE_MIGRATION_FILES_ROUTE + "/data/0/does-not-exist.txt";
        long lastModifiedTime = System.currentTimeMillis() - 60 * 1000; // Setting it to 1 min back
        InstanceFileInfo fileInfos
        = getInstanceFileInfo(new String[]{ fileExists, fileDoesNotExist }, fileSize, lastModifiedTime)
          .get(0);
        Path localFilePath = localPath(fileInfos.fileUrl, downloaderSpy.instanceMetadata());
        createFile(localFilePath.toFile(), fileSize, lastModifiedTime);  // Creating file explicitly as sidecarClient is mocked and doesn't do anything

        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationStreamFileAsync(any(), anyString(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));

        Future<Void> downloadTask = downloaderSpy.getDownloadTask(new SidecarInstanceImpl(SOURCE, PORT), fileInfos);

        assertThat(downloadTask.isComplete()).isTrue();
        assertThat(downloadTask.failed()).isFalse();
        assertThat(Files.getLastModifiedTime(localFilePath).toMillis()).isEqualTo(lastModifiedTime);
    }

    @Test
    void testGetDownloadTaskForFileThatDoesNotExistAtSource(@TempDir Path tmpDir)
    {
        String storageDir = tmpDir.resolve("testGetDownloadTaskForFileThatDoesNotExistAtSource")
                                  .toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 1, mock(Consumer.class), storageDir, dataDirs);
        downloaderSpy.updateState(operationStatus -> operationStatus.toCleaningState(100, 10)
                                                                    .toPreparingState().toDownloadingState(100, 10));

        String fileExists = LIVE_MIGRATION_FILES_ROUTE + "/data/0/dummy.txt";
        InstanceFileInfo fileInfos = getInstanceFileInfo(new String[]{ fileExists }, 0).get(0);

        // Not creating file explicitly so that handlers in download task will attempt to update last modified time
        // of a file that doesn't exist.

        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationStreamFileAsync(any(), anyString(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));

        Future<Void> downloadTask = downloaderSpy.getDownloadTask(new SidecarInstanceImpl(SOURCE, PORT), fileInfos);

        assertThat(downloadTask.isComplete()).isTrue();
    }

    @Test
    void testGetDownloadTaskDownloadFailed(@TempDir Path tmpDir)
    {
        String storageDir = tmpDir.resolve("testGetDownloadTaskDownloadFailed").toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 1, mock(Consumer.class), storageDir, dataDirs);
        downloaderSpy.updateState(operationStatus -> operationStatus.toCleaningState(100, 10)
                                                                    .toPreparingState().toDownloadingState(100, 10));

        String fileExists = LIVE_MIGRATION_FILES_ROUTE + "/data/0/dummy.txt";
        String fileDoesNotExist = LIVE_MIGRATION_FILES_ROUTE + "/data/0/file-does-not-exist.txt";
        InstanceFileInfo fileInfos = getInstanceFileInfo(new String[]{ fileExists, fileDoesNotExist }, 0).get(0);

        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationStreamFileAsync(any(), anyString(), anyString()))
        .thenReturn(CompletableFuture.failedFuture(new IOException("Download failed")));

        Future<Void> downloadTask = downloaderSpy.getDownloadTask(new SidecarInstanceImpl(SOURCE, PORT), fileInfos);

        assertThat(downloadTask.failed()).isTrue();
        assertThat(downloadTask.cause()).isNotNull();
    }

    @Test
    void testGetDownloadTaskFailedToUpdateLastModifiedTime(@TempDir Path tmpDir)
    {
        String storageDir = tmpDir.resolve("testGetDownloadTaskFailedToUpdateLastModifiedTime").toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 1, mock(Consumer.class), storageDir, dataDirs);
        downloaderSpy.updateState(operationStatus -> operationStatus.toCleaningState(100, 10)
                                                                    .toPreparingState().toDownloadingState(100, 10));

        String file = LIVE_MIGRATION_FILES_ROUTE + "/data/0/dummy.txt";
        InstanceFileInfo fileInfos = getInstanceFileInfo(new String[]{ file }, 0).get(0);

        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationStreamFileAsync(any(), anyString(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));

        doReturn(Future.failedFuture(new IOException("Failed to update last modified time")))
        .when(downloaderSpy).updateFileTimestampAsync(any(Path.class), anyLong());

        Future<Void> downloadTask = downloaderSpy.getDownloadTask(new SidecarInstanceImpl(SOURCE, PORT), fileInfos);

        assertThat(downloadTask.failed()).isTrue();
        assertThat(downloadTask.cause()).isNotNull();
    }

    @Test
    void testCanDeleteWhenFileSizesDoNotMatch(@TempDir Path tmpDir) throws IOException
    {
        String storageDir = tmpDir.resolve("testCanDeleteOnFileSizes").toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 1, mock(Consumer.class), storageDir, dataDirs);
        int fileSize = 32;
        long timestamp = System.currentTimeMillis();
        TestFile remoteFile1 = new TestFile(DATA_FILE_DIR, 0, "ks2/t1/4.db", fileSize, timestamp);
        String file1LocalPath = remoteFile1.getFilePath(storageDir);
        // This is the local file for 'remoteFile1'.
        // It has half the size of the remote file, so it is eligible for deletion.
        TestFile file1Local = new TestFile(remoteFile1.dirType, remoteFile1.dirIndex, remoteFile1.relativePath,
                                           remoteFile1.size / 2, remoteFile1.lastModifiedTime);


        TestFile remoteFile2 = new TestFile(DATA_FILE_DIR, 0, "ks2/t1/5.db", fileSize, timestamp);
        String file2LocalPath = remoteFile2.getFilePath(storageDir);

        prepareDataHomeDir(storageDir, List.of(file1Local, remoteFile2));

        Map<String, LiveMigrationFileDownloader.FileAttributes> fileAttrsToCheck = Map.of(
        remoteFile1.getFilePath(storageDir), new LiveMigrationFileDownloader.FileAttributes(remoteFile1.size, remoteFile1.lastModifiedTime),
        remoteFile2.getFilePath(storageDir), new LiveMigrationFileDownloader.FileAttributes(remoteFile2.size, remoteFile2.lastModifiedTime)
        );

        Path dataDir = Path.of(dataDirs.get(0));
        assertThat(downloaderSpy.canDelete(fileAttrsToCheck, Path.of(file1LocalPath), dataDir)).isTrue();
        assertThat(downloaderSpy.canDelete(fileAttrsToCheck, Path.of(file2LocalPath), dataDir)).isFalse();
    }

    @Test
    void testCanDeleteOnDirectories(@TempDir Path tmpDir) throws IOException
    {
        String storageDir = tmpDir.resolve("testCanDeleteOnDirectories").toAbsolutePath().toString();
        List<String> dataDirs = List.of(storageDir + "/data");
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 1, mock(Consumer.class), storageDir, dataDirs);

        int fileSize = 32;
        long timestamp = System.currentTimeMillis();
        TestFile localTable1File = new TestFile(DATA_FILE_DIR, 0, "ks2/t1/1.db", fileSize, timestamp);
        TestFile localTable2Dir = new TestFile(DATA_FILE_DIR, 0, "ks2/t2", -1, timestamp);
        TestFile remoteTable3Dir = new TestFile(DATA_FILE_DIR, 0, "ks2/t3", -1, timestamp);

        prepareDataHomeDir(storageDir, List.of(localTable1File, localTable2Dir, remoteTable3Dir));

        Map<String, LiveMigrationFileDownloader.FileAttributes> attrsToCheck = Map.of(
        remoteTable3Dir.getFilePath(storageDir), new LiveMigrationFileDownloader.FileAttributes(-1, timestamp));

        Path dataDir = Path.of(dataDirs.get(0));
        assertThat(downloaderSpy.canDelete(attrsToCheck, dataDir, dataDir)).isFalse();
        assertThat(downloaderSpy.canDelete(attrsToCheck,
                                           dataDir.resolve(localTable1File.relativePath),
                                           dataDir))
        .isTrue();
        assertThat(downloaderSpy.canDelete(attrsToCheck,
                                           dataDir.resolve(localTable1File.relativePath).getParent(),
                                           dataDir))
        .isTrue();
        assertThat(downloaderSpy.canDelete(attrsToCheck, dataDir.resolve(localTable2Dir.relativePath), dataDir)).isTrue();
        assertThat(downloaderSpy.canDelete(attrsToCheck, dataDir.resolve(remoteTable3Dir.relativePath), dataDir)).isFalse();
    }

    @Test
    void testCreateDirectory(@TempDir Path tempDir) throws InterruptedException
    {
        String storageDir = tempDir.resolve("testCreateDirectory").toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloader = getDownloader(injector, dummyRequest100pThreshold, 0, mock(Consumer.class), storageDir, dataDirs);
        downloader.updateState(operationStatus -> operationStatus.toCleaningState(100, 10)
                                                                 .toPreparingState().toDownloadingState(100, 10));

        String directoryUrl = LIVE_MIGRATION_FILES_ROUTE + "/data/0/ks1/table1";
        InstanceFileInfo directoryInfo = new InstanceFileInfo(directoryUrl, -1, FileType.DIRECTORY, System.currentTimeMillis());

        Future<Void> result = downloader.createDirectory(directoryInfo);
        awaitForFuture(result);

        assertThat(result.succeeded()).isTrue();
        Path path = localPath(directoryUrl, downloader.instanceMetadata());
        assertThat(Files.exists(path)).isTrue();
        assertThat(Files.isDirectory(path)).isTrue();
        assertThat(downloader.operationStatus().filesDownloaded()).isEqualTo(1);
    }

    @Test
    void testCreateDirectoryFailure(@TempDir Path tempDir) throws InterruptedException
    {
        String storageDir = tempDir.resolve("testCreateDirectoryFailure").toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloader = getDownloader(injector, dummyRequest100pThreshold, 0, mock(Consumer.class), storageDir, dataDirs);
        downloader.updateState(operationStatus -> operationStatus.toCleaningState(100, 10)
                                                                 .toPreparingState().toDownloadingState(100, 10));

        // Try to create directory with invalid characters (this should fail on most filesystems)
        String invalidDirectoryUrl = LIVE_MIGRATION_FILES_ROUTE + "/data/0/\0invalid";
        InstanceFileInfo directoryInfo = new InstanceFileInfo(invalidDirectoryUrl, -1, FileType.DIRECTORY, System.currentTimeMillis());

        Future<Void> result = downloader.createDirectory(directoryInfo);
        awaitForFuture(result);

        assertThat(result.failed()).isTrue();
        assertThat(downloader.operationStatus().downloadFailures()).isEqualTo(1);
    }

    @Test
    void testUpdateFileTimestampAsync(@TempDir Path tempDir) throws IOException, InterruptedException
    {
        String storageDir = tempDir.resolve("testUpdateFileTimestamp").toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloader = getDownloader(injector, dummyRequest100pThreshold, 0, mock(Consumer.class), storageDir, dataDirs);

        // Create test file
        Path testFile = tempDir.resolve("test.txt");
        Files.createFile(testFile);

        long newTimestamp = System.currentTimeMillis() - 10000; // 10 seconds ago
        Future<Void> result = downloader.updateFileTimestampAsync(testFile, newTimestamp);
        awaitForFuture(result);

        assertThat(result.succeeded()).isTrue();
        assertThat(Files.getLastModifiedTime(testFile).toMillis()).isEqualTo(newTimestamp);
    }

    @Test
    void testUpdateFileTimestampAsyncNonExistentFile() throws InterruptedException
    {
        String storageDir = "/tmp/testUpdateFileTimestampNonExistent";
        List<String> dataDirs = getDataDirList(storageDir);
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloader = getDownloader(injector, dummyRequest100pThreshold, 0, mock(Consumer.class), storageDir, dataDirs);

        String nonExistentFile = "/tmp/does_not_exist.txt";
        long timestamp = System.currentTimeMillis();

        Future<Void> result = downloader.updateFileTimestampAsync(Path.of(nonExistentFile), timestamp);
        awaitForFuture(result);

        assertThat(result.failed()).isTrue();
        assertThat(result.cause()).isInstanceOf(IOException.class);
    }

    @Test
    void testCreateEmptyFileWithNullParent(@TempDir Path tempDir) throws InterruptedException
    {
        String storageDir = tempDir.resolve("testCreateEmptyFileNullParent").toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloader =
        spy(getDownloader(injector, dummyRequest100pThreshold, 0, mock(Consumer.class), storageDir, dataDirs));
        downloader.updateState(operationStatus -> operationStatus.toCleaningState(100, 10)
                                                                 .toPreparingState().toDownloadingState(100, 10));

        // Create file info with root path (no parent)
        String rootFileUrl = LIVE_MIGRATION_FILES_ROUTE + "/data/0/some_file.txt";
        InstanceFileInfo fileInfo = new InstanceFileInfo(rootFileUrl, 0, FileType.FILE, System.currentTimeMillis());

        Path path = mock(Path.class);
        when(path.getParent()).thenReturn(null);
        when(path.toString()).thenReturn(storageDir + "/data/some_file.txt");
        doReturn(Future.succeededFuture(path))
        .when(downloader).localPathAsync(anyString(), any(InstanceMetadata.class));

        Future<Void> result = downloader.createEmptyFile(fileInfo);
        awaitForFuture(result);

        assertThat(result.succeeded()).isFalse();
        assertThat(result.failed()).isTrue();
        assertThat(result.cause()).isNotNull();
        assertThat(downloader.operationStatus().downloadFailures()).isEqualTo(1);
        assertThat(downloader.operationStatus().filesDownloaded()).isEqualTo(0);
    }

    @Test
    void testFileAttributesClass()
    {
        long size = 1024L;
        long lastModified = System.currentTimeMillis();

        LiveMigrationFileDownloader.FileAttributes attrs = new LiveMigrationFileDownloader.FileAttributes(size, lastModified);

        assertThat(attrs.size).isEqualTo(size);
        assertThat(attrs.lastModifiedTime).isEqualTo(lastModified);
    }

    @Test
    void testCanDeleteWithIOException(@TempDir Path tempDir) throws IOException
    {
        String storageDir = tempDir.resolve("testCanDeleteIOException").toAbsolutePath().toString();
        List<String> dataDirs = getDataDirList(storageDir);
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloader = getDownloader(injector, dummyRequest100pThreshold, 0, mock(Consumer.class), storageDir, dataDirs);

        // Create a file and then delete it to cause IOException when checking size
        Path testFile = tempDir.resolve("test.txt");
        Files.createFile(testFile);
        String absolutePath = testFile.toAbsolutePath().toString();

        Map<String, LiveMigrationFileDownloader.FileAttributes> fileAttrs = Map.of(
        absolutePath, new LiveMigrationFileDownloader.FileAttributes(100, System.currentTimeMillis()));

        // Delete the file to cause IOException
        Files.delete(testFile);

        assertThatThrownBy(() -> downloader.canDelete(fileAttrs, testFile, tempDir))
        .isInstanceOf(FileSystemException.class);
    }

    @Test
    void testCalculateDownloadSize()
    {
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloader = getDownloader(injector, dummyRequest100pThreshold, 0, mock(Consumer.class), "/tmp", dataDirsOne);

        List<InstanceFileInfo> files = List.of(
        new InstanceFileInfo("/file1.txt", 100, FileType.FILE, System.currentTimeMillis()),
        new InstanceFileInfo("/file2.txt", 200, FileType.FILE, System.currentTimeMillis()),
        new InstanceFileInfo("/dir1", -1, FileType.DIRECTORY, System.currentTimeMillis())); // Should be ignored

        long totalSize = downloader.calculateDownloadSize(files);
        assertThat(totalSize).isEqualTo(300L);
    }

    @Test
    void testCanDeleteWhenFileTimestampsDoNotMatch(@TempDir Path tempDir) throws IOException
    {
        String storageDir = tempDir.resolve("testCanDeleteOnFileTimestamps").toAbsolutePath().toString();
        List<String> dataDirs = List.of(storageDir + "/data");
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 1, mock(Consumer.class), storageDir, dataDirs);

        long timestamp = System.currentTimeMillis();
        long outdatedTimestamp = timestamp - 1000;
        int fileSize = 32;
        TestFile wrongTimestampFile = new TestFile(DATA_FILE_DIR, 0, "ks2/t1/4.db", fileSize, outdatedTimestamp);
        String wrongTimestampFileLocalPath = wrongTimestampFile.getFilePath(storageDir);
        TestFile rightTimestampFile = new TestFile(DATA_FILE_DIR, 0, "ks2/t1/5.db", fileSize, timestamp);
        String rightTimestampFileLocalPath = rightTimestampFile.getFilePath(storageDir);

        prepareDataHomeDir(storageDir, List.of(wrongTimestampFile, rightTimestampFile));

        Map<String, LiveMigrationFileDownloader.FileAttributes> fileAttrsToCheck =
        Map.of(wrongTimestampFile.getFilePath(storageDir),
               new LiveMigrationFileDownloader.FileAttributes(wrongTimestampFile.size, timestamp),
               rightTimestampFile.getFilePath(storageDir),
               new LiveMigrationFileDownloader.FileAttributes(rightTimestampFile.size, timestamp));

        Path dataDirPath = Path.of(dataDirs.get(0));
        assertThat(downloaderSpy.canDelete(fileAttrsToCheck, Path.of(wrongTimestampFileLocalPath), dataDirPath)).isTrue();
        assertThat(downloaderSpy.canDelete(fileAttrsToCheck, Path.of(rightTimestampFileLocalPath), dataDirPath)).isFalse();
    }

    void assertFileExists(String dataDir, String relativeFilePath, int expectedFileSize)
    {
        File file = new File(dataDir + "/" + relativeFilePath);
        assertThat(file.exists()).isTrue();
        assertThat(file.length()).isEqualTo(expectedFileSize);
    }

    void assertFileExists(String file, int expectedFileSize)
    {
        File f = new File(file);
        assertThat(f.exists()).isTrue();
        assertThat(f.length()).isEqualTo(expectedFileSize);
    }

    void assertDirExists(String dir)
    {
        File file = new File(dir);
        assertThat(file.isDirectory()).isTrue();
        assertThat(file.exists()).isTrue();
    }

    void assertFileDoesNotExists(String file)
    {
        assertThat(new File(file).exists()).isFalse();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    void prepareDataHomeDir(String storageDir, List<TestFile> testFiles) throws IOException
    {
        for (TestFile testFile : testFiles)
        {
            File f = new File(storageDir + "/" + testFile.dirType.dirType + "/" + testFile.relativePath);

            if (testFile.getFileType() == FileType.DIRECTORY)
            {
                f.mkdirs();
                continue;
            }

            createFile(f, testFile.size, testFile.lastModifiedTime);
        }
    }

    void createFile(String file, int size, long lastModifiedTime) throws IOException
    {
        createFile(new File(file), size, lastModifiedTime);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    void createFile(File f, int size, long lastModifiedTime) throws IOException
    {
        f.getParentFile().mkdirs();
        RandomAccessFile randomAccessFile = new RandomAccessFile(f, "rw");
        randomAccessFile.seek(size - 1);
        randomAccessFile.write(0);
        randomAccessFile.close();
        f.setLastModified(lastModifiedTime);
    }


    @Test
    void testShortlistDownloadFiles(@TempDir Path tmpDir) throws IOException
    {
        String storageDir = tmpDir.resolve("testShortlistDownloadFiles").toAbsolutePath().toString();
        List<String> dataDirs = List.of(storageDir + "/data");
        Consumer<OperationStatus> mockStatusUpdater = mock(Consumer.class);
        Injector injector = getInjector();
        LiveMigrationFileDownloader downloaderSpy =
        getDownloaderSpy(injector, dummyRequest100pThreshold, 1, mockStatusUpdater, storageDir, dataDirs);


        int defaultFileSize = 64;
        long timestamp = System.currentTimeMillis();

        TestFile fileExistsInLocal = new TestFile(DATA_FILE_DIR, 0, "ks1/t2/d.db", defaultFileSize, timestamp);
        TestFile fileDoesNotExistsInLocal = new TestFile(HINTS_DIR, 0, "hints1.db", defaultFileSize, timestamp);
        prepareDataHomeDir(storageDir, List.of(fileExistsInLocal));

        InstanceFilesListResponse instanceFilesListResponse =
        getInstanceFilesListResponse(List.of(fileExistsInLocal, fileDoesNotExistsInLocal));

        // 100% success threshold
        Future<List<InstanceFileInfo>> filesToDownloadFuture =
        downloaderSpy.shortlistDownloadFiles(instanceFilesListResponse, 1.0);

        assertThat(filesToDownloadFuture.isComplete()).isTrue();
        assertThat(filesToDownloadFuture.result().size()).isEqualTo(1);

        // 49% success threshold
        filesToDownloadFuture =
        downloaderSpy.shortlistDownloadFiles(instanceFilesListResponse, 0.49);

        assertThat(filesToDownloadFuture.isComplete()).isTrue();
        assertThat(filesToDownloadFuture.result().size()).isEqualTo(0);
    }

    InstanceFilesListResponse getInstanceFilesListResponse(String[] fileUrls, int size, long lastModifiedTime)
    {
        List<InstanceFileInfo> instanceFileInfoList = new ArrayList<>(fileUrls.length);
        for (String url : fileUrls)
        {
            instanceFileInfoList.add(new InstanceFileInfo(url, size, FileType.FILE, lastModifiedTime));
        }
        return new InstanceFilesListResponse(instanceFileInfoList);
    }

    @Deprecated
    InstanceFilesListResponse getInstanceFilesListResponse(String[] fileUrls, int size)
    {
        return getInstanceFilesListResponse(fileUrls, size, System.currentTimeMillis());
    }

    InstanceFilesListResponse getInstanceFilesListResponse(List<TestFile> testFiles)
    {
        return new InstanceFilesListResponse(getInstanceFileInfo(testFiles));
    }

    Injector getInjector()
    {
        return Guice.createInjector(new LiveMigrationFileDownloaderTestModule());
    }

    LiveMigrationFileDownloader getDownloader(Injector injector,
                                              LiveMigrationDataCopyRequest request,
                                              int currentIteration,
                                              Consumer<OperationStatus> mockStatusUpdater,
                                              List<String> dataDirs)
    {
        String storageDir = Paths.get(dataDirs.get(0)).getParent().toAbsolutePath().toString();
        return getDownloader(injector, request, currentIteration, mockStatusUpdater, storageDir, dataDirs);
    }

    LiveMigrationFileDownloader getDownloader(Injector injector,
                                              LiveMigrationDataCopyRequest request,
                                              int currentIteration,
                                              Consumer<OperationStatus> mockStatusUpdater,
                                              String storageDir,
                                              List<String> dataDirs)
    {
        SidecarClientProvider sidecarClientProvider = injector.getInstance(SidecarClientProvider.class);
        LiveMigrationConfiguration liveMigrationConfig = injector.getInstance(SidecarConfiguration.class)
                                                                 .liveMigrationConfiguration();
        return LiveMigrationFileDownloader.builder()
                                          .id(UUID.randomUUID().toString())
                                          .vertx(vertx)
                                          .sidecarClient(sidecarClientProvider.get())
                                          .request(request)
                                          .iteration(currentIteration)
                                          .statusUpdater(mockStatusUpdater)
                                          .instanceMetadata(InstanceMetadataImpl.builder()
                                                                                .dataDirs(dataDirs)
                                                                                .storageDir(storageDir)
                                                                                .metricRegistry(new MetricRegistry())
                                                                                .id(1)
                                                                                .build())
                                          .liveMigrationConfiguration(liveMigrationConfig)
                                          .source(SOURCE)
                                          .port(PORT)
                                          .executorPools(ExecutorPoolsHelper.createdSharedTestPool(vertx))
                                          .build();
    }

    List<String> getDataDirList(@NotNull String storageDir)
    {
        return List.of(storageDir + "/data");
    }

    LiveMigrationFileDownloader getDownloaderSpy(Injector injector,
                                                 LiveMigrationDataCopyRequest request,
                                                 int currentIteration,
                                                 Consumer<OperationStatus> mockStatusUpdater,
                                                 String storageDir,
                                                 List<String> dataDirs)
    {
        return spy(getDownloader(injector, request, currentIteration, mockStatusUpdater, storageDir, dataDirs));
    }

    List<InstanceFileInfo> getInstanceFileInfo(String[] fileUrls, int size, long lastModifiedTime)
    {
        return Arrays.stream(fileUrls)
                     .map(fileUrl -> new InstanceFileInfo(fileUrl, size, FileType.FILE, lastModifiedTime))
                     .collect(Collectors.toList());
    }

    List<InstanceFileInfo> getInstanceFileInfo(String[] fileUrls, int size)
    {
        return getInstanceFileInfo(fileUrls, size, System.currentTimeMillis());
    }

    List<InstanceFileInfo> getInstanceFileInfo(List<TestFile> testFiles)
    {
        return testFiles.stream().map(TestFile::getInstanceFileInfo).collect(Collectors.toList());
    }

    private static class TestFile
    {
        final LiveMigrationDirType dirType;
        final int dirIndex;
        final String relativePath;
        final int size;
        final long lastModifiedTime;

        public TestFile(LiveMigrationDirType dirType, int dirIndex, String relativePath, int size, long lastModifiedTime)
        {
            this.dirType = dirType;
            this.dirIndex = dirIndex;
            this.relativePath = relativePath;
            this.size = size;
            this.lastModifiedTime = lastModifiedTime;
        }

        InstanceFileInfo getInstanceFileInfo()
        {
            return new InstanceFileInfo(getFileUrl(), size, getFileType(), lastModifiedTime);
        }

        String getFileUrl()
        {
            return LIVE_MIGRATION_FILES_ROUTE + "/" + dirType.dirType + "/" + dirIndex + "/" + relativePath;
        }

        FileType getFileType()
        {
            return -1 == size ? FileType.DIRECTORY : FileType.FILE;
        }

        String getFilePath(String storageDir)
        {
            return storageDir + "/" + dirType.dirType + "/" + relativePath;
        }
    }

    private static class LiveMigrationFileDownloaderTestModule extends AbstractModule
    {
        SidecarClient sidecarClient = mock(SidecarClient.class);
        SidecarClientProvider sidecarClientProvider = mock(SidecarClientProvider.class);
        SidecarConfiguration mockSidecarConfiguration = mock(SidecarConfiguration.class);
        LiveMigrationConfiguration mockLiveMigrationConfig = mock(LiveMigrationConfiguration.class);

        @Override
        protected void configure()
        {
            bind(SidecarClient.class).toInstance(sidecarClient);
            bind(SidecarConfiguration.class).toInstance(mockSidecarConfiguration);
            bind(SidecarClientProvider.class).toInstance(sidecarClientProvider);

            when(sidecarClient.liveMigrationStatus(any(SidecarInstance.class)))
            .thenReturn(CompletableFuture.completedFuture(new LiveMigrationStatus(NOT_COMPLETED, 1L)));
            when(sidecarClientProvider.get()).thenReturn(sidecarClient);
            when(mockSidecarConfiguration.liveMigrationConfiguration()).thenReturn(mockLiveMigrationConfig);
            when(mockLiveMigrationConfig.filesToExclude()).thenReturn(Set.of());
            when(mockLiveMigrationConfig.directoriesToExclude()).thenReturn(Set.of());
        }
    }
}
