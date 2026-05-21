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


import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.apache.cassandra.sidecar.HelperTestModules.DigestAlgorithmProviderTestModule;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.request.LiveMigrationFilesVerificationRequest;
import org.apache.cassandra.sidecar.common.response.DigestResponse;
import org.apache.cassandra.sidecar.common.response.InstanceFileInfo;
import org.apache.cassandra.sidecar.common.response.InstanceFileInfo.FileType;
import org.apache.cassandra.sidecar.common.response.InstanceFilesListResponse;
import org.apache.cassandra.sidecar.common.response.LiveMigrationFilesVerificationResponse;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.config.yaml.LiveMigrationConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.handlers.livemigration.InstanceMetadataTestUtil;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationFilesVerificationTask.State;
import org.apache.cassandra.sidecar.utils.DigestAlgorithm;
import org.apache.cassandra.sidecar.utils.DigestAlgorithmFactory;
import org.apache.cassandra.sidecar.utils.DigestVerifierFactory;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.CDC_RAW_DIR;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.COMMIT_LOG_DIR;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.DATA_FILE_DIR;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.HINTS_DIR;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.LOCAL_SYSTEM_DATA_FILE_DIR;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.SAVED_CACHES_DIR;
import static org.apache.cassandra.sidecar.livemigration.TestFile.getInstanceFilesListResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class LiveMigrationFilesVerificationTaskTest
{

    private static final String SOURCE = "127.0.0.1";
    private static final String DESTINATION = "127.0.0.2";

    private static @NotNull List<TestFile> getTestFiles(long lastModifiedTime)
    {
        int fileSize = 32;
        return List.of(
        new TestFile(DATA_FILE_DIR, 0, "ks1", -1, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 0, "ks1/t1", -1, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 0, "ks1/t1/data.db", fileSize, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 1, "ks1", -1, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 1, "ks1/t2", -1, lastModifiedTime),
        new TestFile(DATA_FILE_DIR, 1, "ks1/t2/data.db", fileSize * 2, lastModifiedTime),
        new TestFile(HINTS_DIR, 0, "empty-file", 0, lastModifiedTime),
        new TestFile(COMMIT_LOG_DIR, 0, "commitlog-7-1.db", fileSize - 1, lastModifiedTime),
        new TestFile(SAVED_CACHES_DIR, 0, "cache.bin", fileSize + 1, lastModifiedTime),
        new TestFile(CDC_RAW_DIR, 0, "commitlog-7-1.db", fileSize / 2, lastModifiedTime),
        new TestFile(LOCAL_SYSTEM_DATA_FILE_DIR, 0, "data.db", 1, lastModifiedTime));
    }

    @Test
    void testNewDigestVerificationTask(@TempDir Path tempDir)
    {
        Injector injector = getInjector();
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);
        LiveMigrationFilesVerificationTask verificationTask = createVerificationTask(injector, instanceMetadata);

        assertThat(verificationTask.hasStarted()).isFalse();
        assertThat(verificationTask.isCancelled()).isFalse();
        assertThat(verificationTask.isCompleted()).isFalse();
        assertThat(verificationTask.id()).isNotNull();
        assertThat(verificationTask.type()).isEqualTo(LiveMigrationFilesVerificationTask.FILES_VERIFICATION_TASK_TYPE);

        LiveMigrationFilesVerificationResponse response = verificationTask.getResponse();
        assertThat(response).isNotNull();
        assertThat(State.valueOf(response.state())).isEqualTo(State.NOT_STARTED);
    }

    @Test
    void testCancelBeforeStartingTheTask(@TempDir Path tempDir)
    {
        // Task is cancelled before starting it.
        // The task state should be 'CANCELLED' even after starting it.
        Injector injector = getInjector();

        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(any()))
        .thenReturn(Future.succeededFuture(new InstanceFilesListResponse(Collections.emptyList()))
                          .toCompletionStage().toCompletableFuture());

        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);
        LiveMigrationFilesVerificationTask digestVerificationTask =
        createVerificationTask(injector, instanceMetadata);

        // Cancel task before starting it
        digestVerificationTask.cancel();

        LiveMigrationFilesVerificationResponse response = digestVerificationTask.getResponse();
        assertThat(response).isNotNull();
        assertThat(State.valueOf(response.state())).isEqualTo(State.CANCELLED);
        assertThat(digestVerificationTask.isCancelled()).isTrue();
        assertThat(digestVerificationTask.isCompleted()).isTrue();
    }

    @Test
    void testCancelAndStartTask(@TempDir Path tempDir)
    {
        // Task is cancelled before starting it.
        // The task state should be 'CANCELLED' even after starting it.
        Injector injector = getInjector();

        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(any()))
        .thenReturn(Future.succeededFuture(new InstanceFilesListResponse(Collections.emptyList()))
                          .toCompletionStage().toCompletableFuture());

        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);
        LiveMigrationFilesVerificationTask verificationTask =
        createVerificationTask(injector, instanceMetadata);

        verificationTask.cancel();
        verificationTask.start();

        LiveMigrationFilesVerificationResponse response = verificationTask.getResponse();
        assertThat(response).isNotNull();
        assertThat(State.valueOf(response.state())).isEqualTo(State.CANCELLED);
        assertThat(verificationTask.isCancelled()).isTrue();
        assertThat(verificationTask.isCompleted()).isTrue();

        // Cancelling task again should not cause any changes
        verificationTask.cancel();
        assertThat(verificationTask.getResponse().state()).isEqualTo(State.CANCELLED.name());
    }

    @Test
    public void testVerifyFilesUsingMD5(@TempDir Path tempDir) throws IOException, InterruptedException
    {
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(20, "md5");
        verifyCompletesSuccessfully(tempDir, request);
    }

    @Test
    public void testVerifyFilesUsingXXHash(@TempDir Path tempDir) throws IOException, InterruptedException
    {
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(20, "XXHash32");
        verifyCompletesSuccessfully(tempDir, request);
    }

    @Test
    public void testCancelCompletedTask(@TempDir Path tempDir) throws IOException, InterruptedException
    {
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(20, "XXHash32");
        LiveMigrationFilesVerificationTask task = verifyCompletesSuccessfully(tempDir, request);

        assertThat(task.isCompleted()).isTrue();
        assertThat(State.valueOf(task.getResponse().state())).isEqualTo(State.COMPLETED);

        task.cancel();
        // State should not change the task got completed already
        assertThat(State.valueOf(task.getResponse().state())).isEqualTo(State.COMPLETED);
        assertThat(task.isCompleted()).isTrue();
    }

    private LiveMigrationFilesVerificationTask verifyCompletesSuccessfully(Path tempDir,
                                                                           LiveMigrationFilesVerificationRequest request) throws IOException, InterruptedException
    {
        Injector injector = getInjector();
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);

        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> filesToDownload = getTestFiles(lastModifiedTime);

        mockListInstanceFilesResponse(injector, filesToDownload);
        mockSourceFileDigestResponse(injector, request, filesToDownload, instanceMetadata);

        LiveMigrationFilesVerificationTask digestVerificationTask =
        createVerificationTask(injector, request, instanceMetadata);
        digestVerificationTask.start();

        awaitForFuture(digestVerificationTask.future());
        assertThat(digestVerificationTask.isCompleted()).isTrue();
        LiveMigrationFilesVerificationResponse response = digestVerificationTask.getResponse();
        assertThat(response).isNotNull();
        assertThat(State.valueOf(response.state())).isEqualTo(State.COMPLETED);
        assertThat(response.isVerificationSuccessful()).isTrue();
        assertThat(response.metadataMismatches()).isEqualTo(0);
        assertThat(response.metadataMatched()).isEqualTo(filesToDownload.size());
        // Digest is compared only for files, so files matched should be equal to
        // number of actual files present.
        assertThat(response.filesMatched()).isEqualTo(filesCount(filesToDownload));
        assertThat(response.digestMismatches()).isEqualTo(0);
        assertThat(response.digestVerificationFailures()).isEqualTo(0);

        return digestVerificationTask;
    }

    @Test
    public void testFilesListingAtSourceFailed(@TempDir Path tempDir) throws IOException, InterruptedException
    {
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(20, "XXHash32");
        Injector injector = getInjector();
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);

        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> filesToDownload = getTestFiles(lastModifiedTime);

        mockListInstanceFilesResponse(injector, filesToDownload);
        mockSourceFileDigestResponse(injector, request, filesToDownload, instanceMetadata);

        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(any(SidecarInstance.class)))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Internal Server Error")));

        LiveMigrationFilesVerificationTask digestVerificationTask =
        createVerificationTask(injector, request, instanceMetadata);
        digestVerificationTask.start();

        Future<Void> completionFuture = digestVerificationTask.future();
        awaitForFuture(completionFuture);
        assertThat(completionFuture.isComplete()).isTrue();
        LiveMigrationFilesVerificationResponse response = digestVerificationTask.getResponse();
        assertThat(response).isNotNull();
        assertThat(State.valueOf(response.state())).isEqualTo(State.FAILED);
        assertThat(response.isVerificationSuccessful()).isFalse();
        assertThat(response.metadataMismatches()).isEqualTo(0);
        assertThat(response.metadataMatched()).isEqualTo(0);

        assertThat(response.filesMatched()).isEqualTo(0);
        assertThat(response.digestMismatches()).isEqualTo(0);
        assertThat(response.digestVerificationFailures()).isEqualTo(0);
    }

    @Test
    public void testFileDigestCallToSourceFailed(@TempDir Path tempDir) throws IOException, InterruptedException
    {
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(20, "XXHash32");
        Injector injector = getInjector();
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);

        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> filesToDownload = getTestFiles(lastModifiedTime);

        mockListInstanceFilesResponse(injector, filesToDownload);
        mockSourceFileDigestResponse(injector, request, filesToDownload, instanceMetadata);

        // Mock file digest call failure
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        TestFile randomFile = getRandomFile(filesToDownload);
        when(sidecarClient.liveMigrationFileDigestAsync(any(SidecarInstance.class),
                                                        eq(randomFile.getFileUrl()),
                                                        anyString()))
        .thenReturn(CompletableFuture.failedFuture(new IOException("File digest call failed")));

        LiveMigrationFilesVerificationTask digestVerificationTask =
        createVerificationTask(injector, request, instanceMetadata);
        digestVerificationTask.start();

        awaitForFuture(digestVerificationTask.future());
        LiveMigrationFilesVerificationResponse response = digestVerificationTask.getResponse();
        assertThat(response).isNotNull();
        assertThat(State.valueOf(response.state())).isEqualTo(State.FAILED);
        assertThat(response.isVerificationSuccessful()).isFalse();
        assertThat(response.metadataMismatches()).isEqualTo(0);
        assertThat(response.metadataMatched()).isEqualTo(filesToDownload.size());

        assertThat(response.filesMatched()).isEqualTo(filesCount(filesToDownload) - 1); // -1 for the digest call failed.
        assertThat(response.digestMismatches()).isEqualTo(0);
        assertThat(response.digestVerificationFailures()).isEqualTo(1);
    }

    @Test
    public void testFailedToCalculateDigestForFilesInLocal(@TempDir Path tempDir) throws IOException, InterruptedException
    {
        String digestAlgorithm = "XXHash32";
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(20, digestAlgorithm);
        Injector injector = getInjector();
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);

        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> filesToDownload = getTestFiles(lastModifiedTime);

        mockListInstanceFilesResponse(injector, filesToDownload);
        mockSourceFileDigestResponse(injector, request, filesToDownload, instanceMetadata);

        TestFile randomFile = getRandomFile(filesToDownload);

        LiveMigrationFilesVerificationTask digestVerificationTask =
        spy(createVerificationTask(injector, request, instanceMetadata));
        doAnswer(invocation -> {
            InstanceFileInfo fileInfo = invocation.getArgument(0);
            if (fileInfo.fileUrl.equals(randomFile.getFileUrl()))
            {
                return Future.failedFuture("Failed to calculate digest for file " + fileInfo.fileUrl);
            }
            return invocation.callRealMethod();
        }).when(digestVerificationTask).verifyDigest(any(InstanceFileInfo.class));

        digestVerificationTask.start();

        awaitForFuture(digestVerificationTask.future());
        LiveMigrationFilesVerificationResponse response = digestVerificationTask.getResponse();
        assertThat(response).isNotNull();
        assertThat(State.valueOf(response.state())).isEqualTo(State.FAILED);
        assertThat(response.isVerificationSuccessful()).isFalse();
        assertThat(response.metadataMismatches()).isEqualTo(0);
        assertThat(response.metadataMatched()).isEqualTo(filesToDownload.size());

        assertThat(response.filesMatched()).isEqualTo(filesCount(filesToDownload) - 1); // -1 for the digest call failed.
        assertThat(response.digestMismatches()).isEqualTo(0);
        assertThat(response.digestVerificationFailures()).isEqualTo(1);
    }

    @Test
    public void testFilesTimestampsAreNotMatching(@TempDir Path tempDir) throws IOException, InterruptedException
    {
        // Verification task should fail when file's last modified timestamps are not matching
        Injector injector = getInjector();
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(20, "md5");
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);

        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> localTestFiles = getTestFiles(lastModifiedTime);

        List<TestFile> filesToDownload = new ArrayList<>(localTestFiles.size());
        for (int i = 0; i < localTestFiles.size(); i++)
        {
            // Since 'i' starts with 0, first file last modified remains the same
            TestFile file = localTestFiles.get(i);
            filesToDownload.add(updateLastModifiedTime(file, file.lastModifiedTime + i + 1));
        }

        mockListInstanceFilesResponse(injector, localTestFiles);
        mockSourceFileDigestResponse(injector, request, filesToDownload, instanceMetadata);

        LiveMigrationFilesVerificationTask digestVerificationTask =
        createVerificationTask(injector, request, instanceMetadata);

        // Start verification task
        digestVerificationTask.start();

        awaitForFuture(digestVerificationTask.future());
        LiveMigrationFilesVerificationResponse response = digestVerificationTask.getResponse();
        assertThat(response).isNotNull();
        assertThat(State.valueOf(response.state())).isEqualTo(State.FAILED);

        assertThat(response.isVerificationSuccessful()).isFalse();
        // directories time stamp is not compared, so they should match
        assertThat(response.metadataMatched()).isEqualTo(dirsCount(localTestFiles));
        assertThat(response.metadataMismatches()).isEqualTo(filesCount(localTestFiles));
        assertThat(response.filesNotFoundAtSource()).isEqualTo(0);
        assertThat(response.filesNotFoundAtDestination()).isEqualTo(0);

        // since the last modified times do not match, verification will not go to digest comparison stage,
        // so the files matched and digest mismatched counts remain zero.
        assertThat(response.filesMatched()).isEqualTo(0);
        assertThat(response.digestMismatches()).isEqualTo(0);
        assertThat(response.digestVerificationFailures()).isEqualTo(0);
    }

    @Test
    public void testFilesSizesAreNotMatching(@TempDir Path tempDir) throws IOException, InterruptedException
    {
        // Verification task should fail when files sizes are not matching
        Injector injector = getInjector();
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(20, "md5");
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);

        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> localTestFiles = getTestFiles(lastModifiedTime);

        List<TestFile> filesToDownload = new ArrayList<>(localTestFiles.size());
        for (int i = 0; i < localTestFiles.size(); i++)
        {
            TestFile file = localTestFiles.get(i);
            filesToDownload.add(updateSize(file, file.size > -1 ? file.size + i + 1 : file.size));
        }

        mockListInstanceFilesResponse(injector, localTestFiles);
        mockSourceFileDigestResponse(injector, request, filesToDownload, instanceMetadata);

        LiveMigrationFilesVerificationTask digestVerificationTask =
        createVerificationTask(injector, request, instanceMetadata);

        // Start verification task
        digestVerificationTask.start();

        awaitForFuture(digestVerificationTask.future());
        LiveMigrationFilesVerificationResponse response = digestVerificationTask.getResponse();
        assertThat(response).isNotNull();
        assertThat(State.valueOf(response.state())).isEqualTo(State.FAILED);

        assertThat(response.isVerificationSuccessful()).isFalse();
        assertThat(response.metadataMatched()).isEqualTo(dirsCount(localTestFiles));
        assertThat(response.metadataMismatches()).isEqualTo(filesCount(localTestFiles));
        assertThat(response.filesNotFoundAtSource()).isEqualTo(0);
        assertThat(response.filesNotFoundAtDestination()).isEqualTo(0);

        // Since the file sizes do not match, verification will not reach digest comparison stage,
        // so the files matched and digest mismatched counts remain zero.
        assertThat(response.filesMatched()).isEqualTo(0);
        assertThat(response.digestMismatches()).isEqualTo(0);
        assertThat(response.digestVerificationFailures()).isEqualTo(0);
    }

    @Test
    public void testFilesDigestsAreNotMatching(@TempDir Path tempDir) throws IOException, InterruptedException
    {
        // Verification task should fail when file digests do not match
        Injector injector = getInjector();
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(20, "md5");
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);

        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> localTestFiles = getTestFiles(lastModifiedTime);

        mockListInstanceFilesResponse(injector, localTestFiles);
        mockSourceFileDigestResponse(injector, request, localTestFiles, instanceMetadata);

        // Now update file contents to re-create that file digests are not matching
        for (TestFile testFile : localTestFiles)
        {
            recreateFile(testFile, instanceMetadata);
        }

        LiveMigrationFilesVerificationTask digestVerificationTask =
        createVerificationTask(injector, request, instanceMetadata);

        // Start verification task
        digestVerificationTask.start();

        awaitForFuture(digestVerificationTask.future());
        LiveMigrationFilesVerificationResponse response = digestVerificationTask.getResponse();
        assertThat(response).isNotNull();
        assertThat(State.valueOf(response.state())).isEqualTo(State.FAILED);

        assertThat(response.isVerificationSuccessful()).isFalse();
        assertThat(response.metadataMatched()).isEqualTo(localTestFiles.size());
        assertThat(response.metadataMismatches()).isEqualTo(0);
        assertThat(response.filesNotFoundAtSource()).isEqualTo(0);
        assertThat(response.filesNotFoundAtDestination()).isEqualTo(0);

        // In this scenario, metadata matches and verification task compares digests for normal files (non-directory)
        // digest verification should fail for the as the file contents got changed.
        int emptyFilesCount = emptyFilesCount(localTestFiles);
        assertThat(response.filesMatched()).isEqualTo(emptyFilesCount);
        assertThat(response.digestMismatches()).isEqualTo(filesCount(localTestFiles) - emptyFilesCount);
        assertThat(response.digestVerificationFailures()).isEqualTo(0);
    }

    @Test
    public void testFewFilesAreMissing(@TempDir Path tempDir) throws IOException, InterruptedException
    {
        // Test the scenario where few files are missing at both source and destination
        Injector injector = getInjector();
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(20, "md5");
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);

        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> testFiles = getTestFiles(lastModifiedTime);
        List<TestFile> localFiles = testFiles.subList(1, testFiles.size()); // Excluding first file
        List<TestFile> filesToDownload = testFiles.subList(0, testFiles.size() - 1); // Excluding last file

        mockListInstanceFilesResponse(injector, localFiles);
        mockSourceFileDigestResponse(injector, request, filesToDownload, instanceMetadata);

        LiveMigrationFilesVerificationTask digestVerificationTask =
        createVerificationTask(injector, request, instanceMetadata);

        // Start verification task
        digestVerificationTask.start();

        awaitForFuture(digestVerificationTask.future());
        LiveMigrationFilesVerificationResponse response = digestVerificationTask.getResponse();
        assertThat(response).isNotNull();
        assertThat(State.valueOf(response.state())).isEqualTo(State.FAILED);

        assertThat(response.isVerificationSuccessful()).isFalse();
        assertThat(response.metadataMatched()).isEqualTo(testFiles.size() - 2);
        assertThat(response.metadataMismatches()).isEqualTo(0);
        assertThat(response.filesNotFoundAtSource()).isEqualTo(1);
        assertThat(response.filesNotFoundAtDestination()).isEqualTo(1);
        assertThat(response.filesMatched()).isEqualTo(0);
        assertThat(response.digestMismatches()).isEqualTo(0);
        assertThat(response.digestVerificationFailures()).isEqualTo(0);
    }

    @Test
    public void testFewDirectoriesAreMissing(@TempDir Path tempDir) throws IOException, InterruptedException
    {
        // Test the scenario where few directories are missing at source and destination
        Injector injector = getInjector();
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(20, "md5");
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);

        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> testFiles = getTestFiles(lastModifiedTime);
        List<TestFile> localFiles = new ArrayList<>(testFiles);
        // Size of test file is set to -1 to indicate that it is directory
        localFiles.add(new TestFile(DATA_FILE_DIR, 0, "newkeyspace", -1, lastModifiedTime));

        List<TestFile> filesToDownload = new ArrayList<>(testFiles);
        filesToDownload.add(new TestFile(DATA_FILE_DIR, 1, "newsrckeyspace", -1, lastModifiedTime));

        mockListInstanceFilesResponse(injector, localFiles);
        mockSourceFileDigestResponse(injector, request, filesToDownload, instanceMetadata);

        LiveMigrationFilesVerificationTask digestVerificationTask =
        createVerificationTask(injector, request, instanceMetadata);

        // Start verification task
        digestVerificationTask.start();

        awaitForFuture(digestVerificationTask.future());
        LiveMigrationFilesVerificationResponse response = digestVerificationTask.getResponse();
        assertThat(response).isNotNull();
        assertThat(State.valueOf(response.state())).isEqualTo(State.FAILED);

        assertThat(response.isVerificationSuccessful()).isFalse();
        assertThat(response.metadataMatched()).isEqualTo(testFiles.size());
        assertThat(response.metadataMismatches()).isEqualTo(0);
        assertThat(response.filesNotFoundAtSource()).isEqualTo(1);
        assertThat(response.filesNotFoundAtDestination()).isEqualTo(1);
        assertThat(response.filesMatched()).isEqualTo(0);
        assertThat(response.digestMismatches()).isEqualTo(0);
        assertThat(response.digestVerificationFailures()).isEqualTo(0);
    }

    @Test
    public void testFileTypesAreNotMatching(@TempDir Path tempDir) throws IOException, InterruptedException
    {
        // Test the scenario where fileType is not matching between source and destination
        // i.e. an entry is expected to be a file, but it is a directory at source and vice versa.
        Injector injector = getInjector();
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(20, "md5");
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);

        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> testFiles = new ArrayList<>(getTestFiles(lastModifiedTime));
        TestFile randomFile1 = getRandomFile(testFiles);
        testFiles.remove(randomFile1);
        TestFile randomFile2 = getRandomFile(testFiles);
        testFiles.remove(randomFile2);

        List<TestFile> localFiles = new ArrayList<>(testFiles);
        localFiles.add(convertToDirectory(randomFile1));
        localFiles.add(randomFile2);

        List<TestFile> filesAtSource = new ArrayList<>(testFiles);
        filesAtSource.add(randomFile1);
        filesAtSource.add(convertToDirectory(randomFile2));

        mockListInstanceFilesResponse(injector, localFiles);
        mockSourceFileDigestResponse(injector, request, filesAtSource, instanceMetadata);

        LiveMigrationFilesVerificationTask digestVerificationTask =
        createVerificationTask(injector, request, instanceMetadata);

        // Start verification task
        digestVerificationTask.start();

        awaitForFuture(digestVerificationTask.future());
        LiveMigrationFilesVerificationResponse response = digestVerificationTask.getResponse();
        assertThat(response).isNotNull();
        assertThat(State.valueOf(response.state())).isEqualTo(State.FAILED);

        assertThat(response.isVerificationSuccessful()).isFalse();

        // files types of randomFile1 and randomFile2 are not matching between source and destination,
        // hence the metadata mismatches should be equal to 2
        assertThat(response.metadataMismatches()).isEqualTo(2);
        assertThat(response.metadataMatched()).isEqualTo(localFiles.size() - 2);
        assertThat(response.filesNotFoundAtSource()).isEqualTo(0);
        assertThat(response.filesNotFoundAtDestination()).isEqualTo(0);

        assertThat(response.filesMatched()).isEqualTo(0);
        assertThat(response.digestMismatches()).isEqualTo(0);
        assertThat(response.digestVerificationFailures()).isEqualTo(0);
    }

    @Test
    public void testAbortIfCancelledWhenTaskNotCancelled(@TempDir Path tempDir)
    {
        // Test that abortIfCancelled returns succeeded future when task is not cancelled
        Injector injector = getInjector();
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);
        LiveMigrationFilesVerificationTask verificationTask = createVerificationTask(injector, instanceMetadata);

        String testValue = "test-value";
        Future<String> result = verificationTask.abortIfCancelled(testValue);

        assertThat(result.succeeded()).isTrue();
        assertThat(result.result()).isEqualTo(testValue);
        assertThat(verificationTask.isCancelled()).isFalse();
    }

    @Test
    public void testAbortIfCancelledWhenTaskCancelledInProgress(@TempDir Path tempDir)
    {
        // Test that abortIfCancelled returns failed future and updates state to CANCELLED
        // when task is cancelled and state is IN_PROGRESS
        Injector injector = getInjector();
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);
        LiveMigrationFilesVerificationTask verificationTask = createVerificationTask(injector, instanceMetadata);

        // Start the task to set state to IN_PROGRESS
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(any()))
        .thenReturn(Future.succeededFuture(new InstanceFilesListResponse(Collections.emptyList()))
                          .toCompletionStage().toCompletableFuture());

        verificationTask.start();
        assertThat(State.valueOf(verificationTask.getResponse().state())).isEqualTo(State.IN_PROGRESS);

        // Cancel the task
        verificationTask.cancel();
        assertThat(verificationTask.isCancelled()).isTrue();

        // Call abortIfCancelled
        String testValue = "test-value";
        Future<String> result = verificationTask.abortIfCancelled(testValue);

        assertThat(result.failed()).isTrue();
        assertThat(result.cause().getMessage()).isEqualTo("Task got cancelled");
        assertThat(State.valueOf(verificationTask.getResponse().state())).isEqualTo(State.CANCELLED);
    }

    @Test
    public void testAbortIfCancelledWhenTaskCancelledButNotInProgress(@TempDir Path tempDir)
    {
        // Test that abortIfCancelled returns failed future but does not update state
        // when task is cancelled but state is not IN_PROGRESS
        Injector injector = getInjector();
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);
        LiveMigrationFilesVerificationTask verificationTask = createVerificationTask(injector, instanceMetadata);

        // Task is in NOT_STARTED state
        assertThat(State.valueOf(verificationTask.getResponse().state())).isEqualTo(State.NOT_STARTED);

        // Cancel the task
        verificationTask.cancel();
        assertThat(verificationTask.isCancelled()).isTrue();

        // Call abortIfCancelled
        String testValue = "test-value";
        Future<String> result = verificationTask.abortIfCancelled(testValue);

        assertThat(result.failed()).isTrue();
        assertThat(result.cause().getMessage()).isEqualTo("Task got cancelled");
        // State should remain NOT_STARTED since compareAndSet will fail
        assertThat(State.valueOf(verificationTask.getResponse().state())).isEqualTo(State.CANCELLED);
    }

    @Test
    public void testAbortIfCancelledWithDifferentTypes(@TempDir Path tempDir)
    {
        // Test that abortIfCancelled works with different generic types
        Injector injector = getInjector();
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);
        LiveMigrationFilesVerificationTask verificationTask = createVerificationTask(injector, instanceMetadata);

        // Test with Integer
        Integer intValue = 42;
        Future<Integer> intResult = verificationTask.abortIfCancelled(intValue);
        assertThat(intResult.succeeded()).isTrue();
        assertThat(intResult.result()).isEqualTo(intValue);

        // Test with List
        List<String> listValue = List.of("a", "b", "c");
        Future<List<String>> listResult = verificationTask.abortIfCancelled(listValue);
        assertThat(listResult.succeeded()).isTrue();
        assertThat(listResult.result()).isEqualTo(listValue);

        // Test with null
        Future<Object> nullResult = verificationTask.abortIfCancelled(null);
        assertThat(nullResult.succeeded()).isTrue();
        assertThat(nullResult.result()).isNull();
    }

    @Test
    public void testCancelDuringValidation(@TempDir Path tempDir) throws IOException, InterruptedException
    {
        // Test that cancel() racing with validation completion doesn't cause IllegalStateException
        // This tests the thread-safety of using tryComplete/tryFail
        Injector injector = getInjector();
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(20, "md5");
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);

        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> filesToDownload = getTestFiles(lastModifiedTime);

        // Create a Promise that we control to simulate async operation
        Promise<InstanceFilesListResponse> listFilesPromise = Promise.promise();

        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(any(SidecarInstance.class)))
        .thenReturn(listFilesPromise.future().toCompletionStage().toCompletableFuture());

        mockSourceFileDigestResponse(injector, request, filesToDownload, instanceMetadata);

        LiveMigrationFilesVerificationTask verificationTask =
        createVerificationTask(injector, request, instanceMetadata);

        // Start the task - it will now be blocked waiting for listFilesPromise
        verificationTask.start();
        assertThat(verificationTask.hasStarted()).isTrue();
        assertThat(State.valueOf(verificationTask.getResponse().state())).isEqualTo(State.IN_PROGRESS);

        // Cancel the task while it's waiting on the promise
        verificationTask.cancel();
        assertThat(verificationTask.isCancelled()).isTrue();

        // Now complete the promise - this creates a race between:
        // - cancel() calling completionPromise.tryFail()
        // - validation completing and calling completionPromise.tryComplete()
        listFilesPromise.complete(getInstanceFilesListResponse(filesToDownload));

        // Wait for the task to complete
        awaitForFuture(verificationTask.future());

        // Verify the task completed without throwing IllegalStateException
        assertThat(verificationTask.isCompleted()).isTrue();
        assertThat(verificationTask.isCancelled()).isTrue();

        // The final state should be CANCELLED since cancel was called
        assertThat(State.valueOf(verificationTask.getResponse().state())).isEqualTo(State.CANCELLED);

        verify(sidecarClient, times(0))
        .liveMigrationFileDigestAsync(any(SidecarInstance.class), anyString(), anyString());
    }

    @Test
    public void testCancelWhileValidatingDigests(@TempDir Path tempDir) throws IOException, InterruptedException
    {
        // Cancels the verification task when the task is comparing files digests.
        Injector injector = getInjector();
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(20, "md5");
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);

        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> filesToDownload = getTestFiles(lastModifiedTime);
        mockListInstanceFilesResponse(injector, filesToDownload);

        for (TestFile testFile: filesToDownload)
        {
            testFile.createFile(instanceMetadata);
        }

        // Create a Promise that we control
        Promise<DigestResponse> filesDigestsPromise = Promise.promise();
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationFileDigestAsync(any(SidecarInstanceImpl.class), anyString(), anyString()))
        .thenReturn(filesDigestsPromise.future().toCompletionStage().toCompletableFuture());

        LiveMigrationFilesVerificationTask verificationTask =
        createVerificationTask(injector, request, instanceMetadata);

        // Start the task
        verificationTask.start();
        assertThat(verificationTask.hasStarted()).isTrue();
        assertThat(State.valueOf(verificationTask.getResponse().state())).isEqualTo(State.IN_PROGRESS);

        verificationTask.cancel();

        // Wait for validation to complete
        awaitForFuture(verificationTask.future());
        assertThat(verificationTask.isCompleted()).isTrue();
        assertThat(State.valueOf(verificationTask.getResponse().state())).isEqualTo(State.CANCELLED);

        filesDigestsPromise.fail("Digest calls failed");

        awaitForFuture(verificationTask.future());

        // CANCELLED is a final state, task status should not change
        assertThat(State.valueOf(verificationTask.getResponse().state())).isEqualTo(State.CANCELLED);
    }

    @Test
    public void testUnknownDigestAlgorithm(@TempDir Path tempDir) throws IOException, InterruptedException
    {
        // Verification task should fail when an unknown digest algorithm is provided
        String unknownAlgorithm = "UnknownDigestAlgo";
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(20, unknownAlgorithm);
        Injector injector = getInjector();
        InstanceMetadata instanceMetadata = InstanceMetadataTestUtil.getInstanceMetadata(DESTINATION, 2, tempDir);

        long lastModifiedTime = System.currentTimeMillis();
        List<TestFile> filesToDownload = getTestFiles(lastModifiedTime);

        mockListInstanceFilesResponse(injector, filesToDownload);

        // Create files locally
        for (TestFile testFile : filesToDownload)
        {
            testFile.createFile(instanceMetadata);
        }

        // Mock digest response from source with unknown algorithm
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationFileDigestAsync(eq(new SidecarInstanceImpl(SOURCE, 9043)),
                                                        anyString(),
                                                        eq(unknownAlgorithm)))
        .thenAnswer(invocationOnMock -> {
            // Return a digest response with the unknown algorithm
            return Future.succeededFuture(new DigestResponse("dummy-digest", unknownAlgorithm))
                         .toCompletionStage().toCompletableFuture();
        });

        LiveMigrationFilesVerificationTask verificationTask =
        createVerificationTask(injector, request, instanceMetadata);

        // Start verification task
        verificationTask.start();

        awaitForFuture(verificationTask.future());
        LiveMigrationFilesVerificationResponse response = verificationTask.getResponse();
        assertThat(response).isNotNull();
        assertThat(State.valueOf(response.state())).isEqualTo(State.FAILED);

        assertThat(response.isVerificationSuccessful()).isFalse();
        assertThat(response.metadataMatched()).isEqualTo(filesToDownload.size());
        assertThat(response.metadataMismatches()).isEqualTo(0);
        assertThat(response.filesNotFoundAtSource()).isEqualTo(0);
        assertThat(response.filesNotFoundAtDestination()).isEqualTo(0);

        // Since the digest algorithm is unknown, verification will fail for all files
        assertThat(response.filesMatched()).isEqualTo(0);
        assertThat(response.digestMismatches()).isEqualTo(0);
        assertThat(response.digestVerificationFailures()).isEqualTo(filesCount(filesToDownload));
    }

    Injector getInjector()
    {
        return Guice.createInjector(new TestModule());
    }

    private <T> void awaitForFuture(Future<T> future) throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(1);
        future.onComplete(res -> latch.countDown());

        //noinspection ResultOfMethodCallIgnored
        latch.await(5, TimeUnit.SECONDS); // Change to latch.await() for debugging
    }

    private LiveMigrationFilesVerificationTask createVerificationTask(Injector injector,
                                                                      InstanceMetadata instanceMetadata)
    {
        LiveMigrationFilesVerificationRequest request = new LiveMigrationFilesVerificationRequest(20, "md5");
        return createVerificationTask(injector, request, instanceMetadata);
    }

    private LiveMigrationFilesVerificationTask createVerificationTask(Injector injector,
                                                                      LiveMigrationFilesVerificationRequest request,
                                                                      InstanceMetadata instanceMetadata)
    {
        ServiceConfigurationImpl serviceConfiguration = new ServiceConfigurationImpl();
        Vertx vertx = injector.getInstance(Vertx.class);
        DigestVerifierFactory digestVerifierFactory = spy(injector.getInstance(DigestVerifierFactory.class));
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);

        return LiveMigrationFilesVerificationTask.builder()
                                                 .id("id")
                                                 .source(LiveMigrationFilesVerificationTaskTest.SOURCE)
                                                 .port(9043)
                                                 .executorPools(new ExecutorPools(vertx, serviceConfiguration))
                                                 .liveMigrationConfiguration(getLiveMigrationConfig())
                                                 .request(request)
                                                 .instanceMetadata(instanceMetadata)
                                                 .sidecarClient(sidecarClient)
                                                 .vertx(vertx)
                                                 .digestVerifierFactory(digestVerifierFactory)
                                                 .build();
    }

    private LiveMigrationConfiguration getLiveMigrationConfig()
    {
        return new LiveMigrationConfigurationImpl(Set.of(), Set.of(), Map.of(SOURCE, DESTINATION), 20);
    }

    private void mockListInstanceFilesResponse(Injector injector, List<TestFile> files)
    {
        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationListInstanceFilesAsync(any(SidecarInstance.class)))
        .thenReturn(CompletableFuture.completedFuture(getInstanceFilesListResponse(files)));
    }

    private void mockSourceFileDigestResponse(Injector injector,
                                              LiveMigrationFilesVerificationRequest request,
                                              List<TestFile> sourceFiles,
                                              InstanceMetadata instanceMetadata) throws IOException
    {
        Map<String, String> digestsByFileUrl = new HashMap<>(sourceFiles.size());
        for (TestFile testFile : sourceFiles)
        {
            testFile.createFile(instanceMetadata);
            digestsByFileUrl.put(testFile.getFileUrl(),
                                 testFile.digest(instanceMetadata, () -> getDigestAlgorithm(injector, request)));
        }

        SidecarClient sidecarClient = injector.getInstance(SidecarClient.class);
        when(sidecarClient.liveMigrationFileDigestAsync(eq(new SidecarInstanceImpl(SOURCE, 9043)),
                                                        anyString(),
                                                        eq(request.digestAlgorithm())))
        .thenAnswer(invocationOnMock -> {
            String fileUrl = invocationOnMock.getArgument(1);
            String digest = digestsByFileUrl.get(fileUrl);

            return Future.succeededFuture(new DigestResponse(digest, request.digestAlgorithm()))
                         .toCompletionStage().toCompletableFuture();
        });
    }

    DigestAlgorithm getDigestAlgorithm(Injector injector,
                                       LiveMigrationFilesVerificationRequest request)
    {
        DigestAlgorithmFactory digestAlgorithmFactory = injector.getInstance(DigestAlgorithmFactory.class);
        return digestAlgorithmFactory.getDigestAlgorithm(request.digestAlgorithm(), 0);
    }

    TestFile updateLastModifiedTime(TestFile file, long lastModifiedTime)
    {
        return new TestFile(file.dirType, file.dirIndex, file.relativePath, file.size, lastModifiedTime);
    }

    TestFile updateSize(TestFile testFile, int size)
    {
        return new TestFile(testFile.dirType, testFile.dirIndex, testFile.relativePath, size, testFile.lastModifiedTime);
    }

    TestFile convertToDirectory(TestFile testFile)
    {
        return new TestFile(testFile.dirType, testFile.dirIndex, testFile.relativePath, -1, testFile.lastModifiedTime);
    }

    TestFile getRandomFile(List<TestFile> testFiles)
    {
        //noinspection OptionalGetWithoutIsPresent
        return testFiles.stream().filter(testFile -> testFile.getFileType() == FileType.FILE).findFirst().get();
    }

    /**
     * When a test file is created, it is filled with random data.
     * Deleting and re-creating file to update the contents of the file.
     *
     * @param testFile test file to re-create
     */
    void recreateFile(TestFile testFile, InstanceMetadata instanceMetadata) throws IOException
    {
        if (testFile.getFileType() == FileType.DIRECTORY)
        {
            // do nothing for directory
            return;
        }
        testFile.deleteFile(instanceMetadata);
        testFile.createFile(instanceMetadata);
    }

    int filesCount(List<TestFile> testFiles)
    {
        return (int) testFiles.stream().filter(testFile -> testFile.getFileType() == FileType.FILE)
                              .count();
    }

    int dirsCount(List<TestFile> testFiles)
    {
        return testFiles.size() - filesCount(testFiles);
    }

    int emptyFilesCount(List<TestFile> testFiles)
    {
        return (int) testFiles.stream().filter(testFile -> testFile.size == 0).count();
    }

    private static class TestModule extends AbstractModule
    {
        @Override
        protected void configure()
        {
            bind(Vertx.class).toInstance(Vertx.vertx());
            bind(DigestVerifierFactory.class);
            bind(SidecarClient.class).toInstance(mock(SidecarClient.class));
            install(new DigestAlgorithmProviderTestModule());
        }
    }
}
