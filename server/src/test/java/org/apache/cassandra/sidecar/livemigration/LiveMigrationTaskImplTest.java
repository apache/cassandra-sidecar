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

import java.util.List;

import org.junit.jupiter.api.Test;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.ExecutorPoolsHelper;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.request.LiveMigrationDataCopyRequest;
import org.apache.cassandra.sidecar.common.response.LiveMigrationTaskResponse;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.utils.SidecarClientProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LiveMigrationTaskImplTest
{
    private static final String SOURCE = "127.0.0.1";
    private static final int PORT = 9043;

    private LiveMigrationTaskImpl createTask()
    {
        return createTask("test-task-id", new LiveMigrationDataCopyRequest(5, 0.8, 10));
    }

    private LiveMigrationTaskImpl createTask(String id, LiveMigrationDataCopyRequest request)
    {
        Vertx vertx = mock(Vertx.class);
        SidecarClientProvider sidecarClientProvider = mock(SidecarClientProvider.class);
        SidecarClient sidecarClient = mock(SidecarClient.class);
        LiveMigrationConfiguration liveMigrationConfiguration = mock(LiveMigrationConfiguration.class);
        InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);

        when(sidecarClientProvider.get()).thenReturn(sidecarClient);

        ExecutorPools executorPools = ExecutorPoolsHelper.createdSharedTestPool(vertx);

        return new LiveMigrationTaskImpl(vertx, executorPools, sidecarClientProvider, liveMigrationConfiguration,
                                         id, request, SOURCE, PORT, instanceMetadata);
    }

    @Test
    void testCancelWithoutPromiseOrDownloader()
    {
        LiveMigrationTaskImpl task = createTask();

        task.cancel();

        assertThat(task.isCompleted()).isTrue();
    }

    @Test
    void testCancelWithPromise()
    {
        LiveMigrationTaskImpl task = createTask();
        task.promise = Promise.promise();

        task.cancel();

        assertThat(task.isCompleted()).isTrue();
        assertThat(task.promise.future().failed()).isTrue();
        assertThat(task.promise.future().cause().getMessage()).isEqualTo("Operation cancelled");
    }

    @Test
    void testCancelWithDownloader()
    {
        LiveMigrationTaskImpl task = createTask();
        LiveMigrationFileDownloader mockDownloader = mock(LiveMigrationFileDownloader.class);
        task.downloader = mockDownloader;

        task.cancel();

        assertThat(task.isCompleted()).isTrue();
        verify(mockDownloader).cancel();
    }

    @Test
    void testCancelAlreadyCancelled()
    {
        LiveMigrationTaskImpl task = createTask();
        LiveMigrationFileDownloader mockDownloader = mock(LiveMigrationFileDownloader.class);
        task.downloader = mockDownloader;

        task.cancel();
        task.cancel();

        verify(mockDownloader, times(1)).cancel();
    }

    @Test
    void testGetResponseEmptyStatusMap()
    {
        LiveMigrationTaskImpl task = createTask();

        LiveMigrationTaskResponse response = task.getResponse();

        assertThat(response).isNotNull();
        assertThat(response.taskId()).isEqualTo("test-task-id");
        assertThat(response.source()).isEqualTo(SOURCE);
        assertThat(response.port()).isEqualTo(PORT);
        assertThat(response.status()).isEmpty();
    }

    @Test
    void testGetResponseWithOperationStatusUpdates()
    {
        LiveMigrationTaskImpl task = createTask();

        // Create actual OperationStatus objects using state transitions
        OperationStatus status1 = OperationStatus.startingState()
                                                 .toCleaningState(1000L, 10)
                                                 .toPreparingState()
                                                 .toDownloadingState(500L, 5)
                                                 // Simulate progress updates
                                                 .incrementFilesDownloaded()
                                                 .incrementDownloadFailures()
                                                 .incrementDownloadFailures()
                                                 .addBytesDownloaded(300L);

        OperationStatus status2 = OperationStatus.startingState()
                                                 .toCleaningState(2000L, 20)
                                                 .toPreparingState()
                                                 .toDownloadingState(1000L, 10)
                                                 .incrementFilesDownloaded()
                                                 .addBytesDownloaded(1000L)
                                                 .toDownloadCompleteState();

        task.statusUpdater(0).accept(status1);
        task.statusUpdater(1).accept(status2);

        LiveMigrationTaskResponse response = task.getResponse();

        assertThat(response).isNotNull();
        assertThat(response.taskId()).isEqualTo("test-task-id");
        assertThat(response.source()).isEqualTo(SOURCE);
        assertThat(response.port()).isEqualTo(PORT);

        List<LiveMigrationTaskResponse.Status> statusList = response.status();
        assertThat(statusList).hasSize(2);

        LiveMigrationTaskResponse.Status responseStatus1 = statusList.get(0);
        assertThat(responseStatus1.iteration()).isEqualTo(0);
        assertThat(responseStatus1.state()).isEqualTo("DOWNLOADING");
        assertThat(responseStatus1.totalSize()).isEqualTo(1000L);
        assertThat(responseStatus1.totalFiles()).isEqualTo(10);
        assertThat(responseStatus1.bytesToDownload()).isEqualTo(500L);
        assertThat(responseStatus1.filesToDownload()).isEqualTo(5);
        assertThat(responseStatus1.filesDownloaded()).isEqualTo(1);
        assertThat(responseStatus1.downloadFailures()).isEqualTo(2);
        assertThat(responseStatus1.bytesDownloaded()).isEqualTo(300L);

        LiveMigrationTaskResponse.Status responseStatus2 = statusList.get(1);
        assertThat(responseStatus2.iteration()).isEqualTo(1);
        assertThat(responseStatus2.state()).isEqualTo("DOWNLOAD_COMPLETE");
        assertThat(responseStatus2.totalSize()).isEqualTo(2000L);
        assertThat(responseStatus2.totalFiles()).isEqualTo(20);
        assertThat(responseStatus2.bytesToDownload()).isEqualTo(1000L); // SUCCESS state doesn't set this
        assertThat(responseStatus2.filesToDownload()).isEqualTo(10); // SUCCESS state doesn't set this
        assertThat(responseStatus2.filesDownloaded()).isEqualTo(1);
        assertThat(responseStatus2.downloadFailures()).isEqualTo(0);
        assertThat(responseStatus2.bytesDownloaded()).isEqualTo(1000L);
    }

    @Test
    void testId()
    {
        LiveMigrationTaskImpl task = createTask();

        assertThat(task.id()).isEqualTo("test-task-id");
    }

    @Test
    void testIsCompletedNotStarted()
    {
        LiveMigrationTaskImpl task = createTask();

        assertThat(task.isCompleted()).isFalse();
    }

    @Test
    void testIsCompletedCancelled()
    {
        LiveMigrationTaskImpl task = createTask();
        task.cancel();

        assertThat(task.isCompleted()).isTrue();
    }

    @Test
    void testGetResponseWithMultipleStateTransitions()
    {
        LiveMigrationTaskImpl task = createTask();

        // Test with different state combinations
        OperationStatus failedStatus = OperationStatus.startingState()
                                                      .toCleaningState(500L, 5)
                                                      .toPreparingState()
                                                      .toDownloadingState(200L, 2)
                                                      .tryFailureState();

        OperationStatus cancelledStatus = OperationStatus.startingState()
                                                         .cancel();

        OperationStatus downloadCompleteStatus = OperationStatus.startingState()
                                                                .toCleaningState(1500L, 15)
                                                                .toPreparingState()
                                                                .toDownloadingState(750L, 8)
                                                                .toDownloadCompleteState();

        task.statusUpdater(0).accept(failedStatus);
        task.statusUpdater(1).accept(cancelledStatus);
        task.statusUpdater(2).accept(downloadCompleteStatus);

        LiveMigrationTaskResponse response = task.getResponse();
        List<LiveMigrationTaskResponse.Status> statusList = response.status();

        assertThat(statusList).hasSize(3);
        assertThat(statusList.get(0).state()).isEqualTo("FAILED");
        assertThat(statusList.get(1).state()).isEqualTo("CANCELLED");
        assertThat(statusList.get(2).state()).isEqualTo("DOWNLOAD_COMPLETE");
    }

    @Test
    void testGetResponseDownloadsInProgress()
    {
        LiveMigrationTaskImpl task = createTask();

        // Test with different state combinations
        OperationStatus downloadingState = OperationStatus.startingState()
                                                          .toCleaningState(500L, 5)
                                                          .toPreparingState()
                                                          .toDownloadingState(200L, 2);


        task.statusUpdater(0).accept(downloadingState);

        List<LiveMigrationTaskResponse.Status> statusList = task.getResponse().status();

        assertThat(statusList).hasSize(1);
        assertThat(statusList.get(0).state()).isEqualTo("DOWNLOADING");
        assertThat(statusList.get(0).bytesToDownload()).isEqualTo(downloadingState.bytesToDownload());
        assertThat(statusList.get(0).filesToDownload()).isEqualTo(downloadingState.filesToDownload());
        assertThat(statusList.get(0).filesDownloaded()).isEqualTo(downloadingState.filesDownloaded());
        assertThat(statusList.get(0).bytesDownloaded()).isEqualTo(downloadingState.bytesDownloaded());
        assertThat(statusList.get(0).downloadFailures()).isEqualTo(downloadingState.downloadFailures());


        // some files downloaded and some failed
        downloadingState.addBytesDownloaded(100);
        downloadingState.incrementFilesDownloaded();
        downloadingState.incrementFilesDownloaded();
        downloadingState.incrementDownloadFailures();

        statusList = task.getResponse().status();

        assertThat(statusList.get(0).bytesToDownload()).isEqualTo(downloadingState.bytesToDownload());
        assertThat(statusList.get(0).filesToDownload()).isEqualTo(downloadingState.filesToDownload());
        assertThat(statusList.get(0).filesDownloaded()).isEqualTo(downloadingState.filesDownloaded());
        assertThat(statusList.get(0).bytesDownloaded()).isEqualTo(downloadingState.bytesDownloaded());
        assertThat(statusList.get(0).downloadFailures()).isEqualTo(downloadingState.downloadFailures());
    }
}
