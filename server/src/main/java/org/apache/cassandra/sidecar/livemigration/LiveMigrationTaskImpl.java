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
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.request.LiveMigrationDataCopyRequest;
import org.apache.cassandra.sidecar.common.response.LiveMigrationTaskResponse;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.utils.SidecarClientProvider;

/**
 * Implementation of live migration task that handles file downloading with retry logic.
 * Manages the lifecycle of data copy operations from source to destination instances.
 */
public class LiveMigrationTaskImpl implements LiveMigrationTask
{
    private final String id;
    private final LiveMigrationDataCopyRequest request;
    private final Map<Integer, OperationStatus> statusMap = new TreeMap<>();
    private final InstanceMetadata instanceMetadata;
    private final String source;
    private final int port;
    private final Vertx vertx;
    private final ExecutorPools executorPools;
    private final SidecarClientProvider sidecarClientProvider;
    private final LiveMigrationConfiguration liveMigrationConfiguration;
    private final LiveMigrationFileDownloadPreCheck preCheck;

    // Indicates overall status of the operation (succeeded or failed).
    // Future returned by downloader changes on next iteration. Using a separate future to track overall operation.
    Promise<OperationStatus> promise;

    @VisibleForTesting
    volatile LiveMigrationFileDownloader downloader;
    private volatile boolean cancelled = false;

    public LiveMigrationTaskImpl(Vertx vertx,
                                 ExecutorPools executorPools,
                                 SidecarClientProvider sidecarClientProvider,
                                 LiveMigrationConfiguration liveMigrationConfiguration,
                                 String id,
                                 LiveMigrationDataCopyRequest request,
                                 String source,
                                 int port,
                                 InstanceMetadata instanceMetadata, LiveMigrationFileDownloadPreCheck preCheck)
    {
        this.vertx = vertx;
        this.executorPools = executorPools;
        this.sidecarClientProvider = sidecarClientProvider;
        this.liveMigrationConfiguration = liveMigrationConfiguration;
        this.id = id;
        this.request = request;
        this.instanceMetadata = instanceMetadata;
        this.source = source;
        this.port = port;
        this.preCheck = preCheck;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String id()
    {
        return id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCompleted()
    {
        return cancelled || (promise != null && promise.future().isComplete());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        promise = Promise.promise();
        int iteration = 0;
        handleRetry(startInternal(iteration), iteration, request.maxIterations, promise);
    }

    Future<OperationStatus> startInternal(int iteration)
    {
        if (cancelled)
        {
            return Future.failedFuture("Data copy task got cancelled.");
        }

        downloader = LiveMigrationFileDownloader.builder()
                                                .id(id)
                                                .vertx(vertx)
                                                .sidecarClient(sidecarClientProvider.get())
                                                .request(request)
                                                .iteration(iteration)
                                                .statusUpdater(this.statusUpdater(iteration))
                                                .instanceMetadata(instanceMetadata)
                                                .liveMigrationConfiguration(liveMigrationConfiguration)
                                                .source(source)
                                                .port(port)
                                                .executorPools(executorPools)
                                                .preCheck(preCheck)
                                                .build();
        return downloader.downloadFiles();
    }

    void handleRetry(Future<OperationStatus> future, int iteration, int maxAttempts, Promise<OperationStatus> operationStatusPromise)
    {
        future.onComplete(ar -> {
            if (ar.succeeded() && ar.result().state() == OperationStatus.State.SUCCESS)
            {
                operationStatusPromise.complete(ar.result());
            }
            else
            {
                if (iteration == maxAttempts || cancelled)
                {
                    operationStatusPromise.fail("Max attempts exceeded or cancelled");
                }
                else
                {
                    int attempt = iteration + 1;
                    handleRetry(startInternal(attempt), attempt, maxAttempts, operationStatusPromise);
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cancel()
    {
        if (cancelled)
            return;

        cancelled = true;
        if (null != promise)
        {
            promise.tryFail("Operation cancelled");
        }

        if (null != downloader)
        {
            downloader.cancel();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LiveMigrationTaskResponse getResponse()
    {
        return new LiveMigrationTaskResponse(id, source, port, request, getStatusResponse());
    }

    Consumer<OperationStatus> statusUpdater(int iteration)
    {
        return (operationStatus) -> statusMap.put(iteration, operationStatus);
    }

    private List<LiveMigrationTaskResponse.Status> getStatusResponse()
    {
        return statusMap.entrySet().stream()
                        .map(entry -> toStatusResponse(entry.getValue(), entry.getKey()))
                        .collect(Collectors.toList());
    }

    private LiveMigrationTaskResponse.Status toStatusResponse(OperationStatus operationStatus, int iteration)
    {
        return new LiveMigrationTaskResponse.Status(iteration,
                                                    operationStatus.state().toString(),
                                                    operationStatus.totalSize(),
                                                    operationStatus.totalFiles(),
                                                    operationStatus.bytesToDownload(),
                                                    operationStatus.filesToDownload(),
                                                    operationStatus.filesDownloaded(),
                                                    operationStatus.downloadFailures(),
                                                    operationStatus.bytesDownloaded());
    }
}
