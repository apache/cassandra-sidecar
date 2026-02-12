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
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileProps;
import io.vertx.core.file.FileSystemException;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.request.LiveMigrationDataCopyRequest;
import org.apache.cassandra.sidecar.common.response.GossipInfoResponse;
import org.apache.cassandra.sidecar.common.response.InstanceFileInfo;
import org.apache.cassandra.sidecar.common.response.InstanceFileInfo.FileType;
import org.apache.cassandra.sidecar.common.response.InstanceFilesListResponse;
import org.apache.cassandra.sidecar.common.response.LiveMigrationStatus.MigrationState;
import org.apache.cassandra.sidecar.concurrent.AsyncConcurrentTaskExecutor;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationInvalidRequestException;
import org.apache.cassandra.sidecar.livemigration.OperationStatus.State;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.localPath;


/**
 * Class responsible for downloading required files from source only once. It compares files in destination/current
 * instance with source instance, and tries to maintain same set of files.
 */
class LiveMigrationFileDownloader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationFileDownloader.class);
    private final Vertx vertx;
    private final LiveMigrationDataCopyRequest request;
    private final int iteration;
    private final Consumer<OperationStatus> statusUpdater;
    private final InstanceMetadata instanceMetadata;
    private final LiveMigrationConfiguration liveMigrationConfiguration;
    private final InstancesMetadata instancesMetadata;
    private final SidecarClient sidecarClient;
    private final String id;
    private final String source;
    private final int port;
    private final String logPrefix;
    private final ExecutorPools executorPools;
    private OperationStatus operationStatus;
    private AsyncConcurrentTaskExecutor<Void> concurrentTaskExecutor;

    protected LiveMigrationFileDownloader(Builder builder)
    {
        this.vertx = builder.vertx;
        this.sidecarClient = builder.sidecarClient;
        this.request = builder.request;
        this.iteration = builder.iteration;
        this.statusUpdater = builder.statusUpdater;
        this.instanceMetadata = builder.instanceMetadata;
        this.liveMigrationConfiguration = builder.liveMigrationConfiguration;
        this.instancesMetadata = builder.instancesMetadata;
        this.id = builder.id;
        this.source = builder.source;
        this.port = builder.port;
        this.executorPools = builder.executorPools;

        this.operationStatus = OperationStatus.startingState();
        this.logPrefix = String.format("liveMigrationRequest=%s iteration=%s ", id, iteration);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Responsible for fetching list of files to download, do the cleanup of unwanted files, and download
     * required files from source.
     *
     * @return Future of operationStatus. Running progress will be updated to {@link #statusUpdater}
     */
    public Future<OperationStatus> downloadFiles()
    {
        return checkGossip()
               .compose(v -> checkLiveMigrationStatusOfSource())
               .compose(v -> fetchSourceFileList())
               .compose(this::cleanupUnnecessaryFiles)
               .compose(this::prepareDownloadList)
               .compose(this::executeDownloadIfNeeded)
               .onSuccess(result -> LOGGER.info("{} Operation completed with status: {}", logPrefix, result))
               .otherwise(this::handleDownloadFailure);
    }

    /**
     * Validates via cluster gossip that destination has not been started.
     * This is a best-effort safety check performed before each file download iteration.
     * <p>
     * Fetches gossip info from cluster instances and checks:
     * <ul>
     *   <li>Source node is present in gossip</li>
     *   <li>Destination node is NOT present in gossip</li>
     * </ul>
     * <p>
     * <strong>Note:</strong> This cannot guarantee destination won't start during data copy.
     * Operators must ensure destination is not started until migration completes.
     *
     * @return Future that succeeds if validation passes, fails with
     *         {@link LiveMigrationInvalidRequestException} if unsafe condition detected
     */
    private Future<Void> checkGossip()
    {
        // Skip gossip check if explicitly requested
        if (request.skipGossipCheck != null && request.skipGossipCheck)
        {
            LOGGER.warn("{} Skipping gossip safety check as requested. " +
                        "This bypasses validation that destination has not been started.", logPrefix);
            return Future.succeededFuture();
        }

        String destination = instanceMetadata.host();
        LOGGER.debug("{} Validating gossip state: source={}, destination={}", logPrefix, source, destination);

        // Apply configuration defaults for optional gossip fetch parameters if not specified by client
        int batchSize = request.gossipFetchBatchSize != null
                        ? request.gossipFetchBatchSize
                        : liveMigrationConfiguration.gossipFetchBatchSize();
        int maxRetries = request.gossipFetchMaxRetries != null
                         ? request.gossipFetchMaxRetries
                         : liveMigrationConfiguration.gossipFetchMaxRetries();

        GossipInfoFetcher gossipFetcher = new GossipInfoFetcher(
        sidecarClient,
        instancesMetadata,
        port,
        batchSize,
        maxRetries);

        return gossipFetcher.fetchGossipInfo()
                            .compose(gossipResponse -> {
                                try
                                {
                                    return validateGossipResponse(gossipResponse, source, destination);
                                }
                                catch (UnknownHostException e)
                                {
                                    return Future.failedFuture(e);
                                }
                            })
                            .onSuccess(v -> LOGGER.debug("{} Gossip validation passed", logPrefix))
                            .onFailure(err -> LOGGER.error("{} Gossip validation failed: {}", logPrefix, err.getMessage()));
    }

    /**
     * Validates the gossip response to ensure source is present in gossip and destination is not present.
     *
     * @param gossipResponse the gossip information from a healthy node
     * @param source         the source instance to check for
     * @param destination    the destination instance to check for
     * @return Future that succeeds if source is found and destination not found,
     * fails if source is not found or destination is found in gossip
     */
    private Future<Void> validateGossipResponse(GossipInfoResponse gossipResponse,
                                                String source,
                                                String destination) throws UnknownHostException
    {
        GossipInfoResponse.GossipInfo srcInfo = findGossipInfo(gossipResponse, source);
        if (srcInfo == null)
        {
            String errorMsg = "SAFETY CHECK FAILED: Source node '" + source + "' not found in cluster gossip. " +
                              "Cannot proceed with data copy. Please fix the source/cluster " +
                              "and then re-trigger data copy task if required";
            LOGGER.error("{} {}", logPrefix, errorMsg);
            return Future.failedFuture(new LiveMigrationInvalidRequestException(errorMsg));
        }

        GossipInfoResponse.GossipInfo destInfo = findGossipInfo(gossipResponse, destination);
        if (destInfo != null)
        {
            // Destination found in gossip - it was started!
            String status = destInfo.statusWithPort();
            String errorMsg = String.format(
            "SAFETY CHECK FAILED: Destination node '%s' found in cluster gossip with status '%s'. " +
            "This indicates Cassandra was previously started on the destination. " +
            "Data copy would overwrite potentially newer data, causing DATA LOSS. Aborting.",
            destination, status);

            LOGGER.error("{} {}", logPrefix, errorMsg);
            return Future.failedFuture(new LiveMigrationInvalidRequestException(errorMsg));
        }

        // Reached here means, source found and destination not found in gossip - safe to proceed
        LOGGER.info("{} Gossip validation passed: destination {} not found in cluster gossip",
                    logPrefix, destination);
        return Future.succeededFuture();
    }

    private GossipInfoResponse.GossipInfo findGossipInfo(GossipInfoResponse gossipResponse,
                                                         String instance) throws UnknownHostException
    {
        InstanceMetadata metadata = instancesMetadata.instanceFromHost(instance);
        try
        {
            // InstanceMetadata may not have ip address populated. Calling 'refreshIpAddress' to ensure
            // ip address is available.
            metadata.refreshIpAddress();
        }
        catch (UnknownHostException e)
        {
            LOGGER.error("{} Failed to resolve ipAddress for instance {}", logPrefix, instance, e);
            throw e;
        }
        String ipAddress = metadata.ipAddress();
        // Composed Gossip info key based on javadoc from:
        // org.apache.cassandra.sidecar.common.response.GossipInfoResponse.get
        String gossipInfoKey = "/" + ipAddress + ":" + metadata.storagePort();
        return gossipResponse.get(gossipInfoKey);
    }

    /**
     * Checks whether the live migration status at the source is NOT_COMPLETED or COMPLETED.
     * This check helps a corner where destination is not aware that live migration was
     * already completed and trying to download the files again.
     *
     * @return a {@link Future} which succeeds when live migration at source is not marked
     * as completed, a failed Future otherwise.
     */
    private Future<Void> checkLiveMigrationStatusOfSource()
    {
        return Future.fromCompletionStage(
                     sidecarClient.liveMigrationStatus(new SidecarInstanceImpl(source, port)))
                     .compose(sourceLiveMigrationStatus -> {
                         if (sourceLiveMigrationStatus.state() == MigrationState.NOT_COMPLETED)
                         {
                             return Future.succeededFuture();
                         }
                         else
                         {
                             return Future.failedFuture(
                             new LiveMigrationInvalidRequestException("Live migration has completed at source."));
                         }
                     });
    }

    private Future<InstanceFilesListResponse> fetchSourceFileList()
    {
        return Future.fromCompletionStage(
                     sidecarClient.liveMigrationListInstanceFilesAsync(new SidecarInstanceImpl(source, port)))
                     .onFailure(cause ->
                                LOGGER.error("{} Failed to obtain list of files from source {}",
                                             logPrefix, source + ':' + port, cause)
                     );
    }

    private Future<InstanceFilesListResponse> cleanupUnnecessaryFiles(InstanceFilesListResponse response)
    {
        updateState(status -> status.toCleaningState(response.getTotalSize(), response.getFiles().size()));
        return executorPools.internal().executeBlocking(() -> this.deleteUnnecessaryFilesAndDirectories(response));
    }

    private Future<List<InstanceFileInfo>> prepareDownloadList(InstanceFilesListResponse response)
    {
        updateState(OperationStatus::toPreparingState);
        return this.shortlistDownloadFiles(response, request.successThreshold);
    }

    private Future<OperationStatus> executeDownloadIfNeeded(List<InstanceFileInfo> instanceFiles)
    {
        if (instanceFiles.isEmpty())
        {
            updateState(OperationStatus::toSuccessState);
            return Future.succeededFuture(operationStatus);
        }

        if (iteration >= request.maxIterations)
        {
            updateState(OperationStatus::tryFailureState);
            LOGGER.warn("{} Retries exhausted to download files. Failing the task.", logPrefix);
            return Future.succeededFuture(operationStatus);
        }

        return performDownload(instanceFiles);
    }

    private Future<OperationStatus> performDownload(List<InstanceFileInfo> instanceFiles)
    {
        long downloadSize = calculateDownloadSize(instanceFiles);
        LOGGER.info("{} Downloading {} files from {}:{}, download size: {}",
                    logPrefix, instanceFiles.size(), source, port, downloadSize);

        updateState(status -> status.toDownloadingState(downloadSize, instanceFiles.size()));
        return sortBySizeAndDownload(instanceFiles);
    }

    private OperationStatus handleDownloadFailure(Throwable cause)
    {
        LOGGER.error("{} Operation FAILED.", logPrefix);

        return updateState(OperationStatus::tryFailureState);
    }

    @VisibleForTesting
    long calculateDownloadSize(List<InstanceFileInfo> instanceFiles)
    {
        return instanceFiles.stream()
                            .filter(info -> info.fileType.equals(FileType.FILE))
                            .mapToLong(info -> info.size)
                            .sum();
    }

    OperationStatus updateState(Function<OperationStatus, OperationStatus> changeStatusFunction)
    {
        operationStatus = changeStatusFunction.apply(operationStatus);
        statusUpdater.accept(operationStatus);
        return operationStatus;
    }

    public void cancel()
    {
        LOGGER.info("{} Operation cancelled explicitly.", logPrefix);
        updateState(OperationStatus::cancel);
        if (concurrentTaskExecutor != null)
        {
            concurrentTaskExecutor.cancelTasks();
        }
    }

    /**
     * Deletes these two types of files:
     * 1. Files/Directories which are present in local but not in source
     * 2. Files with same name exists but size or last modified timestamp doesn't match
     *
     * @param instanceFilesListResponse list of files response received from source
     * @return input param {@code instanceFilesListResponse} as a Future
     */
    @VisibleForTesting
    InstanceFilesListResponse deleteUnnecessaryFilesAndDirectories(InstanceFilesListResponse instanceFilesListResponse) throws IOException
    {
        CassandraInstanceFiles cassandraInstanceFilesList = new CassandraInstanceFilesImpl(instanceMetadata,
                                                                                           liveMigrationConfiguration);
        List<DirVisitor> dirFileVisitorMap = cassandraInstanceFilesList.dirVisitorList();
        Map<String, FileAttributes> sourceFileAttributes = buildSourceFileAttributesMap(instanceFilesListResponse);

        for (DirVisitor dirToVisit : dirFileVisitorMap)
        {
            cleanupDirectory(sourceFileAttributes, dirToVisit);
        }
        return instanceFilesListResponse;
    }

    private Map<String, FileAttributes> buildSourceFileAttributesMap(InstanceFilesListResponse instanceFilesListResponse)
    {
        return instanceFilesListResponse.getFiles()
                                        .stream()
                                        .collect(Collectors.toMap(
                                        fileInfo -> localPath(fileInfo.fileUrl, instanceMetadata).toString(),
                                        fileInfo -> new FileAttributes(fileInfo.size, fileInfo.lastModifiedTime)
                                        ));
    }

    private void cleanupDirectory(Map<String, FileAttributes> sourceFileAttributes, DirVisitor dirToVisit) throws IOException
    {
        Path dir = dirToVisit.homeDirPath;
        MigrationFileVisitor visitor = dirToVisit.fileVisitor;

        LOGGER.info("{} Visiting directory: {}", logPrefix, dir);
        Files.walkFileTree(dir, visitor);
        List<Path> filesToDelete = identifyFilesForDeletion(visitor, sourceFileAttributes, dir);

        LOGGER.info("{} Number of files to delete: {}. Files: {}", logPrefix, filesToDelete.size(), filesToDelete);
        performDeletion(filesToDelete);
    }

    private List<Path> identifyFilesForDeletion(MigrationFileVisitor visitor,
                                                Map<String, FileAttributes> sourceFileAttributes,
                                                Path dir)
    {
        return visitor.validFilePaths()
                      .stream()
                      .filter(localFile -> canDelete(sourceFileAttributes, localFile, dir))
                      .collect(Collectors.toList());
    }

    /**
     * Determines the given path is a valid candidate for deletion if:
     * 1. Files/Directories which are present in local but not in source
     * 2. Files with same name exists but size or last modified time doesn't match
     *
     * @param remoteFileAttrsMap map of remote instance file paths and their metadata (size, last modified time)
     * @param localPath          file path on the local instance
     * @param homeDir            file path of the data home directory
     * @return true if the input path is a valid candidate for deletion, otherwise false
     */
    @VisibleForTesting
    boolean canDelete(Map<String, FileAttributes> remoteFileAttrsMap,
                      Path localPath,
                      Path homeDir)
    {
        String absolutePath = localPath.toAbsolutePath().toString();
        if (homeDir.equals(localPath))
        {
            return false;
        }

        // This condition handles both files and directories
        if (!remoteFileAttrsMap.containsKey(absolutePath))
        {
            // Delete if the local file is not present at remote
            return true;
        }

        try
        {
            FileProps fileProps = vertx.fileSystem().propsBlocking(absolutePath);

            // We should not delete the entire directory based on timestamp because any file change can result in
            // changing the timestamp of the directory.
            if (!fileProps.isDirectory())
            {
                // The first condition includes a special case where time stamp changed but the file size did not change
                // (eg: just flipped some bits etc.)
                return fileProps.size() != remoteFileAttrsMap.get(absolutePath).size
                       || (fileProps.lastModifiedTime() != remoteFileAttrsMap.get(absolutePath).lastModifiedTime);
            }
        }
        catch (FileSystemException e)
        {
            LOGGER.error("{} Could not read properties for file={}", logPrefix, absolutePath, e);
            throw e;
        }
        return false;
    }

    private void performDeletion(List<Path> filesToDelete)
    {
        AtomicInteger filesDeleted = new AtomicInteger(0);
        AtomicInteger directoriesDeleted = new AtomicInteger(0);

        for (Path localPath : filesToDelete)
        {
            if (!Files.exists(localPath))
            {
                continue;
            }

            if (Files.isDirectory(localPath))
            {
                LOGGER.debug("{} Deleting unwanted directory {}", logPrefix, localPath.toAbsolutePath());
                vertx.fileSystem().deleteRecursiveBlocking(localPath.toAbsolutePath().toString(), true);

                directoriesDeleted.incrementAndGet();
            }
            else
            {
                LOGGER.debug("{} Deleting unwanted file: {}", logPrefix, localPath.toAbsolutePath());
                vertx.fileSystem().deleteBlocking(localPath.toString());
                filesDeleted.incrementAndGet();
            }
        }

        LOGGER.info("{} Deleted {} unwanted files and {} unwanted directories from local.",
                    logPrefix, filesDeleted.get(), directoriesDeleted.get());
    }

    /**
     * Shortlists list of files and directories to download from source.
     *
     * @param instanceFilesListResponse list of files response received from source.
     * @param successThreshold          success threshold to determine whether downloader should continue to download or not.
     * @return List of instance files to download. Returned list will be empty if successThreshold meets or no files remained to download.
     */
    @VisibleForTesting
    Future<List<InstanceFileInfo>> shortlistDownloadFiles(InstanceFilesListResponse instanceFilesListResponse,
                                                          double successThreshold)
    {
        List<InstanceFileInfo> filesToDownload =
        instanceFilesListResponse.getFiles().stream()
                                 .filter(instanceFileInfo -> !Files.exists(localPath(instanceFileInfo.fileUrl, instanceMetadata)))
                                 .collect(Collectors.toList());

        // filter out directories while calculating download size
        long downloadSize = filesToDownload.stream()
                                           .filter(instanceFileInfo -> instanceFileInfo.fileType.equals(FileType.FILE))
                                           .map(instanceFileInfo -> instanceFileInfo.size)
                                           .reduce(0L, Long::sum);

        if (downloadSize == 0)
        {
            // downloadSize of zero means that there are no files to download OR
            // only empty directories and/or empty files left to create.
            return Future.succeededFuture(filesToDownload);
        }

        long totalSize = instanceFilesListResponse.getTotalSize();

        // Calculate percentage of data required to download
        double toDownloadPercentage = ((totalSize - downloadSize) * 1.0) / totalSize;
        LOGGER.info("{} Remaining size to download={} bytes, download percentage={}, success threshold={}",
                    logPrefix, downloadSize, toDownloadPercentage, successThreshold);

        if (toDownloadPercentage <= successThreshold)
        {
            return Future.succeededFuture(filesToDownload);
        }

        LOGGER.info("{} Download size met the successThreshold({}). Skipping download {} files of size {}",
                    logPrefix, successThreshold, filesToDownload.size(), downloadSize);
        return Future.succeededFuture(Collections.emptyList());
    }

    /**
     * Sorting by size in descending order so that large files will be downloaded first.
     * Larger files less likely to change compared to small files.
     *
     * @param instanceFileInfoList list of {@link InstanceFileInfo}
     * @return future of {@link  OperationStatus}
     */
    @VisibleForTesting
    Future<OperationStatus> sortBySizeAndDownload(List<InstanceFileInfo> instanceFileInfoList)
    {
        List<Future<Void>> futureList = new ArrayList<>(instanceFileInfoList.size());

        //This will implicitly put directories at the end (because size is explicitly set to -1 for directories).
        instanceFileInfoList.sort(Collections.reverseOrder(Comparator.comparingLong(instanceFileInfo -> instanceFileInfo.size)));

        List<Callable<Future<Void>>> downloadTasks = new ArrayList<>();
        SidecarInstanceImpl instance = new SidecarInstanceImpl(source, port);

        for (InstanceFileInfo file : instanceFileInfoList)
        {
            if (file.fileType == FileType.DIRECTORY)
            {
                futureList.add(createDirectory(file));
            }
            else if (file.fileType == FileType.FILE && file.size == 0)
            {
                futureList.add(createEmptyFile(file));
            }
            else
            {
                Callable<Future<Void>> task = () -> getDownloadTask(instance, file);
                downloadTasks.add(task);
            }
        }

        // start the file downloads with a cap on concurrency
        concurrentTaskExecutor = new AsyncConcurrentTaskExecutor<>(vertx, downloadTasks, request.maxConcurrency);
        List<Future<Void>> taskFutures = concurrentTaskExecutor.start();
        futureList.addAll(taskFutures);

        return Future.join(futureList)
                     .compose(res -> res.succeeded() ? Future.succeededFuture() : Future.failedFuture(res.cause()))
                     .compose(ar -> Future.succeededFuture(this.updateState(OperationStatus::toDownloadCompleteState)),
                              cause -> {
                                  LOGGER.error("{} Failed to download files.  Updating state to {}", logPrefix, State.FAILED, cause);
                                  return Future.succeededFuture(this.updateState(OperationStatus::tryFailureState));
                              });
    }

    @VisibleForTesting
    Future<Void> createDirectory(InstanceFileInfo file)
    {
        return localPathAsync(file.fileUrl, instanceMetadata)
               .compose(path -> vertx.fileSystem().mkdirs(path.toString()))
               .onSuccess(v -> operationStatus.incrementFilesDownloaded())
               .onFailure(e -> {
                   LOGGER.error("{} Could not create directory for url={} ", logPrefix, file.fileUrl, e);
                   operationStatus.incrementDownloadFailures();
               });
    }

    /**
     * Creates an empty file (size = 0) for given remote {@link InstanceFileInfo}.
     *
     * @param file File info of remote.
     * @return Returns {@link Optional#empty()} if file gets created successfully,
     * otherwise returns an Optional of exception
     */
    @VisibleForTesting
    Future<Void> createEmptyFile(InstanceFileInfo file)
    {
        return localPathAsync(file.fileUrl, instanceMetadata)
               .compose(path -> {
                   if (path.getParent() == null)
                   {
                       return Future.failedFuture("Parent directory for path " + path + " is null." +
                                                  " Cannot create empty file.");
                   }
                   return vertx.fileSystem().mkdirs(path.getParent().toAbsolutePath().toString())
                               .compose(v -> vertx.fileSystem().createFile(path.toString()))
                               .compose(v -> updateFileTimestampAsync(path, file.lastModifiedTime));
               })
               .onSuccess(v -> operationStatus.incrementFilesDownloaded())
               .onFailure(e -> {
                   operationStatus.incrementDownloadFailures();
                   LOGGER.error("{} Could not create empty file for url={}", logPrefix, file.fileUrl, e);
               });
    }

    @VisibleForTesting
    Future<Void> updateFileTimestampAsync(Path filePath, long lastModifiedTime)
    {
        try
        {
            Files.setLastModifiedTime(filePath, FileTime.fromMillis(lastModifiedTime));
            return Future.succeededFuture();
        }
        catch (IOException e)
        {
            return Future.failedFuture(e);
        }
    }

    @VisibleForTesting
    Future<Void> getDownloadTask(SidecarInstanceImpl instance,
                                 InstanceFileInfo file)
    {
        return localPathAsync(file.fileUrl, instanceMetadata)
               .compose(path -> Future.fromCompletionStage(sidecarClient.liveMigrationStreamFileAsync(instance, file.fileUrl, path.toString()))
                                      .compose(success -> updateFileTimestampAsync(path, file.lastModifiedTime)))
               .onSuccess(v -> {
                   operationStatus.incrementFilesDownloaded();
                   operationStatus.addBytesDownloaded(file.size);
               })
               .onFailure(cause -> {
                   operationStatus.incrementDownloadFailures();
                   LOGGER.warn("{} Download failed with an exception: ", logPrefix, cause);
               });
    }


    @VisibleForTesting
    Future<Path> localPathAsync(@NotNull String fileUrl,
                                @NotNull InstanceMetadata metadata)
    {
        return Future.future(promise -> promise.complete(localPath(fileUrl, metadata)));
    }

    @VisibleForTesting
    InstanceMetadata instanceMetadata()
    {
        return instanceMetadata;
    }

    @VisibleForTesting
    OperationStatus operationStatus()
    {
        return operationStatus;
    }

    /**
     * {@code LiveMigrationFileDownloader} builder static inner class.
     */
    static class Builder implements DataObjectBuilder<Builder, LiveMigrationFileDownloader>
    {
        public ExecutorPools executorPools;
        private Vertx vertx;
        private SidecarClient sidecarClient;
        private LiveMigrationDataCopyRequest request;
        private int iteration;
        private Consumer<OperationStatus> statusUpdater;
        private InstanceMetadata instanceMetadata;
        private LiveMigrationConfiguration liveMigrationConfiguration;
        private InstancesMetadata instancesMetadata;
        private String id;
        private String source;
        private int port;

        protected Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code vertx} and returns a reference to this Builder enabling method chaining.
         *
         * @param vertx the {@code vertx} to set
         * @return a reference to this Builder
         */
        public Builder vertx(Vertx vertx)
        {
            return update(b -> b.vertx = vertx);
        }

        /**
         * Sets the {@code sidecarClient} and returns a reference to this Builder enabling method chaining.
         *
         * @param sidecarClient the {@code sidecarClient} to set
         * @return a reference to this Builder
         */
        public Builder sidecarClient(SidecarClient sidecarClient)
        {
            return update(b -> b.sidecarClient = sidecarClient);
        }

        /**
         * Sets the {@code request} and returns a reference to this Builder enabling method chaining.
         *
         * @param request the {@code request} to set
         * @return a reference to this Builder
         */
        public Builder request(LiveMigrationDataCopyRequest request)
        {
            return update(b -> b.request = request);
        }

        /**
         * Sets the {@code iteration} and returns a reference to this Builder enabling method chaining.
         *
         * @param iteration the {@code iteration} to set
         * @return a reference to this Builder
         */
        public Builder iteration(int iteration)
        {
            return update(b -> b.iteration = iteration);
        }

        /**
         * Sets the {@code statusUpdater} and returns a reference to this Builder enabling method chaining.
         *
         * @param statusUpdater the {@code statusUpdater} to set
         * @return a reference to this Builder
         */
        public Builder statusUpdater(Consumer<OperationStatus> statusUpdater)
        {
            return update(b -> b.statusUpdater = statusUpdater);
        }

        /**
         * Sets the {@code instanceMetadata} and returns a reference to this Builder enabling method chaining.
         *
         * @param instanceMetadata the {@code instanceMetadata} to set
         * @return a reference to this Builder
         */
        public Builder instanceMetadata(InstanceMetadata instanceMetadata)
        {
            return update(b -> b.instanceMetadata = instanceMetadata);
        }

        /**
         * Sets the {@code liveMigrationConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param liveMigrationConfiguration the {@code liveMigrationConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder liveMigrationConfiguration(LiveMigrationConfiguration liveMigrationConfiguration)
        {
            return update(b -> b.liveMigrationConfiguration = liveMigrationConfiguration);
        }

        /**
         * Sets the {@code instancesMetadata} and returns a reference to this Builder enabling method chaining.
         *
         * @param instancesMetadata the {@code instancesMetadata} to set
         * @return a reference to this Builder
         */
        public Builder instancesMetadata(InstancesMetadata instancesMetadata)
        {
            return update(b -> b.instancesMetadata = instancesMetadata);
        }

        /**
         * Sets the {@code id} and returns a reference to this Builder enabling method chaining.
         *
         * @param id the {@code id} to set
         * @return a reference to this Builder
         */
        public Builder id(String id)
        {
            return update(b -> b.id = id);
        }

        /**
         * Sets the {@code source} and returns a reference to this Builder enabling method chaining.
         *
         * @param source the {@code source} to set
         * @return a reference to this Builder
         */
        public Builder source(String source)
        {
            return update(b -> b.source = source);
        }

        /**
         * Sets the {@code port} and returns a reference to this Builder enabling method chaining.
         *
         * @param port the {@code port} to set
         * @return a reference to this Builder
         */
        public Builder port(int port)
        {
            return update(b -> b.port = port);
        }

        /**
         * Sets the {@code executorPools} and returns a reference to this Builder enabling method chaining.
         *
         * @param executorPools the {@code executorPools} to set
         * @return a reference to this Builder
         */
        public Builder executorPools(ExecutorPools executorPools)
        {
            return update(b -> b.executorPools = executorPools);
        }

        /**
         * Returns a {@code LiveMigrationFileDownloader} built from the parameters previously set.
         *
         * @return a {@code LiveMigrationFileDownloader} built with parameters of this
         * {@code LiveMigrationFileDownloader.Builder}
         */
        @Override
        public LiveMigrationFileDownloader build()
        {
            Objects.requireNonNull(vertx);
            Objects.requireNonNull(sidecarClient);
            Objects.requireNonNull(statusUpdater);
            Objects.requireNonNull(liveMigrationConfiguration);
            Objects.requireNonNull(instancesMetadata);
            Objects.requireNonNull(id);
            Objects.requireNonNull(request);
            Objects.requireNonNull(source);
            Objects.requireNonNull(executorPools);

            return new LiveMigrationFileDownloader(this);
        }
    }

    /**
     * A simple container class to hold attributes of a file in the live migration process.
     * This class stores essential file metadata needed during file operations.
     */
    static class FileAttributes
    {
        public final long size;

        // the latest timestamp at which the file/dir was modified represented in milliseconds.
        public final long lastModifiedTime;


        public FileAttributes(long size, long lastModifiedTime)
        {
            this.size = size;
            this.lastModifiedTime = lastModifiedTime;
        }
    }
}
