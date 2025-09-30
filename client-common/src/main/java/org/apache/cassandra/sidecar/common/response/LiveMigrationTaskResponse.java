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

package org.apache.cassandra.sidecar.common.response;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.request.LiveMigrationDataCopyRequest;
import org.jetbrains.annotations.NotNull;

/**
 * Represents response of Live Migration task.
 */
public class LiveMigrationTaskResponse
{

    private final String taskId;
    private final int maxIterations;
    private final double successThreshold;
    private final int maxConcurrency;
    private final List<Status> statusList;
    private final String source;
    private final int port;

    public LiveMigrationTaskResponse(String taskId,
                                     String source,
                                     int port,
                                     LiveMigrationDataCopyRequest request,
                                     List<Status> statusList)
    {
        this(taskId, source, port, request.maxIterations, request.successThreshold, request.maxConcurrency, statusList);
    }

    @JsonCreator
    public LiveMigrationTaskResponse(@JsonProperty("taskId") String taskId,
                                     @JsonProperty("source") String source,
                                     @JsonProperty("port") int port,
                                     @JsonProperty("maxIterations") int maxIterations,
                                     @JsonProperty("successThreshold") double successThreshold,
                                     @JsonProperty("maxConcurrency") int maxConcurrency,
                                     @JsonProperty("status") List<Status> statusList)
    {
        this.taskId = taskId;
        this.source = source;
        this.port = port;
        this.maxIterations = maxIterations;
        this.successThreshold = successThreshold;
        this.maxConcurrency = maxConcurrency;
        this.statusList = statusList;
    }

    @JsonProperty("taskId")
    public String taskId()
    {
        return taskId;
    }

    @JsonProperty("source")
    public String source()
    {
        return source;
    }

    @JsonProperty("port")
    public int port()
    {
        return port;
    }

    @JsonProperty("maxIterations")
    public int maxIterations()
    {
        return maxIterations;
    }

    @JsonProperty("maxConcurrency")
    public int maxConcurrency()
    {
        return maxConcurrency;
    }

    @JsonProperty("successThreshold")
    public double successThreshold()
    {
        return successThreshold;
    }

    @JsonProperty("status")
    public List<Status> status()
    {
        return statusList;
    }

    /**
     * Represents download status of a Live Migration data copy task per iteration. It provides
     * a snapshot of the operation's progress including file counts, sizes, and current state.
     *
     * <h2>State Information</h2>
     * <p>The {@code state} field represents the current phase of the operation:</p>
     * <ul>
     *   <li><b>STARTING</b> - Operation is initializing</li>
     *   <li><b>CLEANING</b> - Removing unnecessary files from destination</li>
     *   <li><b>PREPARING</b> - Analyzing files and determining what to download</li>
     *   <li><b>DOWNLOADING</b> - Actively downloading files from source</li>
     *   <li><b>DOWNLOAD_COMPLETE</b> - All downloads completed successfully</li>
     *   <li><b>SUCCESS</b> - Operation completed successfully</li>
     *   <li><b>FAILED</b> - Operation failed due to an error</li>
     *   <li><b>CANCELLED</b> - Operation was cancelled by user request</li>
     * </ul>
     *
     * <h2>Progress Metrics</h2>
     * <p>The class provides comprehensive progress information:</p>
     * <ul>
     *   <li><b>File counts:</b> total files at source, files to download, files completed</li>
     *   <li><b>Size metrics:</b> total data size, download size, bytes transferred</li>
     *   <li><b>Error tracking:</b> number of download failures encountered</li>
     * </ul>
     */
    public static class Status
    {
        /**
         * Data copy iteration number
         */
        private final int iteration;

        /**
         * Current state of the live migration operation
         */
        private final String state;

        /**
         * Total size of all files at the source in bytes
         */
        private final long totalSize;

        /**
         * Total number of files available at the source
         */
        private final int totalFiles;

        /**
         * Number of files that need to be downloaded
         */
        private final int filesToDownload;

        /**
         * Number of files successfully downloaded so far
         */
        private final int filesDownloaded;

        /**
         * Number of download operations that failed so far
         */
        private final int downloadFailures;

        /**
         * Total size of data that needs to be downloaded in bytes
         */
        private final long bytesToDownload;

        /**
         * Total number of bytes downloaded so far
         */
        private final long bytesDownloaded;

        /**
         * Creates a new Status instance.
         *
         * @param iteration        iteration number for this status
         * @param state            current state of the operation (must not be null)
         * @param totalSize        total size of all files at source in bytes
         * @param totalFiles       total number of files available at source
         * @param bytesToDownload  total size of data to be downloaded in bytes
         * @param filesToDownload  number of files that need to be downloaded
         * @param filesDownloaded  number of files successfully downloaded
         * @param downloadFailures number of download operations that failed
         * @param bytesDownloaded  total bytes downloaded so far
         */
        @JsonCreator
        public Status(@JsonProperty("iteration") int iteration,
                      @JsonProperty("state") @NotNull String state,
                      @JsonProperty("totalSize") long totalSize,
                      @JsonProperty("totalFiles") int totalFiles,
                      @JsonProperty("bytesToDownload") long bytesToDownload,
                      @JsonProperty("filesToDownload") int filesToDownload,
                      @JsonProperty("filesDownloaded") int filesDownloaded,
                      @JsonProperty("downloadFailures") int downloadFailures,
                      @JsonProperty("bytesDownloaded") long bytesDownloaded)
        {
            Objects.requireNonNull(state);
            this.iteration = iteration;
            this.state = state;
            this.totalSize = totalSize;
            this.totalFiles = totalFiles;
            this.filesToDownload = filesToDownload;
            this.filesDownloaded = filesDownloaded;
            this.downloadFailures = downloadFailures;
            this.bytesToDownload = bytesToDownload;
            this.bytesDownloaded = bytesDownloaded;
        }

        @JsonProperty("iteration")
        public int iteration()
        {
            return iteration;
        }

        /**
         * Returns the current state of the live migration operation.
         *
         * @return current operation state (STARTING, CLEANING, PREPARING, DOWNLOADING,
         * DOWNLOAD_COMPLETE, SUCCESS, FAILED, or CANCELLED)
         */
        @JsonProperty("state")
        public String state()
        {
            return state;
        }

        /**
         * Returns the total size of all files available at the source.
         *
         * @return total size in bytes
         */
        @JsonProperty("totalSize")
        public long totalSize()
        {
            return totalSize;
        }

        /**
         * Returns the total number of files available at the source.
         *
         * @return total file count
         */
        @JsonProperty("totalFiles")
        public int totalFiles()
        {
            return totalFiles;
        }

        /**
         * Returns the total size of data that needs to be downloaded.
         *
         * @return download size in bytes
         */
        @JsonProperty("bytesToDownload")
        public long bytesToDownload()
        {
            return bytesToDownload;
        }

        /**
         * Returns the number of files that need to be downloaded from the source.
         *
         * @return files to download count
         */
        @JsonProperty("filesToDownload")
        public int filesToDownload()
        {
            return filesToDownload;
        }

        /**
         * Returns the number of files that have been successfully downloaded.
         *
         * @return successfully downloaded files count
         */
        @JsonProperty("filesDownloaded")
        public int filesDownloaded()
        {
            return filesDownloaded;
        }

        /**
         * Returns the number of download operations that have failed.
         *
         * @return failed download count
         */
        @JsonProperty("downloadFailures")
        public int downloadFailures()
        {
            return downloadFailures;
        }

        /**
         * Returns the total number of bytes that have been downloaded so far.
         *
         * @return bytes downloaded
         */
        @JsonProperty("bytesDownloaded")
        public long bytesDownloaded()
        {
            return bytesDownloaded;
        }

        @Override
        public String toString()
        {
            return "Status{" +
                   "iteration=" + iteration +
                   ", state='" + state + '\'' +
                   ", totalSize=" + totalSize +
                   ", totalFiles=" + totalFiles +
                   ", filesToDownload=" + filesToDownload +
                   ", filesDownloaded=" + filesDownloaded +
                   ", downloadFailures=" + downloadFailures +
                   ", bytesToDownload=" + bytesToDownload +
                   ", bytesDownloaded=" + bytesDownloaded +
                   '}';
        }
    }
}
