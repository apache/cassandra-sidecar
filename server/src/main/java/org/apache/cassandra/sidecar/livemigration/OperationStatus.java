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

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;

import org.jetbrains.annotations.NotNull;

/**
 * Represents the state and progress of a live migration data copy task using a state machine pattern.
 *
 * <h2>State Machine</h2>
 * The operation can follow these state transitions:
 * <pre>
 * STARTING -&gt; CLEANING -&gt; PREPARING -&gt; DOWNLOADING -&gt; DOWNLOAD_COMPLETE
 *    |          |           |            |
 *    |          |           |            |
 *    |          |           |            |
 *    |          |           |            +-&gt; CANCELLED / FAILED
 *    |          |           +-&gt; SUCCESS
 *    |          |
 *    |          +-&gt; CANCELLED / FAILED
 *    +-&gt; CANCELLED / FAILED
 *
 * Note: CANCELLED and FAILED states can be reached from any non-terminal state.
 *       DOWNLOAD_COMPLETE is a terminal state (no further transitions).
 *       SUCCESS can only be reached from PREPARING state.
 *       Special case: CANCELLED -&gt; FAILED transition is tolerated (returns CANCELLED).
 *       This handles scenarios where failure occurs after cancellation.
 * </pre>
 *
 * <h2>Thread Safety</h2>
 * This class is designed for concurrent access:
 * <ul>
 *   <li>The {@code State} and size fields are immutable once set</li>
 *   <li>Progress counters ({@code filesDownloaded}, {@code downloadFailures}, {@code bytesDownloaded})
 *       are thread-safe using {@code AtomicInteger} and {@code AtomicLong}</li>
 *   <li>State transitions create new immutable instances rather than modifying existing ones</li>
 *   <li>Multiple threads can safely read progress and update counters simultaneously</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * State transitions are managed through factory methods that return new instances.
 * Progress tracking is handled via atomic fields that can be safely updated from multiple threads.
 */
public class OperationStatus
{
    // State of the operation
    private final State state;

    // Total size of data available at source (immutable once set)
    private final long totalSize;

    // Total number of files available at source (immutable once set)
    private final int totalFiles;

    // Size of data to be copied (immutable once set)
    private final long bytesToDownload;

    // Number of files to download from source (immutable once set)
    private final int filesToDownload;

    // Number of files downloaded from source (thread-safe atomic counter)
    private final AtomicInteger filesDownloaded;

    // Number of download failures (thread-safe atomic counter)
    private final AtomicInteger downloadFailures;

    // Size of data downloaded from source in bytes (thread-safe atomic counter)
    private final AtomicLong bytesDownloaded;

    private OperationStatus(@NotNull State state,
                            long totalSize,
                            int totalFiles,
                            long bytesToDownload,
                            int filesToDownload,
                            @NotNull AtomicInteger filesDownloaded,
                            @NotNull AtomicInteger downloadFailures,
                            @NotNull AtomicLong bytesDownloaded)
    {
        this.state = state;
        this.totalSize = totalSize;
        this.totalFiles = totalFiles;
        this.bytesToDownload = bytesToDownload;
        this.filesToDownload = filesToDownload;
        this.filesDownloaded = filesDownloaded;
        this.downloadFailures = downloadFailures;
        this.bytesDownloaded = bytesDownloaded;
    }

    public static OperationStatus getStartingState()
    {
        return new OperationStatus(State.STARTING,
                                   -1,
                                   -1,
                                   -1,
                                   -1,
                                   new AtomicInteger(0),
                                   new AtomicInteger(0),
                                   new AtomicLong(0));
    }

    /**
     * Transitions to the CLEANING state with updated file metadata.
     *
     * @param totalSize  total size of files at the source
     * @param totalFiles total number of files at the source
     * @return new OperationStatus instance in CLEANING state
     * @throws IllegalStateTransitionException if current state cannot transition to CLEANING
     */
    @VisibleForTesting
    public OperationStatus getCleaningState(long totalSize, int totalFiles)
    {
        return new OperationStatus(this.state.toCleaning(),
                                   totalSize,
                                   totalFiles,
                                   this.bytesToDownload,
                                   this.filesToDownload,
                                   this.filesDownloaded,
                                   this.downloadFailures,
                                   this.bytesDownloaded);
    }

    /**
     * Transitions to the PREPARING state.
     *
     * @return new OperationStatus instance in PREPARING state
     * @throws IllegalStateTransitionException if current state cannot transition to PREPARING
     */
    @VisibleForTesting
    public OperationStatus getPreparingState()
    {
        return new OperationStatus(this.state.toPreparing(),
                                   this.totalSize,
                                   this.totalFiles,
                                   this.bytesToDownload,
                                   this.filesToDownload,
                                   this.filesDownloaded,
                                   this.downloadFailures,
                                   this.bytesDownloaded);
    }

    /**
     * Transitions to the DOWNLOADING state with updated download metadata.
     * Resets progress counters for the new download phase.
     *
     * @param bytesToDownload total size of data to be downloaded in bytes
     * @param filesToDownload number of files to be downloaded
     * @return new OperationStatus instance in DOWNLOADING state
     * @throws IllegalStateTransitionException if current state cannot transition to DOWNLOADING
     */
    OperationStatus getDownloadingState(final long bytesToDownload, final int filesToDownload)
    {
        return new OperationStatus(this.state.toDownloading(),
                                   this.totalSize,
                                   this.totalFiles,
                                   bytesToDownload,
                                   filesToDownload,
                                   new AtomicInteger(),
                                   new AtomicInteger(),
                                   new AtomicLong());
    }

    /**
     * Transitions to the DOWNLOAD_COMPLETE state.
     *
     * @return new OperationStatus instance in DOWNLOAD_COMPLETE state
     * @throws IllegalStateTransitionException if current state cannot transition to DOWNLOAD_COMPLETE
     */
    OperationStatus getDownloadCompleteState()
    {
        return new OperationStatus(this.state.toDownloadComplete(),
                                   this.totalSize,
                                   this.totalFiles,
                                   this.bytesToDownload,
                                   this.filesToDownload,
                                   this.filesDownloaded,
                                   this.downloadFailures,
                                   this.bytesDownloaded);
    }

    /**
     * Transitions to the SUCCESS state, indicating successful completion.
     *
     * @return new OperationStatus instance in SUCCESS state
     * @throws IllegalStateTransitionException if current state cannot transition to SUCCESS
     */
    @VisibleForTesting
    public OperationStatus getSuccessState()
    {
        return new OperationStatus(this.state.toSuccess(),
                                   this.totalSize,
                                   this.totalFiles,
                                   this.bytesToDownload,
                                   this.filesToDownload,
                                   this.filesDownloaded,
                                   this.downloadFailures,
                                   this.bytesDownloaded);
    }

    /**
     * Transitions to the FAILED state, indicating operation failure.
     *
     * @return new OperationStatus instance in FAILED state
     * @throws IllegalStateTransitionException if current state cannot transition to FAILED
     */
    OperationStatus tryFailureState()
    {
        return new OperationStatus(this.state.toFailed(),
                                   this.totalSize,
                                   this.totalFiles,
                                   this.bytesToDownload,
                                   this.filesToDownload,
                                   this.filesDownloaded,
                                   this.downloadFailures,
                                   this.bytesDownloaded);
    }

    /**
     * Cancels the task if not completed.
     *
     * @return Returns same state if completed, otherwise returns cancelled state.
     */
    public OperationStatus cancel()
    {
        return new OperationStatus(this.state.toCancelled(),
                                   this.totalSize,
                                   this.totalFiles,
                                   this.bytesToDownload,
                                   this.filesToDownload,
                                   this.filesDownloaded,
                                   this.downloadFailures,
                                   this.bytesDownloaded);
    }

    public State getState()
    {
        return state;
    }

    public State state()
    {
        return state;
    }

    public long totalSize()
    {
        return totalSize;
    }

    public long bytesToDownload()
    {
        return bytesToDownload;
    }

    public int filesToDownload()
    {
        return filesToDownload;
    }

    /**
     * Returns the thread-safe atomic counter for files downloaded.
     * This counter can be safely read and updated from multiple threads.
     *
     * @return atomic counter for files downloaded
     */
    public AtomicInteger filesDownloaded()
    {
        return filesDownloaded;
    }

    /**
     * Returns the thread-safe atomic counter for bytes downloaded.
     * This counter can be safely read and updated from multiple threads.
     *
     * @return atomic counter for bytes downloaded
     */
    public AtomicLong bytesDownloaded()
    {
        return bytesDownloaded;
    }

    public int totalFiles()
    {
        return totalFiles;
    }

    /**
     * Returns the thread-safe atomic counter for download failures.
     * This counter can be safely read and updated from multiple threads.
     *
     * @return atomic counter for download failures
     */
    public AtomicInteger downloadFailures()
    {
        return downloadFailures;
    }

    @Override
    public String toString()
    {
        return "OperationStatus{" +
               "bytesDownloaded=" + bytesDownloaded +
               ", filesDownloaded=" + filesDownloaded +
               ", filesToDownload=" + filesToDownload +
               ", totalSize=" + totalSize +
               ", downloadSize=" + bytesToDownload +
               ", state=" + state +
               '}';
    }

    /**
     * Represents the various states of a live migration data copy operation.
     *
     * <h3>State Descriptions:</h3>
     * <ul>
     *   <li><b>STARTING</b> - Initial state when the operation begins</li>
     *   <li><b>CLEANING</b> - Removing unnecessary files from the destination</li>
     *   <li><b>PREPARING</b> - Analyzing files to determine what needs to be downloaded</li>
     *   <li><b>DOWNLOADING</b> - Actively downloading files from the source</li>
     *   <li><b>DOWNLOAD_COMPLETE</b> - All files have been downloaded (terminal state)</li>
     *   <li><b>SUCCESS</b> - Operation completed successfully (terminal state)</li>
     *   <li><b>FAILED</b> - Operation failed due to an error (terminal state)</li>
     *   <li><b>CANCELLED</b> - Operation was cancelled by user request (terminal state)</li>
     * </ul>
     * <p>
     * Invalid transitions throw {@link IllegalStateTransitionException}.
     */
    public enum State
    {
        CANCELLED(Set.of()),
        FAILED(Set.of()),
        SUCCESS(Set.of()),
        DOWNLOAD_COMPLETE(Set.of()),
        DOWNLOADING(Set.of(DOWNLOAD_COMPLETE, FAILED, CANCELLED)),
        PREPARING(Set.of(DOWNLOADING, SUCCESS, FAILED, CANCELLED)),
        CLEANING(Set.of(PREPARING, FAILED, CANCELLED)),
        STARTING(Set.of(CLEANING, FAILED, CANCELLED));

        private final Set<State> nextStates;

        State(Set<State> nextStates)
        {
            this.nextStates = nextStates;
        }

        private State transitionToState(State toState)
        {
            // Tolerate transition attempts from CANCELLED -> FAILED.
            // This is a special case that happens when a live migration task
            // is cancelled while downloads are in progress.
            if (this == CANCELLED && toState == FAILED)
            {
                return CANCELLED;
            }
            if (nextStates.contains(toState))
            {
                return toState;
            }

            throw new IllegalStateTransitionException(this, toState);
        }

        private State toCancelled()
        {
            return this.transitionToState(CANCELLED);
        }

        private State toFailed()
        {
            return this.transitionToState(FAILED);
        }

        private State toSuccess()
        {
            return this.transitionToState(SUCCESS);
        }

        private State toDownloadComplete()
        {
            return this.transitionToState(DOWNLOAD_COMPLETE);
        }

        private State toDownloading()
        {
            return this.transitionToState(DOWNLOADING);
        }

        private State toPreparing()
        {
            return this.transitionToState(PREPARING);
        }

        private State toCleaning()
        {
            return this.transitionToState(CLEANING);
        }
    }

    /**
     * Exception to indicate that the live migration task is trying to transition to an invalid state.
     */
    public static class IllegalStateTransitionException extends RuntimeException
    {
        public IllegalStateTransitionException(State from, State to)
        {
            super("Cannot move from " + from + " state to " + to + " state.");
        }
    }
}
