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

import org.junit.jupiter.api.Test;

import org.assertj.core.api.ThrowableAssert;

import static org.apache.cassandra.sidecar.livemigration.OperationStatus.State.CANCELLED;
import static org.apache.cassandra.sidecar.livemigration.OperationStatus.State.CLEANING;
import static org.apache.cassandra.sidecar.livemigration.OperationStatus.State.DOWNLOADING;
import static org.apache.cassandra.sidecar.livemigration.OperationStatus.State.DOWNLOAD_COMPLETE;
import static org.apache.cassandra.sidecar.livemigration.OperationStatus.State.FAILED;
import static org.apache.cassandra.sidecar.livemigration.OperationStatus.State.PREPARING;
import static org.apache.cassandra.sidecar.livemigration.OperationStatus.State.STARTING;
import static org.apache.cassandra.sidecar.livemigration.OperationStatus.State.SUCCESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class OperationStatusTest
{
    @Test
    public void testSuccessfulFileDownloadPath()
    {
        // Happy path scenario
        OperationStatus operationStatus = OperationStatus.startingState()
                                                         .toCleaningState(128L, 1)
                                                         .toPreparingState()
                                                         .toDownloadingState(128L, 2)
                                                         .toDownloadCompleteState();
        assertThat(operationStatus.state()).isEqualTo(DOWNLOAD_COMPLETE);
    }

    @Test
    public void testNoFilesToDownloadPath()
    {
        OperationStatus operationStatus = OperationStatus.startingState()
                                                         .toCleaningState(128L, 1)
                                                         .toPreparingState()
                                                         .toSuccessState();

        assertThat(operationStatus.state()).isEqualTo(SUCCESS);
    }

    @Test
    public void testDownloadsFailedPath()
    {
        OperationStatus operationStatus = OperationStatus.startingState()
                                                         .toCleaningState(128L, 1)
                                                         .toPreparingState()
                                                         .toDownloadingState(128L, 2)
                                                         .tryFailureState();
        assertThat(operationStatus.state()).isEqualTo(FAILED);
    }

    @Test
    public void testFailedStateCannotTransitionToOtherState()
    {
        OperationStatus failureStatus = OperationStatus.startingState().tryFailureState();
        assertIllegalStateTransition(() -> failureStatus.toCleaningState(0L, 0));
        assertIllegalStateTransition(() -> failureStatus.toDownloadingState(0L, 0));
        assertIllegalStateTransition(failureStatus::toDownloadCompleteState);
        assertIllegalStateTransition(failureStatus::toSuccessState);
        assertIllegalStateTransition(failureStatus::tryFailureState);
        assertIllegalStateTransition(failureStatus::cancel);
    }

    @Test
    public void testSuccessStateCannotTransitionToOtherState()
    {
        OperationStatus successStatus = OperationStatus.startingState()
                                                       .toCleaningState(0L, 0)
                                                       .toPreparingState()
                                                       .toSuccessState();
        assertIllegalStateTransition(successStatus::toPreparingState);
        assertIllegalStateTransition(() -> successStatus.toDownloadingState(0L, 1));
        assertIllegalStateTransition(successStatus::toDownloadCompleteState);
        assertIllegalStateTransition(successStatus::tryFailureState);
        assertIllegalStateTransition(successStatus::toSuccessState);
    }

    @Test
    public void testCancelledStateCannotTransitionToOtherState()
    {
        OperationStatus cancelledStatus = OperationStatus.startingState().cancel();
        assertIllegalStateTransition(cancelledStatus::toPreparingState);
        assertIllegalStateTransition(() -> cancelledStatus.toCleaningState(0L, 0));
        assertIllegalStateTransition(() -> cancelledStatus.toDownloadingState(0L, 1));
        assertIllegalStateTransition(cancelledStatus::toDownloadCompleteState);
        assertIllegalStateTransition(cancelledStatus::toSuccessState);
        // Special case: CANCELLED -> FAILED is tolerated (returns CANCELLED)
        assertThat(cancelledStatus.tryFailureState().state()).isEqualTo(CANCELLED);
        assertIllegalStateTransition(cancelledStatus::cancel);
    }

    @Test
    public void testDownloadCompleteStateCannotTransitionToOtherState()
    {
        OperationStatus downloadCompleteStatus = OperationStatus.startingState()
                                                                .toCleaningState(100L, 2)
                                                                .toPreparingState()
                                                                .toDownloadingState(50L, 1)
                                                                .toDownloadCompleteState();
        assertThat(downloadCompleteStatus.state()).isEqualTo(DOWNLOAD_COMPLETE);

        assertIllegalStateTransition(downloadCompleteStatus::toPreparingState);
        assertIllegalStateTransition(() -> downloadCompleteStatus.toCleaningState(0L, 0));
        assertIllegalStateTransition(() -> downloadCompleteStatus.toDownloadingState(0L, 1));
        assertIllegalStateTransition(downloadCompleteStatus::toDownloadCompleteState);
        assertIllegalStateTransition(downloadCompleteStatus::toSuccessState);
        assertIllegalStateTransition(downloadCompleteStatus::tryFailureState);
        assertIllegalStateTransition(downloadCompleteStatus::cancel);
    }

    @Test
    public void testDownloadingStateCannotTransitionToSuccess()
    {
        OperationStatus downloadingStatus = OperationStatus.startingState()
                                                           .toCleaningState(100L, 2)
                                                           .toPreparingState()
                                                           .toDownloadingState(50L, 1);
        assertThat(downloadingStatus.state()).isEqualTo(DOWNLOADING);

        // DOWNLOADING can only go to DOWNLOAD_COMPLETE, FAILED, or CANCELLED
        assertIllegalStateTransition(downloadingStatus::toSuccessState);
        assertIllegalStateTransition(downloadingStatus::toPreparingState);
        assertIllegalStateTransition(() -> downloadingStatus.toCleaningState(0L, 0));
        assertIllegalStateTransition(() -> downloadingStatus.toDownloadingState(0L, 1));
    }

    @Test
    public void testInvalidStateTransitions()
    {
        OperationStatus startingStatus = OperationStatus.startingState();

        // STARTING cannot go directly to DOWNLOADING, DOWNLOAD_COMPLETE, SUCCESS, or PREPARING
        assertIllegalStateTransition(() -> startingStatus.toDownloadingState(0L, 1));
        assertIllegalStateTransition(startingStatus::toDownloadCompleteState);
        assertIllegalStateTransition(startingStatus::toSuccessState);
        assertIllegalStateTransition(startingStatus::toPreparingState);

        OperationStatus cleaningStatus = startingStatus.toCleaningState(100L, 2);

        // CLEANING cannot go directly to DOWNLOADING, DOWNLOAD_COMPLETE, SUCCESS
        assertIllegalStateTransition(() -> cleaningStatus.toDownloadingState(0L, 1));
        assertIllegalStateTransition(cleaningStatus::toDownloadCompleteState);
        assertIllegalStateTransition(cleaningStatus::toSuccessState);
        assertIllegalStateTransition(() -> cleaningStatus.toCleaningState(0L, 0));
    }

    @Test
    public void testStartingState()
    {
        OperationStatus startingStatus = OperationStatus.startingState();
        assertThat(startingStatus.state()).isEqualTo(STARTING);
        assertThat(startingStatus.totalSize()).isEqualTo(-1);
        assertThat(startingStatus.totalFiles()).isEqualTo(-1);
        assertThat(startingStatus.bytesToDownload()).isEqualTo(-1);
        assertThat(startingStatus.filesToDownload()).isEqualTo(-1);
        assertThat(startingStatus.filesDownloaded()).isEqualTo(0);
        assertThat(startingStatus.downloadFailures()).isEqualTo(0);
        assertThat(startingStatus.bytesDownloaded()).isEqualTo(0);
    }

    @Test
    public void testStateTransitionFields()
    {
        long totalSize = 1000L;
        int totalFiles = 5;
        long downloadSize = 500L;
        int filesToDownload = 3;

        OperationStatus status = OperationStatus.startingState()
                                                .toCleaningState(totalSize, totalFiles)
                                                .toPreparingState()
                                                .toDownloadingState(downloadSize, filesToDownload);

        assertThat(status.totalSize()).isEqualTo(totalSize);
        assertThat(status.totalFiles()).isEqualTo(totalFiles);
        assertThat(status.bytesToDownload()).isEqualTo(downloadSize);
        assertThat(status.filesToDownload()).isEqualTo(filesToDownload);
        assertThat(status.filesDownloaded()).isEqualTo(0); // Reset during downloading state
        assertThat(status.downloadFailures()).isEqualTo(0); // Reset during downloading state
        assertThat(status.bytesDownloaded()).isEqualTo(0); // Reset during downloading state
    }

    @Test
    public void testAllValidTransitions()
    {
        // Test all valid transitions from each state

        // From STARTING
        OperationStatus fromStarting = OperationStatus.startingState();
        assertThat(fromStarting.toCleaningState(100L, 2).state()).isEqualTo(CLEANING);
        assertThat(fromStarting.tryFailureState().state()).isEqualTo(FAILED);
        assertThat(fromStarting.cancel().state()).isEqualTo(CANCELLED);

        // From CLEANING
        OperationStatus fromCleaning = OperationStatus.startingState().toCleaningState(100L, 2);
        assertThat(fromCleaning.toPreparingState().state()).isEqualTo(PREPARING);
        assertThat(fromCleaning.tryFailureState().state()).isEqualTo(FAILED);
        assertThat(fromCleaning.cancel().state()).isEqualTo(CANCELLED);

        // From PREPARING
        OperationStatus fromPreparing = OperationStatus.startingState()
                                                       .toCleaningState(100L, 2)
                                                       .toPreparingState();
        assertThat(fromPreparing.toDownloadingState(50L, 1).state()).isEqualTo(DOWNLOADING);
        assertThat(fromPreparing.toSuccessState().state()).isEqualTo(SUCCESS);
        assertThat(fromPreparing.tryFailureState().state()).isEqualTo(FAILED);
        assertThat(fromPreparing.cancel().state()).isEqualTo(CANCELLED);

        // From DOWNLOADING
        OperationStatus fromDownloading = OperationStatus.startingState()
                                                         .toCleaningState(100L, 2)
                                                         .toPreparingState()
                                                         .toDownloadingState(50L, 1);
        assertThat(fromDownloading.toDownloadCompleteState().state()).isEqualTo(DOWNLOAD_COMPLETE);
        assertThat(fromDownloading.tryFailureState().state()).isEqualTo(FAILED);
        assertThat(fromDownloading.cancel().state()).isEqualTo(CANCELLED);
    }

    @Test
    public void testCancelledToFailedTransitionTolerance()
    {
        // Test the special case where CANCELLED -> FAILED transition is tolerated
        // This happens when a task is cancelled but some downloads are still in progress
        // and they subsequently fail
        OperationStatus cancelledStatus = OperationStatus.startingState().cancel();
        assertThat(cancelledStatus.state()).isEqualTo(CANCELLED);

        // Attempting to transition from CANCELLED to FAILED should return CANCELLED state
        OperationStatus afterFailureAttempt = cancelledStatus.tryFailureState();
        assertThat(afterFailureAttempt.state()).isEqualTo(CANCELLED);

        assertThat(afterFailureAttempt).isNotSameAs(cancelledStatus);

        assertThat(afterFailureAttempt.totalSize()).isEqualTo(cancelledStatus.totalSize());
        assertThat(afterFailureAttempt.totalFiles()).isEqualTo(cancelledStatus.totalFiles());
    }

    @Test
    public void testIncrementFilesDownloaded()
    {
        OperationStatus status = OperationStatus.startingState()
                                                .toCleaningState(100L, 5)
                                                .toPreparingState()
                                                .toDownloadingState(50L, 2);

        assertThat(status.filesDownloaded()).isEqualTo(0);

        // Test incrementing files downloaded
        status.incrementFilesDownloaded();
        assertThat(status.filesDownloaded()).isEqualTo(1);

        status.incrementFilesDownloaded();
        assertThat(status.filesDownloaded()).isEqualTo(2);
    }

    @Test
    public void testIncrementFilesDownloadedThrowsExceptionWhenCompleted()
    {
        OperationStatus successStatus = OperationStatus.startingState()
                                                       .toCleaningState(100L, 5)
                                                       .toPreparingState()
                                                       .toSuccessState();

        assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(successStatus::incrementFilesDownloaded)
        .withMessage("Cannot increment files downloaded when operation is in state SUCCESS");

        OperationStatus completeStatus = OperationStatus.startingState()
                                                        .toCleaningState(100L, 5)
                                                        .toPreparingState()
                                                        .toDownloadingState(50L, 2)
                                                        .toDownloadCompleteState();

        assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(completeStatus::incrementFilesDownloaded)
        .withMessage("Cannot increment files downloaded when operation is in state DOWNLOAD_COMPLETE");
    }

    @Test
    public void testAddBytesDownloaded()
    {
        OperationStatus status = OperationStatus.startingState()
                                                .toCleaningState(100L, 5)
                                                .toPreparingState()
                                                .toDownloadingState(50L, 2);

        assertThat(status.bytesDownloaded()).isEqualTo(0L);

        // Test adding bytes downloaded
        status.addBytesDownloaded(25L);
        assertThat(status.bytesDownloaded()).isEqualTo(25L);

        status.addBytesDownloaded(15L);
        assertThat(status.bytesDownloaded()).isEqualTo(40L);
    }

    @Test
    public void testAddBytesDownloadedThrowsExceptionWhenCompleted()
    {
        OperationStatus successStatus = OperationStatus.startingState()
                                                       .toCleaningState(100L, 5)
                                                       .toPreparingState()
                                                       .toSuccessState();

        assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> successStatus.addBytesDownloaded(10L))
        .withMessage("Cannot increment bytes downloaded when operation is in state SUCCESS");

        OperationStatus completeStatus = OperationStatus.startingState()
                                                        .toCleaningState(100L, 5)
                                                        .toPreparingState()
                                                        .toDownloadingState(50L, 2)
                                                        .toDownloadCompleteState();

        assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> completeStatus.addBytesDownloaded(10L))
        .withMessage("Cannot increment bytes downloaded when operation is in state DOWNLOAD_COMPLETE");
    }

    @Test
    public void testIncrementDownloadFailures()
    {
        OperationStatus status = OperationStatus.startingState()
                                                .toCleaningState(100L, 5)
                                                .toPreparingState()
                                                .toDownloadingState(50L, 2);

        assertThat(status.downloadFailures()).isEqualTo(0);

        // Test incrementing download failures
        status.incrementDownloadFailures();
        assertThat(status.downloadFailures()).isEqualTo(1);

        status.incrementDownloadFailures();
        assertThat(status.downloadFailures()).isEqualTo(2);
    }

    @Test
    public void testIncrementDownloadFailuresThrowsExceptionWhenCompleted()
    {
        OperationStatus successStatus = OperationStatus.startingState()
                                                       .toCleaningState(100L, 5)
                                                       .toPreparingState()
                                                       .toSuccessState();

        assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(successStatus::incrementDownloadFailures)
        .withMessage("Cannot increment download failures when operation is in state SUCCESS");

        OperationStatus completeStatus = OperationStatus.startingState()
                                                        .toCleaningState(100L, 5)
                                                        .toPreparingState()
                                                        .toDownloadingState(50L, 2)
                                                        .toDownloadCompleteState();

        assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(completeStatus::incrementDownloadFailures)
        .withMessage("Cannot increment download failures when operation is in state DOWNLOAD_COMPLETE");
    }

    @Test
    public void testIncrementCountersInCancelledState()
    {
        // Test that counters can be incremented in CANCELLED state
        // This is allowed because cancellation can happen while downloads are in progress
        OperationStatus cancelledStatus = OperationStatus.startingState()
                                                         .toCleaningState(100L, 5)
                                                         .toPreparingState()
                                                         .toDownloadingState(50L, 2)
                                                         .cancel();

        assertThat(cancelledStatus.state()).isEqualTo(CANCELLED);

        // These should work without throwing exceptions
        cancelledStatus.incrementFilesDownloaded();
        assertThat(cancelledStatus.filesDownloaded()).isEqualTo(1);

        cancelledStatus.addBytesDownloaded(10L);
        assertThat(cancelledStatus.bytesDownloaded()).isEqualTo(10L);

        cancelledStatus.incrementDownloadFailures();
        assertThat(cancelledStatus.downloadFailures()).isEqualTo(1);
    }

    @Test
    public void testIncrementCountersInFailedState()
    {
        // Test that counters cannot be incremented in FAILED state
        OperationStatus failedStatus = OperationStatus.startingState()
                                                      .toCleaningState(100L, 5)
                                                      .toPreparingState()
                                                      .toDownloadingState(50L, 2)
                                                      .tryFailureState();

        assertThat(failedStatus.state()).isEqualTo(FAILED);

        // These should throw exceptions
        assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(failedStatus::incrementFilesDownloaded)
        .withMessage("Cannot increment files downloaded when operation is in state FAILED");

        assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> failedStatus.addBytesDownloaded(10L))
        .withMessage("Cannot increment bytes downloaded when operation is in state FAILED");

        assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(failedStatus::incrementDownloadFailures)
        .withMessage("Cannot increment download failures when operation is in state FAILED");
    }

    @Test
    public void testIncrementCountersInNonDownloadStates()
    {
        // Test that counters cannot be incremented in STARTING, CLEANING, PREPARING states
        OperationStatus startingStatus = OperationStatus.startingState();

        assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(startingStatus::incrementFilesDownloaded)
        .withMessage("Cannot increment files downloaded when operation is in state STARTING");

        OperationStatus cleaningStatus = startingStatus.toCleaningState(100L, 5);

        assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(cleaningStatus::incrementFilesDownloaded)
        .withMessage("Cannot increment files downloaded when operation is in state CLEANING");

        OperationStatus preparingStatus = cleaningStatus.toPreparingState();

        assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(preparingStatus::incrementFilesDownloaded)
        .withMessage("Cannot increment files downloaded when operation is in state PREPARING");
    }

    @Test
    public void testStartingStateTransitions()
    {
        // Test valid transitions from STARTING state
        assertThat(STARTING.toCleaning()).isEqualTo(CLEANING);
        assertThat(STARTING.toFailed()).isEqualTo(FAILED);
        assertThat(STARTING.toCancelled()).isEqualTo(CANCELLED);

        // Test invalid transitions from STARTING state
        assertIllegalStateTransition(STARTING::toPreparing);
        assertIllegalStateTransition(STARTING::toDownloading);
        assertIllegalStateTransition(STARTING::toDownloadComplete);
        assertIllegalStateTransition(STARTING::toSuccess);
    }

    @Test
    public void testCleaningStateTransitions()
    {
        // Test valid transitions from CLEANING state
        assertThat(CLEANING.toPreparing()).isEqualTo(PREPARING);
        assertThat(CLEANING.toFailed()).isEqualTo(FAILED);
        assertThat(CLEANING.toCancelled()).isEqualTo(CANCELLED);

        // Test invalid transitions from CLEANING state
        assertIllegalStateTransition(CLEANING::toCleaning);
        assertIllegalStateTransition(CLEANING::toDownloading);
        assertIllegalStateTransition(CLEANING::toDownloadComplete);
        assertIllegalStateTransition(CLEANING::toSuccess);
    }

    @Test
    public void testPreparingStateTransitions()
    {
        // Test valid transitions from PREPARING state
        assertThat(PREPARING.toDownloading()).isEqualTo(DOWNLOADING);
        assertThat(PREPARING.toSuccess()).isEqualTo(SUCCESS);
        assertThat(PREPARING.toFailed()).isEqualTo(FAILED);
        assertThat(PREPARING.toCancelled()).isEqualTo(CANCELLED);

        // Test invalid transitions from PREPARING state
        assertIllegalStateTransition(PREPARING::toCleaning);
        assertIllegalStateTransition(PREPARING::toPreparing);
        assertIllegalStateTransition(PREPARING::toDownloadComplete);
    }

    @Test
    public void testDownloadingStateTransitions()
    {
        // Test valid transitions from DOWNLOADING state
        assertThat(DOWNLOADING.toDownloadComplete()).isEqualTo(DOWNLOAD_COMPLETE);
        assertThat(DOWNLOADING.toFailed()).isEqualTo(FAILED);
        assertThat(DOWNLOADING.toCancelled()).isEqualTo(CANCELLED);

        // Test invalid transitions from DOWNLOADING state
        assertIllegalStateTransition(DOWNLOADING::toCleaning);
        assertIllegalStateTransition(DOWNLOADING::toPreparing);
        assertIllegalStateTransition(DOWNLOADING::toDownloading);
        assertIllegalStateTransition(DOWNLOADING::toSuccess);
    }

    @Test
    public void testTerminalStateTransitions()
    {
        // Test SUCCESS state - no valid transitions
        assertIllegalStateTransition(SUCCESS::toCleaning);
        assertIllegalStateTransition(SUCCESS::toPreparing);
        assertIllegalStateTransition(SUCCESS::toDownloading);
        assertIllegalStateTransition(SUCCESS::toDownloadComplete);
        assertIllegalStateTransition(SUCCESS::toSuccess);
        assertIllegalStateTransition(SUCCESS::toFailed);
        assertIllegalStateTransition(SUCCESS::toCancelled);

        // Test FAILED state - no valid transitions
        assertIllegalStateTransition(FAILED::toCleaning);
        assertIllegalStateTransition(FAILED::toPreparing);
        assertIllegalStateTransition(FAILED::toDownloading);
        assertIllegalStateTransition(FAILED::toDownloadComplete);
        assertIllegalStateTransition(FAILED::toSuccess);
        assertIllegalStateTransition(FAILED::toFailed);
        assertIllegalStateTransition(FAILED::toCancelled);

        // Test DOWNLOAD_COMPLETE state - no valid transitions
        assertIllegalStateTransition(DOWNLOAD_COMPLETE::toCleaning);
        assertIllegalStateTransition(DOWNLOAD_COMPLETE::toPreparing);
        assertIllegalStateTransition(DOWNLOAD_COMPLETE::toDownloading);
        assertIllegalStateTransition(DOWNLOAD_COMPLETE::toDownloadComplete);
        assertIllegalStateTransition(DOWNLOAD_COMPLETE::toSuccess);
        assertIllegalStateTransition(DOWNLOAD_COMPLETE::toFailed);
        assertIllegalStateTransition(DOWNLOAD_COMPLETE::toCancelled);

        // Test CANCELLED state - no valid transitions except special CANCELLED -> FAILED case
        assertIllegalStateTransition(CANCELLED::toCleaning);
        assertIllegalStateTransition(CANCELLED::toPreparing);
        assertIllegalStateTransition(CANCELLED::toDownloading);
        assertIllegalStateTransition(CANCELLED::toDownloadComplete);
        assertIllegalStateTransition(CANCELLED::toSuccess);
        assertIllegalStateTransition(CANCELLED::toCancelled);

        // Special case: CANCELLED -> FAILED is tolerated (returns CANCELLED)
        assertThat(CANCELLED.toFailed()).isEqualTo(CANCELLED);
    }

    public void assertIllegalStateTransition(ThrowableAssert.ThrowingCallable callable)
    {
        assertThatExceptionOfType(OperationStatus.IllegalStateTransitionException.class).isThrownBy(callable);
    }
}
