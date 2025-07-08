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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
        OperationStatus operationStatus = OperationStatus.getStartingState()
                                                         .getCleaningState(128L, 1)
                                                         .getPreparingState()
                                                         .getDownloadingState(128L, 2)
                                                         .getDownloadCompleteState();
        assertThat(operationStatus.getState()).isEqualTo(DOWNLOAD_COMPLETE);
    }

    @Test
    public void testNoFilesToDownloadPath()
    {
        OperationStatus operationStatus = OperationStatus.getStartingState()
                                                         .getCleaningState(128L, 1)
                                                         .getPreparingState()
                                                         .getSuccessState();

        assertThat(operationStatus.getState()).isEqualTo(SUCCESS);
    }

    @Test
    public void testDownloadsFailedPath()
    {
        OperationStatus operationStatus = OperationStatus.getStartingState()
                                                         .getCleaningState(128L, 1)
                                                         .getPreparingState()
                                                         .getDownloadingState(128L, 2)
                                                         .tryFailureState();
        assertThat(operationStatus.getState()).isEqualTo(FAILED);
    }

    @Test
    public void testFailedStateCannotTransitionToOtherState()
    {
        OperationStatus failureStatus = OperationStatus.getStartingState().tryFailureState();
        assertIllegalStateTransition(() -> failureStatus.getCleaningState(0L, 0));
        assertIllegalStateTransition(() -> failureStatus.getDownloadingState(0L, 0));
        assertIllegalStateTransition(failureStatus::getDownloadCompleteState);
        assertIllegalStateTransition(failureStatus::getSuccessState);
        assertIllegalStateTransition(failureStatus::tryFailureState);
        assertIllegalStateTransition(failureStatus::cancel);
    }

    @Test
    public void testSuccessStateCannotTransitionToOtherState()
    {
        OperationStatus successStatus = OperationStatus.getStartingState()
                                                       .getCleaningState(0L, 0)
                                                       .getPreparingState()
                                                       .getSuccessState();
        assertIllegalStateTransition(successStatus::getPreparingState);
        assertIllegalStateTransition(() -> successStatus.getDownloadingState(0L, 1));
        assertIllegalStateTransition(successStatus::getDownloadCompleteState);
        assertIllegalStateTransition(successStatus::tryFailureState);
        assertIllegalStateTransition(successStatus::getSuccessState);
    }

    @Test
    public void testCancelledStateCannotTransitionToOtherState()
    {
        OperationStatus cancelledStatus = OperationStatus.getStartingState().cancel();
        assertIllegalStateTransition(cancelledStatus::getPreparingState);
        assertIllegalStateTransition(() -> cancelledStatus.getCleaningState(0L, 0));
        assertIllegalStateTransition(() -> cancelledStatus.getDownloadingState(0L, 1));
        assertIllegalStateTransition(cancelledStatus::getDownloadCompleteState);
        assertIllegalStateTransition(cancelledStatus::getSuccessState);
        // Special case: CANCELLED -> FAILED is tolerated (returns CANCELLED)
        assertThat(cancelledStatus.tryFailureState().getState()).isEqualTo(CANCELLED);
        assertIllegalStateTransition(cancelledStatus::cancel);
    }

    @Test
    public void testDownloadCompleteStateCannotTransitionToOtherState()
    {
        OperationStatus downloadCompleteStatus = OperationStatus.getStartingState()
                                                                .getCleaningState(100L, 2)
                                                                .getPreparingState()
                                                                .getDownloadingState(50L, 1)
                                                                .getDownloadCompleteState();
        assertThat(downloadCompleteStatus.getState()).isEqualTo(DOWNLOAD_COMPLETE);

        assertIllegalStateTransition(downloadCompleteStatus::getPreparingState);
        assertIllegalStateTransition(() -> downloadCompleteStatus.getCleaningState(0L, 0));
        assertIllegalStateTransition(() -> downloadCompleteStatus.getDownloadingState(0L, 1));
        assertIllegalStateTransition(downloadCompleteStatus::getDownloadCompleteState);
        assertIllegalStateTransition(downloadCompleteStatus::getSuccessState);
        assertIllegalStateTransition(downloadCompleteStatus::tryFailureState);
        assertIllegalStateTransition(downloadCompleteStatus::cancel);
    }

    @Test
    public void testDownloadingStateCannotTransitionToSuccess()
    {
        OperationStatus downloadingStatus = OperationStatus.getStartingState()
                                                           .getCleaningState(100L, 2)
                                                           .getPreparingState()
                                                           .getDownloadingState(50L, 1);
        assertThat(downloadingStatus.getState()).isEqualTo(DOWNLOADING);

        // DOWNLOADING can only go to DOWNLOAD_COMPLETE, FAILED, or CANCELLED
        assertIllegalStateTransition(downloadingStatus::getSuccessState);
        assertIllegalStateTransition(downloadingStatus::getPreparingState);
        assertIllegalStateTransition(() -> downloadingStatus.getCleaningState(0L, 0));
        assertIllegalStateTransition(() -> downloadingStatus.getDownloadingState(0L, 1));
    }

    @Test
    public void testInvalidStateTransitions()
    {
        OperationStatus startingStatus = OperationStatus.getStartingState();

        // STARTING cannot go directly to DOWNLOADING, DOWNLOAD_COMPLETE, SUCCESS, or PREPARING
        assertIllegalStateTransition(() -> startingStatus.getDownloadingState(0L, 1));
        assertIllegalStateTransition(startingStatus::getDownloadCompleteState);
        assertIllegalStateTransition(startingStatus::getSuccessState);
        assertIllegalStateTransition(startingStatus::getPreparingState);

        OperationStatus cleaningStatus = startingStatus.getCleaningState(100L, 2);

        // CLEANING cannot go directly to DOWNLOADING, DOWNLOAD_COMPLETE, SUCCESS
        assertIllegalStateTransition(() -> cleaningStatus.getDownloadingState(0L, 1));
        assertIllegalStateTransition(cleaningStatus::getDownloadCompleteState);
        assertIllegalStateTransition(cleaningStatus::getSuccessState);
        assertIllegalStateTransition(() -> cleaningStatus.getCleaningState(0L, 0));
    }

    @Test
    public void testGetStartingState()
    {
        OperationStatus startingStatus = OperationStatus.getStartingState();
        assertThat(startingStatus.getState()).isEqualTo(STARTING);
        assertThat(startingStatus.totalSize()).isEqualTo(-1);
        assertThat(startingStatus.totalFiles()).isEqualTo(-1);
        assertThat(startingStatus.bytesToDownload()).isEqualTo(-1);
        assertThat(startingStatus.filesToDownload()).isEqualTo(-1);
        assertThat(startingStatus.filesDownloaded().get()).isEqualTo(0);
        assertThat(startingStatus.downloadFailures().get()).isEqualTo(0);
        assertThat(startingStatus.bytesDownloaded().get()).isEqualTo(0);
    }

    @Test
    public void testStateTransitionFields()
    {
        long totalSize = 1000L;
        int totalFiles = 5;
        long downloadSize = 500L;
        int filesToDownload = 3;

        OperationStatus status = OperationStatus.getStartingState()
                                                .getCleaningState(totalSize, totalFiles)
                                                .getPreparingState()
                                                .getDownloadingState(downloadSize, filesToDownload);

        assertThat(status.totalSize()).isEqualTo(totalSize);
        assertThat(status.totalFiles()).isEqualTo(totalFiles);
        assertThat(status.bytesToDownload()).isEqualTo(downloadSize);
        assertThat(status.filesToDownload()).isEqualTo(filesToDownload);
        assertThat(status.filesDownloaded().get()).isEqualTo(0); // Reset during downloading state
        assertThat(status.downloadFailures().get()).isEqualTo(0); // Reset during downloading state
        assertThat(status.bytesDownloaded().get()).isEqualTo(0); // Reset during downloading state
    }

    @Test
    public void testAtomicCounters()
    {
        OperationStatus status = OperationStatus.getStartingState()
                                                .getCleaningState(1000L, 5)
                                                .getPreparingState()
                                                .getDownloadingState(500L, 3);

        // Test that atomic fields are properly initialized and can be updated
        AtomicInteger filesDownloaded = status.filesDownloaded();
        AtomicInteger downloadFailures = status.downloadFailures();
        AtomicLong bytesDownloaded = status.bytesDownloaded();

        assertThat(filesDownloaded.get()).isEqualTo(0);
        assertThat(downloadFailures.get()).isEqualTo(0);
        assertThat(bytesDownloaded.get()).isEqualTo(0);

        // Simulate progress updates
        filesDownloaded.incrementAndGet();
        downloadFailures.addAndGet(2);
        bytesDownloaded.addAndGet(100L);

        assertThat(status.filesDownloaded().get()).isEqualTo(1);
        assertThat(status.downloadFailures().get()).isEqualTo(2);
        assertThat(status.bytesDownloaded().get()).isEqualTo(100L);

        // Test getter methods return same instances
        assertThat(status.filesDownloaded()).isSameAs(filesDownloaded);
        assertThat(status.bytesDownloaded()).isSameAs(bytesDownloaded);
    }

    @Test
    public void testAllValidTransitions()
    {
        // Test all valid transitions from each state

        // From STARTING
        OperationStatus fromStarting = OperationStatus.getStartingState();
        assertThat(fromStarting.getCleaningState(100L, 2).getState()).isEqualTo(CLEANING);
        assertThat(fromStarting.tryFailureState().getState()).isEqualTo(FAILED);
        assertThat(fromStarting.cancel().getState()).isEqualTo(CANCELLED);

        // From CLEANING
        OperationStatus fromCleaning = OperationStatus.getStartingState().getCleaningState(100L, 2);
        assertThat(fromCleaning.getPreparingState().getState()).isEqualTo(PREPARING);
        assertThat(fromCleaning.tryFailureState().getState()).isEqualTo(FAILED);
        assertThat(fromCleaning.cancel().getState()).isEqualTo(CANCELLED);

        // From PREPARING
        OperationStatus fromPreparing = OperationStatus.getStartingState()
                                                       .getCleaningState(100L, 2)
                                                       .getPreparingState();
        assertThat(fromPreparing.getDownloadingState(50L, 1).getState()).isEqualTo(DOWNLOADING);
        assertThat(fromPreparing.getSuccessState().getState()).isEqualTo(SUCCESS);
        assertThat(fromPreparing.tryFailureState().getState()).isEqualTo(FAILED);
        assertThat(fromPreparing.cancel().getState()).isEqualTo(CANCELLED);

        // From DOWNLOADING
        OperationStatus fromDownloading = OperationStatus.getStartingState()
                                                         .getCleaningState(100L, 2)
                                                         .getPreparingState()
                                                         .getDownloadingState(50L, 1);
        assertThat(fromDownloading.getDownloadCompleteState().getState()).isEqualTo(DOWNLOAD_COMPLETE);
        assertThat(fromDownloading.tryFailureState().getState()).isEqualTo(FAILED);
        assertThat(fromDownloading.cancel().getState()).isEqualTo(CANCELLED);
    }

    @Test
    public void testCancelledToFailedTransitionTolerance()
    {
        // Test the special case where CANCELLED -> FAILED transition is tolerated
        // This happens when a task is cancelled but some downloads are still in progress
        // and they subsequently fail
        OperationStatus cancelledStatus = OperationStatus.getStartingState().cancel();
        assertThat(cancelledStatus.getState()).isEqualTo(CANCELLED);
        
        // Attempting to transition from CANCELLED to FAILED should return CANCELLED state
        OperationStatus afterFailureAttempt = cancelledStatus.tryFailureState();
        assertThat(afterFailureAttempt.getState()).isEqualTo(CANCELLED);

        assertThat(afterFailureAttempt).isNotSameAs(cancelledStatus);
        
        assertThat(afterFailureAttempt.totalSize()).isEqualTo(cancelledStatus.totalSize());
        assertThat(afterFailureAttempt.totalFiles()).isEqualTo(cancelledStatus.totalFiles());
    }

    public void assertIllegalStateTransition(ThrowableAssert.ThrowingCallable callable)
    {
        assertThatExceptionOfType(OperationStatus.IllegalStateTransitionException.class).isThrownBy(callable);
    }
}
