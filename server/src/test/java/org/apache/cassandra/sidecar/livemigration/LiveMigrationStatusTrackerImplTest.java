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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.ExecutorPoolsHelper;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.LiveMigrationStatus;
import org.apache.cassandra.sidecar.common.response.LiveMigrationStatus.MigrationState;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;

import static org.apache.cassandra.sidecar.common.response.LiveMigrationStatus.NOT_COMPLETED_STATUS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LiveMigrationStatusTrackerImplTest
{
    final Vertx vertx = Vertx.vertx();
    final ExecutorPools executorPools = ExecutorPoolsHelper.createdSharedTestPool(vertx);

    @Test
    void testHappyPath(@TempDir Path tempDir) throws InterruptedException
    {
        // Sequence of events:
        // Get the status (should be NOT_COMPLETED)
        // Set the status
        // Get the status again (should be COMPLETED)
        // Clear the status
        // Get the status again (should be NOT_COMPLETED)
        LiveMigrationStatusTrackerImpl tracker = new LiveMigrationStatusTrackerImpl(executorPools);
        InstanceMetadata mockInstanceMetadata = mock(InstanceMetadata.class);
        when(mockInstanceMetadata.host()).thenReturn("test-host");
        when(mockInstanceMetadata.stagingDir()).thenReturn(tempDir.toString());

        // hasMigrationCompleted should return false before setting the status
        assertThat(awaitForFuture(tracker.hasMigrationCompleted(mockInstanceMetadata)).result()).isFalse();

        // Get the status before setting the status
        Future<LiveMigrationStatus> future = tracker.getMigrationStatus(mockInstanceMetadata);
        awaitForFuture(future);
        assertThat(future.result().state()).isEqualTo(MigrationState.NOT_COMPLETED);
        assertThat(future.result().endTime()).isNull();

        // Set the status
        long endTime = System.currentTimeMillis();
        LiveMigrationStatus liveMigrationStatus = new LiveMigrationStatus(MigrationState.COMPLETED, endTime);
        Future<Void> setStatusFuture = tracker.setMigrationStatus(mockInstanceMetadata, liveMigrationStatus);
        awaitForFuture(setStatusFuture);
        assertThat(Files.exists(tempDir.resolve(LiveMigrationStatusTrackerImpl.STATUS_FILE_NAME))).isTrue();

        // hasMigrationCompleted should return true after setting the status.
        assertThat(awaitForFuture(tracker.hasMigrationCompleted(mockInstanceMetadata)).result()).isTrue();

        // Get the status again, now the status should be completed
        Future<LiveMigrationStatus> newStatusFuture = tracker.getMigrationStatus(mockInstanceMetadata);
        awaitForFuture(newStatusFuture);
        assertThat(newStatusFuture.result().state()).isEqualTo(MigrationState.COMPLETED);
        assertThat(newStatusFuture.result().endTime()).isEqualTo(endTime);

        // Now clear the status
        Future<Void> clearStatusFuture = tracker.clearMigrationStatus(mockInstanceMetadata);
        awaitForFuture(clearStatusFuture);
        assertThat(Files.exists(tempDir.resolve(LiveMigrationStatusTrackerImpl.STATUS_FILE_NAME))).isFalse();
        assertThat(clearStatusFuture.succeeded()).isTrue();

        // hasMigrationCompleted should return false after clearing the status.
        assertThat(awaitForFuture(tracker.hasMigrationCompleted(mockInstanceMetadata)).result()).isFalse();

        // Get the status again, now the status should be NOT_COMPLETED
        Future<LiveMigrationStatus> statusFuture = tracker.getMigrationStatus(mockInstanceMetadata);
        awaitForFuture(statusFuture);
        assertThat(statusFuture.result().state()).isEqualTo(MigrationState.NOT_COMPLETED);
        assertThat(statusFuture.result().endTime()).isNull();
    }

    @Test
    void testClearStatusBeforeUpdatingStatus(@TempDir Path tempDir) throws InterruptedException
    {
        LiveMigrationStatusTrackerImpl tracker = new LiveMigrationStatusTrackerImpl(executorPools);
        InstanceMetadata mockInstanceMetadata = mock(InstanceMetadata.class);
        when(mockInstanceMetadata.host()).thenReturn("test-host");
        when(mockInstanceMetadata.stagingDir()).thenReturn(tempDir.toString());

        assertThat(Files.exists(tempDir.resolve(LiveMigrationStatusTrackerImpl.STATUS_FILE_NAME))).isFalse();

        // Clear the status before updating the status
        Future<Void> clearStatusFuture = tracker.clearMigrationStatus(mockInstanceMetadata);
        awaitForFuture(clearStatusFuture);

        assertThat(clearStatusFuture.failed()).isTrue();
        assertThat(clearStatusFuture.cause()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testClearStatusBeforeMarkingMigrationAsComplete(@TempDir Path tempDir) throws InterruptedException
    {
        LiveMigrationStatusTrackerImpl tracker = new LiveMigrationStatusTrackerImpl(executorPools);
        InstanceMetadata mockInstanceMetadata = mock(InstanceMetadata.class);
        when(mockInstanceMetadata.host()).thenReturn("test-host");
        when(mockInstanceMetadata.stagingDir()).thenReturn(tempDir.toString());

        assertThat(Files.exists(tempDir.resolve(LiveMigrationStatusTrackerImpl.STATUS_FILE_NAME))).isFalse();

        // Explicitly update the status as not completed
        Future<Void> setStatusFuture = tracker.setMigrationStatus(mockInstanceMetadata, NOT_COMPLETED_STATUS);
        awaitForFuture(setStatusFuture);
        assertThat(setStatusFuture.succeeded()).isTrue();

        // Now the LiveMigration status file should exist, but the status should be NOT_COMPLETED
        assertThat(Files.exists(tempDir.resolve(LiveMigrationStatusTrackerImpl.STATUS_FILE_NAME))).isTrue();

        // hasMigrationCompleted should return false
        assertThat(awaitForFuture(tracker.hasMigrationCompleted(mockInstanceMetadata)).result()).isFalse();

        // Clear the status before updating the status
        Future<Void> clearStatusFuture = tracker.clearMigrationStatus(mockInstanceMetadata);
        awaitForFuture(clearStatusFuture);
        assertThat(clearStatusFuture.failed()).isTrue();
        assertThat(clearStatusFuture.cause()).isInstanceOf(IllegalStateException.class);

        // status file should exist as clearing status failed.
        assertThat(Files.exists(tempDir.resolve(LiveMigrationStatusTrackerImpl.STATUS_FILE_NAME))).isTrue();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private <T> Future<T> awaitForFuture(Future<T> future) throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(1);
        future.onComplete(res -> latch.countDown());

        latch.await(1000, TimeUnit.MILLISECONDS);
        return future;
    }
}
