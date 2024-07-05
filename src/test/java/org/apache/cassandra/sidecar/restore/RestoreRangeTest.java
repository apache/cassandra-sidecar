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

package org.apache.cassandra.sidecar.restore;

import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.ConsistencyLevel;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.response.data.RestoreRangeJson;
import org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobTest;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test cases for RestoreRange
 */
public class RestoreRangeTest
{
    @Test
    void testEquals()
    {
        RestoreRange range1 = createTestRange();
        RestoreRange range2 = range1.unbuild().build();
        assertThat(range1).isEqualTo(range2);

        RestoreRange range3 = range1.unbuild()
                                    .jobId(UUIDs.timeBased())
                                    .bucketId((short) 2)
                                    .startToken(BigInteger.valueOf(2)).endToken(BigInteger.TEN)
                                    .build();
        assertThat(range3).isNotEqualTo(range1)
                          .isNotEqualTo(range2);
    }

    @Test
    void testCreateTaskFromRange() throws Exception
    {
        RestoreRange range = createTestRange();
        RestoreRangeHandler task = createRestoreRangeHandler(range);
        assertThat(task).describedAs("It should create RestoreRangeTask successfully")
                        .isInstanceOf(RestoreRangeTask.class);
        assertThat(task.elapsedInNanos()).isEqualTo(-1); // not started
        assertThat(task.range()).isSameAs(range);
    }

    @Test
    void testCreateTaskFailsFromCancelledRange() throws Exception
    {
        RestoreRange range = createTestRange();
        range.cancel();
        RestoreRangeHandler handler = createRestoreRangeHandler(range);
        assertFailedHandler(range, handler, "Restore range is cancelled");
    }

    @Test
    void testCreateTaskFailsWhenMissingTacker() throws Exception
    {
        RestoreRange range = createTestRange().unbuild()
                                              // nullify the tracker, i.e. not registered
                                              .restoreJobProgressTracker(null)
                                              .build();
        RestoreRangeHandler handler = createRestoreRangeHandler(range);
        assertFailedHandler(range, handler, "Restore range is missing progress tracker or source slice");
    }

    @Test
    void testCreateTaskFailsWhenMissingSourceSlice() throws Exception
    {
        RestoreRange range = createTestRange().unbuild()
                                              // unset source slice, i.e. nullify slice
                                              .unsetSourceSlice()
                                              .build();
        RestoreRangeHandler handler = createRestoreRangeHandler(range);
        assertFailedHandler(range, handler, "Restore range is missing progress tracker or source slice");
    }

    @Test
    void testCreateTaskFailsToCreate() throws Exception
    {
        /*
         * The test create the test range that belongs to a job that is managed by Sidecar.
         * However, the range is configured with `null` RestoreRangeDatabaseAccessor.
         * It should fail when creating the RestoreRangeTask
         */
        RestoreRange range = createTestRange(true);
        RestoreRangeHandler handler = createRestoreRangeHandler(range);
        assertFailedHandler(range, handler, "Restore range is failed");
    }

    @Test
    void testToJson()
    {
        RestoreRange range = createTestRange();
        RestoreRangeJson restoreRangeJson = range.toJson();
        assertThat(restoreRangeJson.sliceId()).isEqualTo(range.sliceId());
        assertThat((short) restoreRangeJson.bucketId()).isEqualTo(range.bucketId());
        assertThat(restoreRangeJson.key()).isEqualTo(range.sliceKey());
        assertThat(restoreRangeJson.bucket()).isEqualTo(range.sliceBucket());
        assertThat(restoreRangeJson.startToken()).isEqualTo(range.startToken());
        assertThat(restoreRangeJson.endToken()).isEqualTo(range.endToken());
    }

    private void assertFailedHandler(RestoreRange range, RestoreRangeHandler handler, String containsErrorMessage)
    {
        assertThat(handler).describedAs("It should create a Failed handler")
                           .isInstanceOf(RestoreRangeTask.Failed.class);
        assertThat(handler.elapsedInNanos()).describedAs("A failed task should have elapsed time fixed to 0")
                                            .isZero();
        assertThat(handler.range()).isSameAs(range);
        Promise<RestoreRange> promise = Promise.promise();
        handler.handle(promise);
        Future<RestoreRange> future = promise.future();
        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).hasMessageContaining(containsErrorMessage);
    }

    private RestoreRangeHandler createRestoreRangeHandler(RestoreRange range) throws Exception
    {
        StorageClientPool mockClientPool = mock(StorageClientPool.class);
        when(mockClientPool.storageClient(any())).thenReturn(mock(StorageClient.class));
        return range.toAsyncTask(mockClientPool, null, null, 0.1,
                                 null, null, null,
                                 mock(SidecarMetrics.class, RETURNS_DEEP_STUBS));
    }

    public static RestoreRange createTestRange()
    {
        return createTestRange(false);
    }

    public static RestoreRange createTestRange(long start, long end)
    {
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED, null);
        return createTestRange(job, Paths.get("."), false, start, end);
    }

    public static RestoreRange createTestRange(boolean jobManagedBySidecar)
    {
        return createTestRange(Paths.get("."), jobManagedBySidecar);
    }

    public static RestoreRange createTestRange(Path rootDir, boolean jobManagedBySidecar)
    {
        RestoreJob job = RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED, null);
        return createTestRange(job, rootDir, jobManagedBySidecar);
    }

    public static RestoreRange createTestRange(RestoreJob job, Path rootDir, boolean jobManagedBySidecar)
    {
        return createTestRange(job, rootDir, jobManagedBySidecar, 1L, 2L);
    }

    public static RestoreRange createTestRange(RestoreJob job, Path rootDir, boolean jobManagedBySidecar, long start, long end)
    {
        if (jobManagedBySidecar)
        {
            job = job.unbuild().consistencyLevel(ConsistencyLevel.QUORUM).build();
        }
        RestoreProcessor mockProcessor = mock(RestoreProcessor.class);
        InstanceMetadata mockInstance = mock(InstanceMetadata.class);
        when(mockInstance.id()).thenReturn(1);
        RestoreJobProgressTracker tracker = new RestoreJobProgressTracker(job, mockProcessor, mockInstance);
        RestoreSlice slice = RestoreSlice.builder()
                                         .jobId(job.jobId).keyspace("keyspace").table("table")
                                         .sliceId("sliceId-123").bucketId((short) 0)
                                         .storageBucket("myBucket").storageKey("myKey").checksum("checksum")
                                         .startToken(BigInteger.valueOf(start)).endToken(BigInteger.valueOf(end))
                                         .build();

        return RestoreRange.builderFromSlice(slice)
                           .stageDirectory(rootDir, "uploadId")
                           .replicaStatus(Collections.singletonMap("replica1", RestoreRangeStatus.CREATED))
                           .restoreJobProgressTracker(tracker)
                           .ownerInstance(mockInstance)
                           .build();
    }
}
