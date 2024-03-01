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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.ExecutorPoolsHelper;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobTest;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.exceptions.RestoreJobException;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.server.MainModule;

import static org.apache.cassandra.sidecar.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RestoreJobManagerTest
{
    private RestoreJobManager manager;
    private Vertx vertx;
    private ExecutorPools executorPools;
    private static final int jobRecencyDays = 1;

    @TempDir
    private Path testDir;

    @BeforeEach
    void setup()
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(new TestModule()));
        vertx = injector.getInstance(Vertx.class);
        executorPools = ExecutorPoolsHelper.createdSharedTestPool(vertx);
        RestoreProcessor processor = mock(RestoreProcessor.class);
        InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
        when(instanceMetadata.stagingDir()).thenReturn(testDir.toString());

        RestoreJobConfiguration restoreJobConfiguration = mock(RestoreJobConfiguration.class);
        when(restoreJobConfiguration.jobDiscoveryActiveLoopDelayMillis()).thenReturn(0L);
        when(restoreJobConfiguration.jobDiscoveryIdleLoopDelayMillis()).thenReturn(0L);
        when(restoreJobConfiguration.jobDiscoveryRecencyDays()).thenReturn(jobRecencyDays);
        when(restoreJobConfiguration.processMaxConcurrency()).thenReturn(0);
        when(restoreJobConfiguration.restoreJobTablesTtlSeconds()).thenReturn(TimeUnit.DAYS.toSeconds(14) + 1);

        manager = new RestoreJobManager(restoreJobConfiguration,
                                        instanceMetadata,
                                        executorPools,
                                        processor,
                                        false /* do not trigger the first deletion */);
    }

    @AfterEach
    void teardown()
    {
        // close in the fire-and-forget way
        executorPools.close();
        vertx.close();
    }

    @Test
    void testTrySubmit() throws RestoreJobException
    {
        // submit the first time
        RestoreRange range = getTestRange();
        assertThat(manager.trySubmit(range, range.job()))
        .isEqualTo(RestoreJobProgressTracker.Status.CREATED);

        // submit twice
        assertThat(manager.trySubmit(range, range.job()))
        .isEqualTo(RestoreJobProgressTracker.Status.PENDING);

        range.complete();
        assertThat(manager.trySubmit(range, range.job()))
        .isEqualTo(RestoreJobProgressTracker.Status.COMPLETED);
    }

    @Test
    void testTrySubmitAfterJobFailure() throws RestoreJobException
    {
        RestoreRange range = getTestRange();
        assertThat(manager.trySubmit(range, range.job()))
        .isEqualTo(RestoreJobProgressTracker.Status.CREATED);

        RestoreJobFatalException failure = new RestoreJobFatalException("fatal");
        range.fail(failure);
        assertThatThrownBy(() -> manager.trySubmit(range, range.job()))
        .isSameAs(failure);

        // submitting other ranges in the same job should fail too
        RestoreRange anotherRange = getTestRange(range.job());
        assertThatThrownBy(() -> manager.trySubmit(anotherRange, anotherRange.job()))
        .describedAs("Once a range failed, no more range can be submitted")
        .isSameAs(failure);

        // however, ranges from a different job are still permitted
        RestoreRange rangeOfDifferentJob = getTestRange();
        assertThat(manager.trySubmit(rangeOfDifferentJob, rangeOfDifferentJob.job()))
        .isEqualTo(RestoreJobProgressTracker.Status.CREATED);
    }

    @Test
    void testRemoveJobInternal() throws RestoreJobException
    {
        RestoreRange range = getTestRange();
        assertThat(manager.trySubmit(range, range.job()))
        .isEqualTo(RestoreJobProgressTracker.Status.CREATED);

        assertThat(range.isCancelled()).isFalse();

        manager.removeJobInternal(range.jobId()); // it cancels the non-completed ranges

        // removeJobInternal runs async. Wait for at most 2 seconds for the slice to be cancelled
        loopAssert(2, () -> assertThat(range.isCancelled()).isTrue());
    }

    @Test
    void testUpdateRestoreJobForSubmittedRange() throws RestoreJobFatalException
    {
        // test setup and submit range
        RestoreRange range = getTestRange();
        RestoreJob job = range.job();
        assertThat(manager.trySubmit(range, job))
        .isEqualTo(RestoreJobProgressTracker.Status.CREATED);

        assertThat(range.job()).isNotNull();
        assertThat(range.job()).isSameAs(job);

        // update with the same job, it should read the same job reference back from the range
        manager.updateRestoreJob(job);
        assertThat(range.job()).isSameAs(job);

        // update with the updated job, it should read the reference of the update job from the range
        RestoreJob updatedJob = RestoreJobTest.createNewTestingJob(range.jobId());
        manager.updateRestoreJob(updatedJob);
        assertThat(range.job()).isNotSameAs(job);
        assertThat(range.job()).isSameAs(updatedJob);
    }

    @Test
    void testCheckDirectoryIsObsolete() throws IOException
    {
        Path jobDir = newDir(RestoreJobUtil.prefixedJobId(UUIDs.startOf(System.currentTimeMillis())));
        // not old enough
        assertThat(manager.isObsoleteRestoreJobDir(jobDir)).isFalse();
        // still not old enough (not 1 day yet)
        jobDir = newDir(RestoreJobUtil.prefixedJobId(UUIDs.startOf(System.currentTimeMillis()
                                                                   - TimeUnit.DAYS.toMillis(jobRecencyDays)
                                                                   + 9000)));
        assertThat(manager.isObsoleteRestoreJobDir(jobDir)).isFalse();

        // invalid format: missing 'restore-' prefix
        jobDir = newDir(UUIDs.startOf(System.currentTimeMillis()
                                      - TimeUnit.DAYS.toMillis(jobRecencyDays + 1)).toString());
        assertThat(manager.isObsoleteRestoreJobDir(jobDir)).isFalse();
        // invalid format
        jobDir = newDir("foo");
        assertThat(manager.isObsoleteRestoreJobDir(jobDir)).isFalse();
        // invalid format: not timeuuid
        jobDir = newDir(RestoreJobUtil.prefixedJobId(UUID.randomUUID()));
        assertThat(manager.isObsoleteRestoreJobDir(jobDir)).isFalse();
        // dir not exist
        jobDir = testDir.resolve("I_do_not_exist");
        assertThat(manager.isObsoleteRestoreJobDir(jobDir)).isFalse();
        // it is not a directory
        jobDir = testDir.resolve("I_am_file");
        assertThat(jobDir.toFile().createNewFile()).isTrue();
        assertThat(manager.isObsoleteRestoreJobDir(jobDir)).isFalse();


        // format is good; directory is older than jobRecencyDays
        jobDir = newDir(RestoreJobUtil.prefixedJobId(UUIDs.startOf(System.currentTimeMillis()
                                                                   - TimeUnit.DAYS.toMillis(jobRecencyDays)
                                                                   - 1)));
        assertThat(manager.isObsoleteRestoreJobDir(jobDir)).isTrue();
    }

    @Test
    void testDeleteObsoleteData() throws IOException
    {
        long nowMillis = System.currentTimeMillis();
        Path oldJobDir = newDir(RestoreJobUtil.prefixedJobId(UUIDs.startOf(nowMillis
                                                                           - TimeUnit.DAYS.toMillis(jobRecencyDays)
                                                                           - 1)));
        createFileInDirectory(oldJobDir, 5);

        Path olderJobDir
        = newDir(RestoreJobUtil.prefixedJobId(UUIDs.startOf(nowMillis
                                                            - TimeUnit.DAYS.toMillis(jobRecencyDays + 1))));
        createFileInDirectory(olderJobDir, 5);

        Path newJobDir = newDir(RestoreJobUtil.prefixedJobId(UUIDs.startOf(nowMillis)));
        createFileInDirectory(newJobDir, 5);

        manager.deleteObsoleteDataAsync();
        loopAssert(3, 10, () -> {
            assertThat(Files.exists(oldJobDir)).describedAs("Should be deleted").isFalse();
            assertThat(Files.exists(olderJobDir)).describedAs("Should be deleted").isFalse();
            assertThat(Files.exists(newJobDir)).describedAs("Should survive").isTrue();
            assertThat(newJobDir.toFile().list())
            .describedAs("Should have 5 files intact")
            .hasSize(5);
        });
    }

    private RestoreRange getTestRange()
    {
        return getTestRange(RestoreJobTest.createNewTestingJob(UUIDs.timeBased()));
    }

    private RestoreRange getTestRange(RestoreJob job)
    {
        InstanceMetadata owner = mock(InstanceMetadata.class);
        when(owner.id()).thenReturn(1);
        RestoreSlice slice = RestoreSlice
                             .builder()
                             .jobId(job.jobId)
                             .sliceId("testSliceId")
                             .bucketId((short) 0)
                             .storageKey("storageKey")
                             .storageBucket("storageBucket")
                             .build();
        RestoreJobProgressTracker tracker = manager.progressTrackerUnsafe(job);
        return RestoreRange.builderFromEntireSlice(slice)
                           .restoreJobProgressTracker(tracker)
                           .ownerInstance(owner)
                           .stageDirectory(testDir, "uploadId")
                           .replicaStatus(Collections.emptyMap())
                           .build();
    }

    private Path newDir(String name) throws IOException
    {
        Path dir = testDir.resolve(name);
        Files.createDirectories(dir);
        return dir;
    }

    private void createFileInDirectory(Path path, int nFiles) throws IOException
    {
        for (int i = 0; i < nFiles; i++)
        {
            Files.createFile(Paths.get(path.toString(), "file" + i));
        }
        assertThat(path.toFile().list())
        .describedAs("listing files in " + path.toAbsolutePath())
        .hasSize(nFiles);
    }
}
