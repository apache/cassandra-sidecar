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

package org.apache.cassandra.sidecar.job;

import java.util.Collection;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;

import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link LocalJobManager}
 */
class LocalJobManagerTest
{
    private static final UUID OPERATION_ID = UUID.randomUUID();
    private static final UUID NODE_ID_1 = UUID.randomUUID();
    private static final UUID NODE_ID_2 = UUID.randomUUID();

    private Vertx vertx;
    private ExecutorPools executorPools;
    private LocalJobManager manager;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());
        manager = new LocalJobManager(executorPools);
    }

    @AfterEach
    void cleanup()
    {
        TestResourceReaper.create().with(vertx).with(executorPools).close();
    }

    @Test
    void testSubmitJobExecutesAsync()
    {
        LocalJob job = createTestJob(NODE_ID_1);
        Future<Void> result = manager.submitJob(job);
        loopAssert(2, () -> {
            assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
            assertThat(result.succeeded()).isTrue();
        });
    }

    @Test
    void testSubmitDuplicateDoesNotReplaceExisting()
    {
        LocalJob first = createTestJob(NODE_ID_1);
        LocalJob second = createTestJob(NODE_ID_1);

        manager.submitJob(first);
        Future<Void> duplicate = manager.submitJob(second);

        assertThat(manager.getJob(OPERATION_ID, NODE_ID_1)).isSameAs(first);
        assertThat(duplicate.succeeded()).isTrue();
        assertThat(second.status()).isEqualTo(OperationalJobStatus.CREATED);
    }

    @Test
    void testGetJobReturnsActiveJob()
    {
        LocalJob job = createTestJob(NODE_ID_1);
        manager.submitJob(job);
        assertThat(manager.getJob(OPERATION_ID, NODE_ID_1)).isSameAs(job);
    }

    @Test
    void testGetJobReturnsNullWhenNone()
    {
        assertThat(manager.getJob(OPERATION_ID, NODE_ID_1)).isNull();
    }

    @Test
    void testCompletedJobIsStillTrackedUntilRemoveJob()
    {
        LocalJob job = createTestJob(NODE_ID_1);
        manager.submitJob(job);
        loopAssert(2, () -> assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED));

        assertThat(manager.getJob(OPERATION_ID, NODE_ID_1)).isSameAs(job);

        assertThat(manager.removeJob(OPERATION_ID, NODE_ID_1, job)).isTrue();
        assertThat(manager.getJob(OPERATION_ID, NODE_ID_1)).isNull();
    }

    @Test
    void testRemoveJobWithDuplicateHandleKeepsTrackedJob()
    {
        LocalJob tracked = createTestJob(NODE_ID_1);
        LocalJob duplicate = createTestJob(NODE_ID_1);
        manager.submitJob(tracked);
        manager.submitJob(duplicate);

        assertThat(manager.removeJob(OPERATION_ID, NODE_ID_1, duplicate)).isFalse();
        assertThat(manager.getJob(OPERATION_ID, NODE_ID_1)).isSameAs(tracked);
    }

    @Test
    void testActiveJobsReturnsAllTracked()
    {
        LocalJob job1 = createTestJob(NODE_ID_1);
        LocalJob job2 = createTestJob(NODE_ID_2);
        manager.submitJob(job1);
        manager.submitJob(job2);

        Collection<LocalJob> active = manager.activeJobs();
        assertThat(active).containsExactlyInAnyOrder(job1, job2);
    }

    @Test
    void testSameNodeAcrossOperationsTrackedIndependently()
    {
        UUID otherOperationId = UUID.randomUUID();
        LocalJob job1 = createTestJob(OPERATION_ID, NODE_ID_1);
        LocalJob job2 = createTestJob(otherOperationId, NODE_ID_1);

        manager.submitJob(job1);
        manager.submitJob(job2);

        assertThat(manager.getJob(OPERATION_ID, NODE_ID_1)).isSameAs(job1);
        assertThat(manager.getJob(otherOperationId, NODE_ID_1)).isSameAs(job2);
        assertThat(manager.activeJobs()).containsExactlyInAnyOrder(job1, job2);

        assertThat(manager.removeJob(OPERATION_ID, NODE_ID_1, job1)).isTrue();
        assertThat(manager.getJob(OPERATION_ID, NODE_ID_1)).isNull();
        assertThat(manager.getJob(otherOperationId, NODE_ID_1)).isSameAs(job2);
    }

    private static LocalJob createTestJob(UUID nodeId)
    {
        return createTestJob(OPERATION_ID, nodeId);
    }

    private static LocalJob createTestJob(UUID operationId, UUID nodeId)
    {
        return new LocalJob(operationId, nodeId, "127.0.0.1", OperationType.DRAIN)
        {
            @Override
            protected void executeInternal()
            {
            }
        };
    }
}
