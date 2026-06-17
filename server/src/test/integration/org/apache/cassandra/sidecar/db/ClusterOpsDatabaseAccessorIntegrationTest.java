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

package org.apache.cassandra.sidecar.db;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link ClusterOpsDatabaseAccessor}
 */
class ClusterOpsDatabaseAccessorIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void testCrudOperations()
    {
        waitForSchemaReady(10, TimeUnit.SECONDS);

        ClusterOpsDatabaseAccessor accessor = injector.getInstance(ClusterOpsDatabaseAccessor.class);
        String clusterName = maybeGetSession().getCluster().getMetadata().getClusterName();

        assertThat(accessor.findJob(clusterName, UUIDs.timeBased()))
                .withFailMessage("findJob should return null for a job ID that was never persisted")
                .isNull();

        assertThat(accessor.findAllJobs(clusterName, 10))
                .withFailMessage("findAllJobs should return an empty list when no jobs exist")
                .isEmpty();

        UUID jobId1 = UUIDs.timeBased();
        List<List<UUID>> nodeOrder = Arrays.asList(
                Arrays.asList(UUID.randomUUID(), UUID.randomUUID()),
                Arrays.asList(UUID.randomUUID())
        );
        Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
        OperationalJobRecord job1 = new OperationalJobRecord(jobId1, OperationType.REPAIR, OperationalJobStatus.CREATED,
                                                             null, Instant.now(), null, nodeOrder, metadata);
        accessor.persistJob(clusterName, job1);

        OperationalJobRecord found = accessor.findJob(clusterName, jobId1);
        assertThat(found)
                .withFailMessage("findJob should return the persisted job with all fields intact")
                .isNotNull()
                .satisfies(job -> {
                    assertThat(job.jobId()).isEqualTo(jobId1);
                    assertThat(job.operationType()).isEqualTo(OperationType.REPAIR);
                    assertThat(job.status()).isEqualTo(OperationalJobStatus.CREATED);
                    assertThat(job.startTime()).isNull();
                    assertThat(job.lastUpdate()).isNotNull();
                    assertThat(job.failureReason()).isNull();
                    assertThat(job.nodeExecutionOrder()).isEqualTo(nodeOrder);
                    assertThat(job.operationMetadata()).isEqualTo(metadata);
                });

        UUID jobId2 = UUIDs.timeBased();
        OperationalJobRecord job2 = new OperationalJobRecord(jobId2, OperationType.DECOMMISSION, OperationalJobStatus.CREATED);
        accessor.persistJob(clusterName, job2);

        OperationalJobRecord found2 = accessor.findJob(clusterName, jobId2);
        assertThat(found2)
                .withFailMessage("findJob should return the persisted job with null nullable fields preserved")
                .isNotNull()
                .satisfies(job -> {
                    assertThat(job.jobId()).isEqualTo(jobId2);
                    assertThat(job.operationType()).isEqualTo(OperationType.DECOMMISSION);
                    assertThat(job.nodeExecutionOrder()).isNull();
                    assertThat(job.operationMetadata()).isNull();
                });

        Instant beforeRunning = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        accessor.updateJobStatus(clusterName, jobId1, OperationType.REPAIR, OperationalJobStatus.RUNNING, null);
        OperationalJobRecord updated = accessor.findJob(clusterName, jobId1);
        assertThat(updated)
                .withFailMessage("findJob should reflect the updated status while preserving other fields")
                .isNotNull()
                .satisfies(job -> {
                    assertThat(job.status()).isEqualTo(OperationalJobStatus.RUNNING);
                    assertThat(job.startTime()).isNotNull();
                    assertThat(job.startTime()).isAfterOrEqualTo(beforeRunning);
                    assertThat(job.lastUpdate()).isAfterOrEqualTo(beforeRunning);
                    assertThat(job.nodeExecutionOrder()).isEqualTo(nodeOrder);
                });

        Instant firstStartTime = updated.startTime();
        accessor.updateJobStatus(clusterName, jobId1, OperationType.REPAIR, OperationalJobStatus.RUNNING, null);
        OperationalJobRecord afterSecondRunning = accessor.findJob(clusterName, jobId1);
        assertThat(afterSecondRunning.startTime())
                .withFailMessage("start_time should not change on subsequent RUNNING updates")
                .isEqualTo(firstStartTime);
        assertThat(afterSecondRunning.lastUpdate())
                .withFailMessage("last_update should advance on each status update")
                .isAfterOrEqualTo(updated.lastUpdate());

        accessor.updateJobStatus(clusterName, jobId1, OperationType.REPAIR, OperationalJobStatus.FAILED, "node unreachable");
        OperationalJobRecord failed = accessor.findJob(clusterName, jobId1);
        assertThat(failed)
                .isNotNull()
                .satisfies(job -> {
                    assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
                    assertThat(job.failureReason()).isEqualTo("node unreachable");
                    assertThat(job.startTime()).isEqualTo(firstStartTime);
                });

        UUID jobId3 = UUIDs.timeBased();
        accessor.persistJob(clusterName, new OperationalJobRecord(jobId3, OperationType.REPAIR, OperationalJobStatus.CREATED));
        List<OperationalJobRecord> allJobs = accessor.findAllJobs(clusterName, 10);
        assertThat(allJobs)
                .withFailMessage("findAllJobs should return every persisted job when the limit exceeds total count")
                .hasSize(3);

        List<OperationalJobRecord> limited = accessor.findAllJobs(clusterName, 2);
        assertThat(limited)
                .withFailMessage("findAllJobs should truncate results to the specified limit")
                .hasSize(2);

        assertThat(allJobs.get(0).jobId())
                .withFailMessage("findAllJobs should return the most recently created job first")
                .isEqualTo(jobId3);

        OperationalJobRecord job1Updated = new OperationalJobRecord(jobId1, OperationType.REPAIR,
                                                                     OperationalJobStatus.SUCCEEDED,
                                                                     null, Instant.now(), null, nodeOrder, metadata);
        accessor.persistJob(clusterName, job1Updated);
        OperationalJobRecord afterUpsert = accessor.findJob(clusterName, jobId1);
        assertThat(afterUpsert)
                .withFailMessage("findJob should reflect the overwritten status after upsert with the same job ID")
                .isNotNull()
                .satisfies(job -> {
                    assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
                });
    }
}
