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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link ClusterOpsDatabaseAccessor}
 */
class ClusterOpsDatabaseAccessorIntTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void testCrudOperations()
    {
        waitForSchemaReady(10, TimeUnit.SECONDS);

        ClusterOpsDatabaseAccessor accessor = injector.getInstance(ClusterOpsDatabaseAccessor.class);
        String clusterName = maybeGetSession().getCluster().getMetadata().getClusterName();

        // findJob returns null for non-existent job
        assertThat(accessor.findJob(clusterName, UUIDs.timeBased())).isNull();

        // findAllJobs returns empty when no jobs exist
        assertThat(accessor.findAllJobs(clusterName, 10)).isEmpty();

        // persistJob + findJob with all fields supplied
        UUID jobId1 = UUIDs.timeBased();
        List<List<UUID>> nodeOrder = Arrays.asList(
                Arrays.asList(UUID.randomUUID(), UUID.randomUUID()),
                Arrays.asList(UUID.randomUUID())
        );
        Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
        OperationalJobRecord job1 = new OperationalJobRecord(jobId1, "restart", OperationalJobStatus.CREATED,
                                                             nodeOrder, metadata);
        accessor.persistJob(clusterName, job1);

        OperationalJobRecord found = accessor.findJob(clusterName, jobId1);
        assertThat(found).isNotNull();
        assertThat(found.jobId()).isEqualTo(jobId1);
        assertThat(found.operationType()).isEqualTo("restart");
        assertThat(found.status()).isEqualTo(OperationalJobStatus.CREATED);
        assertThat(found.nodeExecutionOrder()).isEqualTo(nodeOrder);
        assertThat(found.operationMetadata()).isEqualTo(metadata);

        // persistJob with null nullable fields
        UUID jobId2 = UUIDs.timeBased();
        OperationalJobRecord job2 = new OperationalJobRecord(jobId2, "decommission", OperationalJobStatus.CREATED);
        accessor.persistJob(clusterName, job2);

        OperationalJobRecord found2 = accessor.findJob(clusterName, jobId2);
        assertThat(found2).isNotNull();
        assertThat(found2.jobId()).isEqualTo(jobId2);
        assertThat(found2.operationType()).isEqualTo("decommission");
        assertThat(found2.nodeExecutionOrder()).isNull();
        assertThat(found2.operationMetadata()).isNull();

        // updateJobStatus
        accessor.updateJobStatus(clusterName, jobId1, "restart", OperationalJobStatus.RUNNING);
        OperationalJobRecord updated = accessor.findJob(clusterName, jobId1);
        assertThat(updated).isNotNull();
        assertThat(updated.status()).isEqualTo(OperationalJobStatus.RUNNING);
        assertThat(updated.nodeExecutionOrder()).isEqualTo(nodeOrder);

        // findAllJobs returns all jobs
        UUID jobId3 = UUIDs.timeBased();
        accessor.persistJob(clusterName, new OperationalJobRecord(jobId3, "restart", OperationalJobStatus.CREATED));
        List<OperationalJobRecord> allJobs = accessor.findAllJobs(clusterName, 10);
        assertThat(allJobs).hasSize(3);

        // findAllJobs respects limit
        List<OperationalJobRecord> limited = accessor.findAllJobs(clusterName, 2);
        assertThat(limited).hasSize(2);

        // findAllJobs returns in DESC order by operation_id (most recent first)
        assertThat(allJobs.get(0).jobId()).isEqualTo(jobId3);

        // persistJob upsert: same job ID with different status overwrites
        OperationalJobRecord job1Updated = new OperationalJobRecord(jobId1, "restart",
                                                                     OperationalJobStatus.SUCCEEDED,
                                                                     nodeOrder, metadata);
        accessor.persistJob(clusterName, job1Updated);
        OperationalJobRecord afterUpsert = accessor.findJob(clusterName, jobId1);
        assertThat(afterUpsert).isNotNull();
        assertThat(afterUpsert.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
    }
}
