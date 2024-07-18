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

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Future;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobTest;
import org.apache.cassandra.sidecar.restore.RingTopologyRefresher.ReplicaByTokenRangePerKeyspace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class RingTopologyRefresherTest
{
    private final ReplicaByTokenRangePerKeyspace replicaByTokenRangePerKeyspace = new ReplicaByTokenRangePerKeyspace();

    @Test
    void testRegisterJobs()
    {
        assertThat(replicaByTokenRangePerKeyspace.isEmpty()).isTrue();

        // job1 and job2 belongs to the same keyspace
        RestoreJob job1 = RestoreJobTest.createNewTestingJob(UUIDs.timeBased());
        RestoreJob job2 = RestoreJobTest.createNewTestingJob(UUIDs.timeBased());
        replicaByTokenRangePerKeyspace.register(job1);
        assertThat(replicaByTokenRangePerKeyspace.isEmpty()).isFalse();
        assertThat(replicaByTokenRangePerKeyspace.allJobsUnsafe()).containsExactlyInAnyOrder(job1.jobId);
        assertThat(replicaByTokenRangePerKeyspace.jobsByKeyspaceUnsafe()).containsOnlyKeys(job1.keyspaceName);
        assertThat(replicaByTokenRangePerKeyspace.jobsByKeyspaceUnsafe().get(job1.keyspaceName)).containsExactlyInAnyOrder(job1.jobId);

        replicaByTokenRangePerKeyspace.register(job2);
        assertThat(replicaByTokenRangePerKeyspace.isEmpty()).isFalse();
        assertThat(replicaByTokenRangePerKeyspace.allJobsUnsafe()).containsExactlyInAnyOrder(job1.jobId, job2.jobId);
        assertThat(replicaByTokenRangePerKeyspace.jobsByKeyspaceUnsafe()).containsOnlyKeys(job1.keyspaceName);
        assertThat(replicaByTokenRangePerKeyspace.jobsByKeyspaceUnsafe().get(job1.keyspaceName))
        .containsExactlyInAnyOrder(job1.jobId, job2.jobId);
        assertThat(replicaByTokenRangePerKeyspace.promisesUnsafe()).isEmpty();
        assertThat(replicaByTokenRangePerKeyspace.mappingUnsafe()).isEmpty();

        // job3 belongs to a different keyspace
        RestoreJob job3 = RestoreJobTest.createTestingJob(UUIDs.timeBased(), "job3ks", RestoreJobStatus.CREATED, null);
        replicaByTokenRangePerKeyspace.register(job3);
        assertThat(replicaByTokenRangePerKeyspace.isEmpty()).isFalse();
        assertThat(replicaByTokenRangePerKeyspace.allJobsUnsafe()).containsExactlyInAnyOrder(job1.jobId, job2.jobId, job3.jobId);
        assertThat(replicaByTokenRangePerKeyspace.jobsByKeyspaceUnsafe()).containsOnlyKeys(job1.keyspaceName, job3.keyspaceName);
        assertThat(replicaByTokenRangePerKeyspace.jobsByKeyspaceUnsafe().get(job3.keyspaceName)).containsExactlyInAnyOrder(job3.jobId);
        assertThat(replicaByTokenRangePerKeyspace.promisesUnsafe()).isEmpty();
        assertThat(replicaByTokenRangePerKeyspace.mappingUnsafe()).isEmpty();
    }

    @Test
    void testUnregisterJobs()
    {
        assertThat(replicaByTokenRangePerKeyspace.isEmpty()).isTrue();

        // register 2 jobs of the same keyspace and set up the promise and mapping
        RestoreJob job1 = RestoreJobTest.createNewTestingJob(UUIDs.timeBased());
        RestoreJob job2 = RestoreJobTest.createNewTestingJob(UUIDs.timeBased());
        TokenRangeReplicasResponse mockTopology = mock(TokenRangeReplicasResponse.class);
        Future<TokenRangeReplicasResponse> future1 = replicaByTokenRangePerKeyspace.futureOf(job1);
        Future<TokenRangeReplicasResponse> future2 = replicaByTokenRangePerKeyspace.futureOf(job2);
        assertThat(future1)
        .describedAs("topology futures are the same since the jobs belong to the same keyspace")
        .isSameAs(future2);
        assertThat(future1.isComplete()).isFalse();
        assertThat(replicaByTokenRangePerKeyspace.mappingUnsafe()).isEmpty();
        // load the topology should complete the futures
        replicaByTokenRangePerKeyspace.load(ks -> mockTopology);
        assertThat(future1.isComplete()).isTrue();
        assertThat(future1.result()).isSameAs(mockTopology);
        assertThat(replicaByTokenRangePerKeyspace.isEmpty()).isFalse();
        assertThat(replicaByTokenRangePerKeyspace.allJobsUnsafe()).containsExactlyInAnyOrder(job1.jobId, job2.jobId);
        assertThat(replicaByTokenRangePerKeyspace.jobsByKeyspaceUnsafe()).containsOnlyKeys(job1.keyspaceName);
        assertThat(replicaByTokenRangePerKeyspace.promisesUnsafe()).containsOnlyKeys(job1.keyspaceName);
        assertThat(replicaByTokenRangePerKeyspace.mappingUnsafe()).containsEntry(job1.keyspaceName, mockTopology);

        // now unregister the jobs. start with job1; because the job2 is still registered, the mapping remains
        replicaByTokenRangePerKeyspace.unregister(job1);
        assertThat(replicaByTokenRangePerKeyspace.allJobsUnsafe()).containsExactlyInAnyOrder(job2.jobId);
        assertThat(replicaByTokenRangePerKeyspace.jobsByKeyspaceUnsafe()).containsOnlyKeys(job1.keyspaceName);
        assertThat(replicaByTokenRangePerKeyspace.promisesUnsafe()).containsOnlyKeys(job1.keyspaceName);
        assertThat(replicaByTokenRangePerKeyspace.mappingUnsafe()).containsEntry(job1.keyspaceName, mockTopology);
        // now unregister job2
        replicaByTokenRangePerKeyspace.unregister(job2);
        assertThat(replicaByTokenRangePerKeyspace.isEmpty()).isTrue();
        assertThat(replicaByTokenRangePerKeyspace.allJobsUnsafe()).isEmpty();
        assertThat(replicaByTokenRangePerKeyspace.jobsByKeyspaceUnsafe()).isEmpty();
        assertThat(replicaByTokenRangePerKeyspace.promisesUnsafe()).isEmpty();
        assertThat(replicaByTokenRangePerKeyspace.mappingUnsafe()).isEmpty();
    }

    @Test
    void testUnregisterPendingRefreshShouldFailPromise()
    {
        RestoreJob job = RestoreJobTest.createNewTestingJob(UUIDs.timeBased());
        Future<TokenRangeReplicasResponse> future = replicaByTokenRangePerKeyspace.futureOf(job);
        assertThat(future.isComplete()).isFalse();
        replicaByTokenRangePerKeyspace.unregister(job);
        assertThat(future.failed()).isTrue();
        assertThat(future.cause())
        .hasMessage("Unable to retrieve topology for restoreJob. jobId=" + job.jobId + " keyspace=" + job.keyspaceName);
    }

    @Test
    void testIgnoreFailedLoad()
    {
        replicaByTokenRangePerKeyspace.register(RestoreJobTest.createNewTestingJob(UUIDs.timeBased()));
        assertThat(replicaByTokenRangePerKeyspace.mappingUnsafe()).isEmpty();
        replicaByTokenRangePerKeyspace.load(ks -> {
            throw new RuntimeException("Load topology failed");
        });
        // the load function fails, and mapping is still empty.
        assertThat(replicaByTokenRangePerKeyspace.mappingUnsafe()).isEmpty();
    }

    @Test
    void testLoadAndGetMapping()
    {
        RestoreJob job = RestoreJobTest.createNewTestingJob(UUIDs.timeBased());
        Future<TokenRangeReplicasResponse> future = replicaByTokenRangePerKeyspace.futureOf(job);
        assertThat(future.isComplete()).isFalse();
        assertThat(replicaByTokenRangePerKeyspace.forRestoreJob(job)).isNull();
        TokenRangeReplicasResponse mockTopology = mock(TokenRangeReplicasResponse.class);
        replicaByTokenRangePerKeyspace.load(ks -> mockTopology);
        assertThat(future.isComplete()).isTrue();
        assertThat(future.result()).isSameAs(mockTopology);
        assertThat(replicaByTokenRangePerKeyspace.forRestoreJob(job)).isSameAs(mockTopology);
    }
}
