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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.junit5.VertxExtension;
import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.request.data.CreateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.request.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.foundation.RestoreJobSecretsGen;
import org.apache.cassandra.sidecar.server.SidecarServerEvents;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class RestoreJobsDatabaseAccessorIntTest extends IntegrationTestBase
{
    QualifiedTableName qualifiedTableName = new QualifiedTableName("ks", "tbl");
    RestoreJobSecrets secrets = RestoreJobSecretsGen.genRestoreJobSecrets();
    long expiresAtMillis = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);

    @CassandraIntegrationTest
    void testCrudOperations()
    {
        CountDownLatch latch = new CountDownLatch(1);
        RestoreJobDatabaseAccessor accessor = injector.getInstance(RestoreJobDatabaseAccessor.class);
        vertx.eventBus()
             .localConsumer(SidecarServerEvents.ON_SIDECAR_SCHEMA_INITIALIZED.address(), msg -> latch.countDown());

        awaitLatchOrTimeout(latch, 10, TimeUnit.SECONDS);
        assertThat(latch.getCount()).describedAs("Sidecar schema not initialized").isZero();
        assertThat(accessor.findAllRecent(3)).isEmpty();

        // update this job
        UUID jobId = createJob(accessor);
        List<RestoreJob> foundJobs = accessor.findAllRecent(3);
        assertThat(foundJobs).hasSize(1);
        assertJob(foundJobs.get(0), jobId, RestoreJobStatus.CREATED, expiresAtMillis, secrets);
        assertJob(accessor.find(jobId), jobId, RestoreJobStatus.CREATED, expiresAtMillis, secrets);
        UpdateRestoreJobRequestPayload markSucceeded
        = new UpdateRestoreJobRequestPayload(null, null, RestoreJobStatus.SUCCEEDED, null);
        accessor.update(markSucceeded, jobId);
        assertJob(accessor.find(jobId), jobId, RestoreJobStatus.SUCCEEDED, expiresAtMillis, secrets);

        // abort this job with reason
        jobId = createJob(accessor);
        foundJobs = accessor.findAllRecent(3);
        assertThat(foundJobs).hasSize(2);
        accessor.abort(jobId, "Reason");
        assertJob(accessor.find(jobId), jobId, RestoreJobStatus.ABORTED, expiresAtMillis, secrets, "Reason");

        // abort this job w/o reason
        jobId = createJob(accessor);
        foundJobs = accessor.findAllRecent(3);
        assertThat(foundJobs).hasSize(3);
        accessor.abort(jobId, null);
        assertJob(accessor.find(jobId), jobId, RestoreJobStatus.ABORTED, expiresAtMillis, secrets, null);
    }

    private UUID createJob(RestoreJobDatabaseAccessor accessor)
    {
        UUID jobId = UUIDs.timeBased();
        CreateRestoreJobRequestPayload payload = CreateRestoreJobRequestPayload.builder(secrets, expiresAtMillis)
                                                                               .jobId(jobId)
                                                                               .jobAgent("agent")
                                                                               .build();
        accessor.create(payload, qualifiedTableName);

        return jobId;
    }

    private void assertJob(RestoreJob job, UUID jobId, RestoreJobStatus status, long expiresAtMillis,
                           RestoreJobSecrets secrets)
    {
        assertJob(job, jobId, status, expiresAtMillis, secrets, null);
    }

    private void assertJob(RestoreJob job, UUID jobId, RestoreJobStatus status, long expiresAtMillis,
                           RestoreJobSecrets secrets, String abortReason)
    {
        assertThat(job).isNotNull();
        assertThat(job.jobId).isEqualTo(jobId);
        assertThat(job.jobAgent).isEqualTo("agent");
        assertThat(job.keyspaceName).isEqualTo("ks");
        assertThat(job.tableName).isEqualTo("tbl");
        assertThat(job.status).isEqualTo(status);
        if (abortReason != null)
        {
            assertThat(job.statusWithOptionalDescription()).isEqualTo(String.format("%s: %s", status, abortReason));
        }
        assertThat(job.status).isEqualTo(status);
        assertThat(job.expireAt.getTime()).isEqualTo(expiresAtMillis);
        assertThat(job.secrets).isEqualTo(secrets);
    }
}
