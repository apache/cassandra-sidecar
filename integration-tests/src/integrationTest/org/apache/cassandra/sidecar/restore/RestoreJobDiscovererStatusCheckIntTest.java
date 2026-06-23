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

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.data.StorageCredentials;
import org.apache.cassandra.sidecar.common.request.data.CreateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.request.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.yaml.RestoreJobConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.db.RestoreJobDatabaseAccessor;
import org.apache.cassandra.sidecar.testing.SharedClusterIntegrationTestBase;

import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the fast status-check loop owned by {@link RestoreJobDiscoverer} (its non-static inner
 * {@code StatusCheckTask}) reacts to a restore-job status transition within the configured fast-loop
 * interval, without waiting for the slow discovery loop.
 *
 * <p>The test seeds an in-flight job, runs one discovery pass to populate the in-flight set,
 * then flips the job's status directly in the {@code sidecar_internal} keyspace and asserts
 * that the discoverer sees the terminal status (via {@link RestoreJobDiscoverer#inflightJobIds()})
 * well within the slow-loop interval. The slow loop is configured to a much larger interval so
 * a positive observation can only be the work of the fast status-check loop.
 */
class RestoreJobDiscovererStatusCheckIntTest extends SharedClusterIntegrationTestBase
{
    private static final QualifiedTableName JOB_TABLE = new QualifiedTableName("ks", "tbl");

    @Override
    protected Function<SidecarConfigurationImpl.Builder, SidecarConfigurationImpl.Builder> configurationOverrides()
    {
        return builder -> builder.restoreJobConfiguration(
        RestoreJobConfigurationImpl.builder()
                                   .jobDiscoveryStatusCheckInterval(MillisecondBoundConfiguration.parse("200ms"))
                                   // Make slow loops effectively never fire during the test so a
                                   // positive observation can only be the fast loop reacting.
                                   .jobDiscoveryActiveLoopDelay(MillisecondBoundConfiguration.parse("1h"))
                                   .jobDiscoveryIdleLoopDelay(MillisecondBoundConfiguration.parse("1h"))
                                   .build());
    }

    @Override
    protected void initializeSchemaForTest()
    {
        // Restore job rows live in sidecar_internal; no user schema setup required.
    }

    @Override
    protected void beforeTestStart()
    {
        waitForSchemaReady(30, TimeUnit.SECONDS);
    }

    @Test
    void testFastLoopDetectsTerminalTransition()
    {
        RestoreJobDatabaseAccessor jobAccessor = serverWrapper.injector.getInstance(RestoreJobDatabaseAccessor.class);
        RestoreJobDiscoverer discoverer = serverWrapper.injector.getInstance(RestoreJobDiscoverer.class);

        UUID jobId = UUIDs.timeBased();
        long expireAt = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
        CreateRestoreJobRequestPayload createPayload = CreateRestoreJobRequestPayload
                                                       .builder(genRestoreJobSecrets(), expireAt)
                                                       .jobId(jobId)
                                                       .jobAgent("int-test")
                                                       .build();
        jobAccessor.create(createPayload, JOB_TABLE);

        // Run one discovery pass synchronously so the job lands in the discoverer's in-flight set.
        // We do not wait for the configured slow loop to fire (it's pinned to 1h above).
        discoverer.tryExecuteDiscovery();
        assertThat(discoverer.inflightJobIds())
        .describedAs("Discovery should track the newly created CREATED job as in-flight")
        .contains(jobId);

        // Simulate another sidecar (or the storage system) marking the job SUCCEEDED in the shared DB.
        jobAccessor.update(UpdateRestoreJobRequestPayload.builder()
                                                         .withStatus(RestoreJobStatus.SUCCEEDED)
                                                         .build(),
                           jobId);

        // The slow loop is effectively disabled, so this can only succeed via the 200ms fast loop.
        loopAssert(10, 200, () -> assertThat(discoverer.inflightJobIds())
                                  .describedAs("Fast status-check loop should detect the terminal transition")
                                  .doesNotContain(jobId));
    }

    private static RestoreJobSecrets genRestoreJobSecrets()
    {
        return new RestoreJobSecrets(genStorageCredentials("read"), genStorageCredentials("write"));
    }

    private static StorageCredentials genStorageCredentials(String permission)
    {
        long nonce = ThreadLocalRandom.current().nextLong();
        return StorageCredentials.builder()
                                 .accessKeyId(permission + "-accessKeyId-" + nonce)
                                 .secretAccessKey(permission + "-secretAccessKey-" + nonce)
                                 .sessionToken(permission + "-sessionToken-" + nonce)
                                 .region(permission + "-region-" + nonce)
                                 .build();
    }
}
