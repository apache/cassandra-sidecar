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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.google.inject.AbstractModule;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.request.data.CreateSliceRequestPayload;
import org.apache.cassandra.sidecar.common.request.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.yaml.RestoreJobConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreRangeDatabaseAccessor;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;

import static org.apache.cassandra.sidecar.restore.RestoreJobTestUtils.assertRestoreRange;
import static org.apache.cassandra.sidecar.restore.RestoreJobTestUtils.createJob;
import static org.apache.cassandra.sidecar.restore.RestoreJobTestUtils.disableRestoreProcessor;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@code STAGE_READY} / {@code IMPORT_READY} phase signals delivered via
 * the {@code UpdateRestoreJobHandler} REST endpoint trigger immediate processing on the
 * receiving Sidecar instance, without waiting for the next discovery loop cycle.
 *
 * <p>Both discovery loop delays are pinned to 1h via {@link #configurationOverrides()},
 * so any successful observation of restore ranges within seconds can only be the work of
 * the wake-up path added by CASSSIDECAR-454 — not the periodic discovery loop.
 */
class RestoreJobDiscovererPhaseSignalIntTest extends SharedClusterSidecarIntegrationTestBase
{
    private static final QualifiedName USER_KEYSPACE_TABLE = new QualifiedName("restore_phase_signal_ks", "t");
    private static final QualifiedTableName SIDECAR_QUALIFIED_TABLE =
    new QualifiedTableName(USER_KEYSPACE_TABLE.keyspace(), USER_KEYSPACE_TABLE.table());

    @Override
    protected Function<SidecarConfigurationImpl.Builder, SidecarConfigurationImpl.Builder> configurationOverrides()
    {
        return builder -> builder.restoreJobConfiguration(
        RestoreJobConfigurationImpl.builder()
                                   // Pin both discovery loops to 1h. Any range created within seconds
                                   // is necessarily the wake-up path and not the discovery loop.
                                   .jobDiscoveryActiveLoopDelay(MillisecondBoundConfiguration.parse("1h"))
                                   .jobDiscoveryIdleLoopDelay(MillisecondBoundConfiguration.parse("1h"))
                                   .build());
    }

    @Override
    protected void startSidecar(ICluster<? extends IInstance> cluster) throws InterruptedException
    {
        // Disable the RestoreProcessor so range submission stops at the database write,
        // letting the test assert on RestoreRangeDatabaseAccessor without S3/import side effects.
        serverWrapper = startSidecarWithInstances(cluster, (AbstractModule) disableRestoreProcessor());
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(USER_KEYSPACE_TABLE, Map.of("datacenter1", 1));
        createTestTable(USER_KEYSPACE_TABLE, "CREATE TABLE %s (id text PRIMARY KEY, name text);");
    }

    @Override
    protected void beforeTestStart()
    {
        waitForSchemaReady(30, TimeUnit.SECONDS);
    }

    @Test
    void testStageReadyImmediatelySubmitsSlices()
    {
        RestoreJobTestUtils.RestoreJobClient testClient = restoreJobClient();
        UUID jobId = createJob(testClient, SIDECAR_QUALIFIED_TABLE);
        short bucketId = 0;
        CreateSliceRequestPayload slicePayload = new CreateSliceRequestPayload(
        "sliceId-stage", bucketId, "bucket", "key", "checksum",
        BigInteger.valueOf(500L), BigInteger.valueOf(1500L), 100L, 100L);
        testClient.createRestoreSlice(SIDECAR_QUALIFIED_TABLE, jobId, slicePayload);

        RestoreRangeDatabaseAccessor rangeAccessor =
        serverWrapper.injector.getInstance(RestoreRangeDatabaseAccessor.class);
        assertThat(rangeAccessor.findAll(jobId, bucketId)).isEmpty();

        testClient.updateRestoreJob(SIDECAR_QUALIFIED_TABLE, jobId,
                                    UpdateRestoreJobRequestPayload.builder()
                                                                  .withStatus(RestoreJobStatus.STAGE_READY)
                                                                  .build());

        // Single-node cluster owns the entire ring, so the slice (500, 1500] is not trimmed.
        // Ranges are stored with exclusive start, so the slice's start of 500 surfaces as 499.
        loopAssert(10, 500, () -> {
            List<RestoreRange> ranges = rangeAccessor.findAll(jobId, bucketId);
            assertThat(ranges)
            .describedAs("STAGE_READY should immediately create restore ranges via the wake-up path")
            .hasSize(1);
            assertRestoreRange(ranges.get(0), 499L, 1500L);
        });
    }

    @Test
    void testImportReadyAfterStageReadyDoesNotCreateDuplicateRanges()
    {
        RestoreJobTestUtils.RestoreJobClient testClient = restoreJobClient();
        UUID jobId = createJob(testClient, SIDECAR_QUALIFIED_TABLE);
        short bucketId = 0;
        CreateSliceRequestPayload slicePayload = new CreateSliceRequestPayload(
        "sliceId-dup", bucketId, "bucket", "key", "checksum",
        BigInteger.valueOf(1L), BigInteger.valueOf(1500L), 100L, 100L);
        testClient.createRestoreSlice(SIDECAR_QUALIFIED_TABLE, jobId, slicePayload);

        testClient.updateRestoreJob(SIDECAR_QUALIFIED_TABLE, jobId,
                                    UpdateRestoreJobRequestPayload.builder()
                                                                  .withStatus(RestoreJobStatus.STAGE_READY)
                                                                  .build());

        RestoreRangeDatabaseAccessor rangeAccessor =
        serverWrapper.injector.getInstance(RestoreRangeDatabaseAccessor.class);
        loopAssert(10, 500, () -> assertThat(rangeAccessor.findAll(jobId, bucketId)).isNotEmpty());
        int rangeCountAfterStageReady = rangeAccessor.findAll(jobId, bucketId).size();

        testClient.updateRestoreJob(SIDECAR_QUALIFIED_TABLE, jobId,
                                    UpdateRestoreJobRequestPayload.builder()
                                                                  .withStatus(RestoreJobStatus.IMPORT_READY)
                                                                  .build());

        loopAssert(5, 500, () -> assertThat(rangeAccessor.findAll(jobId, bucketId))
                                 .describedAs("IMPORT_READY after STAGE_READY should not create duplicate ranges")
                                 .hasSize(rangeCountAfterStageReady));
    }

    private RestoreJobTestUtils.RestoreJobClient restoreJobClient()
    {
        return RestoreJobTestUtils.client(trustedClient(), "localhost", serverWrapper.serverPort);
    }
}
