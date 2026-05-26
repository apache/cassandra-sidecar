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

package org.apache.cassandra.sidecar.restore.jobdiscoverer;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import com.google.inject.AbstractModule;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.data.SSTableImportOptions;
import org.apache.cassandra.sidecar.common.request.data.CreateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.request.data.CreateSliceRequestPayload;
import org.apache.cassandra.sidecar.common.request.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobDatabaseAccessor;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreRangeDatabaseAccessor;
import org.apache.cassandra.sidecar.restore.RestoreJobDiscoverer;
import org.apache.cassandra.sidecar.restore.RestoreJobTestUtils;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;

import static org.apache.cassandra.sidecar.restore.RestoreJobTestUtils.createJob;
import static org.apache.cassandra.sidecar.restore.RestoreJobTestUtils.disableRestoreProcessor;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration tests verifying that SAI import options (failOnMissingIndex, validateIndexChecksum) are
 * correctly persisted and propagated through the restore job pipeline: create job -> persist -> discover -> create ranges.
 */
class RestoreJobDiscovererSAIOptionsIntTest extends SharedClusterSidecarIntegrationTestBase
{
    private static final SimpleCassandraVersion MIN_VERSION_WITH_SAI = SimpleCassandraVersion.create("5.0.0");
    private static final String TEST_KEYSPACE = "sai_import_options_ks";
    private static final QualifiedName TABLE_NAME = new QualifiedName(TEST_KEYSPACE, "test_table");

    @Override
    protected void beforeClusterProvisioning()
    {
        SimpleCassandraVersion version = SimpleCassandraVersion.create(testVersion.version());
        assumeThat(version)
        .as("SAI indexes are only available in Cassandra 5.0 and later")
        .isGreaterThanOrEqualTo(MIN_VERSION_WITH_SAI);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .nodesPerDc(3)
                    .tokenSupplier(nodeIndex ->
                        Collections.singletonList(String.valueOf(new long[]{ 0, 1000L, 2000L }[nodeIndex - 1])));
    }

    @Override
    protected void startSidecar(ICluster<? extends IInstance> cluster) throws InterruptedException
    {
        serverWrapper = startSidecarWithInstances(List.of(cluster.get(1)), (AbstractModule) disableRestoreProcessor());
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, Map.of("datacenter1", 2));
        createTestTable(TABLE_NAME, "CREATE TABLE %s (id text PRIMARY KEY, name text)");
    }

    @Override
    protected void beforeTestStart()
    {
        waitForSchemaReady(30, TimeUnit.SECONDS);
    }

    @Test
    void testDefaultImportOptions()
    {
        QualifiedTableName tableName = new QualifiedTableName(TABLE_NAME.keyspace(), TABLE_NAME.table());
        RestoreJobTestUtils.RestoreJobClient testClient =
                RestoreJobTestUtils.client(trustedClient(), "localhost", serverWrapper.serverPort);

        // create job with default options
        UUID jobId = createJob(testClient, tableName);

        RestoreJobDatabaseAccessor jobAccessor = serverWrapper.injector.getInstance(RestoreJobDatabaseAccessor.class);
        RestoreJob job = jobAccessor.find(jobId);
        SSTableImportOptions importOptions = job.importOptions;
        // verify default SAI options on persisted job
        assertThat(importOptions.failOnMissingIndex()).isFalse();
        assertThat(importOptions.validateIndexChecksum()).isFalse();

        short bucketId = 0;
        CreateSliceRequestPayload slicePayload = new CreateSliceRequestPayload("sliceId", bucketId, "bucket", "key",
                "checksum", BigInteger.valueOf(1L), BigInteger.valueOf(1600L),
                100L, 100L);
        testClient.createRestoreSlice(tableName, jobId, slicePayload);
        testClient.updateRestoreJob(tableName, jobId,
                UpdateRestoreJobRequestPayload.builder().withStatus(RestoreJobStatus.STAGE_READY).build());

        RestoreJobDiscoverer restoreJobDiscoverer = serverWrapper.injector.getInstance(RestoreJobDiscoverer.class);
        restoreJobDiscoverer.tryExecuteDiscovery();

        // verify ranges were created and link to the correct job. Wrapped in loopAssert because the
        // STAGE_READY PATCH triggers an asynchronous wake-up (CASSSIDECAR-454) on a worker thread; the
        // wake-up and the synchronous tryExecuteDiscovery above race on isExecuting, and ranges may not
        // be visible immediately after either path returns.
        RestoreRangeDatabaseAccessor rangeDatabaseAccessor = serverWrapper.injector.getInstance(RestoreRangeDatabaseAccessor.class);
        loopAssert(10, 500, () -> {
            List<RestoreRange> ranges = rangeDatabaseAccessor.findAll(jobId, bucketId);
            assertThat(ranges).isNotEmpty();
            for (RestoreRange range : ranges)
            {
                assertThat(range.jobId()).isEqualTo(jobId);
            }
        });

        // Re-read the job after discovery to confirm default importOptions are preserved
        RestoreJob jobAfterDiscovery = jobAccessor.find(jobId);
        assertThat(jobAfterDiscovery.importOptions.failOnMissingIndex())
                .describedAs("failOnMissingIndex should be false by default after discovery")
                .isFalse();
        assertThat(jobAfterDiscovery.importOptions.validateIndexChecksum())
                .describedAs("validateIndexChecksum should be false by default after discovery")
                .isFalse();
    }

    @Test
    void testSaiImportOptionsPropagatedToRestoreRanges()
    {
        QualifiedTableName tableName = new QualifiedTableName(TABLE_NAME.keyspace(), TABLE_NAME.table());
        RestoreJobTestUtils.RestoreJobClient testClient =
        RestoreJobTestUtils.client(trustedClient(), "localhost", serverWrapper.serverPort);

        // create job with SAI options enabled
        Consumer<CreateRestoreJobRequestPayload.Builder> enableSaiOptions = builder ->
            builder.updateImportOptions(opts -> opts.failOnMissingIndex(true).validateIndexChecksum(true));
        UUID jobId = createJob(testClient, tableName, enableSaiOptions);

        // verify SAI options are persisted on the job
        RestoreJobDatabaseAccessor jobAccessor = serverWrapper.injector.getInstance(RestoreJobDatabaseAccessor.class);
        RestoreJob job = jobAccessor.find(jobId);
        SSTableImportOptions importOptions = job.importOptions;
        assertThat(importOptions.failOnMissingIndex()).isTrue();
        assertThat(importOptions.validateIndexChecksum()).isTrue();
        // verify other defaults are preserved
        assertThat(importOptions.resetLevel()).isTrue();
        assertThat(importOptions.clearRepaired()).isTrue();
        assertThat(importOptions.verifySSTables()).isTrue();
        assertThat(importOptions.verifyTokens()).isTrue();
        assertThat(importOptions.invalidateCaches()).isTrue();
        assertThat(importOptions.extendedVerify()).isTrue();
        assertThat(importOptions.copyData()).isFalse();

        // create slice and discover ranges
        short bucketId = 0;
        CreateSliceRequestPayload slicePayload = new CreateSliceRequestPayload("sliceId", bucketId, "bucket", "key",
                                                                               "checksum", BigInteger.valueOf(1L), BigInteger.valueOf(1600L),
                                                                               100L, 100L);
        testClient.createRestoreSlice(tableName, jobId, slicePayload);
        testClient.updateRestoreJob(tableName, jobId,
                                    UpdateRestoreJobRequestPayload.builder().withStatus(RestoreJobStatus.STAGE_READY).build());

        RestoreJobDiscoverer restoreJobDiscoverer = serverWrapper.injector.getInstance(RestoreJobDiscoverer.class);
        restoreJobDiscoverer.tryExecuteDiscovery();

        // verify ranges were created by the discoverer and link back to the correct job. Wrapped in
        // loopAssert because the STAGE_READY PATCH triggers an asynchronous wake-up (CASSSIDECAR-454)
        // on a worker thread; the wake-up and the synchronous tryExecuteDiscovery above race on
        // isExecuting, and ranges may not be visible immediately after either path returns.
        RestoreRangeDatabaseAccessor rangeDatabaseAccessor = serverWrapper.injector.getInstance(RestoreRangeDatabaseAccessor.class);
        loopAssert(10, 500, () -> {
            List<RestoreRange> ranges = rangeDatabaseAccessor.findAll(jobId, bucketId);
            assertThat(ranges).isNotEmpty();
            for (RestoreRange range : ranges)
            {
                assertThat(range.jobId()).isEqualTo(jobId);
            }
        });

        // Re-read the job after discovery to confirm importOptions are still intact.
        RestoreJob jobAfterDiscovery = jobAccessor.find(jobId);
        assertThat(jobAfterDiscovery.importOptions.failOnMissingIndex())
        .describedAs("failOnMissingIndex should be true after discovery")
        .isTrue();
        assertThat(jobAfterDiscovery.importOptions.validateIndexChecksum())
        .describedAs("validateIndexChecksum should be true after discovery")
        .isTrue();
    }
}
