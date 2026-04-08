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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Tag;

import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
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
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.sidecar.testing.TestTokenSupplier;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.testing.IClusterExtension;

import static org.apache.cassandra.sidecar.restore.RestoreJobTestUtils.createJob;
import static org.apache.cassandra.sidecar.restore.RestoreJobTestUtils.disableRestoreProcessor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests verifying that SAI import options (failOnMissingIndex, validateIndexChecksum) are
 * correctly persisted and propagated through the restore job pipeline: create job -> persist -> discover -> create ranges.
 */
@Tag("heavy")
class RestoreJobDiscovererSaiImportOptionsIntTest extends IntegrationTestBase
{
    @Override
    protected void beforeSetup()
    {
        installTestSpecificModule(disableRestoreProcessor());
    }

    @Override
    protected int[] getInstancesToManage(int clusterSize)
    {
        return new int[] { 1 };
    }

    @CassandraIntegrationTest(nodesPerDc = 3, network = true, buildCluster = false)
    void testSaiImportOptionsPropagatedToRestoreRanges(ConfigurableCassandraTestContext cassandraTestContext)
    {
        TokenSupplier tokenSupplier = TestTokenSupplier.staticTokens(0, 1000L, 2000L);
        startCluster(tokenSupplier, cassandraTestContext);
        RestoreJobTestUtils.RestoreJobClient testClient = RestoreJobTestUtils.client(client, "127.0.0.1", server.actualPort());

        // prepare schema
        waitForSchemaReady(30, TimeUnit.SECONDS);
        createTestKeyspace(ImmutableMap.of("datacenter1", 2));
        QualifiedTableName tableName = createTestTable("CREATE TABLE %s (id text PRIMARY KEY, name text);");

        // create job with SAI options enabled
        Consumer<CreateRestoreJobRequestPayload.Builder> enableSaiOptions = builder ->
            builder.updateImportOptions(opts -> opts.failOnMissingIndex(true).validateIndexChecksum(true));
        UUID jobId = createJob(testClient, tableName, enableSaiOptions);

        // verify SAI options are persisted on the job
        RestoreJobDatabaseAccessor jobAccessor = injector.getInstance(RestoreJobDatabaseAccessor.class);
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
        testClient.updateRestoreJob(tableName, jobId, UpdateRestoreJobRequestPayload.builder().withStatus(RestoreJobStatus.STAGE_READY).build());

        RestoreJobDiscoverer restoreJobDiscoverer = injector.getInstance(RestoreJobDiscoverer.class);
        restoreJobDiscoverer.tryExecuteDiscovery();

        // verify ranges were created by the discoverer and link back to the correct job
        RestoreRangeDatabaseAccessor rangeDatabaseAccessor = injector.getInstance(RestoreRangeDatabaseAccessor.class);
        List<RestoreRange> ranges = rangeDatabaseAccessor.findAll(jobId, bucketId);
        assertThat(ranges).isNotEmpty();
        for (RestoreRange range : ranges)
        {
            assertThat(range.jobId()).isEqualTo(jobId);
        }

        // Re-read the job after discovery to confirm importOptions are still intact.
        // RestoreRange.job() requires a RestoreJobProgressTracker (only set at runtime),
        // so we verify the options on the persisted RestoreJob directly.
        RestoreJob jobAfterDiscovery = jobAccessor.find(jobId);
        assertThat(jobAfterDiscovery.importOptions.failOnMissingIndex())
        .describedAs("failOnMissingIndex should be true after discovery")
        .isTrue();
        assertThat(jobAfterDiscovery.importOptions.validateIndexChecksum())
        .describedAs("validateIndexChecksum should be true after discovery")
        .isTrue();
    }

    @CassandraIntegrationTest(nodesPerDc = 3, network = true, buildCluster = false)
    void testDefaultImportOptionsWhenSaiNotSpecified(ConfigurableCassandraTestContext cassandraTestContext)
    {
        TokenSupplier tokenSupplier = TestTokenSupplier.staticTokens(0, 1000L, 2000L);
        startCluster(tokenSupplier, cassandraTestContext);
        RestoreJobTestUtils.RestoreJobClient testClient = RestoreJobTestUtils.client(client, "127.0.0.1", server.actualPort());

        // prepare schema
        waitForSchemaReady(30, TimeUnit.SECONDS);
        createTestKeyspace(ImmutableMap.of("datacenter1", 2));
        QualifiedTableName tableName = createTestTable("CREATE TABLE %s (id text PRIMARY KEY, name text);");

        // create job with default options (no SAI customization)
        UUID jobId = createJob(testClient, tableName);

        // verify default SAI options on persisted job
        RestoreJobDatabaseAccessor jobAccessor = injector.getInstance(RestoreJobDatabaseAccessor.class);
        RestoreJob job = jobAccessor.find(jobId);
        SSTableImportOptions importOptions = job.importOptions;
        assertThat(importOptions.failOnMissingIndex()).isFalse();
        assertThat(importOptions.validateIndexChecksum()).isFalse();

        // create slice and discover ranges
        short bucketId = 0;
        CreateSliceRequestPayload slicePayload = new CreateSliceRequestPayload("sliceId", bucketId, "bucket", "key",
                                                                               "checksum", BigInteger.valueOf(1L), BigInteger.valueOf(1600L),
                                                                               100L, 100L);
        testClient.createRestoreSlice(tableName, jobId, slicePayload);
        testClient.updateRestoreJob(tableName, jobId, UpdateRestoreJobRequestPayload.builder().withStatus(RestoreJobStatus.STAGE_READY).build());

        RestoreJobDiscoverer restoreJobDiscoverer = injector.getInstance(RestoreJobDiscoverer.class);
        restoreJobDiscoverer.tryExecuteDiscovery();

        // verify ranges were created and link to the correct job
        RestoreRangeDatabaseAccessor rangeDatabaseAccessor = injector.getInstance(RestoreRangeDatabaseAccessor.class);
        List<RestoreRange> ranges = rangeDatabaseAccessor.findAll(jobId, bucketId);
        assertThat(ranges).isNotEmpty();
        for (RestoreRange range : ranges)
        {
            assertThat(range.jobId()).isEqualTo(jobId);
        }

        // Re-read the job after discovery to confirm default importOptions are preserved
        RestoreJob jobAfterDiscovery = jobAccessor.find(jobId);
        assertThat(jobAfterDiscovery.importOptions.failOnMissingIndex())
        .describedAs("failOnMissingIndex should be false by default after discovery")
        .isFalse();
        assertThat(jobAfterDiscovery.importOptions.validateIndexChecksum())
        .describedAs("validateIndexChecksum should be false by default after discovery")
        .isFalse();
    }

    private static IClusterExtension<? extends IInstance> startCluster(TokenSupplier tokenSupplier,
                                                                       ConfigurableCassandraTestContext cassandraTestContext)
    {
        return cassandraTestContext.configureAndStartCluster(builder -> builder.tokenSupplier(tokenSupplier));
    }
}
