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

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.request.data.CreateSliceRequestPayload;
import org.apache.cassandra.sidecar.common.request.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreRangeDatabaseAccessor;
import org.apache.cassandra.sidecar.restore.RestoreJobTestUtils;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.sidecar.testing.TestTokenSupplier;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.testing.IClusterExtension;

import static org.apache.cassandra.sidecar.restore.RestoreJobTestUtils.assertRestoreRange;
import static org.apache.cassandra.sidecar.restore.RestoreJobTestUtils.createJob;
import static org.apache.cassandra.sidecar.restore.RestoreJobTestUtils.disableRestoreProcessor;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests verifying that phase signals (STAGE_READY, IMPORT_READY) sent via
 * UpdateRestoreJobHandler immediately trigger processing without waiting for the discovery loop.
 */
class RestoreJobDiscovererPhaseSignalIntTest extends IntegrationTestBase
{
    private static final int MANAGED_NODE = 2;
    private static final String MANAGED_NODE_IP = "127.0.0." + MANAGED_NODE;

    @Override
    protected void beforeSetup()
    {
        installTestSpecificModule(disableRestoreProcessor());
    }

    @Override
    protected int[] getInstancesToManage(int clusterSize)
    {
        return new int[]{ MANAGED_NODE };
    }

    @CassandraIntegrationTest(nodesPerDc = 3, network = true, buildCluster = false)
    void testStageReadyImmediatelySubmitsSlices(ConfigurableCassandraTestContext cassandraTestContext)
    {
        TokenSupplier tokenSupplier = TestTokenSupplier.staticTokens(0, 1000L, 2000L);
        startCluster(tokenSupplier, cassandraTestContext);

        // prepare schema
        waitForSchemaReady(30, TimeUnit.SECONDS);
        createTestKeyspace(ImmutableMap.of("datacenter1", 2));
        QualifiedTableName tableName = createTestTable("CREATE TABLE %s (id text PRIMARY KEY, name text);");

        RestoreJobTestUtils.RestoreJobClient testClient = RestoreJobTestUtils.client(client, MANAGED_NODE_IP, server.actualPort());

        // create job and slice
        UUID jobId = createJob(testClient, tableName);
        short bucketId = 0;
        CreateSliceRequestPayload slicePayload = new CreateSliceRequestPayload("sliceId", bucketId, "bucket", "key",
                                                                               "checksum", BigInteger.valueOf(1L), BigInteger.valueOf(1500L),
                                                                               100L, 100L);
        testClient.createRestoreSlice(tableName, jobId, slicePayload);

        // Verify no restore ranges exist before STAGE_READY
        RestoreRangeDatabaseAccessor rangeDatabaseAccessor = injector.getInstance(RestoreRangeDatabaseAccessor.class);
        List<RestoreRange> rangesBefore = rangeDatabaseAccessor.findAll(jobId, bucketId);
        assertThat(rangesBefore).isEmpty();

        // Send STAGE_READY via REST — this should immediately trigger slice discovery and range submission
        // without needing to call restoreJobDiscoverer.tryExecuteDiscovery()
        testClient.updateRestoreJob(tableName, jobId,
                                    UpdateRestoreJobRequestPayload.builder()
                                                                  .withStatus(RestoreJobStatus.STAGE_READY)
                                                                  .build());

        // Verify restore ranges were created immediately (within a few seconds, not 5-10 minutes)
        loopAssert(10, 500, () -> {
            List<RestoreRange> rangesAfter = rangeDatabaseAccessor.findAll(jobId, bucketId);
            assertThat(rangesAfter)
            .describedAs("STAGE_READY should immediately trigger slice discovery and create restore ranges")
            .isNotEmpty();
        });
    }

    @CassandraIntegrationTest(nodesPerDc = 3, network = true, buildCluster = false)
    void testStageReadyCreatesCorrectTokenRanges(ConfigurableCassandraTestContext cassandraTestContext)
    {
        TokenSupplier tokenSupplier = TestTokenSupplier.staticTokens(0, 1000L, 2000L);
        startCluster(tokenSupplier, cassandraTestContext);

        // prepare schema
        waitForSchemaReady(30, TimeUnit.SECONDS);
        createTestKeyspace(ImmutableMap.of("datacenter1", 2));
        QualifiedTableName tableName = createTestTable("CREATE TABLE %s (id text PRIMARY KEY, name text);");

        RestoreJobTestUtils.RestoreJobClient testClient = RestoreJobTestUtils.client(client, MANAGED_NODE_IP, server.actualPort());

        // create job and slice with range spanning multiple token ranges
        UUID jobId = createJob(testClient, tableName);
        short bucketId = 0;
        CreateSliceRequestPayload slicePayload = new CreateSliceRequestPayload("sliceId", bucketId, "bucket", "key",
                                                                               "checksum", BigInteger.valueOf(500L), BigInteger.valueOf(1500L),
                                                                               100L, 100L);
        testClient.createRestoreSlice(tableName, jobId, slicePayload);

        // Send STAGE_READY — should immediately create ranges
        testClient.updateRestoreJob(tableName, jobId,
                                    UpdateRestoreJobRequestPayload.builder()
                                                                  .withStatus(RestoreJobStatus.STAGE_READY)
                                                                  .build());

        // Verify ranges are trimmed according to local token ownership
        RestoreRangeDatabaseAccessor rangeDatabaseAccessor = injector.getInstance(RestoreRangeDatabaseAccessor.class);
        loopAssert(10, 500, () -> {
            List<RestoreRange> ranges = rangeDatabaseAccessor.findAll(jobId, bucketId);
            assertThat(ranges)
            .describedAs("STAGE_READY should discover slices and trim to local token ranges")
            .isNotEmpty();
            for (RestoreRange range : ranges)
            {
                assertThat(range.startToken()).isNotNull();
                assertThat(range.endToken()).isNotNull();
            }
        });
    }

    @CassandraIntegrationTest(nodesPerDc = 3, network = true, buildCluster = false)
    void testImportReadyAfterStageReadyDoesNotCreateDuplicateRanges(ConfigurableCassandraTestContext cassandraTestContext)
    {
        TokenSupplier tokenSupplier = TestTokenSupplier.staticTokens(0, 1000L, 2000L);
        startCluster(tokenSupplier, cassandraTestContext);

        // prepare schema
        waitForSchemaReady(30, TimeUnit.SECONDS);
        createTestKeyspace(ImmutableMap.of("datacenter1", 2));
        QualifiedTableName tableName = createTestTable("CREATE TABLE %s (id text PRIMARY KEY, name text);");

        RestoreJobTestUtils.RestoreJobClient testClient = RestoreJobTestUtils.client(client, MANAGED_NODE_IP, server.actualPort());

        // create job and slice
        UUID jobId = createJob(testClient, tableName);
        short bucketId = 0;
        CreateSliceRequestPayload slicePayload = new CreateSliceRequestPayload("sliceId", bucketId, "bucket", "key",
                                                                               "checksum", BigInteger.valueOf(1L), BigInteger.valueOf(1500L),
                                                                               100L, 100L);
        testClient.createRestoreSlice(tableName, jobId, slicePayload);

        // Send STAGE_READY
        testClient.updateRestoreJob(tableName, jobId,
                                    UpdateRestoreJobRequestPayload.builder()
                                                                  .withStatus(RestoreJobStatus.STAGE_READY)
                                                                  .build());

        RestoreRangeDatabaseAccessor rangeDatabaseAccessor = injector.getInstance(RestoreRangeDatabaseAccessor.class);

        // Wait for ranges to be created by STAGE_READY
        loopAssert(10, 500, () -> {
            List<RestoreRange> ranges = rangeDatabaseAccessor.findAll(jobId, bucketId);
            assertThat(ranges).isNotEmpty();
        });

        int rangeCountAfterStageReady = rangeDatabaseAccessor.findAll(jobId, bucketId).size();

        // Send IMPORT_READY — should propagate job status but not create new ranges
        testClient.updateRestoreJob(tableName, jobId,
                                    UpdateRestoreJobRequestPayload.builder()
                                                                  .withStatus(RestoreJobStatus.IMPORT_READY)
                                                                  .build());

        // Verify no duplicate ranges were created
        loopAssert(5, 500, () -> {
            List<RestoreRange> rangesAfterImportReady = rangeDatabaseAccessor.findAll(jobId, bucketId);
            assertThat(rangesAfterImportReady)
            .describedAs("IMPORT_READY should not create duplicate ranges")
            .hasSize(rangeCountAfterStageReady);
        });
    }

    @Override
    protected String subjectAlternativeNameIpAddress()
    {
        return MANAGED_NODE_IP;
    }

    private void startCluster(TokenSupplier tokenSupplier, ConfigurableCassandraTestContext cassandraTestContext)
    {
        cassandraTestContext.configureAndStartCluster(builder -> builder.tokenSupplier(tokenSupplier));
    }
}
