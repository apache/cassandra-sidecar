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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.request.data.CreateSliceRequestPayload;
import org.apache.cassandra.sidecar.common.request.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreRangeDatabaseAccessor;
import org.apache.cassandra.sidecar.restore.RestoreJobDiscoverer;
import org.apache.cassandra.sidecar.restore.RestoreJobTestUtils;
import org.apache.cassandra.sidecar.restore.RingTopologyRefresher;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.sidecar.testing.TestTokenSupplier;
import org.apache.cassandra.sidecar.testing.bytebuddy.BBHelperMovingNode;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

import static org.apache.cassandra.sidecar.restore.RestoreJobTestUtils.assertRestoreRange;
import static org.apache.cassandra.sidecar.restore.RestoreJobTestUtils.createJob;
import static org.apache.cassandra.sidecar.restore.RestoreJobTestUtils.disableRestoreProcessor;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the scenario that token move is triggered while restore job is in progress
 */
class RestoreJobDiscovererNodeMovingIntTest extends IntegrationTestBase
{
    private static final int MANAGED_CASSANDRA_NODE_NUM = 2;
    private static final String MANAGED_CASSANDRA_NODE_IP = "127.0.0." + MANAGED_CASSANDRA_NODE_NUM;

    @Override
    protected void beforeSetup()
    {
        installTestSpecificModule(disableRestoreProcessor());
    }

    @CassandraIntegrationTest(nodesPerDc = 3, network = true, buildCluster = false)
    void testUpdateRestoreRangesWhenNodeMoving(ConfigurableCassandraTestContext cassandraTestContext)
    {
        TokenSupplier tokenSupplier = TestTokenSupplier.staticTokens(0, 1000L, 2000L);
        UpgradeableCluster cluster = startCluster(tokenSupplier, cassandraTestContext);
        long newToken = 1500L;
        try
        {
            test(BBHelperMovingNode.transientStateStart, cluster, newToken);
        }
        finally
        {
            BBHelperMovingNode.transientStateEnd.countDown();
        }
    }

    @Override
    protected int[] getInstancesToManage(int clusterSize)
    {
        return new int[] { MANAGED_CASSANDRA_NODE_NUM };
    }

    private void test(CountDownLatch transientStateStart, UpgradeableCluster cluster, long moveTargetToken)
    {
        // prepare schema
        waitForSchemaReady(30, TimeUnit.SECONDS);
        createTestKeyspace(ImmutableMap.of("datacenter1", 2));
        QualifiedTableName tableName = createTestTable("CREATE TABLE %s (id text PRIMARY KEY, name text);");

        // create job
        RestoreJobTestUtils.RestoreJobClient testClient = RestoreJobTestUtils.client(client, MANAGED_CASSANDRA_NODE_IP, server.actualPort());
        UUID jobId = createJob(testClient, tableName);

        // create slice
        short bucketId = 0;
        CreateSliceRequestPayload slicePayload = new CreateSliceRequestPayload("sliceId", bucketId, "bucket", "key",
                                                                               "checksum", BigInteger.valueOf(1001L), BigInteger.valueOf(1600L),
                                                                               100L, 100L);
        testClient.createRestoreSlice(tableName, jobId, slicePayload);

        // STAGE_READY is required in order to discover slices; update the restore job status
        testClient.updateRestoreJob(tableName, jobId, UpdateRestoreJobRequestPayload.builder().withStatus(RestoreJobStatus.STAGE_READY).build());

        // first discovery run
        RestoreJobDiscoverer restoreJobDiscoverer = injector.getInstance(RestoreJobDiscoverer.class);
        restoreJobDiscoverer.tryExecuteDiscovery();

        RingTopologyRefresher ringTopologyRefresher = injector.getInstance(RingTopologyRefresher.class);
        Map<Integer, Set<TokenRange>> localTokenRanges = ringTopologyRefresher.localTokenRanges(tableName.keyspace(), true);
        assertThat(localTokenRanges)
        .hasSize(1)
        .containsEntry(MANAGED_CASSANDRA_NODE_NUM,
                       ImmutableSet.of(new TokenRange(Long.MIN_VALUE, 0),
                                       new TokenRange(0, 1000),
                                       new TokenRange(2000, Long.MAX_VALUE)));

        // assert that no restore ranges are create
        RestoreRangeDatabaseAccessor rangeDatabaseAccessor = injector.getInstance(RestoreRangeDatabaseAccessor.class);
        List<RestoreRange> ranges = rangeDatabaseAccessor.findAll(jobId, bucketId);
        assertThat(ranges)
        .describedAs("No RestoreRange is created because no slice overlaps with local token ranges")
        .isEmpty();

        // start move in the background
        IUpgradeableInstance seed = cluster.get(1);
        IUpgradeableInstance movingNode = cluster.get(MANAGED_CASSANDRA_NODE_NUM);
        startAsync("move token of node" + movingNode.config().num() + " to " + moveTargetToken,
                   () -> movingNode.nodetoolResult("move", "--", Long.toString(moveTargetToken))
                                   .asserts()
                                   .success());

        // Wait until nodes have reached expected state
        awaitLatchOrThrow(transientStateStart, 2, TimeUnit.MINUTES, "transientStateStart");
        ClusterUtils.awaitRingState(seed, movingNode, "Moving");

        // Fetch the local token ranges again;
        // RingTopologyRefresher should detect the topology change and notify RestoreJobDiscover via #onRingTopologyChanged
        localTokenRanges = ringTopologyRefresher.localTokenRanges(tableName.keyspace(), true);
        assertThat(localTokenRanges)
        .hasSize(1)
        .containsEntry(MANAGED_CASSANDRA_NODE_NUM,
                       ImmutableSet.of(new TokenRange(Long.MIN_VALUE, 0),
                                       new TokenRange(0, 1000),
                                       new TokenRange(1000, 1500), // range (1000, 1500] is gained due to move
                                       new TokenRange(2000, Long.MAX_VALUE)));

        // Using loopAssert because #onRingTopologyChanged runs in another thread. It takes some time to reflect the RestoreRange update
        loopAssert(10, 500, () -> {
            List<RestoreRange> restoreRanges = rangeDatabaseAccessor.findAll(jobId, bucketId);
            assertThat(restoreRanges)
            .describedAs("A restore range should be created. After the topology change, now the slice is partially owned by the local node")
            .hasSize(1);
            assertRestoreRange(restoreRanges.get(0), 1000L, 1500L);
        });
    }

    @Override
    protected String subjectAlternativeNameIpAddress()
    {
        return MANAGED_CASSANDRA_NODE_IP;
    }

    private static UpgradeableCluster startCluster(TokenSupplier tokenSupplier, ConfigurableCassandraTestContext cassandraTestContext)
    {
        BBHelperMovingNode.reset();
        return cassandraTestContext.configureAndStartCluster(builder -> {
            builder.withInstanceInitializer((cl, num) -> BBHelperMovingNode.install(cl, num, 2));
            builder.withTokenSupplier(tokenSupplier);
        });
    }
}
