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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
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
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

import static org.apache.cassandra.sidecar.restore.RestoreJobTestUtils.createJob;
import static org.apache.cassandra.sidecar.restore.RestoreJobTestUtils.disableRestoreProcessor;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;

class RestoreJobDiscovererNodeLeftIntTest extends IntegrationTestBase
{
    private static final int NODE_1 = 1;
    private static final int NODE_LEFT = 2;

    @Override
    protected void beforeSetup()
    {
        installTestSpecificModule(disableRestoreProcessor());
    }

    @Override
    protected int[] getInstancesToManage(int clusterSize)
    {
        return new int[] { NODE_1, NODE_LEFT };
    }

    @CassandraIntegrationTest(nodesPerDc = 3, network = true, buildCluster = false)
    void testUpdateRestoreRangesWhenNodeMoved(ConfigurableCassandraTestContext cassandraTestContext)
    {
        TokenSupplier tokenSupplier = TestTokenSupplier.staticTokens(0, 1000L, 2000L);
        UpgradeableCluster cluster = startCluster(tokenSupplier, cassandraTestContext);
        RestoreJobTestUtils.RestoreJobClient testClient = RestoreJobTestUtils.client(client, "127.0.0.1", server.actualPort());
        test(testClient, cluster);
    }

    private void test(RestoreJobTestUtils.RestoreJobClient testClient, UpgradeableCluster cluster)
    {
        // prepare schema
        waitForSchemaReady(30, TimeUnit.SECONDS);
        createTestKeyspace(ImmutableMap.of("datacenter1", 2));
        QualifiedTableName tableName = createTestTable("CREATE TABLE %s (id text PRIMARY KEY, name text);");

        // create job
        UUID jobId = createJob(testClient, tableName);

        // create slice
        short bucketId = 0;
        CreateSliceRequestPayload slicePayload = new CreateSliceRequestPayload("sliceId", bucketId, "bucket", "key",
                                                                               "checksum", BigInteger.valueOf(1L), BigInteger.valueOf(1600L),
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
        .hasSize(2)
        .containsEntry(NODE_1, ImmutableSet.of(new TokenRange(Long.MIN_VALUE, 0),
                                               new TokenRange(1000, 2000),
                                               new TokenRange(2000, Long.MAX_VALUE)))
        .containsEntry(NODE_LEFT, ImmutableSet.of(new TokenRange(Long.MIN_VALUE, 0),
                                               new TokenRange(0, 1000),
                                               new TokenRange(2000, Long.MAX_VALUE)));

        // assert that no restore ranges are create
        RestoreRangeDatabaseAccessor rangeDatabaseAccessor = injector.getInstance(RestoreRangeDatabaseAccessor.class);
        List<RestoreRange> ranges = rangeDatabaseAccessor.findAll(jobId, bucketId);
        Collections.sort(ranges, RestoreRange.TOKEN_BASED_NATURAL_ORDER);
        assertThat(ranges).hasSize(2);
        assertThat(ranges.get(0).tokenRange()).isEqualTo(new TokenRange(0, 1000)); // node 2
        assertThat(ranges.get(1).tokenRange()).isEqualTo(new TokenRange(1000, 1600)); // node 1

        // Decommission
        IUpgradeableInstance node = cluster.get(NODE_LEFT);
        // testing keyspace has RF == 2. Using --force does not hurt fault tolerance.
        node.nodetoolResult("decommission", "--force").asserts().success();

        // Fetch the local token ranges again;
        // RingTopologyRefresher should detect the topology change and notify RestoreJobDiscover via #onRingTopologyChanged
        localTokenRanges = ringTopologyRefresher.localTokenRanges(tableName.keyspace(), true);
        assertThat(localTokenRanges)
        .hasSize(1)
        .containsEntry(NODE_1, ImmutableSet.of(new TokenRange(Long.MIN_VALUE, 0),
                                               new TokenRange(0, 2000), // expanded from (1000, 2000] to (0, 2000] due to left node
                                               new TokenRange(2000, Long.MAX_VALUE)))
        .doesNotContainKey(NODE_LEFT);

        // Using loopAssert because #onRingTopologyChanged runs in another thread. It takes some time to reflect the RestoreRange update
        loopAssert(30, 1000, () -> {
            List<RestoreRange> restoreRanges = rangeDatabaseAccessor.findAll(jobId, bucketId);
            assertThat(restoreRanges)
            .hasSize(2);
            assertThat(restoreRanges.get(0).tokenRange()).isEqualTo(new TokenRange(0, 1000)); // node 1 now gains the range and create the restoreRange
            assertThat(restoreRanges.get(1).tokenRange()).isEqualTo(new TokenRange(1000, 1600));
        });
    }

    private static UpgradeableCluster startCluster(TokenSupplier tokenSupplier, ConfigurableCassandraTestContext cassandraTestContext)
    {
        return cassandraTestContext.configureAndStartCluster(builder -> {
            builder.withTokenSupplier(tokenSupplier);
        });
    }
}
