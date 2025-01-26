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

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.sidecar.testing.TestTokenSupplier;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

import static org.apache.cassandra.sidecar.restore.RestoreJobTestUtils.disableRestoreProcessor;

/**
 * Test the scenario that node has moved token while restore job is in progress
 */
class RestoreJobDiscovererNodeMovedIntTest extends IntegrationTestBase
{
    @Override
    protected void beforeSetup()
    {
        installTestSpecificModule(disableRestoreProcessor());
    }

    @Override
    protected int[] getInstancesToManage(int clusterSize)
    {
        return new int[] { 1, 2 };
    }

    @CassandraIntegrationTest(nodesPerDc = 3, network = true, buildCluster = false)
    void testUpdateRestoreRangesWhenNodeMoved(ConfigurableCassandraTestContext cassandraTestContext)
    {
        TokenSupplier tokenSupplier = TestTokenSupplier.staticTokens(0, 1000L, 2000L);
        UpgradeableCluster cluster = startCluster(tokenSupplier, cassandraTestContext);
        long newToken = 1500L;
        test(cluster, newToken);
    }

    private void test(UpgradeableCluster cluster, long moveTargetToken)
    {

    }

    private static UpgradeableCluster startCluster(TokenSupplier tokenSupplier, ConfigurableCassandraTestContext cassandraTestContext)
    {
        return cassandraTestContext.configureAndStartCluster(builder -> {
            builder.withTokenSupplier(tokenSupplier);
        });
    }
}
