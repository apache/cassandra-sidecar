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

package org.apache.cassandra.sidecar.routes.tokenrange;

import java.util.Collections;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cluster expansion scenarios integration tests for token range replica mapping endpoint with the in-jvm
 * dtest framework.
 */
@ExtendWith(VertxExtension.class)
public class JoiningTest extends JoiningBaseTest
{
    @CassandraIntegrationTest(nodesPerDc = 3, newNodesPerDc = 1, network = true)
    void retrieveMappingWithKeyspaceWithAddNode(VertxTestContext context) throws Exception
    {
        createTestKeyspace(ImmutableMap.of("replication_factor", DEFAULT_RF));
        UpgradeableCluster cluster = sidecarTestContext.cluster();
        IUpgradeableInstance instance = cluster.get(1);
        IUpgradeableInstance newInstance = ClusterUtils.addInstance(cluster,
                                                                    instance.config().localDatacenter(),
                                                                    instance.config().localRack(),
                                                                    inst -> inst.with(Feature.NETWORK,
                                                                                      Feature.GOSSIP,
                                                                                      Feature.JMX,
                                                                                      Feature.NATIVE_PROTOCOL));
        cluster.get(4).startup(cluster);
        ClusterUtils.awaitRingState(instance, newInstance, "Normal");

        retrieveMappingWithKeyspace(context, TEST_KEYSPACE, response -> {
            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
            TokenRangeReplicasResponse mappingResponse = response.bodyAsJson(TokenRangeReplicasResponse.class);
            assertMappingResponseOK(mappingResponse, DEFAULT_RF, Collections.singleton("datacenter1"));
            context.completeNow();
        });
    }
}
