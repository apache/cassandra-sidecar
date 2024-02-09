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

package org.apache.cassandra.sidecar.server;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.net.TrafficShapingOptions;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.data.GossipInfoResponse;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.CassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class TrafficShapingTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void testInboundTrafficShaping(VertxTestContext context, CassandraTestContext cassandraTestContext)
    throws Exception
    {
        TrafficShapingOptions updatedShapingOptions = new TrafficShapingOptions().setInboundGlobalBandwidth(64 * 1024);
        // by default traffic shaping limits are set to 0, with this limit we should see a change in test run time
        server.updateTrafficShapingOptions(updatedShapingOptions);

        String testRoute = "/api/v1/cassandra/gossip";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      GossipInfoResponse gossipResponse = response.bodyAsJson(GossipInfoResponse.class);
                      assertThat(gossipResponse).isNotNull()
                                                .hasSize(1);
                      GossipInfoResponse.GossipInfo gossipInfo = gossipResponse.values().iterator().next();
                      assertThat(gossipInfo).isNotEmpty();
                      assertThat(gossipInfo.generation()).isNotNull();
                      assertThat(gossipInfo.heartbeat()).isNotNull();
                      assertThat(gossipInfo.hostId()).isNotNull();
                      String releaseVersion = cassandraTestContext.cluster().getFirstRunningInstance()
                                                                  .getReleaseVersionString();
                      assertThat(gossipInfo.releaseVersion()).startsWith(releaseVersion);
                      context.completeNow();
                  }));
        });
    }

    @CassandraIntegrationTest
    void testOutboundTrafficShaping(VertxTestContext context, CassandraTestContext cassandraTestContext)
    throws Exception
    {
        TrafficShapingOptions updatedShapingOptions = new TrafficShapingOptions()
                                                      .setOutboundGlobalBandwidth().setInboundGlobalBandwidth();
        // by default traffic shaping limits are set to 0, with this limit we should see a change in test run time
        server.updateTrafficShapingOptions(updatedShapingOptions);

        String testRoute = "/api/v1/cassandra/gossip";
        testWithClient(context, client -> {
            long startTime = System.currentTimeMillis();
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      GossipInfoResponse gossipResponse = response.bodyAsJson(GossipInfoResponse.class);
                      long timeTaken = System.currentTimeMillis() - startTime;
                      assertThat(timeTaken).isGreaterThan(TimeUnit.SECONDS.toMillis(30));

                      assertThat(gossipResponse).isNotNull()
                                                .hasSize(1);
                      GossipInfoResponse.GossipInfo gossipInfo = gossipResponse.values().iterator().next();
                      assertThat(gossipInfo).isNotEmpty();
                      assertThat(gossipInfo.generation()).isNotNull();
                      assertThat(gossipInfo.heartbeat()).isNotNull();
                      assertThat(gossipInfo.hostId()).isNotNull();
                      String releaseVersion = cassandraTestContext.cluster().getFirstRunningInstance()
                                                                  .getReleaseVersionString();
                      assertThat(gossipInfo.releaseVersion()).startsWith(releaseVersion);
                      context.completeNow();
                  }));
        });
    }


}
