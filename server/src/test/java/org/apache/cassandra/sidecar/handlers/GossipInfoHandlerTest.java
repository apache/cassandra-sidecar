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

package org.apache.cassandra.sidecar.handlers;

import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.response.GossipInfoResponse;
import org.apache.cassandra.sidecar.common.server.ClusterMembershipOperations;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link GossipInfoHandler}
 */
@ExtendWith(VertxExtension.class)
public class GossipInfoHandlerTest extends CommonTest
{
    ClusterMembershipOperations ops = mock(ClusterMembershipOperations.class);

    @Test
    void testGetGossipInfo(VertxTestContext context)
    {
        when(ops.gossipInfo()).thenReturn(SAMPLE_GOSSIP_INFO);
        when(delegate.clusterMembershipOperations()).thenReturn(ops);

        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/cassandra/gossip";
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  GossipInfoResponse gossipResponse = response.bodyAsJson(GossipInfoResponse.class);
                  assertThat(gossipResponse).isNotNull();
                  GossipInfoResponse.GossipInfo gossipInfo = gossipResponse.get("/127.0.0.1:7000");
                  assertThat(gossipInfo).isNotNull().hasSize(6);
                  assertThat(gossipInfo.generation()).isEqualTo("1668100877");
                  assertThat(gossipInfo.heartbeat()).isEqualTo("242");
                  assertThat(gossipInfo.load()).isEqualTo("88971.0");
                  assertThat(gossipInfo.statusWithPort())
                  .isEqualTo("NORMAL,-9223372036854775808");
                  assertThat(gossipInfo.sstableVersions())
                  .isEqualTo(Collections.singletonList("big-nb"));
                  assertThat(gossipInfo.tokens()).isEqualTo("<hidden>");
                  context.completeNow();
              }));
    }

    private static final String SAMPLE_GOSSIP_INFO =
    "/127.0.0.1:7000\n" +
    "  generation:1668100877\n" +
    "  heartbeat:242\n" +
    "  LOAD:211:88971.0\n" +
    "  STATUS_WITH_PORT:19:NORMAL,-9223372036854775808\n" +
    "  SSTABLE_VERSIONS:6:big-nb\n" +
    "  TOKENS:18:<hidden>";
}
