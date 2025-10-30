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

package org.apache.cassandra.sidecar.routes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpResponseExpectation;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.cassandra.sidecar.common.response.GossipInfoResponse;
import org.apache.cassandra.sidecar.common.response.HealthResponse;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Basic routes testings in Sidecar
 */
class RoutesIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    @BeforeEach
    void ensureGossipRunning()
    {
        // This is fast when it's already enabled (no-op on the Cassandra side)
        assertThat(cluster.getFirstRunningInstance().nodetool("enablegossip")).isEqualTo(0);
    }

    @Test
    void healthHappyPathTest()
    {
        HttpResponse<Buffer> response = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", "/api/v1/__health")
                                                                   .send());
        assertThat(response.bodyAsJsonObject().getString("status")).isEqualTo("OK");
    }

    @Test
    void retrieveGossipInfo()
    {
        String testRoute = "/api/v1/cassandra/gossip";
        HttpResponse<Buffer> response = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", testRoute)
                                                                   .send()
                                                                   .expecting(HttpResponseExpectation.SC_OK));
        GossipInfoResponse gossipResponse = response.bodyAsJson(GossipInfoResponse.class);
        assertThat(gossipResponse).isNotNull()
                                  .hasSize(1);
        GossipInfoResponse.GossipInfo gossipInfo = gossipResponse.values().iterator().next();
        assertThat(gossipInfo).isNotEmpty();
        assertThat(gossipInfo.generation()).isNotNull();
        assertThat(gossipInfo.heartbeat()).isNotNull();
        assertThat(gossipInfo.hostId()).isNotNull();
        String releaseVersion = cluster.getFirstRunningInstance().getReleaseVersionString();
        releaseVersion = stripSnapshot(releaseVersion);
        assertThat(gossipInfo.releaseVersion()).startsWith(releaseVersion);
    }

    private String stripSnapshot(String version)
    {
        return version.replace("-SNAPSHOT", "");
    }

    @Test
    void testGossipHealth()
    {
        int disableGossip = cluster.getFirstRunningInstance().nodetool("disablegossip");
        assertThat(disableGossip).isEqualTo(0);

        HealthResponse gossipHealth = getGossipHealth();
        assertThat(gossipHealth.status()).isEqualTo("NOT_OK");
        assertThat(cluster.getFirstRunningInstance().nodetool("enablegossip")).isEqualTo(0);
        gossipHealth = getGossipHealth();
        assertThat(gossipHealth.status()).isEqualTo("OK");
    }

    private HealthResponse getGossipHealth()
    {
        String testRoute = "/api/v1/cassandra/gossip/__health";
        HttpResponse<Buffer> response = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", testRoute)
                                                                   .send()
                                                                   .expecting(HttpResponseExpectation.SC_OK));
        assertThat(response.statusCode()).isEqualTo(OK.code());
        return response.bodyAsJson(HealthResponse.class);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        // Do nothing
    }
}
