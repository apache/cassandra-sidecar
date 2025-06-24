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

import org.junit.jupiter.api.Test;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpResponseExpectation;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;

import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the {@link  org.apache.cassandra.sidecar.handlers.cassandra.NodeSettingsHandler}
 */
class NodeSettingsIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    @Test
    void testNodeSettings()
    {
        HttpResponse<Buffer> response = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", "/api/v1/cassandra/settings")
                                                                   .send()
                                                                   .expecting(HttpResponseExpectation.SC_OK));
        NodeSettings nodeSettings = response.bodyAsJson(NodeSettings.class);
        assertThat(nodeSettings).isNotNull();
        assertThat(nodeSettings.partitioner()).isEqualTo("org.apache.cassandra.dht.Murmur3Partitioner");
        assertThat(nodeSettings.datacenter()).isEqualTo("datacenter1");

        cluster.stopUnchecked(cluster.getFirstRunningInstance());

        loopAssert(60, () -> {
            HttpResponse<Buffer> responseAfterStop = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", "/api/v1/cassandra/settings")
                                                                                .send()
                                                                                .expecting(HttpResponseExpectation.SC_SERVICE_UNAVAILABLE));
            assertThat(responseAfterStop).isNotNull();
            assertThat(responseAfterStop.statusCode()).isEqualTo(SERVICE_UNAVAILABLE.code());
            assertThat(responseAfterStop.bodyAsJsonObject().getString("message")).contains("NodeSettings unavailable");
        });
    }

    @Override
    protected void initializeSchemaForTest()
    {
        // Do nothing
    }
}
