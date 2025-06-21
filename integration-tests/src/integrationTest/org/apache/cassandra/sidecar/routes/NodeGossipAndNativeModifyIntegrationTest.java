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

import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Test;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Combines gossip‐toggle and native‐transport‐toggle route tests into a single suite
 * so that Cassandra+Sidecar come up exactly once per JVM.
 */
public class NodeGossipAndNativeModifyIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{

    @Test
    void testGossipToggle()
    {
        // 1) STOP gossip
        HttpResponse<Buffer> gossipStopResponse = getBlocking(
        trustedClient()
        .put(serverWrapper.serverPort, "localhost", "/api/v1/cassandra/gossip")
        .sendBuffer(Buffer.buffer("{\"state\":\"stop\"}")));
        assertThat(gossipStopResponse.statusCode()).isEqualTo(OK.code());
        assertThat(gossipStopResponse.bodyAsJsonObject().getString("status")).isEqualTo("OK");

        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        // 2) gossip should now be NOT_OK
        HttpResponse<Buffer> gossipHealthDown = getBlocking(
        trustedClient()
        .get(serverWrapper.serverPort, "localhost", "/api/v1/cassandra/gossip/__health")
        .send());
        assertThat(gossipHealthDown.statusCode()).isEqualTo(OK.code());
        assertThat(gossipHealthDown.bodyAsJsonObject().getString("status")).isEqualTo("NOT_OK");

        // 3) START gossip
        HttpResponse<Buffer> startGossipResponse = getBlocking(
        trustedClient()
        .put(serverWrapper.serverPort, "localhost", "/api/v1/cassandra/gossip")
        .sendBuffer(Buffer.buffer("{\"state\":\"start\"}")));
        assertThat(startGossipResponse.statusCode()).isEqualTo(OK.code());
        assertThat(startGossipResponse.bodyAsJsonObject().getString("status")).isEqualTo("OK");

        // wait for native-transport to actually stop
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        // 4) gossip should now be OK
        HttpResponse<Buffer> gossipHealthUp = getBlocking(
        trustedClient()
        .get(serverWrapper.serverPort, "localhost", "/api/v1/cassandra/gossip/__health")
        .send());
        assertThat(gossipHealthUp.statusCode()).isEqualTo(OK.code());
        assertThat(gossipHealthUp.bodyAsJsonObject().getString("status")).isEqualTo("OK");
    }

    @Test
    void testNativeTransportToggle()
    {
        // 1) STOP native
        HttpResponse<Buffer> nativeStopResponse = getBlocking(
        trustedClient()
        .put(serverWrapper.serverPort, "localhost", "/api/v1/cassandra/native")
        .sendBuffer(Buffer.buffer("{\"state\":\"stop\"}")));
        assertThat(nativeStopResponse.statusCode()).isEqualTo(OK.code());
        assertThat(nativeStopResponse.bodyAsJsonObject().getString("status")).isEqualTo("OK");

        Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

        // 2) native should now be NOT_OK
        HttpResponse<Buffer> nativeHealthDown = getBlocking(
        trustedClient()
        .get(serverWrapper.serverPort, "localhost", "/api/v1/cassandra/native/__health")
        .send());
        assertThat(nativeHealthDown.statusCode()).isEqualTo(SERVICE_UNAVAILABLE.code());
        assertThat(nativeHealthDown.bodyAsJsonObject().getString("status")).isEqualTo("NOT_OK");

        // 3) START native
        HttpResponse<Buffer> startNativeResponse = getBlocking(
        trustedClient()
        .put(serverWrapper.serverPort, "localhost", "/api/v1/cassandra/native")
        .sendBuffer(Buffer.buffer("{\"state\":\"start\"}")));
        assertThat(startNativeResponse.statusCode()).isEqualTo(OK.code());
        assertThat(startNativeResponse.bodyAsJsonObject().getString("status")).isEqualTo("OK");

        Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

        // 4) native should now be OK
        HttpResponse<Buffer> nativeHealthUp = getBlocking(
        trustedClient()
        .get(serverWrapper.serverPort, "localhost", "/api/v1/cassandra/native/__health")
        .send());
        assertThat(nativeHealthUp.statusCode()).isEqualTo(OK.code());
        assertThat(nativeHealthUp.bodyAsJsonObject().getString("status")).isEqualTo("OK");
    }

    @Override
    protected void initializeSchemaForTest()
    {
        // Do nothing
    }
}
