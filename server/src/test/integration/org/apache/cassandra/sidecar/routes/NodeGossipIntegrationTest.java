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

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the /cassandra/gossip endpoint with cassandra container.
 */
@ExtendWith(VertxExtension.class)
public class NodeGossipIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest(nodesPerDc = 1)
    void gossipToggleAndHealth(VertxTestContext ctx)
    {
        // 1) STOP gossip
        testWithClient(client -> client.put(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/gossip")
                                       .sendBuffer(Buffer.buffer("{\"state\":\"stop\"}"), ctx.succeeding(resp -> {
                                           assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                                       })));

        Uninterruptibles.sleepUninterruptibly(3, SECONDS);

        // 2) Health should now be NOT_OK
        testWithClient(client -> client.get(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/gossip/__health")
                                       .send(ctx.succeeding((HttpResponse<Buffer> resp) -> {
                                           assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                                           String status = resp.bodyAsJsonObject().getString("status");
                                           assertThat(status).isEqualTo("NOT_OK");
                                       })));

        // 3) START gossip
        testWithClient(client -> client.put(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/gossip")
                                       .sendBuffer(Buffer.buffer("{\"state\":\"start\"}"), ctx.succeeding(resp -> {
                                           assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                                       })));

        Uninterruptibles.sleepUninterruptibly(3, SECONDS);

        // 4) Health should now be OK
        testWithClient(client -> client.get(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/gossip/__health")
                                       .send(ctx.succeeding((HttpResponse<Buffer> resp) -> {
                                           assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                                           String status = resp.bodyAsJsonObject().getString("status");
                                           assertThat(status).isEqualTo("OK");
                                           ctx.completeNow();
                                       })));
    }
}
