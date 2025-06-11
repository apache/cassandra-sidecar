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
 * Test the /cassandra/native endpoint with cassandra container.
 */
@ExtendWith(VertxExtension.class)
public class NodeNativeIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest(nodesPerDc = 1)
    void nativeTransportToggleAndHealth(VertxTestContext ctx)
    {
        // 1) STOP native transport → expect 202 Accepted
        testWithClient(client ->
                       client.put(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/native")
                             .sendBuffer(Buffer.buffer("{\"state\":\"stop\"}"), ctx.succeeding(resp -> {
                                 assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                             }))
        );

        Uninterruptibles.sleepUninterruptibly(15, SECONDS);

        // 2) GET health → expect NOT_OK
        testWithClient(client -> client.get(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/native/__health")
                                       .send(ctx.succeeding((HttpResponse<Buffer> resp) -> {
                                           assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
                                           String status = resp.bodyAsJsonObject().getString("status");
                                           assertThat(status).isEqualTo("NOT_OK");
                                       })));

        // 3) START native transport → expect 202 Accepted
        testWithClient(client ->
                       client.put(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/native")
                             .sendBuffer(Buffer.buffer("{\"state\":\"start\"}"), ctx.succeeding(resp -> {
                                 assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                             }))
        );

        Uninterruptibles.sleepUninterruptibly(60, SECONDS);

        // 4) GET health → expect OK
        testWithClient(client -> client.get(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/native/__health")
                                       .send(ctx.succeeding((HttpResponse<Buffer> resp) -> {
                                           assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                                           String status = resp.bodyAsJsonObject().getString("status");
                                           assertThat(status).isEqualTo("OK");
                                       })));

        ctx.completeNow();
    }
}

