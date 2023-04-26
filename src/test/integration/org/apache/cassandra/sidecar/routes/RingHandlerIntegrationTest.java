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


import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.IntegrationTestBase;
import org.apache.cassandra.sidecar.common.data.RingEntry;
import org.apache.cassandra.sidecar.common.data.RingResponse;
import org.apache.cassandra.sidecar.common.testing.CassandraIntegrationTest;
import org.apache.cassandra.sidecar.common.testing.CassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the /cassandra/ring endpoint with cassandra container.
 */
@ExtendWith(VertxExtension.class)
class RingHandlerIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void retrieveRingWithoutKeyspace(VertxTestContext context) throws Exception
    {
        String testRoute = "/api/v1/cassandra/ring";
        testWithClient(context, client -> {
            client.get(config.getPort(), "localhost", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      assertRingResponseOK(response);
                      context.completeNow();
                  }));
        });
    }

    @CassandraIntegrationTest
    void retrieveRingWithUnknownKeyspace(VertxTestContext context) throws Exception
    {
        retrieveRingWithKeyspace(context, "unknown_ks", response -> {
            int errorCode = HttpResponseStatus.NOT_FOUND.code();
            assertThat(response.statusCode()).isEqualTo(errorCode);
            JsonObject error = response.bodyAsJsonObject();
            assertThat(error.getInteger("code")).isEqualTo(errorCode);
            assertThat(error.getString("message")).contains("The keyspace unknown_ks, does not exist");

            context.completeNow();
        });
    }

    @CassandraIntegrationTest
    void retrieveRingWithExistingKeyspace(VertxTestContext context,
                                          CassandraTestContext cassandraTestContext) throws Exception
    {
        createTestKeyspace(cassandraTestContext);
        retrieveRingWithKeyspace(context, TEST_KEYSPACE, response -> {
            assertRingResponseOK(response);
            context.completeNow();
        });
    }

    void retrieveRingWithKeyspace(VertxTestContext context, String keyspace,
                                  Handler<HttpResponse<Buffer>> verifier) throws Exception
    {
        String testRoute = "/api/v1/cassandra/ring/keyspaces/" + keyspace;
        testWithClient(context, client -> {
            client.get(config.getPort(), "localhost", testRoute)
                  .send(context.succeeding(verifier));
        });
    }

    void assertRingResponseOK(HttpResponse<Buffer> response)
    {
        RingResponse ringResponse = response.bodyAsJson(RingResponse.class);
        assertThat(ringResponse).isNotNull()
                                .hasSize(1);
        RingEntry entry = ringResponse.poll();
        assertThat(entry).isNotNull();
        assertThat(entry.datacenter()).isEqualTo("datacenter1");
        assertThat(entry.address()).isNotEmpty();
        assertThat(entry.port()).isEqualTo(7000);
        assertThat(entry.status()).isEqualTo("Up");
        assertThat(entry.state()).isEqualTo("Normal");
        assertThat(entry.load()).isNotEmpty();
        // there is just 1 node, so own 100%; the format should be right, i.e. "##0.00%"
        assertThat(entry.owns()).isEqualTo("100.00%");
        assertThat(entry.token()).isEqualTo("0");
        assertThat(entry.fqdn()).isNotEmpty();
        assertThat(entry.hostId()).isNotEmpty();
    }

}
