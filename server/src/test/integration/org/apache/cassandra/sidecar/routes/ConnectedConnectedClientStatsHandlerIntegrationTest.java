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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.response.ConnectedClientStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.ClientConnectionEntry;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the client-stats endpoint with cassandra container.
 */
@ExtendWith(VertxExtension.class)
public class ConnectedConnectedClientStatsHandlerIntegrationTest extends IntegrationTestBase
{
    private static final int DEFAULT_CONNECTION_COUNT = 2;

    @CassandraIntegrationTest
    void retrieveClientStatsDefault(VertxTestContext context)
    throws Exception
    {
        Map<String, Boolean> expectedParams = Collections.singletonMap("summary", true);
        String testRoute = "/api/v1/cassandra/stats/connected-clients";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      assertClientStatsResponse(response, expectedParams);
                      context.completeNow();
                  }));
        });
    }

    @CassandraIntegrationTest
    void retrieveClientStatsListConnections(VertxTestContext context)
    throws Exception
    {
        Map<String, Boolean> expectedParams = Collections.singletonMap("summary", false);
        String testRoute = "/api/v1/cassandra/stats/connected-clients?summary=false";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      assertClientStatsResponse(response, expectedParams);
                      context.completeNow();
                  }));
        });
    }

    @CassandraIntegrationTest
    void retrieveClientStatsMultipleConnections(VertxTestContext context)
    throws Exception
    {
        // Creates an additional connection pair
        createTestKeyspace();

        Map<String, Boolean> expectedParams = Collections.singletonMap("summary", true);
        String testRoute = "/api/v1/cassandra/stats/connected-clients?summary=true";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      assertClientStatsResponse(response, expectedParams, 4);
                      context.completeNow();
                  }));
        });
    }

    /**
     * Expects unrecognized params to be ignored and invalid value for the expected parameter to be defaulted to true
     * to prevent heavyweight query in the bad request case.
     */
    @CassandraIntegrationTest
    void retrieveClientStatsInvalidParamaterValue(VertxTestContext context)
    throws Exception
    {
        Map<String, Boolean> expectedParams = new HashMap<>();
        expectedParams.put("summary", true);
        String testRoute = "/api/v1/cassandra/stats/connected-clients?summary=123&bad-arg=xyz";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      assertClientStatsResponse(response, expectedParams);
                      context.completeNow();
                  }));
        });
    }

    void assertClientStatsResponse(HttpResponse<Buffer> response, Map<String, Boolean> params)
    {
        assertClientStatsResponse(response, params, DEFAULT_CONNECTION_COUNT);
    }
    void assertClientStatsResponse(HttpResponse<Buffer> response, Map<String, Boolean> params, int expectedConnections)
    {
        boolean isSummary = params.get("summary");

        logger.info("Response:" + response.bodyAsString());
        ConnectedClientStatsResponse clientStats = response.bodyAsJson(ConnectedClientStatsResponse.class);
        assertThat(clientStats).isNotNull();
        assertThat(clientStats.connectionsByUser()).isNotEmpty();
        assertThat(clientStats.connectionsByUser()).containsKey("anonymous");
        assertThat(clientStats.totalConnectedClients()).isEqualTo(expectedConnections);

        List<ClientConnectionEntry> stats = clientStats.clientConnections();
        if (isSummary)
        {
            assertThat(stats).isNull();
        }
        else
        {
            assertThat(stats.size()).isEqualTo(expectedConnections);
            for (ClientConnectionEntry stat : stats)
            {
                assertThat(stat.address()).contains("127.0.0.1");
                // Test uses default WebClient without options
                assertThat(stat.sslEnabled()).isEqualTo(false);
                assertThat(stat.driverName()).isEqualTo("DataStax Java Driver");
                assertThat(stat.driverVersion()).isNotNull();
                assertThat(stat.username()).isEqualTo("anonymous");
            }
        }
    }
}
