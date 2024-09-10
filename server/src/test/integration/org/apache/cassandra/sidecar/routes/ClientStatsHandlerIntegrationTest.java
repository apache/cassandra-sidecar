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
import java.util.Map;

import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.response.ClientStatsResponse;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.CassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the client-stats endpoint with cassandra container.
 */
@ExtendWith(VertxExtension.class)
public class ClientStatsHandlerIntegrationTest extends IntegrationTestBase
{

    @CassandraIntegrationTest
    void retrieveClientStatsDefault(VertxTestContext context, CassandraTestContext cassandraTestContext)
    throws Exception
    {
        String testRoute = "/api/v1/storage/client-stats";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      assertClientStatsResponse(response, Collections.emptyMap(), cassandraTestContext.version.toString());
                      context.completeNow();
                  }));
        });
    }

    @CassandraIntegrationTest
    void retrieveClientStatsWithClientOptions(VertxTestContext context, CassandraTestContext cassandraTestContext)
    throws Exception
    {
        Map<String, Boolean> expectedParams = Collections.singletonMap("client-options", true);
        String testRoute = "/api/v1/storage/client-stats?client-options=true";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      assertClientStatsResponse(response, expectedParams, cassandraTestContext.version.toString());
                      context.completeNow();
                  }));
        });
    }

    @CassandraIntegrationTest
    void retrieveClientStatsByProtocol(VertxTestContext context, CassandraTestContext cassandraTestContext)
    throws Exception
    {
        Map<String, Boolean> expectedParams = Collections.singletonMap("by-protocol", true);
        String testRoute = "/api/v1/storage/client-stats?by-protocol=true";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      assertClientStatsResponse(response, expectedParams, cassandraTestContext.version.toString());
                      context.completeNow();
                  }));
        });
    }

    @CassandraIntegrationTest
    void retrieveClientStatsVerbose(VertxTestContext context, CassandraTestContext cassandraTestContext)
    throws Exception
    {
        Map<String, Boolean> expectedParams = Collections.singletonMap("verbose", true);
        String testRoute = "/api/v1/storage/client-stats?verbose=true";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      assertClientStatsResponse(response, expectedParams, cassandraTestContext.version.toString());
                      context.completeNow();
                  }));
        });
    }

    @CassandraIntegrationTest
    void retrieveClientStatsListConns(VertxTestContext context, CassandraTestContext cassandraTestContext)
    throws Exception
    {
        Map<String, Boolean> expectedParams = Collections.singletonMap("list-connections", true);
        String testRoute = "/api/v1/storage/client-stats?list-connections=true";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      assertClientStatsResponse(response, expectedParams, cassandraTestContext.version.toString());
                      context.completeNow();
                  }));
        });
    }

    @CassandraIntegrationTest
    void retrieveClientStatsListConnsAndVerbose(VertxTestContext context, CassandraTestContext cassandraTestContext)
    throws Exception
    {
        Map<String, Boolean> expectedParams = new HashMap<>();
        expectedParams.put("verbose", true);
        expectedParams.put("list-connections", true);
        String testRoute = "/api/v1/storage/client-stats?list-connections=true&verbose=true";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      assertClientStatsResponse(response, expectedParams, cassandraTestContext.version.toString());
                      context.completeNow();
                  }));
        });
    }

    @CassandraIntegrationTest
    void retrieveClientStatsMultipleParams(VertxTestContext context, CassandraTestContext cassandraTestContext)
    throws Exception
    {
        Map<String, Boolean> expectedParams = new HashMap<>();
        expectedParams.put("verbose", true);
        expectedParams.put("list-connections", true);
        expectedParams.put("by-protocol", true);
        String testRoute = "/api/v1/storage/client-stats?list-connections=true&verbose=true&by-protocol=true";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      assertClientStatsResponse(response, expectedParams, cassandraTestContext.version.toString());
                      context.completeNow();
                  }));
        });
    }

    @CassandraIntegrationTest
    void retrieveClientStatsInvalidParams(VertxTestContext context, CassandraTestContext cassandraTestContext)
    throws Exception
    {
        Map<String, Boolean> expectedParams = new HashMap<>();
        expectedParams.put("verbose", false);
        expectedParams.put("list-connections", false);
        expectedParams.put("by-protocol", false);
        String testRoute = "/api/v1/storage/client-stats?list-connections=abc&verbose=123&by-protocol=xyz";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      assertClientStatsResponse(response, expectedParams, cassandraTestContext.version.toString());
                      context.completeNow();
                  }));
        });
    }

    void assertClientStatsResponse(HttpResponse<Buffer> response, Map<String, Boolean> params, String cassandraVersion)
    {
        // No client options in 4.0
        boolean isClientOptions = params.getOrDefault("client-options", false);
        boolean isVerbose = params.getOrDefault("verbose", false);
        boolean isByProtocol = params.getOrDefault("by-protocol", false);
        boolean isListConnections = params.getOrDefault("list-connections", false);

        logger.info("Response:" + response.bodyAsString());
        ClientStatsResponse clientStats = response.bodyAsJson(ClientStatsResponse.class);
        assertThat(clientStats).isNotNull();
        assertThat(clientStats.connectionsByUser()).isNotEmpty();
        assertThat(clientStats.connectionsByUser()).containsKey("anonymous");
        assertThat(clientStats.totalConnectedClients()).isGreaterThan(0);

        if (isByProtocol)
        {
            assertThat(clientStats.remoteInetAddress()).contains("127.0.0.1");
            assertThat(clientStats.protocolVersion()).isNotNull();
            assertThat(clientStats.lastSeenTime()).isNotNull();
        }
        else
        {
            assertThat(clientStats.remoteInetAddress()).isNull();
            assertThat(clientStats.protocolVersion()).isNull();
            assertThat(clientStats.lastSeenTime()).isNull();
        }

        // These stats are only returned from newer C* versions. Existing 5.x version referenced from
        // integration tests do not support these stats.
        assertThat(clientStats.authenticationMetadata()).isNull();
        assertThat(clientStats.authenticationMode()).isNull();

        if (isListConnections || isVerbose || isClientOptions)
        {
            assertThat(clientStats.address()).contains("127.0.0.1");
            // Test uses default WebClient without options
            assertThat(clientStats.ssl()).isEqualTo(false);
            assertThat(clientStats.driverName()).isEqualTo("DataStax Java Driver");
            assertThat(clientStats.driverVersion()).isNotNull();
            assertThat(clientStats.user()).isEqualTo("anonymous");

        }
        else
        {
            assertThat(clientStats.address()).isNull();
            // Test uses default WebClient without options
            assertThat(clientStats.ssl()).isNull();
            assertThat(clientStats.driverName()).isNull();
            assertThat(clientStats.driverVersion()).isNull();
            assertThat(clientStats.user()).isNull();
        }

        if (!cassandraVersion.equals("4.0.0") && (isClientOptions || isVerbose))
        {
            assertThat(clientStats.clientOptions()).isNotEmpty();
        }
        else
        {
            assertThat(clientStats.clientOptions()).isNull();
        }
    }
}
