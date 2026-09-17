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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import io.vertx.core.http.HttpResponseExpectation;
import org.apache.cassandra.sidecar.common.response.SchemaResponse;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;

import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;

class CassandraSchemaRouteIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace("testkeyspace", Map.of("replication_factor", 1));
        createTestKeyspace("\"Cycling\"", Map.of("replication_factor", 1));
        createTestKeyspace("\"keyspace\"", Map.of("replication_factor", 1));
    }

    @Test
    void testListKeyspaces()
    {
        String testRoute = "/api/v1/schema/keyspaces";
        SchemaResponse response = getBlocking(trustedClient()
                                              .get(serverWrapper.serverPort, "localhost", testRoute)
                                              .send()
                                              .expecting(HttpResponseExpectation.SC_OK))
                                  .bodyAsJson(SchemaResponse.class);
        assertThat(response).isNotNull();
        assertThat(response.keyspace()).isNull();
        assertThat(response.schema()).isNotNull();
    }

    @Test
    void testSchemaHandlerKeyspaceDoesNotExist()
    {
        String testRoute = "/api/v1/schema/keyspaces/non_existent";
        getBlocking(trustedClient()
                    .get(serverWrapper.serverPort, "localhost", testRoute)
                    .send()
                    .expecting(HttpResponseExpectation.SC_NOT_FOUND));
    }

    @Test
    void testSchemaHandlerWithKeyspace()
    {
        String testRoute = "/api/v1/schema/keyspaces/testkeyspace";
        SchemaResponse response = getBlocking(trustedClient()
                                              .get(serverWrapper.serverPort, "localhost", testRoute)
                                              .send()
                                              .expecting(HttpResponseExpectation.SC_OK))
                                  .bodyAsJson(SchemaResponse.class);
        assertThat(response).isNotNull();
        assertThat(response.keyspace()).isEqualTo("testkeyspace");
        assertThat(response.schema()).isNotNull();
    }

    @Test
    void testSchemaHandlerWithCaseSensitiveKeyspace()
    {
        String testRoute = "/api/v1/schema/keyspaces/\"Cycling\"";
        SchemaResponse response = getBlocking(trustedClient()
                                              .get(serverWrapper.serverPort, "localhost", testRoute)
                                              .send()
                                              .expecting(HttpResponseExpectation.SC_OK))
                                  .bodyAsJson(SchemaResponse.class);
        assertThat(response).isNotNull();
        assertThat(response.keyspace()).isEqualTo("Cycling");
        assertThat(response.schema()).isNotNull();
    }

    @Test
    void testSchemaHandlerWithReservedKeywordKeyspace()
    {
        String testRoute = "/api/v1/schema/keyspaces/\"keyspace\"";
        SchemaResponse response = getBlocking(trustedClient()
                                              .get(serverWrapper.serverPort, "localhost", testRoute)
                                              .send()
                                              .expecting(HttpResponseExpectation.SC_OK))
                                  .bodyAsJson(SchemaResponse.class);
        assertThat(response).isNotNull();
        assertThat(response.keyspace()).isEqualTo("keyspace");
        assertThat(response.schema()).isNotNull();
    }

    @Test
    void testSchemaResponseForEachKeyspace()
    {
        for (String keyspace : List.of("testkeyspace", "\"Cycling\"", "\"keyspace\""))
        {
            String testRoute = "/api/v1/schema/keyspaces/" + keyspace;
            SchemaResponse response = getBlocking(trustedClient()
                                                  .get(serverWrapper.serverPort, "localhost", testRoute)
                                                  .send()
                                                  .expecting(HttpResponseExpectation.SC_OK))
                                      .bodyAsJson(SchemaResponse.class);
            assertThat(response.schema())
            .describedAs("Schema of %s must be the schema rendered by Cassandra", keyspace)
            .isEqualTo(describeKeyspace(keyspace));
        }
    }

    @Test
    void testAllKeyspacesPresentInFullSchema()
    {
        // test keyspaces are created before Sidecar starts, so they are cached by the time it serves requests
        String testRoute = "/api/v1/schema/keyspaces";
        SchemaResponse response = getBlocking(trustedClient()
                                              .get(serverWrapper.serverPort, "localhost", testRoute)
                                              .send()
                                              .expecting(HttpResponseExpectation.SC_OK))
                                  .bodyAsJson(SchemaResponse.class);
        assertThat(response.schema()).contains(describeKeyspace("testkeyspace"))
                                     .contains(describeKeyspace("\"Cycling\""))
                                     .contains(describeKeyspace("\"keyspace\""))
                                     // system keyspaces are served as well
                                     .contains("CREATE KEYSPACE system_traces");
    }

    String describeKeyspace(String maybeQuotedKeyspace)
    {
        try (Cluster driverCluster = createDriverCluster(cluster.delegate());
             Session session = driverCluster.connect())
        {
            List<String> statements = new ArrayList<>();
            for (Row row : session.execute("DESCRIBE KEYSPACE " + maybeQuotedKeyspace).all())
            {
                statements.add(row.getString("create_statement"));
            }
            assertThat(statements).isNotEmpty();
            return String.join("\n\n", statements);
        }
    }
}
