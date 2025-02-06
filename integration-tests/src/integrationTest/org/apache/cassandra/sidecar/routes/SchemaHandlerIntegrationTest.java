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

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.vertx.ext.web.client.predicate.ResponsePredicate;
import org.apache.cassandra.sidecar.common.response.SchemaResponse;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;

import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;

class SchemaHandlerIntegrationTest extends SharedClusterSidecarIntegrationTestBase
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
        SchemaResponse response = getBlocking(trustedClient().get(server.actualPort(), "localhost", testRoute)
                                                             .expect(ResponsePredicate.SC_OK)
                                                             .send())
                                  .bodyAsJson(SchemaResponse.class);
        assertThat(response).isNotNull();
        assertThat(response.keyspace()).isNull();
        assertThat(response.schema()).isNotNull();
    }

    @Test
    void testSchemaHandlerKeyspaceDoesNotExist()
    {
        String testRoute = "/api/v1/schema/keyspaces/non_existent";
        getBlocking(trustedClient().get(server.actualPort(), "localhost", testRoute)
                                   .expect(ResponsePredicate.SC_NOT_FOUND)
                                   .send());
    }

    @Test
    void testSchemaHandlerWithKeyspace()
    {
        String testRoute = "/api/v1/schema/keyspaces/testkeyspace";
        SchemaResponse response = getBlocking(trustedClient().get(server.actualPort(), "localhost", testRoute)
                                                             .expect(ResponsePredicate.SC_OK)
                                                             .send())
                                  .bodyAsJson(SchemaResponse.class);
        assertThat(response).isNotNull();
        assertThat(response.keyspace()).isEqualTo("testkeyspace");
        assertThat(response.schema()).isNotNull();
    }

    @Test
    void testSchemaHandlerWithCaseSensitiveKeyspace()
    {
        String testRoute = "/api/v1/schema/keyspaces/\"Cycling\"";
        SchemaResponse response = getBlocking(trustedClient().get(server.actualPort(), "localhost", testRoute)
                                                             .expect(ResponsePredicate.SC_OK)
                                                             .send())
                                  .bodyAsJson(SchemaResponse.class);
        assertThat(response).isNotNull();
        assertThat(response.keyspace()).isEqualTo("Cycling");
        assertThat(response.schema()).isNotNull();
    }

    @Test
    void testSchemaHandlerWithReservedKeywordKeyspace()
    {
        String testRoute = "/api/v1/schema/keyspaces/\"keyspace\"";
        SchemaResponse response = getBlocking(trustedClient().get(server.actualPort(), "localhost", testRoute)
                                                             .expect(ResponsePredicate.SC_OK)
                                                             .send())
                                  .bodyAsJson(SchemaResponse.class);
        assertThat(response).isNotNull();
        assertThat(response.keyspace()).isEqualTo("keyspace");
        assertThat(response.schema()).isNotNull();
    }
}
