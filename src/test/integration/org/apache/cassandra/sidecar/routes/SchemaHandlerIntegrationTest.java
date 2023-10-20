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

import com.datastax.driver.core.Session;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.data.SchemaResponse;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the {@link SchemaHandler}
 */
@ExtendWith(VertxExtension.class)
class SchemaHandlerIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void schemaHandlerNoKeyspace(VertxTestContext context) throws Exception
    {
        String testRoute = "/api/v1/schema/keyspaces";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      SchemaResponse schemaResponse = response.bodyAsJson(SchemaResponse.class);
                      assertThat(schemaResponse).isNotNull();
                      assertThat(schemaResponse.keyspace()).isNull();
                      assertThat(schemaResponse.schema()).isNotNull();
                      context.completeNow();
                  }));
        });
    }

    @CassandraIntegrationTest
    void schemaHandlerKeyspaceDoesNotExist(VertxTestContext context) throws Exception
    {
        String testRoute = "/api/v1/schema/keyspaces/non_existent";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_NOT_FOUND)
                  .send(context.succeedingThenComplete());
        });
    }

    @CassandraIntegrationTest
    void schemaHandlerWithKeyspace(VertxTestContext context) throws Exception
    {
        createTestKeyspace();

        String testRoute = "/api/v1/schema/keyspaces/testkeyspace";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      SchemaResponse schemaResponse = response.bodyAsJson(SchemaResponse.class);
                      assertThat(schemaResponse).isNotNull();
                      assertThat(schemaResponse.keyspace()).isEqualTo("testkeyspace");
                      assertThat(schemaResponse.schema()).isNotNull();
                      context.completeNow();
                  }));
        });
    }

    @CassandraIntegrationTest
    void schemaHandlerWithCaseSensitiveKeyspace(VertxTestContext context) throws Exception
    {
        try (Session session = maybeGetSession())
        {
            session.execute("CREATE KEYSPACE \"Cycling\"" +
                            " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };");
        }

        String testRoute = "/api/v1/schema/keyspaces/\"Cycling\"";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      SchemaResponse schemaResponse = response.bodyAsJson(SchemaResponse.class);
                      assertThat(schemaResponse).isNotNull();
                      assertThat(schemaResponse.keyspace()).isEqualTo("Cycling");
                      assertThat(schemaResponse.schema()).isNotNull();
                      context.completeNow();
                  }));
        });
    }

    @CassandraIntegrationTest
    void schemaHandlerWithReservedKeywordKeyspace(VertxTestContext context) throws Exception
    {
        try (Session session = maybeGetSession())
        {
            session.execute("CREATE KEYSPACE \"keyspace\"" +
                            " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };");
        }

        String testRoute = "/api/v1/schema/keyspaces/\"keyspace\"";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .expect(ResponsePredicate.SC_OK)
                  .send(context.succeeding(response -> {
                      SchemaResponse schemaResponse = response.bodyAsJson(SchemaResponse.class);
                      assertThat(schemaResponse).isNotNull();
                      assertThat(schemaResponse.keyspace()).isEqualTo("keyspace");
                      assertThat(schemaResponse.schema()).isNotNull();
                      context.completeNow();
                  }));
        });
    }
}
