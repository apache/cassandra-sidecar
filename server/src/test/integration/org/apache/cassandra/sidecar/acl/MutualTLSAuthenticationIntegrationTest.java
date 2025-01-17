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

package org.apache.cassandra.sidecar.acl;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLHandshakeException;

import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.Session;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.response.SchemaResponse;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.CassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class MutualTLSAuthenticationIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest()
    void testAuthenticatedAdminRequest(VertxTestContext context) throws Exception
    {
        String testRoute = "/api/v1/schema/keyspaces";
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/admin");
        WebClient client = createClient(clientKeystorePath, truststorePath);
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .send(context.succeeding(response -> {
                  SchemaResponse schemaResponse = response.bodyAsJson(SchemaResponse.class);
                  assertThat(schemaResponse).isNotNull();
                  assertThat(schemaResponse.keyspace()).isNull();
                  assertThat(schemaResponse.schema()).isNotNull();
                  context.completeNow();
              }));
    }

    @CassandraIntegrationTest()
    void testAssociatingRoleWithNonAdminIdentity(VertxTestContext context, CassandraTestContext cassandraTestContext) throws Exception
    {
        waitForSchemaReady(1, TimeUnit.MINUTES);
        if (cassandraTestContext.version.major == 5)
        {
            insertIdentityRole("spiffe://cassandra/sidecar/test", "cassandra-role");
            grantSidecarPermission("cassandra-role", "cluster", "SCHEMA:READ");
        }

        // wait for cache refresh to pick by granted SCHEMA:READ permission
        Thread.sleep(2000L);

        String testRoute = "/api/v1/schema/keyspaces";
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test");
        WebClient client = createClient(clientKeystorePath, truststorePath);

        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .send(context.succeeding(response -> {
                  if (cassandraTestContext.version.major == 5)
                  {
                      assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                      SchemaResponse schemaResponse = response.bodyAsJson(SchemaResponse.class);
                      assertThat(schemaResponse).isNotNull();
                      assertThat(schemaResponse.keyspace()).isNull();
                      assertThat(schemaResponse.schema()).isNotNull();
                  }
                  else
                  {
                      assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.UNAUTHORIZED.code());
                  }
                  context.completeNow();
              }));
    }

    @CassandraIntegrationTest()
    void testUnAuthenticatedIdentity(VertxTestContext context) throws Exception
    {
        String testRoute = "/api/v1/schema/keyspaces";
        // identity not present in identity_to_role table
        Path clientKeystorePath = clientKeystorePath("spiffe://random/unauthenticated/identity");
        WebClient client = createClient(clientKeystorePath, truststorePath);

        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.UNAUTHORIZED.code());
                  context.completeNow();
              }));
    }

    @CassandraIntegrationTest()
    void testExpiredCertificate(VertxTestContext context) throws Exception
    {
        String testRoute = "/api/v1/schema/keyspaces";
        Path clientKeystorePath = clientKeystorePath("spiffe://random/unauthenticated/identity", true);
        WebClient client = createClient(clientKeystorePath, truststorePath);

        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .send(context.failing(response -> {
                  assertThat(response.getCause()).isInstanceOf(SSLHandshakeException.class);
                  context.completeNow();
              }));
    }

    @CassandraIntegrationTest()
    void testInvalidIdentity(VertxTestContext context) throws Exception
    {
        String testRoute = "/api/v1/schema/keyspaces";
        Path clientKeystorePath = clientKeystorePath("identity_not_spiffe");
        WebClient client = createClient(clientKeystorePath, truststorePath);

        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.UNAUTHORIZED.code());
                  context.completeNow();
              }));
    }

    private void insertIdentityRole(String identity, String role)
    {
        Session session = maybeGetSession();
        session.execute("INSERT INTO system_auth.identity_to_role (identity, role) VALUES (\'" + identity + "\',\'" + role + "\');");
    }

    private void grantSidecarPermission(String role, String resource, String permission)
    {
        Session session = maybeGetSession();
        session.execute(String.format("INSERT INTO sidecar_internal.role_permissions_v1 (role, resource, permissions) " +
                                      "VALUES ('%s', '%s', {'%s'})", role, resource, permission));
    }
}
