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

package org.apache.cassandra.sidecar.cluster.locator;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.response.ConnectedClientStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.ClientConnectionEntry;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.AuthMode;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.CassandraTestContext;

import static org.apache.cassandra.sidecar.testing.IntegrationTestModule.ADMIN_IDENTITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Test for authenticated {@link org.apache.cassandra.sidecar.cluster.CQLSessionProviderImpl}
 */
@ExtendWith(VertxExtension.class)
class CqlSessionProviderIntegrationTest extends IntegrationTestBase
{
    private static final int MIN_VERSION_WITH_MTLS = 5;

    @CassandraIntegrationTest(authMode = AuthMode.PASSWORD)
    void testWithUsernamePassword(VertxTestContext context) throws Exception
    {
        waitForSchemaReady(30, TimeUnit.SECONDS);
        retrieveClientStats(context, "cassandra", false);
    }

    @CassandraIntegrationTest(enableSsl = true)
    void testWithSSLOnly(VertxTestContext context) throws Exception
    {
        sidecarTestContext.setUsernamePassword(null, null);
        waitForSchemaReady(30, TimeUnit.SECONDS);
        // we enable only SSL and do not set any authenticator, hence username is "anonymous"
        retrieveClientStats(context, "anonymous", true);
    }

    @CassandraIntegrationTest(enableSsl = true, authMode = AuthMode.PASSWORD)
    void testWithSSLOnlyWithUsername(VertxTestContext context) throws Exception
    {
        sidecarTestContext.setUsernamePassword("cassandra", "cassandra");
        waitForSchemaReady(30, TimeUnit.SECONDS);
        // we enable only SSL and do not set any authenticator, hence username is "anonymous"
        retrieveClientStats(context, "cassandra", true);
    }

    @CassandraIntegrationTest(authMode = AuthMode.MUTUAL_TLS)
    void testWithMTLS(VertxTestContext context, CassandraTestContext cassandraTestContext) throws Exception
    {
        // mTLS authentication was added in Cassandra starting 5.0 version
        assumeThat(cassandraTestContext.version.major)
        .withFailMessage("mTLS authentication is not supported in 4.0 Cassandra version")
        .isGreaterThanOrEqualTo(MIN_VERSION_WITH_MTLS);

        insertIdentityRole(cassandraTestContext, ADMIN_IDENTITY, "cassandra");
        waitForSchemaReady(30, TimeUnit.SECONDS);

        retrieveClientStats(context, "cassandra", true);
    }

    private void retrieveClientStats(VertxTestContext context, String expectedUsername, boolean checkSsl) throws Exception
    {
        String testRoute = "/api/v1/cassandra/stats/connected-clients?summary=false";
        WebClient client = mTLSClient();
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .send(context.succeeding(response -> {
                  context.verify(() -> {
                      ConnectedClientStatsResponse clientStatsResponse = response.bodyAsJson(ConnectedClientStatsResponse.class);
                      assertThat(clientStatsResponse).isNotNull();

                      boolean seeSslConnection = false;
                      for (ClientConnectionEntry entry : clientStatsResponse.clientConnections())
                      {
                          assertThat(entry.username()).isEqualTo(expectedUsername);
                          if (checkSsl && entry.sslEnabled())
                          {
                              seeSslConnection = true;
                              break;
                          }
                      }
                      // We expect some connections to be non-SSL (i.e. for identity setup)
                      // and some connections to be SSL (Sidecar connecting to the cluster)
                      // so from the list of client connections we should see at least
                      // two (regular+control) connections.
                      assertSslConnectionIfNeeded(checkSsl, seeSslConnection);
                  });
                  context.completeNow();
              }));
    }

    private void assertSslConnectionIfNeeded(boolean checkSsl, boolean seeSslConnection)
    {
        if (checkSsl)
        {
            assertThat(seeSslConnection)
            .describedAs("Did not see any SSL connection")
            .isTrue();
        }
    }

    private void insertIdentityRole(CassandraTestContext cassandraContext, String identity, String role)
    {
        String statement = String.format("INSERT INTO system_auth.identity_to_role (identity, role) VALUES ('%s','%s')",
                                         identity, role);
        cassandraContext.cluster().schemaChangeIgnoringStoppedInstances(statement);
    }
}
