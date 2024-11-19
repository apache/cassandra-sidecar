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

import com.datastax.driver.core.Session;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.sidecar.common.response.ConnectedClientStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.ClientConnectionEntry;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.yaml.KeyStoreConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SslConfigurationImpl;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

import static org.apache.cassandra.sidecar.testing.IntegrationTestModule.ADMIN_IDENTITY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for authenticated {@link org.apache.cassandra.sidecar.cluster.CQLSessionProviderImpl}
 */
@ExtendWith(VertxExtension.class)
class CqlSessionProviderWithAuthIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest(buildCluster = false)
    void testWithUsernamePassword(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext) throws Exception
    {
        configureAndStartCluster(cassandraContext, true, false);
        sidecarTestContext.refreshInstancesConfig();
        runTest(cassandraContext.version.major, context, "Password");
    }

    @CassandraIntegrationTest(buildCluster = false)
    void testWithSSLOnly(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext) throws Exception
    {
        configureAndStartCluster(cassandraContext, false, false);
        sidecarTestContext.refreshInstancesConfig();
        runTest(cassandraContext.version.major, context, "Unauthenticated");
    }

    @CassandraIntegrationTest(buildCluster = false)
    void testWithMTLS(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext) throws Exception
    {
        // mTLS is supported starting 5.0
        if (cassandraContext.version.major == 4)
        {
            context.completeNow();
            return;
        }
        configureAndStartCluster(cassandraContext, false, true);
        waitForSchemaReady(1, TimeUnit.MINUTES);
        insertIdentityRole(ADMIN_IDENTITY, "cassandra-role");
        runTest(cassandraContext.version.major, context, "Unauthenticated");
    }

    private void configureAndStartCluster(ConfigurableCassandraTestContext cassandraContext, boolean withPassword, boolean withMTLS)
    {
        cassandraContext.configureAndStartCluster(builder -> {
            builder.appendConfig(config -> config.set("client_encryption_options.enabled", "true")
                                                 .set("client_encryption_options.optional", "false")
                                                 .set("client_encryption_options.require_client_auth", "true")
                                                 .set("client_encryption_options.keystore", serverKeystorePath.toAbsolutePath().toString())
                                                 .set("client_encryption_options.keystore_password", serverKeystorePassword)
                                                 .set("client_encryption_options.truststore", truststorePath.toAbsolutePath().toString())
                                                 .set("client_encryption_options.truststore_password", truststorePassword))
            ;
            if (withPassword)
            {
                setPasswordAuthenticator(cassandraContext, builder);
                return;
            }
            if (withMTLS)
            {
                setMTLSAuthenticator(builder);
            }
        });
    }

    private void setPasswordAuthenticator(ConfigurableCassandraTestContext cassandraContext, UpgradeableCluster.Builder builder)
    {
        if (cassandraContext.version.major == 5)
        {
            builder.appendConfig(config -> config.set("authenticator.class_name", "PasswordAuthenticator")
                                                 .set("role_manager.class_name", "CassandraRoleManager")
                                                 .set("authorizer", "CassandraAuthorizer"));
            return;
        }
        builder.appendConfig(config -> config.set("authenticator", "PasswordAuthenticator")
                                             .set("role_manager", "CassandraRoleManager")
                                             .set("authorizer", "CassandraAuthorizer"));
    }

    private void setMTLSAuthenticator(UpgradeableCluster.Builder  builder)
    {
        builder.appendConfig(config -> config.set("authenticator.class_name", "org.apache.cassandra.auth.MutualTlsWithPasswordFallbackAuthenticator")
                                             .set("authenticator.parameters.validator_class_name", "org.apache.cassandra.auth.SpiffeCertificateValidator"));
    }


    private void runTest(int cassandraMajorVersion, VertxTestContext context, String expectedAuthenticationMode) throws Exception
    {
        // authentication mode was introduced after 4.0 version
        if (cassandraMajorVersion == 4)
        {
            retrieveClientStats(context, null);
        }
        else
        {
            retrieveClientStats(context, expectedAuthenticationMode);
        }
    }

    private void retrieveClientStats(VertxTestContext context, String expectedAuthenticationMode) throws Exception
    {
        String testRoute = "/api/v1/cassandra/stats/connected-clients?summary=false";
        WebClient client = mTLSClient();
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .send(context.succeeding(response -> {
                  ConnectedClientStatsResponse clientStatsResponse = response.bodyAsJson(ConnectedClientStatsResponse.class);
                  assertThat(clientStatsResponse).isNotNull();

                  for (ClientConnectionEntry entry : clientStatsResponse.clientConnections())
                  {
                      assertThat(entry.sslEnabled()).isTrue();
                      if (expectedAuthenticationMode != null)
                      {
                          assertThat(entry.authenticationMode()).isEqualTo(expectedAuthenticationMode);
                      }
                  }
                  context.completeNow();
              }));
    }

    private void insertIdentityRole(String identity, String role)
    {
        Session session = maybeGetSession();
        session.execute("INSERT INTO system_auth.identity_to_role (identity, role) VALUES (\'" + identity + "\',\'" + role + "\');");
    }

    private SslConfiguration clientSslConfig()
    {
        return SslConfigurationImpl.builder()
                                   .enabled(true)
                                   .keystore(new KeyStoreConfigurationImpl(clientKeystorePath.toAbsolutePath().toString(), clientKeystorePassword, "PKCS12"))
                                   .truststore(new KeyStoreConfigurationImpl(truststorePath.toAbsolutePath().toString(), truststorePassword, "PKCS12"))
                                   .build();
    }
}
