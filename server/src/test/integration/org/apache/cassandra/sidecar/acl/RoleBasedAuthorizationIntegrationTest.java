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
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.Session;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.yaml.KeyStoreConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SslConfigurationImpl;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

import static org.apache.cassandra.sidecar.testing.IntegrationTestModule.ADMIN_IDENTITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Test for role based access control in Sidecar
 */
@ExtendWith(VertxExtension.class)
public class RoleBasedAuthorizationIntegrationTest extends IntegrationTestBase
{
    private static final int MIN_VERSION_WITH_MTLS = 5;

    @CassandraIntegrationTest(buildCluster = false)
    void testForAdmin(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext) throws Exception
    {
        // starts cluster for 5.0 and above version test
        startClusterWithMtlsAndAuthorizer(cassandraContext);

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "sample_keyspace");
        // uses client keystore with admin identity. Admins bypass authorization checks
        verifyAccess(context, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath);
    }

    @CassandraIntegrationTest(buildCluster = false)
    void testForSuperUser(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext) throws Exception
    {
        // starts cluster for 5.0 and above version test
        startClusterWithMtlsAndAuthorizer(cassandraContext);

        createRole("test_role", true);
        insertIdentityRole("spiffe://cassandra/sidecar/test_user", "test_role");

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "sample_keyspace");
        // uses client keystore with superuser identity
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");
        verifyAccess(context, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath);
    }

    @CassandraIntegrationTest(buildCluster = false)
    void testForNonAdmin(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext) throws Exception
    {
        // starts cluster for 5.0 and above version test
        startClusterWithMtlsAndAuthorizer(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole("spiffe://cassandra/sidecar/test_user", "test_role");

        // grant permission for not non super user
        grantKeyspacePermission("sample_keyspace", "test_role");
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "sample_keyspace");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");
        verifyAccess(context, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath);
    }

    @CassandraIntegrationTest(buildCluster = false)
    void testGrantingForTable(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext)
    throws Exception
    {
        // starts cluster for 5.0 and above version test
        startClusterWithMtlsAndAuthorizer(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole("spiffe://cassandra/sidecar/test_user", "test_role");

        grantSidecarPermission("test_role", "data/sample_keyspace/sample_table", "CREATE:SNAPSHOT");

        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "sample_keyspace", "sample_table");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        Checkpoint checkpoint = context.checkpoint(2);

        // CREATE:SNAPSHOT permission granted for data/sample_keyspace/sample_table
        verifyAccess(context, checkpoint, HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, false);

        // DELETE:SNAPSHOT permission not granted for data/sample_keyspace/sample_table
        verifyAccess(context, checkpoint, HttpMethod.DELETE, createSnapshotRoute, clientKeystorePath, true);
    }

    @CassandraIntegrationTest(buildCluster = false)
    void testEndpointWithOrAuthorization(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext)
    throws Exception
    {
        // starts cluster for 5.0 and above version test
        startClusterWithMtlsAndAuthorizer(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole("spiffe://cassandra/sidecar/test_user", "test_role");

        grantSidecarPermission("test_role", "data/sample_keyspace", "VIEW:*");
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "sample_keyspace");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        // schema endpoint for keyspaces accepts CREATE, ALTER, DROP or DESCRIBE cassandra permissions.
        // cassandra permission for test_role on sample_keyspace not granted, sidecar permission VIEW:* is used to
        // grant access
        verifyAccess(context, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath);
    }

    @CassandraIntegrationTest(buildCluster = false)
    void testWildcardActionForAllTargets(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext)
    throws Exception
    {
        // starts cluster for 5.0 and above version test
        startClusterWithMtlsAndAuthorizer(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole("spiffe://cassandra/sidecar/test_user", "test_role");

        // VIEW action allowed across targets for same resource. VIEW:* for cluster resource allows VIEW:SCHEMA,
        // VIEW:CDC, VIEW:CLUSTER etc
        grantSidecarPermission("test_role", "cluster", "VIEW:*");

        String timeSkewRoute = "/api/v1/time-skew";
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        Checkpoint checkpoint = context.checkpoint(4);

        // Uses sidecar permission VIEW:* added
        verifyAccess(context, checkpoint, HttpMethod.GET, timeSkewRoute, clientKeystorePath, false);

        String schemaRoute = "/api/v1/cassandra/schema";
        verifyAccess(context, checkpoint, HttpMethod.GET, schemaRoute, clientKeystorePath, false);

        String ringRoute = "/api/v1/cassandra/ring";
        // Allows VIEW:CLUSTER too
        verifyAccess(context, checkpoint, HttpMethod.GET, ringRoute, clientKeystorePath, false);

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "sample_keyspace");
        // Does not allow finding schema for keyspace, keyspace schema route requires access specific to
        // data/sample_keyspace resource
        verifyAccess(context, checkpoint, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath, true);
    }

    @CassandraIntegrationTest(buildCluster = false)
    void testResourceWideActions(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext) throws Exception
    {
        // starts cluster for 5.0 and above version test
        startClusterWithMtlsAndAuthorizer(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole("spiffe://cassandra/sidecar/test_user", "test_role");

        grantSidecarPermission("test_role", "data/sample_keyspace", "*:*");

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "sample_keyspace");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        Checkpoint checkpoint = context.checkpoint(3);

        // *:* permission across resource data/test_keyspace allows all possible actions across all possible targets,
        // Such as VIEW:SCHEMA, VIEW:CLUSTER etc.
        verifyAccess(context, checkpoint, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath, false);

        String keyspaceRingRoute = String.format("/api/v1/cassandra/ring/keyspaces/%s", "sample_keyspace");
        // VIEW:CLUSTER granted
        verifyAccess(context, checkpoint, HttpMethod.GET, keyspaceRingRoute, clientKeystorePath, false);

        String viewSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/%s",
                                                 "sample_keyspace", "sample_table", "sample_snapshot");
        // does not allow VIEW:SNAPSHOT which requires table resource too
        verifyAccess(context, checkpoint, HttpMethod.GET, viewSnapshotRoute, clientKeystorePath, true);
    }

    @CassandraIntegrationTest(buildCluster = false)
    void testAllWildcardActionsForTarget(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext)
    throws Exception
    {
        // starts cluster for 5.0 and above version test
        startClusterWithMtlsAndAuthorizer(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole("spiffe://cassandra/sidecar/test_user", "test_role");

        grantSidecarPermission("test_role", "data/sample_keyspace/sample_table", "*:SNAPSHOT");

        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "sample_keyspace", "sample_table");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        Checkpoint checkpoint = context.checkpoint(2);

        // *:SNAPSHOT permission across data/sample_resource/sample_table allows all possible actions for SNAPSHOT target
        // such as CREATE:SNAPSHOT, VIEW:SNAPSHOT, DELETE:SNAPSHOT. Does not allow STREAM:SSTABLE or other actions
        verifyAccess(context, checkpoint, HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, false);

        String streamSSTableRoute =  String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/%s/components/%s",
                                                   "sample_keyspace", "sample_table", "my-snapshot", "nc-1-big-Data.db");
        verifyAccess(context, checkpoint, HttpMethod.GET, streamSSTableRoute, clientKeystorePath, true);
    }

    @CassandraIntegrationTest(buildCluster = false)
    void testEndpointRequiringMultipleActions(VertxTestContext context, ConfigurableCassandraTestContext cassandraContext)
    throws Exception
    {
       // starts cluster for 5.0 and above version test
        startClusterWithMtlsAndAuthorizer(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole("spiffe://cassandra/sidecar/test_user", "test_role");

        grantSidecarPermission("test_role", "data/sample_keyspace/sample_table", "CREATE:SNAPSHOT");

        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "sample_keyspace", "sample_table");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        String streamRoute
        =  String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/%s/components/%s",
                         "sample_keyspace", "sample_table", "my-snapshot", "nc-1-big-Data.db");

        // CREATE:SNAPSHOT permission granted for data/sample_keyspace/sample_table
        WebClient client = createClient(clientKeystorePath, truststorePath);
        client.put(server.actualPort(), "127.0.0.1", createSnapshotRoute)
              .send()
              .compose(createResp -> {
                  assertThat(createResp.statusCode()).isEqualTo(HttpResponseStatus.OK.code());

                  // grant sidecar permission for streaming
                  updateSidecarPermission("test_role", "data/sample_keyspace/sample_table", "STREAM:SSTABLE");

                  // wait for cache refresh
                  Uninterruptibles.sleepUninterruptibly(3000, TimeUnit.MILLISECONDS);

                  // STREAM SSTable request requires both Sidecar STREAM:SSTABLE permission and Cassandra's SELECT
                  // permission on a table it accesses data.
                  return streamRequest(client, streamRoute);
              })
              .compose(deniedStreamResp -> {
                  // access denied without SELECT permission
                  assertThat(deniedStreamResp.statusCode()).isEqualTo(HttpResponseStatus.FORBIDDEN.code());

                  // grant SELECT permission with cassandra role
                  grantTablePermission("sample_keyspace", "sample_table", "test_role");

                  // wait for cache refresh
                  Uninterruptibles.sleepUninterruptibly(3000, TimeUnit.MILLISECONDS);

                  return streamRequest(client, streamRoute);
              })
              .onFailure(context::failNow)
              .onComplete(acceptedStreamResp -> {
                  if (acceptedStreamResp.cause() != null)
                  {
                      context.failNow(acceptedStreamResp.cause());
                      return;
                  }

                  // request goes through with both permissions granted
                  assertThat(acceptedStreamResp.result().statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  context.completeNow();
              });
    }

    private void startClusterWithMtlsAndAuthorizer(ConfigurableCassandraTestContext  cassandraContext) throws Exception
    {
        // mTLS authentication was added in Cassandra starting 5.0 version
        assumeThat(cassandraContext.version.major)
        .withFailMessage("mTLS authentication is not supported in 4.0 Cassandra version")
        .isGreaterThanOrEqualTo(MIN_VERSION_WITH_MTLS);

        cassandraContext.configureAndStartCluster(builder -> {
            builder.appendConfig(config -> config.set("authenticator.class_name",
                                                      "org.apache.cassandra.auth.MutualTlsWithPasswordFallbackAuthenticator")
                                                 .set("authenticator.parameters",
                                                      Collections.singletonMap("validator_class_name", "org.apache.cassandra.auth.SpiffeCertificateValidator"))
                                                 .set("role_manager", "CassandraRoleManager")
                                                 .set("authorizer", "CassandraAuthorizer")
                                                 .set("client_encryption_options.enabled", "true")
                                                 .set("client_encryption_options.optional", "true")
                                                 .set("client_encryption_options.require_client_auth", "true")
                                                 .set("client_encryption_options.require_endpoint_verification", "false")
                                                 .set("client_encryption_options.keystore", serverKeystorePath.toAbsolutePath().toString())
                                                 .set("client_encryption_options.keystore_password", serverKeystorePassword)
                                                 .set("client_encryption_options.truststore", truststorePath.toAbsolutePath().toString())
                                                 .set("client_encryption_options.truststore_password", truststorePassword));
        });
        waitForSchemaReady(30, TimeUnit.SECONDS);

        // required for authentication of sidecar requests to Cassandra. Only superusers can grant permissions
        insertIdentityRole(ADMIN_IDENTITY, "cassandra");
        createKeyspaceTable();

        // Add keystore for Sidecar
        sidecarTestContext.setSslConfiguration(sslConfigWithKeystoreTruststore());
        Thread.sleep(2000);
    }

    private void createKeyspaceTable()
    {
        createKeyspace("sample_keyspace");
        createTable("sample_keyspace", "sample_table");
    }

    private void createRole(String role, boolean superUser)
    {
        Session session = maybeGetSession();
        session.execute("CREATE ROLE " + role + " WITH PASSWORD = 'password' AND SUPERUSER = " + superUser + " AND LOGIN = true;");
    }

    private void insertIdentityRole(String identity, String role)
    {
        Session session = maybeGetSession();
        session.execute("INSERT INTO system_auth.identity_to_role (identity, role) VALUES (\'" + identity + "\',\'" + role + "\');");
    }

    private void createKeyspace(String keyspace)
    {
        Session session = maybeGetSession();
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'3'}");
    }

    private void createTable(String keyspace, String table)
    {
        Session session = maybeGetSession();
        session.execute(String.format("CREATE TABLE %s.%s (a int, b text, PRIMARY KEY (a));", keyspace, table));
        session.execute("INSERT INTO " + keyspace + "." + table + " (a, b) VALUES (1, 'text');");
    }

    private void grantKeyspacePermission(String keyspace, String role)
    {
        Session session = maybeGetSession();
        session.execute("GRANT ALL PERMISSIONS ON KEYSPACE " + keyspace + " TO " + role);
    }

    private void grantTablePermission(String keyspace, String table, String role)
    {
        Session session = maybeGetSession();
        session.execute("GRANT ALL PERMISSIONS ON " + keyspace + "." + table + " TO " + role);
    }

    private void grantSidecarPermission(String role, String resource, String permission)
    {
        Session session = maybeGetSession();
        session.execute(String.format("INSERT INTO sidecar_internal.role_permissions_v1 (role, resource, permissions) " +
                                      "VALUES ('%s', '%s', {'%s'})", role, resource, permission));
    }

    private void updateSidecarPermission(String role, String resource, String permission)
    {
        Session session = maybeGetSession();
        session.execute(String.format("UPDATE sidecar_internal.role_permissions_v1 SET permissions = permissions + {'%s'} " +
                                      "where role = '%s' and resource = '%s'", permission, role, resource));
    }

    private SslConfiguration sslConfigWithKeystoreTruststore()
    {
        return SslConfigurationImpl
               .builder()
               .enabled(true)
               .keystore(new KeyStoreConfigurationImpl(clientKeystorePath.toAbsolutePath().toString(), clientKeystorePassword, "PKCS12"))
               .truststore(new KeyStoreConfigurationImpl(truststorePath.toAbsolutePath().toString(), truststorePassword, "PKCS12"))
               .build();
    }

    private void verifyAccess(VertxTestContext context, HttpMethod method, String testRoute, Path clientKeystorePath)
    {
        Checkpoint checkpoint = context.checkpoint();
        verifyAccess(context, checkpoint, method, testRoute, clientKeystorePath, false);
    }

    private void verifyAccess(VertxTestContext context, Checkpoint checkpoint, HttpMethod method,
                              String testRoute, Path clientKeystorePath, boolean expectForbidden)
    {
        WebClient client = createClient(clientKeystorePath, truststorePath);
        client.request(method, server.actualPort(), "127.0.0.1", testRoute)
              .send(context.succeeding(response -> {
                  context.verify(() -> {
                      if (expectForbidden)
                      {
                          assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.FORBIDDEN.code());
                          return;
                      }
                      assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  });
                  checkpoint.flag();
              }));
    }

    private Future<HttpResponse<Buffer>> streamRequest(WebClient client, String route)
    {
        return client.get(server.actualPort(), "127.0.0.1", route).send();
    }
}
