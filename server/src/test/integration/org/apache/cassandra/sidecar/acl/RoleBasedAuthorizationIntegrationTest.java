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
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.AuthMode;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.CassandraTestContext;

import static org.apache.cassandra.sidecar.testing.IntegrationTestModule.ADMIN_IDENTITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Test for role based access control in Sidecar
 */
@ExtendWith(VertxExtension.class)
class RoleBasedAuthorizationIntegrationTest extends IntegrationTestBase
{
    private static final int MIN_VERSION_WITH_MTLS = 5;

    @CassandraIntegrationTest(authMode = AuthMode.MUTUAL_TLS)
    void testForAdmin(VertxTestContext context, CassandraTestContext cassandraContext) throws Exception
    {
        prepareForTest(cassandraContext);

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "sample_keyspace");
        // uses client keystore with admin identity. Admins bypass authorization checks
        verifyAccess(context, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath);
    }

    @CassandraIntegrationTest(authMode = AuthMode.MUTUAL_TLS)
    void testForSuperUser(VertxTestContext context, CassandraTestContext cassandraContext) throws Exception
    {
        prepareForTest(cassandraContext);

        createRole("test_role", true);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/test_user", "test_role");

        // wait for cache refreshes to pick up superuser status
        Thread.sleep(2000);

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "sample_keyspace");
        // uses client keystore with superuser identity
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");
        verifyAccess(context, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath);
    }

    @CassandraIntegrationTest(authMode = AuthMode.MUTUAL_TLS)
    void testForNonAdmin(VertxTestContext context, CassandraTestContext cassandraContext) throws Exception
    {
        prepareForTest(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/test_user", "test_role");

        // grant permission for non super user
        grantKeyspacePermission("sample_keyspace", "test_role");

        // wait for cache refreshes to pick up granted permissions
        Thread.sleep(2000);

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "sample_keyspace");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");
        verifyAccess(context, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath);
    }

    @CassandraIntegrationTest(authMode = AuthMode.MUTUAL_TLS)
    void testGrantingForTable(VertxTestContext context, CassandraTestContext cassandraContext)
    throws Exception
    {
        prepareForTest(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/test_user", "test_role");

        grantSidecarPermission("test_role", "data/sample_keyspace/sample_table", "CREATE:SNAPSHOT");

        // wait for cache refreshes to pick up granted permission
        Thread.sleep(2000);

        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "sample_keyspace", "sample_table");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        Checkpoint checkpoint = context.checkpoint(2);

        // CREATE:SNAPSHOT permission granted for data/sample_keyspace/sample_table
        verifyAccess(context, checkpoint, HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, false);

        // DELETE:SNAPSHOT permission not granted for data/sample_keyspace/sample_table
        verifyAccess(context, checkpoint, HttpMethod.DELETE, createSnapshotRoute, clientKeystorePath, true);
    }

    @CassandraIntegrationTest(authMode = AuthMode.MUTUAL_TLS)
    void testGrantingSidecarPermissionForAllTables(VertxTestContext context, CassandraTestContext cassandraContext)
    throws Exception
    {
        prepareForTest(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/test_user", "test_role");

        grantSidecarPermission("test_role", "data/sample_keyspace", "CREATE:SNAPSHOT");

        // wait for cache refreshes to pick up granted permission
        Thread.sleep(2000);

        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "sample_keyspace", "sample_table");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        Checkpoint checkpoint = context.checkpoint(2);

        // CREATE:SNAPSHOT permission granted for data/sample_keyspace/sample_table with data/sample_keyspace grant
        verifyAccess(context, checkpoint, HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, false);

        // DELETE:SNAPSHOT permission not granted for data/sample_keyspace/sample_table
        verifyAccess(context, checkpoint, HttpMethod.DELETE, createSnapshotRoute, clientKeystorePath, true);
    }

    @CassandraIntegrationTest(authMode = AuthMode.MUTUAL_TLS)
    void testGrantingSidecarPermissionForAllTablesExceptKeyspace(VertxTestContext context, CassandraTestContext cassandraContext)
    throws Exception
    {
        prepareForTest(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/test_user", "test_role");

        grantSidecarPermission("test_role", "data/sample_keyspace/*", "CREATE:SNAPSHOT");

        // wait for cache refreshes to pick up granted permission
        Thread.sleep(2000);

        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "sample_keyspace", "sample_table");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        Checkpoint checkpoint = context.checkpoint(2);

        // CREATE:SNAPSHOT permission granted for data/sample_keyspace/sample_table with data/sample_keyspace grant
        verifyAccess(context, checkpoint, HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, false);

        // READ:SCHEMA is not granted since it expects permissions at keyspace level
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "sample_keyspace");
        verifyAccess(context, checkpoint, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath, true);
    }

    @CassandraIntegrationTest(authMode = AuthMode.MUTUAL_TLS)
    void testGrantingPermissionsForAtDataLevel(VertxTestContext context, CassandraTestContext cassandraContext)
    throws Exception
    {
        prepareForTest(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/test_user", "test_role");

        grantSidecarPermission("test_role", "data", "CREATE:SNAPSHOT");

        // wait for cache refreshes to pick up granted permission
        Thread.sleep(2000);

        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "sample_keyspace", "sample_table");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        Checkpoint checkpoint = context.checkpoint(2);

        // CREATE:SNAPSHOT permission granted for data/sample_keyspace/sample_table with data resource grant
        verifyAccess(context, checkpoint, HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, false);

        // DELETE:SNAPSHOT permission not granted for data/sample_keyspace/sample_table not granted
        verifyAccess(context, checkpoint, HttpMethod.DELETE, createSnapshotRoute, clientKeystorePath, true);
    }

    @CassandraIntegrationTest(authMode = AuthMode.MUTUAL_TLS)
    void testEndpointWithOrAuthorization(VertxTestContext context, CassandraTestContext cassandraContext)
    throws Exception
    {
        prepareForTest(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/test_user", "test_role");

        grantSidecarPermission("test_role", "data/sample_keyspace", "READ:*");

        // wait for cache refreshes to pick up granted permission
        Thread.sleep(2000);

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "sample_keyspace");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        // schema endpoint for keyspaces accepts CREATE, ALTER, DROP or DESCRIBE cassandra permissions.
        // cassandra permission for test_role on sample_keyspace not granted, sidecar permission READ:* is used to
        // grant access
        verifyAccess(context, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath);
    }

    @CassandraIntegrationTest(authMode = AuthMode.MUTUAL_TLS)
    void testWildcardActionForAllTargets(VertxTestContext context, CassandraTestContext cassandraContext)
    throws Exception
    {
        prepareForTest(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/test_user", "test_role");

        // READ action allowed across targets for same resource. READ:* for cluster resource allows READ:SCHEMA,
        // READ:CDC, READ:GOSSIP, READ:RING etc
        grantSidecarPermission("test_role", "cluster", "READ:*");

        // wait for cache refreshes to pick up granted permission
        Thread.sleep(2000);

        String timeSkewRoute = "/api/v1/time-skew";
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        Checkpoint checkpoint = context.checkpoint(4);

        // Uses sidecar permission READ:* added
        verifyAccess(context, checkpoint, HttpMethod.GET, timeSkewRoute, clientKeystorePath, false);

        String schemaRoute = "/api/v1/cassandra/schema";
        verifyAccess(context, checkpoint, HttpMethod.GET, schemaRoute, clientKeystorePath, false);

        String ringRoute = "/api/v1/cassandra/ring";
        // Allows READ:RING too
        verifyAccess(context, checkpoint, HttpMethod.GET, ringRoute, clientKeystorePath, false);

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "sample_keyspace");
        // Does not allow finding schema for keyspace, keyspace schema route requires access specific to
        // data/sample_keyspace resource
        verifyAccess(context, checkpoint, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath, true);
    }

    @CassandraIntegrationTest(authMode = AuthMode.MUTUAL_TLS)
    void testGrantingPermissionsWithWildcardSubparts(VertxTestContext context, CassandraTestContext cassandraContext)
    throws Exception
    {
        prepareForTest(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/test_user", "test_role");

        grantSidecarPermission("test_role", "cluster", "READ:SCHEMA,GOSSIP");

        // wait for cache refreshes to pick up granted permission
        Thread.sleep(2000);

        String schemaRoute = "/api/v1/cassandra/schema";
        String gossipRoute = "/api/v1/cassandra/gossip";
        String ringRoute = "/api/v1/cassandra/ring";
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        Checkpoint checkpoint = context.checkpoint(3);

        // READ:SCHEMA permission granted for cluster with READ:SCHEMA,GOSSIP
        verifyAccess(context, checkpoint, HttpMethod.GET, schemaRoute, clientKeystorePath, false);

        // READ:GOSSIP permission granted for cluster with READ:SCHEMA,GOSSIP
        verifyAccess(context, checkpoint, HttpMethod.GET, gossipRoute, clientKeystorePath, false);

        // READ:RING permission not granted with READ:SCHEMA,GOSSIP
        verifyAccess(context, checkpoint, HttpMethod.GET, ringRoute, clientKeystorePath, true);
    }

    @CassandraIntegrationTest(authMode = AuthMode.MUTUAL_TLS)
    void testResourceWideActions(VertxTestContext context, CassandraTestContext cassandraContext) throws Exception
    {
        prepareForTest(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/test_user", "test_role");

        grantSidecarPermission("test_role", "data/sample_keyspace", "*:*");

        // wait for cache refreshes to pick up granted permission
        Thread.sleep(2000);

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "sample_keyspace");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        Checkpoint checkpoint = context.checkpoint(3);

        // *:* permission across resource data/test_keyspace allows all possible actions across all possible targets,
        // Such as READ:SCHEMA for given keyspace, READ:RING for given keyspace etc.
        verifyAccess(context, checkpoint, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath, false);

        String keyspaceRingRoute = String.format("/api/v1/cassandra/ring/keyspaces/%s", "sample_keyspace");
        // READ:RING for keyspace granted
        verifyAccess(context, checkpoint, HttpMethod.GET, keyspaceRingRoute, clientKeystorePath, false);


        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "sample_keyspace", "sample_table");
        // allows CREATE:SNAPSHOT which requires table resource too, since data/sample_keyspace grant is a wider grant
        verifyAccess(context, checkpoint, HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, false);
    }

    @CassandraIntegrationTest(authMode = AuthMode.MUTUAL_TLS)
    void testAllWildcardActionsForTarget(VertxTestContext context, CassandraTestContext cassandraContext)
    throws Exception
    {
        prepareForTest(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/test_user", "test_role");

        grantSidecarPermission("test_role", "data/sample_keyspace/sample_table", "*:SNAPSHOT");

        // wait for cache refreshes to pick up granted permission
        Thread.sleep(2000);

        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "sample_keyspace", "sample_table");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        Checkpoint checkpoint = context.checkpoint(2);

        // *:SNAPSHOT permission across data/sample_resource/sample_table allows all possible actions for SNAPSHOT target
        // such as CREATE:SNAPSHOT, READ:SNAPSHOT, DELETE:SNAPSHOT. Does not allow STREAM:SSTABLE or other actions
        verifyAccess(context, checkpoint, HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, false);

        String streamSSTableRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/%s/components/%s",
                                                  "sample_keyspace", "sample_table", "my-snapshot", "nc-1-big-Data.db");
        verifyAccess(context, checkpoint, HttpMethod.GET, streamSSTableRoute, clientKeystorePath, true);
    }

    @CassandraIntegrationTest(authMode = AuthMode.MUTUAL_TLS)
    void testEndpointRequiringMultipleActions(VertxTestContext context, CassandraTestContext cassandraContext)
    throws Exception
    {
        prepareForTest(cassandraContext);

        createRole("test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/test_user", "test_role");

        grantSidecarPermission("test_role", "data/sample_keyspace/sample_table", "CREATE:SNAPSHOT");

        // wait for cache refreshes to pick up granted permission
        Thread.sleep(2000);

        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "sample_keyspace", "sample_table");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/test_user");

        String streamRoute
        = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/%s/components/%s",
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

    private void prepareForTest(CassandraTestContext cassandraContext) throws Exception
    {
        // mTLS authentication was added in Cassandra starting 5.0 version
        assumeThat(cassandraContext.version.major)
        .withFailMessage("mTLS authentication is not supported in 4.0 Cassandra version")
        .isGreaterThanOrEqualTo(MIN_VERSION_WITH_MTLS);

        // required for authentication of sidecar requests to Cassandra. Only superusers can grant permissions
        insertIdentityRole(cassandraContext, ADMIN_IDENTITY, "cassandra");

        waitForSchemaReady(30, TimeUnit.SECONDS);
        createKeyspaceTable();
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

    private void insertIdentityRole(CassandraTestContext cassandraContext, String identity, String role)
    {
        cassandraContext.cluster()
                        .schemaChangeIgnoringStoppedInstances("INSERT INTO system_auth.identity_to_role (identity, role) VALUES (\'" + identity + "\',\'" + role + "\');");
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
