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
import java.util.concurrent.CountDownLatch;
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

    private Path nonAdminClientKeystorePath;

    @CassandraIntegrationTest(authMode = AuthMode.MUTUAL_TLS)
    void testAuthorizationScenarios(VertxTestContext context, CassandraTestContext cassandraContext) throws Exception
    {
        prepareForTest(cassandraContext);

        // wait for cache refreshes
        Thread.sleep(2000);

        // permissions for test cases below are granted during prepareForTest to save cache refresh time. Please
        // refer to grantRequiredPermissions to check permissions granted for a test to understand verifications done in
        // test
        testForAdmin(context);
        testForSuperUser(context);
        testForNonAdmin(context);
        testGrantingForTable(context);
        testGrantingForKeyspace(context);
        testGrantingAllTablesExceptKeyspace(context);
        testGrantingAtDataLevel(context);
        testEndpointWithOrAuthorization(context);
        testWildcardActionForAllTargets(context);
        testAllWildcardActionsForTarget(context);
        testGrantingWithWildcardSubparts(context);
        testResourceWideActions(context);
        testEndpointRequiringMultipleActions(context);
        context.completeNow();
    }

    void testForAdmin(VertxTestContext context)
    {
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "test_keyspace");
        // uses client keystore with admin identity. Admins bypass authorization checks
        CountDownLatch countDownLatch = new CountDownLatch(1);
        verifyAccess(context, countDownLatch, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath, false);
    }

    void testForSuperUser(VertxTestContext context) throws Exception
    {
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "test_keyspace");
        // uses client keystore with superuser identity
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/super_user_test_user");
        CountDownLatch countDownLatch = new CountDownLatch(1);
        verifyAccess(context, countDownLatch, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath, false);
    }

    void testForNonAdmin(VertxTestContext context)
    {
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "test_keyspace");
        CountDownLatch countDownLatch = new CountDownLatch(1);
        verifyAccess(context, countDownLatch, HttpMethod.GET, keyspaceSchemaRoute, nonAdminClientKeystorePath, false);
    }

    void testGrantingForTable(VertxTestContext context)
    {
        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "grant_table_test_keyspace", "test_table");

        CountDownLatch countDownLatch = new CountDownLatch(2);

        // CREATE:SNAPSHOT permission granted for data/grant_table_test_keyspace/test_table
        verifyAccess(context, countDownLatch, HttpMethod.PUT, createSnapshotRoute, nonAdminClientKeystorePath, false);

        // DELETE:SNAPSHOT permission not granted for data/grant_table_test_keyspace/test_table
        verifyAccess(context, countDownLatch, HttpMethod.DELETE, createSnapshotRoute, nonAdminClientKeystorePath, true);
    }

    void testGrantingForKeyspace(VertxTestContext context)
    {
        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "grant_keyspace_test_keyspace", "test_table");

        CountDownLatch countDownLatch = new CountDownLatch(2);

        // CREATE:SNAPSHOT permission granted for data/grant_keyspace_test_keyspace/test_table with
        // data/grant_tables_test_keyspace grant
        verifyAccess(context, countDownLatch, HttpMethod.PUT, createSnapshotRoute, nonAdminClientKeystorePath, false);

        // DELETE:SNAPSHOT permission not granted for data/grant_keyspace_test_keyspace/test_table
        verifyAccess(context, countDownLatch, HttpMethod.DELETE, createSnapshotRoute, nonAdminClientKeystorePath, true);
    }

    void testGrantingAllTablesExceptKeyspace(VertxTestContext context)
    {
        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "grant_tables_except_keyspace_test_keyspace", "test_table");

        CountDownLatch countDownLatch = new CountDownLatch(2);

        // CREATE:SNAPSHOT permission granted for data/grant_tables_except_keyspace_test_keyspace/test_table
        // with data/grant_tables_except_keyspace_test_keyspace grant
        verifyAccess(context, countDownLatch, HttpMethod.PUT, createSnapshotRoute, nonAdminClientKeystorePath, false);

        // READ:SCHEMA is not granted since it expects permissions at keyspace level
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "grant_tables_except_keyspace_test_keyspace");
        verifyAccess(context, countDownLatch, HttpMethod.GET, keyspaceSchemaRoute, nonAdminClientKeystorePath, true);
    }

    void testGrantingAtDataLevel(VertxTestContext context) throws Exception
    {
        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "test_keyspace", "test_table");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/grant_data_test_user");

        CountDownLatch countDownLatch = new CountDownLatch(2);

        // CREATE:SNAPSHOT permission granted for data/test_keyspace/test_table with data resource grant
        verifyAccess(context, countDownLatch, HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, false);

        // DELETE:SNAPSHOT permission not granted for data/test_keyspace/test_table not granted
        verifyAccess(context, countDownLatch, HttpMethod.DELETE, createSnapshotRoute, clientKeystorePath, true);
    }

    void testEndpointWithOrAuthorization(VertxTestContext context)
    {
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "orAuthorization_test_keyspace");

        CountDownLatch countDownLatch = new CountDownLatch(1);

        // schema endpoint for keyspaces accepts CREATE, ALTER, DROP or DESCRIBE cassandra permissions.
        // cassandra permission for test_role on sample_keyspace not granted, sidecar permission READ:* is used to
        // grant access
        verifyAccess(context, countDownLatch, HttpMethod.GET, keyspaceSchemaRoute, nonAdminClientKeystorePath, false);
    }

    void testWildcardActionForAllTargets(VertxTestContext context) throws Exception
    {
        String timeSkewRoute = "/api/v1/time-skew";
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/wildcard_across_targets_test_user");

        CountDownLatch countDownLatch = new CountDownLatch(4);

        // Uses sidecar permission READ:* added
        verifyAccess(context, countDownLatch, HttpMethod.GET, timeSkewRoute, clientKeystorePath, false);

        String schemaRoute = "/api/v1/cassandra/schema";
        // Allows READ:SCHEMA
        verifyAccess(context, countDownLatch, HttpMethod.GET, schemaRoute, clientKeystorePath, false);

        String ringRoute = "/api/v1/cassandra/ring";
        // Allows READ:RING too
        verifyAccess(context, countDownLatch, HttpMethod.GET, ringRoute, clientKeystorePath, false);

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "test_keyspace");
        // Does not allow finding schema for keyspace, keyspace schema route requires access specific to
        // data/test_keyspace resource
        verifyAccess(context, countDownLatch, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath, true);
    }

    void testAllWildcardActionsForTarget(VertxTestContext context)
    {
        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "all_wildcard_actions_for_target_test_keyspace", "test_table");

        CountDownLatch countDownLatch = new CountDownLatch(2);

        // *:SNAPSHOT permission across ata/all_wildcard_actions_for_target_test_keyspace/test_table allows all
        // possible actions for SNAPSHOT target such as CREATE:SNAPSHOT, READ:SNAPSHOT, DELETE:SNAPSHOT.
        // Does not allow STREAM:SSTABLE or other actions
        verifyAccess(context, countDownLatch, HttpMethod.PUT, createSnapshotRoute, nonAdminClientKeystorePath, false);

        String streamSSTableRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/%s/components/%s",
                                                  "all_wildcard_actions_for_target_test_keyspace", "test_table",
                                                  "my-snapshot", "nc-1-big-Data.db");
        verifyAccess(context, countDownLatch, HttpMethod.GET, streamSSTableRoute, nonAdminClientKeystorePath, true);
    }

    void testGrantingWithWildcardSubparts(VertxTestContext context) throws Exception
    {
        String schemaRoute = "/api/v1/cassandra/schema";
        String gossipRoute = "/api/v1/cassandra/gossip";
        String ringRoute = "/api/v1/cassandra/ring";
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/wildcard_with_subparts_test_user");

        CountDownLatch countDownLatch = new CountDownLatch(3);

        // READ:SCHEMA permission granted for cluster with READ:SCHEMA,GOSSIP
        verifyAccess(context, countDownLatch, HttpMethod.GET, schemaRoute, clientKeystorePath, false);

        // READ:GOSSIP permission granted for cluster with READ:SCHEMA,GOSSIP
        verifyAccess(context, countDownLatch, HttpMethod.GET, gossipRoute, clientKeystorePath, false);

        // READ:RING permission not granted with READ:SCHEMA,GOSSIP
        verifyAccess(context, countDownLatch, HttpMethod.GET, ringRoute, clientKeystorePath, true);
    }

    void testResourceWideActions(VertxTestContext context)
    {
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "resource_wide_actions_test_keyspace");

        CountDownLatch countDownLatch = new CountDownLatch(4);

        // *:* permission across resource data/resource_wide_actions_test_keyspace allows all possible actions
        // across all possible targets, Such as READ:SCHEMA for given keyspace, READ:RING for given keyspace etc.
        verifyAccess(context, countDownLatch, HttpMethod.GET, keyspaceSchemaRoute, nonAdminClientKeystorePath, false);

        String keyspaceRingRoute = String.format("/api/v1/cassandra/ring/keyspaces/%s", "resource_wide_actions_test_keyspace");
        // READ:RING for keyspace granted
        verifyAccess(context, countDownLatch, HttpMethod.GET, keyspaceRingRoute, nonAdminClientKeystorePath, false);


        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "resource_wide_actions_test_keyspace", "test_table");
        // allows CREATE:SNAPSHOT which requires table resource too, since data/resource_wide_actions_test_keyspace
        // grant is a wider grant
        verifyAccess(context, countDownLatch, HttpMethod.PUT, createSnapshotRoute, nonAdminClientKeystorePath, false);

        // does not allow access on a different keyspace
        String disallowedKeyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "test_keyspace");
        verifyAccess(context, countDownLatch, HttpMethod.GET, disallowedKeyspaceSchemaRoute, nonAdminClientKeystorePath, true);
    }

    void testEndpointRequiringMultipleActions(VertxTestContext context)
    {
        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "multiple_permissions_required_test_keyspace", "test_table");

        String streamRoute
        = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/%s/components/%s",
                        "multiple_permissions_required_test_keyspace", "test_table", "my-snapshot", "nc-1-big-Data.db");

        CountDownLatch countDownLatch = new CountDownLatch(1);

        // CREATE:SNAPSHOT permission granted for data/multiple_permissions_required_test_keyspace/test_table
        WebClient client = createClient(nonAdminClientKeystorePath, truststorePath);
        client.put(server.actualPort(), "127.0.0.1", createSnapshotRoute)
              .send()
              .compose(createResp -> {
                  assertThat(createResp.statusCode()).isEqualTo(HttpResponseStatus.OK.code());

                  // grant sidecar permission for streaming
                  updateSidecarPermission("non_admin_test_role",
                                          "data/multiple_permissions_required_test_keyspace/test_table",
                                          "STREAM:SSTABLE");

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
                  grantTablePermission("multiple_permissions_required_test_keyspace", "test_table", "non_admin_test_role");

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
                  countDownLatch.countDown();
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
        createRequiredKeyspaceTables();
        createRequiredRoles(cassandraContext);
        grantRequiredPermissions();
        createRequiredKeystores();
    }

    private void insertIdentityRole(CassandraTestContext cassandraContext, String identity, String role)
    {
        cassandraContext.cluster()
                        .schemaChangeIgnoringStoppedInstances("INSERT INTO system_auth.identity_to_role (identity, role) VALUES (\'" + identity + "\',\'" + role + "\');");
    }

    private void createRequiredKeyspaceTables()
    {
        createKeyspace("test_keyspace");
        createKeyspace("non_admin_test_keyspace");
        createKeyspace("grant_table_test_keyspace");
        createKeyspace("grant_keyspace_test_keyspace");
        createKeyspace("grant_tables_except_keyspace_test_keyspace");
        createKeyspace("orAuthorization_test_keyspace");
        createKeyspace("resource_wide_actions_test_keyspace");
        createKeyspace("all_wildcard_actions_for_target_test_keyspace");
        createKeyspace("multiple_permissions_required_test_keyspace");
        createTable("test_keyspace", "test_table");
        createTable("non_admin_test_keyspace", "test_table");
        createTable("grant_table_test_keyspace", "test_table");
        createTable("grant_keyspace_test_keyspace", "test_table");
        createTable("grant_tables_except_keyspace_test_keyspace", "test_table");
        createTable("orAuthorization_test_keyspace", "test_table");
        createTable("resource_wide_actions_test_keyspace", "test_table");
        createTable("all_wildcard_actions_for_target_test_keyspace", "test_table");
        createTable("multiple_permissions_required_test_keyspace", "test_table");
    }

    private void createRequiredRoles(CassandraTestContext cassandraContext)
    {
        createRole("super_user_test_role", true);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/super_user_test_user", "super_user_test_role");

        createRole("non_admin_test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/non_admin_test_user", "non_admin_test_role");

        createRole("grant_data_test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/grant_data_test_user", "grant_data_test_role");

        createRole("wildcard_across_targets_test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/wildcard_across_targets_test_user", "wildcard_across_targets_test_role");

        createRole("wildcard_with_subparts_test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/wildcard_with_subparts_test_user", "wildcard_with_subparts_test_role");
    }

    private void grantRequiredPermissions()
    {
        // permission for testForNonAdmin
        grantKeyspacePermission("non_admin_test_keyspace", "non_admin_test_role");

        // permission for testGrantingForTable
        grantSidecarPermission("non_admin_test_role", "data/grant_table_test_keyspace/test_table", "CREATE:SNAPSHOT");

        // permission for testGrantingForKeyspace
        grantSidecarPermission("non_admin_test_role", "data/grant_keyspace_test_keyspace", "CREATE:SNAPSHOT");

        // permission for testGrantingAllTablesExceptKeyspace
        grantSidecarPermission("non_admin_test_role", "data/grant_tables_except_keyspace_test_keyspace/*", "CREATE:SNAPSHOT");

        // permission for testGrantingAtDataLevel
        grantSidecarPermission("grant_data_test_role", "data", "CREATE:SNAPSHOT");

        // permission for testEndpointWithOrAuthorization
        grantSidecarPermission("non_admin_test_role", "data/orAuthorization_test_keyspace", "READ:*");

        // permission for testWildcardActionForAllTargets
        // READ action allowed across targets for same resource. READ:* for cluster resource allows READ:SCHEMA,
        // READ:CDC, READ:GOSSIP, READ:RING etc
        grantSidecarPermission("wildcard_across_targets_test_role", "cluster", "READ:*");

        // permission for testAllWildcardActionsForTarget
        grantSidecarPermission("non_admin_test_role",
                               "data/all_wildcard_actions_for_target_test_keyspace/test_table",
                               "*:SNAPSHOT");

        // permission for testGrantingWithWildcardSubparts
        grantSidecarPermission("wildcard_with_subparts_test_role", "cluster", "READ:SCHEMA,GOSSIP");

        // permission for testResourceWideActions
        grantSidecarPermission("non_admin_test_role", "data/resource_wide_actions_test_keyspace", "*:*");

        // permission for testEndpointRequiringMultipleActions
        grantSidecarPermission("non_admin_test_role",
                               "data/multiple_permissions_required_test_keyspace/test_table",
                               "CREATE:SNAPSHOT");
    }

    private void createRequiredKeystores() throws Exception
    {
        nonAdminClientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/non_admin_test_user");
    }

    private void createRole(String role, boolean superUser)
    {
        Session session = maybeGetSession();
        session.execute("CREATE ROLE " + role + " WITH PASSWORD = 'password' AND SUPERUSER = " + superUser + " AND LOGIN = true;");
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

    private void verifyAccess(VertxTestContext context, CountDownLatch countDownLatch, HttpMethod method,
                              String testRoute, Path clientKeystorePath, boolean expectForbidden)
    {
        WebClient client = createClient(clientKeystorePath, truststorePath);
        client.request(method, server.actualPort(), "127.0.0.1", testRoute)
              .send(response -> {
                  if (response.cause() != null)
                  {
                      context.failNow(response.cause());
                      countDownLatch.countDown();
                      return;
                  }

                  if (expectForbidden)
                  {
                      assertThat(response.result().statusCode()).isEqualTo(HttpResponseStatus.FORBIDDEN.code());
                  }
                  else
                  {
                      assertThat(response.result().statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  }
                  countDownLatch.countDown();
              });
    }

    private Future<HttpResponse<Buffer>> streamRequest(WebClient client, String route)
    {
        return client.get(server.actualPort(), "127.0.0.1", route).send();
    }
}
