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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.Session;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.response.ListSnapshotFilesResponse;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.AuthMode;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.CassandraTestContext;

import static org.apache.cassandra.sidecar.testing.IntegrationTestModule.ADMIN_IDENTITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Test for role based access control in Sidecar
 * Note:
 * - Do not add new test cases in this class. Add them into test method, example refer to testForAdmin.
 * - Create a new keyspace or test role for each test method as required to prevent permissions overlapping
 */
@ExtendWith(VertxExtension.class)
class RoleBasedAuthorizationIntegrationTest extends IntegrationTestBase
{
    private static final int MIN_VERSION_WITH_MTLS = 5;

    private Path nonAdminClientKeystorePath;
    private CountDownLatch testCompleteLatch;

    @CassandraIntegrationTest(authMode = AuthMode.MUTUAL_TLS)
    void testAuthorizationScenarios(VertxTestContext context, CassandraTestContext cassandraContext) throws Exception
    {
        prepareForTest(cassandraContext);

        // wait for cache refreshes
        Thread.sleep(3000);

        testCompleteLatch = new CountDownLatch(32);

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
        testGrantingWithWildcardSubparts(context);
        testEndpointRequiringMultipleActions(context);
        testGrantingBulkReadFeaturePermission(context);
        testGrantingBulkReadFeaturePermissionAcrossTables(context);
        testGrantingBulkReadFeaturePermissionAcrossData(context);
        testGrantingBulkWriteFeaturePermission(context);
        testGrantingBothBulkReadAndWriteFeaturePermission(context);
        testGrantingAllAnalyticsRelatedPermissions(context);
        testGrantingCdcFeaturePermission(context);

        assertThat(testCompleteLatch.await(4, TimeUnit.MINUTES)).isTrue();
        context.completeNow();
    }

    void testForAdmin(VertxTestContext context)
    {
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "test_keyspace");
        // uses client keystore with admin identity. Admins bypass authorization checks

        verifyAccess(context, testCompleteLatch, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath, false);
    }

    void testForSuperUser(VertxTestContext context) throws Exception
    {
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "test_keyspace");
        // uses client keystore with superuser identity
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/super_user_test_user");

        verifyAccess(context, testCompleteLatch, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath, false);
    }

    void testForNonAdmin(VertxTestContext context)
    {
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "non_admin_test_keyspace");

        verifyAccess(context, testCompleteLatch, HttpMethod.GET, keyspaceSchemaRoute, nonAdminClientKeystorePath, false);
    }

    void testGrantingForTable(VertxTestContext context)
    {
        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "grant_table_test_keyspace", "test_table");

        // SNAPSHOT:CREATE permission granted for data/grant_table_test_keyspace/test_table
        verifyAccess(context, testCompleteLatch, HttpMethod.PUT, createSnapshotRoute, nonAdminClientKeystorePath, false);

        // SNAPSHOT:DELETE permission not granted for data/grant_table_test_keyspace/test_table
        verifyAccess(context, testCompleteLatch, HttpMethod.DELETE, createSnapshotRoute, nonAdminClientKeystorePath, true);
    }

    void testGrantingForKeyspace(VertxTestContext context)
    {
        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "grant_keyspace_test_keyspace", "test_table");

        // SNAPSHOT:CREATE permission granted for data/grant_keyspace_test_keyspace/test_table with
        // data/grant_tables_test_keyspace grant
        verifyAccess(context, testCompleteLatch, HttpMethod.PUT, createSnapshotRoute, nonAdminClientKeystorePath, false);

        // access not granted for different keyspace
        String notAllowedSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                       "not_allowed_keyspace", "test_table");
        verifyAccess(context, testCompleteLatch, HttpMethod.PUT, notAllowedSnapshotRoute, nonAdminClientKeystorePath, true);

        // SNAPSHOT:DELETE permission not granted for data/grant_keyspace_test_keyspace/test_table
        verifyAccess(context, testCompleteLatch, HttpMethod.DELETE, createSnapshotRoute, nonAdminClientKeystorePath, true);
    }

    void testGrantingAllTablesExceptKeyspace(VertxTestContext context)
    {
        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "grant_tables_except_keyspace_test_keyspace", "test_table");

        // SNAPSHOT:CREATE permission granted for data/grant_tables_except_keyspace_test_keyspace/test_table
        // with data/grant_tables_except_keyspace_test_keyspace grant
        verifyAccess(context, testCompleteLatch, HttpMethod.PUT, createSnapshotRoute, nonAdminClientKeystorePath, false);

        // SCHEMA:READ is not granted since it expects permissions at keyspace level
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "grant_tables_except_keyspace_test_keyspace");
        verifyAccess(context, testCompleteLatch, HttpMethod.GET, keyspaceSchemaRoute, nonAdminClientKeystorePath, true);
    }

    void testGrantingAtDataLevel(VertxTestContext context) throws Exception
    {
        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "test_keyspace", "test_table");
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/grant_data_test_user");

        // SNAPSHOT:CREATE permission granted for data/test_keyspace/test_table with data resource grant
        verifyAccess(context, testCompleteLatch, HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, false);

        // SNAPSHOT:DELETE permission not granted for data/test_keyspace/test_table not granted
        verifyAccess(context, testCompleteLatch, HttpMethod.DELETE, createSnapshotRoute, clientKeystorePath, true);
    }

    void testGrantingWithWildcardSubparts(VertxTestContext context) throws Exception
    {
        String schemaRoute = "/api/v1/cassandra/schema";
        String gossipRoute = "/api/v1/cassandra/gossip";
        String ringRoute = "/api/v1/cassandra/ring";
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/wildcard_with_subparts_test_user");

        // SCHEMA:READ permission granted for cluster with GOSSIP,SCHEMA:READ
        verifyAccess(context, testCompleteLatch, HttpMethod.GET, schemaRoute, clientKeystorePath, false);

        // GOSSIP:READ permission granted for cluster with GOSSIP,SCHEMA:READ
        verifyAccess(context, testCompleteLatch, HttpMethod.GET, gossipRoute, clientKeystorePath, false);

        // RING:READ permission not granted with GOSSIP,SCHEMA:READ
        verifyAccess(context, testCompleteLatch, HttpMethod.GET, ringRoute, clientKeystorePath, true);
    }

    void testEndpointRequiringMultipleActions(VertxTestContext context)
    {
        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "multiple_permissions_required_test_keyspace", "test_table");

        String listSnapshotRoute
        = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/%s",
                        "multiple_permissions_required_test_keyspace", "test_table", "my-snapshot");

        // SNAPSHOT:CREATE permission granted for data/multiple_permissions_required_test_keyspace/test_table
        WebClient client = createClient(nonAdminClientKeystorePath, truststorePath);

        createReq(client, HttpMethod.PUT, createSnapshotRoute)
        .compose(createResp -> {
            // grant sidecar permission for streaming
            updateSidecarPermission("non_admin_test_role",
                                    "data/multiple_permissions_required_test_keyspace/test_table",
                                    "SNAPSHOT:READ");

            // wait for cache refresh
            return waitForCacheRefresh(2000)
                   .compose(v -> createReq(client, HttpMethod.GET, listSnapshotRoute));
        })
        .compose(listSnapshotResp -> {
            ListSnapshotFilesResponse snapshotFiles = listSnapshotResp.bodyAsJson(ListSnapshotFilesResponse.class);
            List<ListSnapshotFilesResponse.FileInfo> filesToStream =
            snapshotFiles.snapshotFilesInfo()
                         .stream()
                         .filter(info -> info.fileName.endsWith("-Data.db"))
                         .sorted(Comparator.comparing(o -> o.fileName))
                         .collect(Collectors.toList());

            // grant sidecar permission for streaming
            updateSidecarPermission("non_admin_test_role",
                                    "data/multiple_permissions_required_test_keyspace/test_table",
                                    "SNAPSHOT:STREAM");

            return waitForCacheRefresh(2000)
                   // STREAM SSTable request requires both Sidecar SNAPSHOT:STREAM permission and Cassandra's SELECT
                   // permission on a table it accesses data.
                   .compose(v -> createReq(client, HttpMethod.GET, filesToStream.get(0).componentDownloadUrl()))
                   .compose(deniedStreamResp -> {
                       // request denied without SELECT permission
                       assertThat(deniedStreamResp.statusCode()).isEqualTo(HttpResponseStatus.FORBIDDEN.code());

                       // grant SELECT permission with cassandra role
                       grantTablePermission("multiple_permissions_required_test_keyspace", "test_table", "non_admin_test_role");

                       return waitForCacheRefresh(2000);
                   }).compose(v -> createReq(client, HttpMethod.GET, filesToStream.get(0).componentDownloadUrl()));
        })
        .onSuccess(acceptedStreamResp ->  {
            // stream request goes through with both SNAPSHOT:STREAM and SELECT permissions
            assertThat(acceptedStreamResp.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
            testCompleteLatch.countDown();
        })
        .onFailure(context::failNow);
    }

    void testGrantingBulkReadFeaturePermission(VertxTestContext context) throws Exception
    {
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/bulk_read_test_user");

        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "grant_bulk_read_test_keyspace", "test_table");

        // SNAPSHOT:CREATE permission granted for data/grant_bulk_read_test_keyspace/test_table with ANALYTICS:READ_DIRECT
        verifyAccess(context, testCompleteLatch, HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, false);

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "grant_bulk_read_test_keyspace");

        // SCHEMA:READ permission granted for data/grant_bulk_read_test_keyspace/test_table with ANALYTICS:READ_DIRECT
        verifyAccess(context, testCompleteLatch, HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath, false);

        String topologyRoute = String.format("/api/v1/keyspaces/%s/token-range-replicas", "grant_bulk_read_test_keyspace");
        // TOPOLOGY:READ permission not granted with ANALYTICS:READ_DIRECT
        verifyAccess(context, testCompleteLatch, HttpMethod.GET, topologyRoute, clientKeystorePath, true);
    }

    void testGrantingBulkReadFeaturePermissionAcrossTables(VertxTestContext context) throws Exception
    {
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/bulk_read_test_user");

        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "grant_bulk_read_across_tables_test_keyspace", "test_table");

        // SNAPSHOT:CREATE permission granted for data/grant_bulk_read_across_tables_test_keyspace/test_table with ANALYTICS:READ_DIRECT
        verifyAccess(context, testCompleteLatch, HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, false);

        String createSnapshotRouteTable2 = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                         "grant_bulk_read_across_tables_test_keyspace", "test_table2");

        // SNAPSHOT:CREATE for different table also granted with keyspace scoped ANALYTICS:READ_DIRECT permission
        verifyAccess(context, testCompleteLatch, HttpMethod.PUT, createSnapshotRouteTable2, clientKeystorePath, false);

        String createSnapshotRouteKeyspace2 = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                            "test_keyspace", "test_table2");

        // SNAPSHOT:CREATE for different keyspace not granted with keyspace scoped ANALYTICS:READ_DIRECT permission
        verifyAccess(context, testCompleteLatch, HttpMethod.PUT, createSnapshotRouteKeyspace2, clientKeystorePath, true);
    }

    void testGrantingBulkReadFeaturePermissionAcrossData(VertxTestContext context) throws Exception
    {
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/bulk_read_across_data_test_user");

        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot-2",
                                                   "test_keyspace", "test_table");

        // SNAPSHOT:CREATE permission granted for data/test_keyspace/test_table with ANALYTICS:READ_DIRECT permission
        verifyAccess(context, testCompleteLatch, HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, false);

        String createSnapshotRouteKeyspace2 = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot-3",
                                                            "non_admin_test_keyspace", "test_table");

        // SNAPSHOT:CREATE for different keyspace also granted with data scoped ANALYTICS:READ_DIRECT permission
        verifyAccess(context, testCompleteLatch, HttpMethod.PUT, createSnapshotRouteKeyspace2, clientKeystorePath, false);
    }

    void testGrantingBulkWriteFeaturePermission(VertxTestContext context) throws Exception
    {
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/bulk_write_test_user");

        String topologyRoute = String.format("/api/v1/keyspaces/%s/token-range-replicas", "grant_bulk_write_test_keyspace");
        // TOPOLOGY:READ permission granted with ANALYTICS:WRITE_DIRECT
        verifyAccess(context, testCompleteLatch, HttpMethod.GET, topologyRoute, clientKeystorePath, false);

        String keyspaceRingRoute = String.format("/api/v1/cassandra/ring/keyspaces/%s", "grant_bulk_write_test_keyspace");

        // RING:READ permission not granted for data/grant_bulk_write_test_keyspace with ANALYTICS:WRITE_DIRECT
        verifyAccess(context, testCompleteLatch, HttpMethod.GET, keyspaceRingRoute, clientKeystorePath, true);
    }

    void testGrantingBothBulkReadAndWriteFeaturePermission(VertxTestContext context) throws Exception
    {
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/bulk_read_write_test_user");

        String topologyRoute = String.format("/api/v1/keyspaces/%s/token-range-replicas", "grant_bulk_read_write_test_keyspace");
        // TOPOLOGY:READ permission granted with ANALYTICS:READ_DIRECT,WRITE_DIRECT
        verifyAccess(context, testCompleteLatch, HttpMethod.GET, topologyRoute, clientKeystorePath, false);

        String keyspaceRingRoute = String.format("/api/v1/cassandra/ring/keyspaces/%s", "grant_bulk_read_write_test_keyspace");

        // RING:READ permission granted for data/grant_bulk_read_write_test_keyspace with ANALYTICS:READ_DIRECT,WRITE_DIRECT
        verifyAccess(context, testCompleteLatch, HttpMethod.GET, keyspaceRingRoute, clientKeystorePath, false);

        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "grant_bulk_read_write_test_keyspace", "test_table");

        // SNAPSHOT:CREATE permission granted for data/grant_bulk_read_write_test_keyspace/test_table with ANALYTICS:READ_DIRECT,WRITE_DIRECT
        verifyAccess(context, testCompleteLatch, HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, false);
    }

    void testGrantingAllAnalyticsRelatedPermissions(VertxTestContext context) throws Exception
    {
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/all_analytics_permission_test_user");

        String topologyRoute = String.format("/api/v1/keyspaces/%s/token-range-replicas", "all_analytics_permission_test_keyspace");
        // TOPOLOGY:READ permission under ANALYTICS:WRITE_DIRECT granted with ANALYTICS:*
        verifyAccess(context, testCompleteLatch, HttpMethod.GET, topologyRoute, clientKeystorePath, false);

        String keyspaceRingRoute = String.format("/api/v1/cassandra/ring/keyspaces/%s", "all_analytics_permission_test_keyspace");

        // RING:READ permission under ANALYTICS:READ_DIRECT granted with ANALYTICS:*
        verifyAccess(context, testCompleteLatch, HttpMethod.GET, keyspaceRingRoute, clientKeystorePath, false);
    }

    void testGrantingCdcFeaturePermission(VertxTestContext context) throws Exception
    {
        String listCdcPath = "/api/v1/cdc/segments";
        Path clientKeystorePath = clientKeystorePath("spiffe://cassandra/sidecar/cdc_test_user");
        // CDC permission granted with CDC
        WebClient client = createClient(clientKeystorePath, truststorePath);
        createReq(client, HttpMethod.GET, listCdcPath)
        .onFailure(context::failNow)
        .onSuccess(listResp -> {
            // CDC permission granted
            // CDC is not turned on for cluster, hence 500 or 503 expected
            assertThat(listResp.statusCode()).isIn(HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                                                   HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
            testCompleteLatch.countDown();
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
        String statement = String.format("INSERT INTO system_auth.identity_to_role (identity, role) VALUES ('%s','%s')",
                                         identity, role);
        cassandraContext.cluster().schemaChangeIgnoringStoppedInstances(statement);
    }

    private void createRequiredKeyspaceTables()
    {
        createKeyspace("test_keyspace");
        createKeyspace("non_admin_test_keyspace");
        createKeyspace("grant_table_test_keyspace");
        createKeyspace("grant_keyspace_test_keyspace");
        createKeyspace("grant_tables_except_keyspace_test_keyspace");
        createKeyspace("multiple_permissions_required_test_keyspace");
        createKeyspace("grant_bulk_read_test_keyspace");
        createKeyspace("grant_bulk_read_across_tables_test_keyspace");
        createKeyspace("grant_bulk_write_test_keyspace");
        createKeyspace("grant_bulk_read_write_test_keyspace");
        createKeyspace("all_analytics_permission_test_keyspace");
        createTable("test_keyspace", "test_table");
        createTable("non_admin_test_keyspace", "test_table");
        createTable("grant_table_test_keyspace", "test_table");
        createTable("grant_keyspace_test_keyspace", "test_table");
        createTable("grant_tables_except_keyspace_test_keyspace", "test_table");
        createTable("multiple_permissions_required_test_keyspace", "test_table");
        createTable("grant_bulk_read_test_keyspace", "test_table");
        createTable("grant_bulk_read_across_tables_test_keyspace", "test_table");
        createTable("grant_bulk_read_across_tables_test_keyspace", "test_table2");
        createTable("grant_bulk_write_test_keyspace", "test_table");
        createTable("grant_bulk_read_write_test_keyspace", "test_table");
        createTable("all_analytics_permission_test_keyspace", "test_table");
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

        createRole("bulk_read_test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/bulk_read_test_user", "bulk_read_test_role");

        createRole("bulk_read_across_data_test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/bulk_read_across_data_test_user", "bulk_read_across_data_test_role");

        createRole("bulk_write_test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/bulk_write_test_user", "bulk_write_test_role");

        createRole("bulk_read_write_test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/bulk_read_write_test_user", "bulk_read_write_test_role");

        createRole("all_analytics_permission_test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/all_analytics_permission_test_user", "all_analytics_permission_test_role");

        createRole("cdc_test_role", false);
        insertIdentityRole(cassandraContext, "spiffe://cassandra/sidecar/cdc_test_user", "cdc_test_role");
    }

    private void grantRequiredPermissions()
    {
        // permission for testForNonAdmin
        grantSidecarPermission("non_admin_test_role", "data/non_admin_test_keyspace", "SCHEMA:READ");

        // permission for testGrantingForTable
        grantSidecarPermission("non_admin_test_role", "data/grant_table_test_keyspace/test_table", "SNAPSHOT:CREATE");

        // permission for testGrantingForKeyspace
        grantSidecarPermission("non_admin_test_role", "data/grant_keyspace_test_keyspace", "SNAPSHOT:CREATE");

        // permission for testGrantingAllTablesExceptKeyspace
        grantSidecarPermission("non_admin_test_role", "data/grant_tables_except_keyspace_test_keyspace/*", "SNAPSHOT:CREATE");

        // permission for testGrantingAtDataLevel
        grantSidecarPermission("grant_data_test_role", "data", "SNAPSHOT:CREATE");

        // permission for testGrantingWithWildcardSubparts
        grantSidecarPermission("wildcard_with_subparts_test_role", "cluster", "GOSSIP,SCHEMA:READ");

        // permission for testEndpointRequiringMultipleActions
        grantSidecarPermission("non_admin_test_role",
                               "data/multiple_permissions_required_test_keyspace/test_table",
                               "SNAPSHOT:CREATE");

        // permission for testGrantingBulkReadFeaturePermission
        grantSidecarPermission("bulk_read_test_role", "data/grant_bulk_read_test_keyspace/test_table", "ANALYTICS:READ_DIRECT");

        // permission for testGrantingBulkReadFeaturePermissionAcrossTables
        grantSidecarPermission("bulk_read_test_role", "data/grant_bulk_read_across_tables_test_keyspace", "ANALYTICS:READ_DIRECT");

        // permission for testGrantingBulkReadFeaturePermissionAcrossData
        grantSidecarPermission("bulk_read_across_data_test_role", "data", "ANALYTICS:READ_DIRECT");

        // permission for testGrantingBulkWriteFeaturePermission
        grantSidecarPermission("bulk_write_test_role", "data/grant_bulk_write_test_keyspace/test_table", "ANALYTICS:WRITE_DIRECT");

        // permission for testGrantingBothBulkReadAndWriteFeaturePermission
        grantSidecarPermission("bulk_read_write_test_role", "data/grant_bulk_read_write_test_keyspace/test_table", "ANALYTICS:READ_DIRECT,WRITE_DIRECT");

        // permission for testGrantingAllAnalyticsRelatedPermissions
        grantSidecarPermission("all_analytics_permission_test_role", "data/all_analytics_permission_test_keyspace/test_table", "ANALYTICS:*");

        // permission for testGrantingCdcFeaturePermission
        grantSidecarPermission("cdc_test_role", "cluster", "CDC");
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
                      return;
                  }

                  if (expectForbidden)
                  {
                      if (response.result().statusCode() != HttpResponseStatus.FORBIDDEN.code())
                      {
                          context.failNow(HttpResponseStatus.FORBIDDEN.code() + " expected but got " + response.result().statusCode());
                          return;
                      }
                  }
                  else
                  {
                      if (response.result().statusCode() != HttpResponseStatus.OK.code())
                      {
                          context.failNow(HttpResponseStatus.OK.code() + " expected but got " + response.result().statusCode());
                          return;
                      }
                  }
                  countDownLatch.countDown();
              });
    }

    private  Future<HttpResponse<Buffer>> createReq(WebClient client, HttpMethod method, String route)
    {
        return client.request(method, server.actualPort(), "127.0.0.1", route).send();
    }

    // Helper method to wait for cache refresh
    private Future<Void> waitForCacheRefresh(long durationMillis)
    {
        Promise<Void> promise = Promise.promise();
        vertx.setTimer(durationMillis, id -> promise.complete());
        return promise.future();
    }
}
