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

package org.apache.cassandra.sidecar.acl.authorization;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.sidecar.common.response.ListSnapshotFilesResponse;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;
import org.apache.cassandra.sidecar.config.ParameterizedClassConfiguration;
import org.apache.cassandra.sidecar.config.PeriodicTaskConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.AccessControlConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.CacheConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.KeyStoreConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ParameterizedClassConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.PeriodicTaskConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SslConfigurationImpl;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.db.schema.SidecarSchemaInitializer;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.modules.multibindings.ClassKey;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.MultiBindingTypeResolver;
import org.apache.cassandra.sidecar.modules.multibindings.PeriodicTaskMapKeys;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.testing.MtlsTestHelper;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;
import org.apache.cassandra.sidecar.testing.SharedExecutorNettyOptions;
import org.apache.cassandra.sidecar.testing.TemporaryCqlSessionProvider;
import org.apache.cassandra.sidecar.utils.CacheFactory;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;

import static org.apache.cassandra.sidecar.db.schema.SidecarRolePermissionsSchema.ROLE_PERMISSIONS_TABLE;
import static org.apache.cassandra.testing.DriverTestUtils.buildContactPoints;
import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TlsTestUtils.getSSLOptions;
import static org.apache.cassandra.testing.TlsTestUtils.waitForExistingRoles;
import static org.apache.cassandra.testing.TlsTestUtils.withAuthenticatedSession;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Test for role based access control in Sidecar
 * Note:
 * - Create a new keyspace or test role for each test method as required to prevent permissions overlapping
 * Note: locally when running authorization tests you need to comment out version 4.1 in
 * {@link org.apache.cassandra.testing.TestVersionSupplier}. Authorization tests do not run for 4.1, hence 5.1 run
 * gets skipped too.
 */
class RoleBasedAuthorizationIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    protected static final int MIN_VERSION_WITH_MTLS = 5;

    private static final String ADMIN_IDENTITY = "spiffe://cassandra/sidecar/admin";
    // CASSANDRA_IDENTITY is only used to configure schemas for test setup, do not use this identity for anything else
    private static final String CASSANDRA_IDENTITY = "spiffe://cassandra/sidecar/cassandra_role";
    // An identity for the Sidecar role
    private static final String SIDECAR_ROLE_IDENTITY = "spiffe://cassandra/sidecar/sidecar_role";
    private static final String CREATE_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS %s (a int, b text, PRIMARY KEY (a))";
    private static final Logger LOGGER = LoggerFactory.getLogger(RoleBasedAuthorizationIntegrationTest.class);

    public static final RoleWithIdentityTestScenario SUPERUSER =
    new RoleWithIdentityTestScenario("test_keyspace", "super_user_test_role", "spiffe://cassandra/sidecar/super_user_test_user")
    .superuser();
    public static final RoleWithIdentityTestScenario NON_SUPERUSER_ROLE_WITH_TRANSITIVE_SUPERUSER_ROLE =
    new RoleWithIdentityTestScenario("test_keyspace", "non_super_user_with_transitive_super_user_role",
                                     "spiffe://cassandra/sidecar/non_super_user_with_transitive_super_user");
    public static final RoleWithIdentityTestScenario NON_ADMIN_READ_TEST_KEYSPACE_ROLE =
    new RoleWithIdentityTestScenario("non_admin_test_keyspace", "non_admin_test_role", "spiffe://cassandra/sidecar/non_admin_test_user")
    .addPermission("data/non_admin_test_keyspace", "SCHEMA:READ");
    public static final RoleWithIdentityTestScenario NON_ADMIN_CACHE_REVOCATION_TEST_KEYSPACE_ROLE =
    new RoleWithIdentityTestScenario("non_admin_cache_revocation_test_keyspace", "non_admin_test_role", "spiffe://cassandra/sidecar/non_admin_test_user")
    .addPermission("data/non_admin_cache_revocation_test_keyspace", "SCHEMA:READ");
    public static final RoleWithIdentityTestScenario NON_ADMIN_CACHE_FORBIDDEN_TEST_KEYSPACE_ROLE =
    new RoleWithIdentityTestScenario("non_admin_cache_forbidden_test_keyspace", "non_admin_test_role", "spiffe://cassandra/sidecar/non_admin_test_user")
    .addPermission("data/non_admin_cache_forbidden_test_keyspace", "GOSSIP:READ");
    public static final RoleWithIdentityTestScenario NON_ADMIN_CREATE_SNAPSHOT_TEST_KEYSPACE_ROLE =
    new RoleWithIdentityTestScenario("grant_table_test_keyspace", "non_admin_test_role", "spiffe://cassandra/sidecar/non_admin_test_user")
    .addPermission("data/grant_table_test_keyspace/test_table", "SNAPSHOT:CREATE");
    public static final RoleWithIdentityTestScenario NON_ADMIN_CREATE_SNAPSHOT_KEYSPACE_LEVEL_ROLE =
    new RoleWithIdentityTestScenario("grant_keyspace_test_keyspace", "non_admin_test_role", "spiffe://cassandra/sidecar/non_admin_test_user")
    .addPermission("data/grant_keyspace_test_keyspace", "SNAPSHOT:CREATE");
    public static final RoleWithIdentityTestScenario NON_ADMIN_CREATE_SNAPSHOT_ALL_TABLES_ROLE =
    new RoleWithIdentityTestScenario("grant_tables_except_keyspace_test_keyspace", "non_admin_test_role", "spiffe://cassandra/sidecar/non_admin_test_user")
    .addPermission("data/grant_tables_except_keyspace_test_keyspace/*", "SNAPSHOT:CREATE");
    public static final RoleWithIdentityTestScenario NON_ADMIN_MULTIPLE_PERMISSIONS_ROLE =
    new RoleWithIdentityTestScenario("multiple_permissions_required_test_keyspace", "non_admin_test_role", "spiffe://cassandra/sidecar/non_admin_test_user")
    .addPermission("data/multiple_permissions_required_test_keyspace/test_table", "SNAPSHOT:CREATE");
    public static final RoleWithIdentityTestScenario NON_ADMIN_GRANT_DATA_LEVEL_ROLE =
    new RoleWithIdentityTestScenario("test_keyspace", "grant_data_test_role", "spiffe://cassandra/sidecar/grant_data_test_user")
    .addPermission("data", "SNAPSHOT:CREATE");
    public static final RoleWithIdentityTestScenario BULK_READ_PERMISSION_ROLE =
    new RoleWithIdentityTestScenario("grant_bulk_read_test_keyspace", "bulk_read_test_role", "spiffe://cassandra/sidecar/bulk_read_test_user")
    .addPermission("data/grant_bulk_read_across_tables_test_keyspace", "ANALYTICS:READ_DIRECT")
    .addPermission("data/grant_bulk_read_test_keyspace/test_table", "ANALYTICS:READ_DIRECT");
    public static final RoleWithIdentityTestScenario BULK_READ_PERMISSIONS_TEST_TABLE_ROLE =
    new RoleWithIdentityTestScenario("grant_bulk_read_across_tables_test_keyspace", "bulk_read_across_data_test_role",
                                     "spiffe://cassandra/sidecar/bulk_read_across_data_test_user")
    .addPermission("data", "ANALYTICS:READ_DIRECT");
    public static final RoleWithIdentityTestScenario BULK_READ_PERMISSIONS_TEST_TABLE_2_ROLE =
    new RoleWithIdentityTestScenario("grant_bulk_read_across_tables_test_keyspace", "bulk_read_across_data_test_role",
                                     "spiffe://cassandra/sidecar/bulk_read_across_data_test_user")
    .table("test_table2");
    public static final RoleWithIdentityTestScenario BULK_WRITE_PERMISSION_ROLE =
    new RoleWithIdentityTestScenario("grant_bulk_write_test_keyspace", "bulk_write_test_role", "spiffe://cassandra/sidecar/bulk_write_test_user")
    .addPermission("data/grant_bulk_write_test_keyspace/test_table", "ANALYTICS:WRITE_DIRECT");
    public static final RoleWithIdentityTestScenario CDC_PERMISSION_ROLE =
    new RoleWithIdentityTestScenario(null, "cdc_test_role", "spiffe://cassandra/sidecar/cdc_test_user")
    .addPermission("cluster", "CDC");
    public static final RoleWithIdentityTestScenario ALL_ANALYTICS_PERMISSION_ROLE =
    new RoleWithIdentityTestScenario("all_analytics_permission_test_keyspace", "all_analytics_permission_test_role",
                                     "spiffe://cassandra/sidecar/all_analytics_permission_test_user")
    .addPermission("data/all_analytics_permission_test_keyspace/test_table", "ANALYTICS:*");
    public static final RoleWithIdentityTestScenario BULK_READ_WRITE_PERMISSION_ROLE =
    new RoleWithIdentityTestScenario("grant_bulk_read_write_test_keyspace", "bulk_read_write_test_role", "spiffe://cassandra/sidecar/bulk_read_write_test_user")
    .addPermission("data/grant_bulk_read_write_test_keyspace/test_table", "ANALYTICS:READ_DIRECT,WRITE_DIRECT");
    public static final RoleWithIdentityTestScenario WILDCARD_ACROSS_TARGETS_ROLE =
    new RoleWithIdentityTestScenario(null, "wildcard_across_targets_test_role", "spiffe://cassandra/sidecar/wildcard_across_targets_test_user");
    public static final RoleWithIdentityTestScenario WILDCARD_SUBPARTS_ROLE =
    new RoleWithIdentityTestScenario(null, "wildcard_with_subparts_test_role", "spiffe://cassandra/sidecar/wildcard_with_subparts_test_user")
    .addPermission("cluster", "GOSSIP,SCHEMA:READ");

    // Describe all test scenarios
    static final List<RoleWithIdentityTestScenario> ROLE_WITH_IDENTITY_TEST_SCENARIOS = List.of(SUPERUSER,
                                                                                                NON_SUPERUSER_ROLE_WITH_TRANSITIVE_SUPERUSER_ROLE,
                                                                                                NON_ADMIN_READ_TEST_KEYSPACE_ROLE,
                                                                                                NON_ADMIN_CACHE_REVOCATION_TEST_KEYSPACE_ROLE,
                                                                                                NON_ADMIN_CACHE_FORBIDDEN_TEST_KEYSPACE_ROLE,
                                                                                                NON_ADMIN_CREATE_SNAPSHOT_TEST_KEYSPACE_ROLE,
                                                                                                NON_ADMIN_CREATE_SNAPSHOT_KEYSPACE_LEVEL_ROLE,
                                                                                                NON_ADMIN_CREATE_SNAPSHOT_ALL_TABLES_ROLE,
                                                                                                NON_ADMIN_MULTIPLE_PERMISSIONS_ROLE,
                                                                                                NON_ADMIN_GRANT_DATA_LEVEL_ROLE,
                                                                                                BULK_READ_PERMISSION_ROLE,
                                                                                                BULK_READ_PERMISSIONS_TEST_TABLE_ROLE,
                                                                                                BULK_READ_PERMISSIONS_TEST_TABLE_2_ROLE,
                                                                                                BULK_WRITE_PERMISSION_ROLE,
                                                                                                BULK_READ_WRITE_PERMISSION_ROLE,
                                                                                                ALL_ANALYTICS_PERMISSION_ROLE,
                                                                                                CDC_PERMISSION_ROLE,
                                                                                                WILDCARD_ACROSS_TARGETS_ROLE,
                                                                                                WILDCARD_SUBPARTS_ROLE
    );

    private Path nonAdminClientKeystorePath;

    @Override
    protected void beforeClusterProvisioning()
    {
        // mTLS authentication was added in Cassandra starting 5.0 version
        assumeThat(SimpleCassandraVersion.create(testVersion.version()).major)
        .as("mTLS authentication is not supported in 4.0 and 4.1 Cassandra versions")
        .isGreaterThanOrEqualTo(MIN_VERSION_WITH_MTLS);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .additionalInstanceConfig(Map.of("authenticator", "org.apache.cassandra.auth.PasswordAuthenticator"));
    }

    @Override
    protected void afterClusterProvisioned()
    {
        IInstance instance = cluster.getFirstRunningInstance();

        // Configure an admin identity so we can perform CQL queries
        // This is a hack for Cassandra 5.0, which doesn't truly support
        // MutualTlsWithPasswordFallbackAuthenticator
        configureAdminAndSidecarIdentity(instance);

        // Stop the instance and update the configuration to start using MutualTlsAuthenticator
        cluster.stopUnchecked(instance);

        // Update the cluster configuration
        IInstanceConfig instanceConfig = instance.config();
        instanceConfig.set("authenticator.class_name", "org.apache.cassandra.auth.MutualTlsAuthenticator");
        instanceConfig.set("authenticator.parameters", Map.of("validator_class_name",
                                                              "org.apache.cassandra.auth.SpiffeCertificateValidator"));

        instanceConfig.set("role_manager", "CassandraRoleManager");
        instanceConfig.set("authorizer", "CassandraAuthorizer");
        instanceConfig.set("client_encryption_options.enabled", "true");
        instanceConfig.set("client_encryption_options.optional", "true");
        instanceConfig.set("client_encryption_options.require_client_auth", "true");
        instanceConfig.set("client_encryption_options.require_endpoint_verification", "false");
        instanceConfig.set("client_encryption_options.keystore", mtlsTestHelper.serverKeyStorePath());
        instanceConfig.set("client_encryption_options.keystore_password", mtlsTestHelper.serverKeyStorePassword());
        instanceConfig.set("client_encryption_options.truststore", mtlsTestHelper.trustStorePath());
        instanceConfig.set("client_encryption_options.truststore_password", mtlsTestHelper.trustStorePassword());
        instanceConfig.set("credentials_update_interval", "50ms");

        // Start the instance again
        instance.startup();
    }

    /**
     * @return the configuration with overrides for access control configuration
     */
    @Override
    protected Function<SidecarConfigurationImpl.Builder, SidecarConfigurationImpl.Builder> configurationOverrides()
    {
        return builder -> {
            Map<String, String> params = Map.of("certificate_validator", "io.vertx.ext.auth.mtls.impl.CertificateValidatorImpl",
                                                "certificate_identity_extractor", "org.apache.cassandra.sidecar.acl.authentication.CassandraIdentityExtractor");
            ParameterizedClassConfiguration mTLSConfig
            = new ParameterizedClassConfigurationImpl("org.apache.cassandra.sidecar.acl.authentication.MutualTlsAuthenticationHandlerFactory",
                                                      params);
            ParameterizedClassConfiguration rbacConfig
            = new ParameterizedClassConfigurationImpl("org.apache.cassandra.sidecar.acl.authorization.RoleBasedAuthorizationProvider",
                                                      Map.of());

            CacheConfiguration permissionCacheConfiguration = CacheConfigurationImpl.builder()
                                                                                    .expireAfterAccess(MillisecondBoundConfiguration.parse("100ms"))
                                                                                    .build();

            AccessControlConfiguration accessControlConfiguration = AccessControlConfigurationImpl.builder()
                                                                                                  .enabled(true)
                                                                                                  .authenticatorsConfiguration(List.of(mTLSConfig))
                                                                                                  .authorizerConfiguration(rbacConfig)
                                                                                                  .adminIdentities(Set.of(ADMIN_IDENTITY))
                                                                                                  .permissionCacheConfiguration(permissionCacheConfiguration)
                                                                                                  .build();

            KeyStoreConfiguration truststoreConfiguration = new KeyStoreConfigurationImpl(mtlsTestHelper.trustStorePath(),
                                                                                          mtlsTestHelper.trustStorePassword(),
                                                                                          mtlsTestHelper.trustStoreType(),
                                                                                          SecondBoundConfiguration.parse("1d"));

            KeyStoreConfiguration keyStoreConfiguration = new KeyStoreConfigurationImpl(mtlsTestHelper.serverKeyStorePath(),
                                                                                        mtlsTestHelper.serverKeyStorePassword(),
                                                                                        mtlsTestHelper.serverKeyStoreType(),
                                                                                        SecondBoundConfiguration.parse("1d"));

            SslConfigurationImpl sslConfiguration = SslConfigurationImpl.builder()
                                                                        .enabled(true)
                                                                        .clientAuth("REQUEST")
                                                                        .keystore(keyStoreConfiguration)
                                                                        .truststore(truststoreConfiguration)
                                                                        .build();

            PeriodicTaskConfiguration healthCheckConfiguration = PeriodicTaskConfigurationImpl.Builder
                                                                 .builder()
                                                                 .enabled(true)
                                                                 .initialDelay(MillisecondBoundConfiguration.ONE)
                                                                 .executeInterval(MillisecondBoundConfiguration.ONE)
                                                                 .build();

            return builder.accessControlConfiguration(accessControlConfiguration)
                          .healthCheckConfiguration(healthCheckConfiguration)
                          .sslConfiguration(sslConfiguration);
        };
    }

    @Override
    protected void startSidecar(ICluster<? extends IInstance> cluster) throws InterruptedException
    {
        serverWrapper = startSidecarWithInstances(cluster, new TestModule(mtlsTestHelper, cluster));
    }

    @Override
    protected void beforeTestStart()
    {
        long startNanos = System.nanoTime();
        // wait for the schema initialization
        waitForSchemaReady(10, TimeUnit.SECONDS);
        LOGGER.info("Waited {} nanos for schema initialization", System.nanoTime() - startNanos);
    }

    @Test
    void testForAdmin() throws Exception
    {
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "test_keyspace");
        // Uses client keystore with admin identity. Configured admin identities bypass authorization checks
        Path clientKeystorePath = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                     certificateBuilder.addSanUriName(ADMIN_IDENTITY));

        verifyAccess(HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));
    }

    @Test
    void testForSuperUser() throws Exception
    {
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "test_keyspace");
        // uses client keystore with superuser identity
        Path clientKeystorePath = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                     certificateBuilder.addSanUriName("spiffe://cassandra/sidecar/super_user_test_user"));

        verifyAccess(HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));
    }

    @Test
    void testForTransitiveSuperUser() throws Exception
    {
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "test_keyspace");
        // uses client keystore with superuser identity
        Path clientKeystorePath =
        mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                           certificateBuilder.addSanUriName("spiffe://cassandra/sidecar/non_super_user_with_transitive_super_user"));

        verifyAccess(HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));
    }

    @Test
    void testForNonAdmin()
    {
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "non_admin_test_keyspace");

        verifyAccess(HttpMethod.GET, keyspaceSchemaRoute, nonAdminClientKeystorePath, assertStatus(HttpResponseStatus.OK));
    }

    @Test
    void testGrantingForTable()
    {
        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "grant_table_test_keyspace", "test_table");

        // SNAPSHOT:CREATE permission granted for data/grant_table_test_keyspace/test_table
        verifyAccess(HttpMethod.PUT, createSnapshotRoute, nonAdminClientKeystorePath, assertStatus(HttpResponseStatus.OK));

        // SNAPSHOT:DELETE permission not granted for data/grant_table_test_keyspace/test_table
        verifyAccess(HttpMethod.DELETE, createSnapshotRoute, nonAdminClientKeystorePath, assertStatus(HttpResponseStatus.FORBIDDEN));
    }

    @Test
    void testGrantingForKeyspace()
    {
        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "grant_keyspace_test_keyspace", "test_table");

        // SNAPSHOT:CREATE permission granted for data/grant_keyspace_test_keyspace/test_table with
        // data/grant_tables_test_keyspace grant
        verifyAccess(HttpMethod.PUT, createSnapshotRoute, nonAdminClientKeystorePath, assertStatus(HttpResponseStatus.OK));

        // access not granted for different keyspace
        String notAllowedSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                       "not_allowed_keyspace", "test_table");
        verifyAccess(HttpMethod.PUT, notAllowedSnapshotRoute, nonAdminClientKeystorePath, assertStatus(HttpResponseStatus.FORBIDDEN));

        // SNAPSHOT:DELETE permission not granted for data/grant_keyspace_test_keyspace/test_table
        verifyAccess(HttpMethod.DELETE, createSnapshotRoute, nonAdminClientKeystorePath, assertStatus(HttpResponseStatus.FORBIDDEN));
    }

    @Test
    void testGrantingAllTablesExceptKeyspace()
    {
        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "grant_tables_except_keyspace_test_keyspace", "test_table");

        // SNAPSHOT:CREATE permission granted for data/grant_tables_except_keyspace_test_keyspace/test_table
        // with data/grant_tables_except_keyspace_test_keyspace grant
        verifyAccess(HttpMethod.PUT, createSnapshotRoute, nonAdminClientKeystorePath, assertStatus(HttpResponseStatus.OK));

        // SCHEMA:READ is not granted since it expects permissions at keyspace level
        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "grant_tables_except_keyspace_test_keyspace");
        verifyAccess(HttpMethod.GET, keyspaceSchemaRoute, nonAdminClientKeystorePath, assertStatus(HttpResponseStatus.FORBIDDEN));
    }

    @Test
    void testGrantingAtDataLevel() throws Exception
    {
        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "test_keyspace", "test_table");
        Path clientKeystorePath = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                     certificateBuilder.addSanUriName("spiffe://cassandra/sidecar/grant_data_test_user"));

        // SNAPSHOT:CREATE permission granted for data/test_keyspace/test_table with data resource grant
        verifyAccess(HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));

        // SNAPSHOT:DELETE permission not granted for data/test_keyspace/test_table not granted
        verifyAccess(HttpMethod.DELETE, createSnapshotRoute, clientKeystorePath, assertStatus(HttpResponseStatus.FORBIDDEN));
    }

    @Test
    void testGrantingWithWildcardSubparts() throws Exception
    {
        String schemaRoute = "/api/v1/cassandra/schema";
        String gossipRoute = "/api/v1/cassandra/gossip";
        String ringRoute = "/api/v1/cassandra/ring";
        Path clientKeystorePath =
        mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                           certificateBuilder.addSanUriName("spiffe://cassandra/sidecar/wildcard_with_subparts_test_user"));

        // SCHEMA:READ permission granted for cluster with GOSSIP,SCHEMA:READ
        verifyAccess(HttpMethod.GET, schemaRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));

        // GOSSIP:READ permission granted for cluster with GOSSIP,SCHEMA:READ
        verifyAccess(HttpMethod.GET, gossipRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));

        // RING:READ permission not granted with GOSSIP,SCHEMA:READ
        verifyAccess(HttpMethod.GET, ringRoute, clientKeystorePath, assertStatus(HttpResponseStatus.FORBIDDEN));
    }

    @Test
    void testEndpointRequiringMultipleActions()
    {
        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "multiple_permissions_required_test_keyspace", "test_table");

        String listSnapshotRoute
        = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/%s",
                        "multiple_permissions_required_test_keyspace", "test_table", "my-snapshot");

        // SNAPSHOT:CREATE permission granted for data/multiple_permissions_required_test_keyspace/test_table
        verifyAccess(HttpMethod.PUT, createSnapshotRoute, nonAdminClientKeystorePath, assertStatus(HttpResponseStatus.OK));

        String[] componentDownloadUrl = new String[1];
        Path clientKeystorePath = cassandraIdentityClientKeyStore();
        SSLOptions sslOptions = getSSLOptions(clientKeystorePath.toString(),
                                              mtlsTestHelper.clientKeyStorePassword(),
                                              mtlsTestHelper.trustStorePath(),
                                              mtlsTestHelper.trustStorePassword());
        withAuthenticatedSession(cluster.get(1), "cassandra", "cassandra", session -> {
            // grant sidecar permission for streaming
            updateSidecarPermission(session,
                                    "non_admin_test_role",
                                    "data/multiple_permissions_required_test_keyspace/test_table",
                                    "SNAPSHOT:READ");

            invalidateAuthorizationHandlerCaches();

            verifyAccess(HttpMethod.GET, listSnapshotRoute, nonAdminClientKeystorePath, response -> {
                assertThat(response).isNotNull();
                assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                ListSnapshotFilesResponse snapshotFiles = response.bodyAsJson(ListSnapshotFilesResponse.class);
                List<ListSnapshotFilesResponse.FileInfo> filesToStream =
                snapshotFiles.snapshotFilesInfo()
                             .stream()
                             .filter(info -> info.fileName.endsWith("-Data.db"))
                             .sorted(Comparator.comparing(o -> o.fileName))
                             .collect(Collectors.toList());
                assertThat(filesToStream).isNotNull().isNotEmpty();
                componentDownloadUrl[0] = filesToStream.get(0).componentDownloadUrl();
            });

            // grant sidecar permission for streaming
            updateSidecarPermission(session,
                                    "non_admin_test_role",
                                    "data/multiple_permissions_required_test_keyspace/test_table",
                                    "SNAPSHOT:STREAM");

            invalidateAuthorizationHandlerCaches();

            // STREAM SSTable request requires both Sidecar SNAPSHOT:STREAM permission and Cassandra's SELECT
            // permission on a table it accesses data.

            // request denied without SELECT permission
            verifyAccess(HttpMethod.GET, componentDownloadUrl[0], nonAdminClientKeystorePath, assertStatus(HttpResponseStatus.FORBIDDEN));

            // grant SELECT permission to non_admin_test_role
            grantTablePermission(session, "multiple_permissions_required_test_keyspace", "test_table", "non_admin_test_role");
        }, sslOptions);

        invalidateAuthorizationHandlerCaches();

        // request goes through with SELECT permission
        verifyAccess(HttpMethod.GET, componentDownloadUrl[0], nonAdminClientKeystorePath, assertStatus(HttpResponseStatus.OK));
    }

    @Test
    void testGrantingBulkReadFeaturePermission() throws Exception
    {
        Path clientKeystorePath = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                     certificateBuilder.addSanUriName("spiffe://cassandra/sidecar/bulk_read_test_user"));

        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "grant_bulk_read_test_keyspace", "test_table");

        // SNAPSHOT:CREATE permission granted for data/grant_bulk_read_test_keyspace/test_table with ANALYTICS:READ_DIRECT
        verifyAccess(HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "grant_bulk_read_test_keyspace");

        // SCHEMA:READ permission granted for data/grant_bulk_read_test_keyspace/test_table with ANALYTICS:READ_DIRECT
        verifyAccess(HttpMethod.GET, keyspaceSchemaRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));

        String topologyRoute = String.format("/api/v1/keyspaces/%s/token-range-replicas", "grant_bulk_read_test_keyspace");
        // TOPOLOGY:READ permission not granted with ANALYTICS:READ_DIRECT
        verifyAccess(HttpMethod.GET, topologyRoute, clientKeystorePath, assertStatus(HttpResponseStatus.FORBIDDEN));

        String tableStatsRoute = String.format("/api/v1/cassandra/keyspaces/%s/tables/%s/stats",
                                               "grant_bulk_read_test_keyspace", "test_table");
        // STATS permission granted with ANALYTICS:READ_DIRECT
        verifyAccess(HttpMethod.GET, tableStatsRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));

        String clusterStatsRoute = "/api/v1/cassandra/stats/streams";
        // STATS permission at cluster scope not granted with ANALYTICS:READ_DIRECT
        verifyAccess(HttpMethod.GET, clusterStatsRoute, clientKeystorePath, assertStatus(HttpResponseStatus.FORBIDDEN));
    }

    @Test
    void testGrantingBulkReadFeaturePermissionAcrossTables() throws Exception
    {
        Path clientKeystorePath = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                     certificateBuilder.addSanUriName("spiffe://cassandra/sidecar/bulk_read_test_user"));

        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "grant_bulk_read_across_tables_test_keyspace", "test_table");

        // SNAPSHOT:CREATE permission granted for data/grant_bulk_read_across_tables_test_keyspace/test_table with ANALYTICS:READ_DIRECT
        verifyAccess(HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));

        String createSnapshotRouteTable2 = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                         "grant_bulk_read_across_tables_test_keyspace", "test_table2");

        // SNAPSHOT:CREATE for different table also granted with keyspace scoped ANALYTICS:READ_DIRECT permission
        verifyAccess(HttpMethod.PUT, createSnapshotRouteTable2, clientKeystorePath, assertStatus(HttpResponseStatus.OK));

        String createSnapshotRouteKeyspace2 = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                            "test_keyspace", "test_table2");

        // SNAPSHOT:CREATE for different keyspace not granted with keyspace scoped ANALYTICS:READ_DIRECT permission
        verifyAccess(HttpMethod.PUT, createSnapshotRouteKeyspace2, clientKeystorePath, assertStatus(HttpResponseStatus.FORBIDDEN));

        String tableStatsRoute = String.format("/api/v1/cassandra/keyspaces/%s/tables/%s/stats",
                                               "grant_bulk_read_across_tables_test_keyspace", "test_table");
        // STATS permission granted with ANALYTICS:READ_DIRECT
        verifyAccess(HttpMethod.GET, tableStatsRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));
    }

    @Test
    void testGrantingBulkReadFeaturePermissionAcrossData() throws Exception
    {
        Path clientKeystorePath =
        mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                           certificateBuilder.addSanUriName("spiffe://cassandra/sidecar/bulk_read_across_data_test_user"));

        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot-2",
                                                   "test_keyspace", "test_table");

        // SNAPSHOT:CREATE permission granted for data/test_keyspace/test_table with ANALYTICS:READ_DIRECT permission
        verifyAccess(HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));

        String createSnapshotRouteKeyspace2 = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot-3",
                                                            "non_admin_test_keyspace", "test_table");

        // SNAPSHOT:CREATE for different keyspace also granted with data scoped ANALYTICS:READ_DIRECT permission
        verifyAccess(HttpMethod.PUT, createSnapshotRouteKeyspace2, clientKeystorePath, assertStatus(HttpResponseStatus.OK));
    }

    @Test
    void testGrantingBulkWriteFeaturePermission() throws Exception
    {
        Path clientKeystorePath = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                     certificateBuilder.addSanUriName("spiffe://cassandra/sidecar/bulk_write_test_user"));

        String topologyRoute = String.format("/api/v1/keyspaces/%s/token-range-replicas", "grant_bulk_write_test_keyspace");
        // TOPOLOGY:READ permission granted with ANALYTICS:WRITE_DIRECT
        verifyAccess(HttpMethod.GET, topologyRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));

        String keyspaceRingRoute = String.format("/api/v1/cassandra/ring/keyspaces/%s", "grant_bulk_write_test_keyspace");

        // RING:READ permission not granted for data/grant_bulk_write_test_keyspace with ANALYTICS:WRITE_DIRECT
        verifyAccess(HttpMethod.GET, keyspaceRingRoute, clientKeystorePath, assertStatus(HttpResponseStatus.FORBIDDEN));
    }

    @Test
    void testGrantingBothBulkReadAndWriteFeaturePermission() throws Exception
    {
        Path clientKeystorePath = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                     certificateBuilder.addSanUriName("spiffe://cassandra/sidecar/bulk_read_write_test_user"));

        String topologyRoute = String.format("/api/v1/keyspaces/%s/token-range-replicas", "grant_bulk_read_write_test_keyspace");
        // TOPOLOGY:READ permission granted with ANALYTICS:READ_DIRECT,WRITE_DIRECT
        verifyAccess(HttpMethod.GET, topologyRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));

        String keyspaceRingRoute = String.format("/api/v1/cassandra/ring/keyspaces/%s", "grant_bulk_read_write_test_keyspace");

        // RING:READ permission granted for data/grant_bulk_read_write_test_keyspace with ANALYTICS:READ_DIRECT,WRITE_DIRECT
        verifyAccess(HttpMethod.GET, keyspaceRingRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));

        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot",
                                                   "grant_bulk_read_write_test_keyspace", "test_table");

        // SNAPSHOT:CREATE permission granted for data/grant_bulk_read_write_test_keyspace/test_table with ANALYTICS:READ_DIRECT,WRITE_DIRECT
        verifyAccess(HttpMethod.PUT, createSnapshotRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));
    }

    @Test
    void testGrantingAllAnalyticsRelatedPermissions() throws Exception
    {
        Path clientKeystorePath =
        mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                           certificateBuilder.addSanUriName("spiffe://cassandra/sidecar/all_analytics_permission_test_user"));

        String topologyRoute = String.format("/api/v1/keyspaces/%s/token-range-replicas", "all_analytics_permission_test_keyspace");
        // TOPOLOGY:READ permission under ANALYTICS:WRITE_DIRECT granted with ANALYTICS:*
        verifyAccess(HttpMethod.GET, topologyRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));

        String keyspaceRingRoute = String.format("/api/v1/cassandra/ring/keyspaces/%s", "all_analytics_permission_test_keyspace");

        // RING:READ permission under ANALYTICS:READ_DIRECT granted with ANALYTICS:*
        verifyAccess(HttpMethod.GET, keyspaceRingRoute, clientKeystorePath, assertStatus(HttpResponseStatus.OK));
    }

    @Test
    void testGrantingCdcFeaturePermission() throws Exception
    {
        String listCdcPath = "/api/v1/cdc/segments";
        Path clientKeystorePath = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                     certificateBuilder.addSanUriName("spiffe://cassandra/sidecar/cdc_test_user"));
        // CDC permission granted with CDC
        WebClient client = trustedClient(clientKeystorePath.toString(), mtlsTestHelper.clientKeyStorePassword(),
                                         mtlsTestHelper.trustStorePath(), mtlsTestHelper.trustStorePassword());
        HttpResponse<Buffer> listResp = createRequest(client, HttpMethod.GET, listCdcPath);

        // CDC permission granted
        // CDC is not turned on for cluster, hence 500 or 503 expected
        assertThat(listResp.statusCode()).isIn(HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                                               HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
    }

    @Test
    void testAuthorizationCaching()
    {
        SidecarMetrics metrics = serverWrapper.injector.getInstance(SidecarMetrics.class);

        CacheStats baseline = metrics.server().cache().authorizationCacheMetrics.snapshot();

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "non_admin_test_keyspace");

        WebClient client = trustedClient(nonAdminClientKeystorePath.toString(), mtlsTestHelper.clientKeyStorePassword(),
                                         mtlsTestHelper.trustStorePath(), mtlsTestHelper.trustStorePassword());
        try
        {
            createMultipleRequests(client, HttpMethod.GET, keyspaceSchemaRoute, 2, HttpResponseStatus.OK.code());
        }
        finally
        {
            client.close();
        }

        // Verify cache stats, 1 hit 1 miss
        CacheStats callStats = metrics.server().cache().authorizationCacheMetrics.snapshot();
        assertThat(callStats.missCount()).isEqualTo(1);
        assertThat(callStats.hitCount()).isEqualTo(1);
    }

    @Test
    void testAuthorizationCachingWithPermissionRevocation()
    {
        SidecarMetrics metrics = serverWrapper.injector.getInstance(SidecarMetrics.class);

        CacheStats baseline = metrics.server().cache().authorizationCacheMetrics.snapshot();

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "non_admin_cache_revocation_test_keyspace");

        WebClient client = trustedClient(nonAdminClientKeystorePath.toString(), mtlsTestHelper.clientKeyStorePassword(),
                                         mtlsTestHelper.trustStorePath(), mtlsTestHelper.trustStorePassword());

        createMultipleRequests(client, HttpMethod.GET, keyspaceSchemaRoute, 2, HttpResponseStatus.OK.code());

        CacheStats callStats = metrics.server().cache().authorizationCacheMetrics.snapshot();
        assertThat(callStats.missCount()).isEqualTo(1);
        assertThat(callStats.hitCount()).isEqualTo(1);

        // Revoke permission
        Path clientKeystorePath = cassandraIdentityClientKeyStore();
        SSLOptions sslOptions = getSSLOptions(clientKeystorePath.toString(),
                                              mtlsTestHelper.clientKeyStorePassword(),
                                              mtlsTestHelper.trustStorePath(),
                                              mtlsTestHelper.trustStorePassword());
        withAuthenticatedSession(cluster.get(1), "cassandra", "cassandra", session -> {
            session.execute(String.format("DELETE FROM sidecar_internal.role_permissions_v1 " +
                                          "WHERE role = '%s' AND resource = 'data/%s'", "non_admin_test_role",
                                          "non_admin_cache_revocation_test_keyspace"));
        }, sslOptions);

        invalidateAuthorizationHandlerCaches();

        try
        {
            // After cache expires, verify permission revocation takes effect
            createMultipleRequests(client, HttpMethod.GET, keyspaceSchemaRoute, 2, HttpResponseStatus.FORBIDDEN.code());
        }
        finally
        {
            client.close();
        }

        CacheStats finalCallStats = metrics.server().cache().authorizationCacheMetrics.snapshot();
        // After cache expires, we should see a new miss and a hit for subsequent call
        assertThat(finalCallStats.missCount()).isEqualTo(1);
        assertThat(finalCallStats.hitCount()).isEqualTo(1);
    }

    @Test
    void testAuthorizationCachingForForbiddenRequests()
    {
        SidecarMetrics metrics = serverWrapper.injector.getInstance(SidecarMetrics.class);

        CacheStats baseline = metrics.server().cache().authorizationCacheMetrics.snapshot();

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "non_admin_cache_forbidden_test_keyspace");

        // user has GOSSIP:READ permission but not SCHEMA:READ
        WebClient client = trustedClient(nonAdminClientKeystorePath.toString(), mtlsTestHelper.clientKeyStorePassword(),
                                         mtlsTestHelper.trustStorePath(), mtlsTestHelper.trustStorePassword());

        try
        {
            createMultipleRequests(client, HttpMethod.GET, keyspaceSchemaRoute, 2, HttpResponseStatus.FORBIDDEN.code());
        }
        finally
        {
            client.close();
        }

        // Verify cache stats, 1 hit 1 miss
        CacheStats callStats = metrics.server().cache().authorizationCacheMetrics.snapshot();
        assertThat(callStats.missCount()).isEqualTo(1);
        assertThat(callStats.hitCount()).isEqualTo(1);
    }

    @Test
    void testSameUserAccessingDifferentRoutes() throws Exception
    {
        SidecarMetrics metrics = serverWrapper.injector.getInstance(SidecarMetrics.class);

        CacheStats baseline = metrics.server().cache().authorizationCacheMetrics.snapshot();

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "non_admin_test_keyspace");
        String createSnapshotRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/my-snapshot-different-access",
                                                   "grant_table_test_keyspace", "test_table");

        verifyAccess(HttpMethod.GET, keyspaceSchemaRoute, nonAdminClientKeystorePath, assertStatus(HttpResponseStatus.OK));
        verifyAccess(HttpMethod.PUT, createSnapshotRoute, nonAdminClientKeystorePath, assertStatus(HttpResponseStatus.OK));

        // Verify cache stats, both miss
        CacheStats callStats = metrics.server().cache().authorizationCacheMetrics.snapshot();
        assertThat(callStats.missCount()).isEqualTo(2);
        assertThat(callStats.hitCount()).isEqualTo(0);
    }

    @Test
    void testAdminBypassCaching() throws Exception
    {
        SidecarMetrics metrics = serverWrapper.injector.getInstance(SidecarMetrics.class);

        CacheStats baseline = metrics.server().cache().authorizationCacheMetrics.snapshot();

        String keyspaceSchemaRoute = String.format("/api/v1/keyspaces/%s/schema", "non_admin_test_keyspace");
        // Uses client keystore with admin identity. Configured admin identities bypass authorization checks
        Path clientKeystorePath = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                     certificateBuilder.addSanUriName(ADMIN_IDENTITY));

        WebClient client = trustedClient(clientKeystorePath.toString(), mtlsTestHelper.clientKeyStorePassword(),
                                         mtlsTestHelper.trustStorePath(), mtlsTestHelper.trustStorePassword());
        try
        {
            createMultipleRequests(client, HttpMethod.GET, keyspaceSchemaRoute, 2, HttpResponseStatus.OK.code());
        }
        finally
        {
            client.close();
        }

        // Verify cache stats, 1 hit 1 miss
        CacheStats callStats = metrics.server().cache().authorizationCacheMetrics.snapshot();
        assertThat(callStats.missCount()).isEqualTo(1);
        assertThat(callStats.hitCount()).isEqualTo(1);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        Path clientKeystorePath = cassandraIdentityClientKeyStore();

        createRequiredKeystores();
        SSLOptions sslOptions = getSSLOptions(clientKeystorePath.toString(),
                                              mtlsTestHelper.clientKeyStorePassword(),
                                              mtlsTestHelper.trustStorePath(),
                                              mtlsTestHelper.trustStorePassword());
        withAuthenticatedSession(cluster.get(1), "cassandra", "cassandra", session -> {
            // Required for authentication of sidecar requests to Cassandra. Only superusers can grant permissions
            createTestKeyspace(session, "sidecar_internal", DC1_RF1);
            createSidecarRolesPermissionsTable(session);
            createRequiredKeyspaceTables(session);
            createRequiredRoles(session);
        }, sslOptions);
    }

    private void createSidecarRolesPermissionsTable(Session session)
    {
        String statement = String.format("CREATE TABLE IF NOT EXISTS sidecar_internal.%s ("
                                         + "role text,"
                                         + "resource text,"
                                         + "permissions set<text>,"
                                         + "PRIMARY KEY(role, resource))",
                                         ROLE_PERMISSIONS_TABLE);
        session.execute(statement);
    }

    private void createRequiredKeyspaceTables(Session session)
    {
        for (RoleWithIdentityTestScenario scenario : ROLE_WITH_IDENTITY_TEST_SCENARIOS)
        {
            if (scenario.keyspace == null || scenario.table == null)
                continue;

            QualifiedName table = new QualifiedName(scenario.keyspace, scenario.table);
            createTestKeyspace(session, table, DC1_RF1);
            createTestTable(session, table, CREATE_TABLE_STATEMENT);
            session.execute("INSERT INTO " + table + " (a, b) VALUES (1, 'text');");
        }
    }

    private void createRequiredRoles(Session session)
    {
        for (RoleWithIdentityTestScenario mapping : ROLE_WITH_IDENTITY_TEST_SCENARIOS)
        {
            session.execute("CREATE ROLE IF NOT EXISTS \"" + mapping.role + "\" " +
                            "WITH SUPERUSER = " + mapping.superuser + " " +
                            "AND LOGIN = true");
            session.execute(String.format("ADD IDENTITY IF NOT EXISTS '%s' TO ROLE '%s'", mapping.identity, mapping.role));

            for (RoleWithIdentityTestScenario.Permission permission : mapping.permissions)
            {
                session.execute(String.format("INSERT INTO sidecar_internal.role_permissions_v1 (role, resource, permissions) " +
                                              "VALUES ('%s', '%s', {'%s'})", mapping.role, permission.resource, permission.permission));
            }
        }

        // grant a superuser role transitively to the non_super_user_with_transitive_super_user_role role
        session.execute("GRANT super_user_test_role TO non_super_user_with_transitive_super_user_role");
    }

    private void createRequiredKeystores()
    {
        try
        {
            nonAdminClientKeystorePath = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                            certificateBuilder.addSanUriName("spiffe://cassandra/sidecar/non_admin_test_user"));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to create required keystores", e);
        }
    }

    private void grantTablePermission(Session session, String keyspace, String table, String role)
    {
        session.execute("GRANT ALL PERMISSIONS ON " + keyspace + "." + table + " TO " + role);
    }

    private void updateSidecarPermission(Session session, String role, String resource, String permission)
    {
        session.execute(String.format("UPDATE sidecar_internal.role_permissions_v1 SET permissions = permissions + {'%s'} " +
                                      "where role = '%s' and resource = '%s'", permission, role, resource));
    }

    private void invalidateAuthorizationHandlerCaches()
    {
        CacheFactory factory = serverWrapper.injector.getInstance(CacheFactory.class);
        Cache<AuthorizationCacheKey, Future<Boolean>> authorizationCache = factory.endpointAuthorizationCache();
        authorizationCache.invalidateAll();
    }

    private void verifyAccess(HttpMethod method, String testRoute, Path clientKeystorePath, Verifier<HttpResponse<Buffer>> assertions)
    {
        verifyAccess(method, testRoute, clientKeystorePath.toString(), assertions);
    }

    private void verifyAccess(HttpMethod method, String testRoute, String clientKeystorePath, Verifier<HttpResponse<Buffer>> assertions)
    {
        WebClient client = trustedClient(clientKeystorePath, mtlsTestHelper.clientKeyStorePassword(),
                                         mtlsTestHelper.trustStorePath(), mtlsTestHelper.trustStorePassword());
        try
        {
            HttpResponse<Buffer> response = getBlocking(client.request(method, serverWrapper.serverPort, "127.0.0.1", testRoute).send());
            assertions.accept(response);
        }
        finally
        {
            client.close();
        }
    }

    private HttpResponse<Buffer> createRequest(WebClient client, HttpMethod method, String route)
    {
        return getBlocking(client.request(method, serverWrapper.serverPort, "127.0.0.1", route).send());
    }

    private void createMultipleRequests(WebClient client, HttpMethod method, String route, int times,
                                        int expectedResponseCode)
    {
        List<Future<HttpResponse<Buffer>>> futures = new ArrayList<>();
        for (int i = 0; i < times; i++)
        {
            futures.add(createNonBlockingRequest(client, method, route));
        }

        // Now block for response
        for (int i = 0; i < times; i++)
        {
            HttpResponse<Buffer> response = getBlocking(futures.get(i));
            assertThat(response.statusCode()).isEqualTo(expectedResponseCode);
        }
    }

    private Future<HttpResponse<Buffer>> createNonBlockingRequest(WebClient client, HttpMethod method, String route)
    {
        return client.request(method, serverWrapper.serverPort, "127.0.0.1", route).send();
    }

    private void configureAdminAndSidecarIdentity(IInstance instance)
    {
        waitForExistingRoles(() -> withAuthenticatedSession(instance, "cassandra", "cassandra", session -> {

            // TODO: it would be a good idea to scope down Sidecar role to permissions it needs
            session.execute("CREATE ROLE IF NOT EXISTS \"" + "sidecar_role" + "\" " +
                            "WITH SUPERUSER = " + true + " " +
                            "AND LOGIN = true");

            String statement1 = String.format("ADD IDENTITY IF NOT EXISTS '%s' TO ROLE '%s'", CASSANDRA_IDENTITY, "cassandra");
            session.execute(statement1);
            String statement = String.format("ADD IDENTITY IF NOT EXISTS '%s' TO ROLE '%s'", SIDECAR_ROLE_IDENTITY, "sidecar_role");
            session.execute(statement);
        }, null));
    }

    private Path cassandraIdentityClientKeyStore()
    {
        Path clientKeystorePath;
        try
        {
            clientKeystorePath = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                    certificateBuilder.addSanUriName(CASSANDRA_IDENTITY));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return clientKeystorePath;
    }

    /**
     * Encapsulates information needed to configure a test as well as how an identity maps to a role
     */
    static class RoleWithIdentityTestScenario
    {
        private final String keyspace;
        private String table;
        private final String role;
        private final String identity;
        private boolean superuser;
        private final List<Permission> permissions = new ArrayList<>();

        RoleWithIdentityTestScenario(String keyspace, String role, String identity)
        {
            this.keyspace = keyspace;
            this.table = "test_table";
            this.role = role;
            this.identity = identity;
        }

        RoleWithIdentityTestScenario superuser()
        {
            this.superuser = true;
            return this;
        }

        RoleWithIdentityTestScenario table(String table)
        {
            this.table = table;
            return this;
        }

        RoleWithIdentityTestScenario addPermission(String resource, String permission)
        {
            permissions.add(new Permission(resource, permission));
            return this;
        }

        static class Permission
        {
            private final String resource;
            private final String permission;

            Permission(String resource, String permission)
            {
                this.resource = resource;
                this.permission = permission;
            }
        }
    }

    static class TestModule extends AbstractModule
    {
        private final MtlsTestHelper mtlsTestHelper;
        private final ICluster<? extends IInstance> cluster;

        TestModule(MtlsTestHelper mtlsTestHelper, ICluster<? extends IInstance> cluster)
        {
            this.mtlsTestHelper = mtlsTestHelper;
            this.cluster = cluster;
        }

        @Provides
        @Singleton
        public CQLSessionProvider cqlSessionProvider()
        {
            List<InetSocketAddress> contactPoints = buildContactPoints(cluster);

            // Issue a certificate for the Sidecar Role to talk to the Cassandra database
            Path clientKeystoreForSidecarToCassandraConnections;
            try
            {
                clientKeystoreForSidecarToCassandraConnections = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                                                    certificateBuilder.addSanUriName(SIDECAR_ROLE_IDENTITY));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }

            SSLOptions sslOptions = getSSLOptions(clientKeystoreForSidecarToCassandraConnections.toString(),
                                                  mtlsTestHelper.clientKeyStorePassword(),
                                                  mtlsTestHelper.trustStorePath(),
                                                  mtlsTestHelper.trustStorePassword());
            return new TemporaryCqlSessionProvider(contactPoints,
                                                   SharedExecutorNettyOptions.INSTANCE,
                                                   sslOptions);
        }

        /**
         * This is an example of replacing a bound object. In the resolver below, the SidecarSchemaInitializer
         * from production code is removed and replaced by the SidecarSchemaInitializer provided in this module.
         * See {@link #sidecarSchemaInitializer(SidecarConfiguration, CQLSessionProvider, SidecarMetrics, SidecarSchema, ClusterLease)}
         */
        @Provides
        @Singleton
        MultiBindingTypeResolver<PeriodicTask> periodicTaskTypeResolver(Map<Class<? extends ClassKey>, PeriodicTask> periodicTaskMap)
        {
            return () -> {
                Map<Class<? extends ClassKey>, PeriodicTask> map = new HashMap<>(periodicTaskMap);
                map.remove(PeriodicTaskMapKeys.SidecarSchemaInitializerTaskKey.class);
                return map;
            };
        }

        // @formatter:off
        static class TestSidecarSchemaInitializerTaskKey implements ClassKey {}
        // @formatter:on
        @ProvidesIntoMap
        @KeyClassMapKey(TestSidecarSchemaInitializerTaskKey.class)
        PeriodicTask sidecarSchemaInitializer(SidecarConfiguration configuration,
                                              CQLSessionProvider cqlSessionProvider,
                                              SidecarMetrics sidecarMetrics,
                                              SidecarSchema sidecarSchema,
                                              ClusterLease clusterLease)
        {
            return new SidecarSchemaInitializer(configuration,
                                                cqlSessionProvider,
                                                sidecarSchema.sidecarInternalKeyspace(),
                                                sidecarMetrics.server().schema(),
                                                sidecarSchema,
                                                clusterLease)
            {
                @Override
                public DurationSpec delay()
                {
                    return MillisecondBoundConfiguration.ONE;
                }
            };
        }
    }
}
