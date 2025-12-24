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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.github.benmanes.caffeine.cache.Cache;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.acl.IdentityToRoleCache;
import org.apache.cassandra.sidecar.acl.authorization.AuthorizationCacheKey;
import org.apache.cassandra.sidecar.acl.authorization.RoleAuthorizationsCache;
import org.apache.cassandra.sidecar.acl.authorization.SuperUserCache;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;
import org.apache.cassandra.sidecar.config.ParameterizedClassConfiguration;
import org.apache.cassandra.sidecar.config.yaml.AccessControlConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.CacheConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.KeyStoreConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ParameterizedClassConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SslConfigurationImpl;
import org.apache.cassandra.sidecar.testing.MtlsTestHelper;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;
import org.apache.cassandra.sidecar.testing.TemporaryCqlSessionProvider;
import org.apache.cassandra.sidecar.utils.CacheFactory;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;

import static java.net.URLEncoder.encode;
import static org.apache.cassandra.testing.DriverTestUtils.buildContactPoints;
import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TlsTestUtils.getSSLOptions;
import static org.apache.cassandra.testing.TlsTestUtils.withAuthenticatedSession;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration test for cache invalidation endpoints
 */
@ExtendWith(VertxExtension.class)
class InvalidateCacheIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    protected static final int MIN_VERSION_WITH_MTLS = 5;
    private static final String CASSANDRA_IDENTITY = "spiffe://cassandra/sidecar/cassandra_role";
    private static final String SIDECAR_ROLE_IDENTITY = "spiffe://cassandra/sidecar/sidecar_role";
    private static final String TEST_USER_IDENTITY = "spiffe://cassandra/sidecar/test_user";
    private static final String TEST_USER2_IDENTITY = "spiffe://cassandra/sidecar/test_user2";
    private static final String TEST_SUPERUSER_IDENTITY = "spiffe://cassandra/sidecar/test_superuser";
    private static final String TEST_SUPERUSER2_IDENTITY = "spiffe://cassandra/sidecar/test_superuser2";
    private static final String TEST_SUPERUSER3_IDENTITY = "spiffe://cassandra/sidecar/test_superuser3";
    private static final String SCHEMA_ROUTE = "/api/v1/cassandra/schema";
    private static final String CACHE_INVALIDATE_ROUTE_TEMPLATE = "/api/v1/caches/%s/invalidate";

    private Path testUserKeystorePath;
    private Path testUser2KeystorePath;
    private Path superuserKeystorePath;
    private Path superuser2KeystorePath;
    private Path superuser3KeystorePath;

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
        configureAdminAndSidecarIdentity(instance);

        cluster.stopUnchecked(instance);

        var instanceConfig = instance.config();
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

        instance.startup();
    }

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
                                                                                    .expireAfterAccess(MillisecondBoundConfiguration.parse("5s"))
                                                                                    .build();

            AccessControlConfiguration accessControlConfiguration = AccessControlConfigurationImpl.builder()
                                                                                                  .enabled(true)
                                                                                                  .authenticatorsConfiguration(List.of(mTLSConfig))
                                                                                                  .authorizerConfiguration(rbacConfig)
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

            return builder.accessControlConfiguration(accessControlConfiguration)
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
        waitForSchemaReady(10, TimeUnit.SECONDS);
        try
        {
            testUserKeystorePath = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                      certificateBuilder.addSanUriName(TEST_USER_IDENTITY));
            testUser2KeystorePath = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                       certificateBuilder.addSanUriName(TEST_USER2_IDENTITY));
            superuserKeystorePath = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                       certificateBuilder.addSanUriName(TEST_SUPERUSER_IDENTITY));
            superuser2KeystorePath = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                        certificateBuilder.addSanUriName(TEST_SUPERUSER2_IDENTITY));
            superuser3KeystorePath = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                        certificateBuilder.addSanUriName(TEST_SUPERUSER3_IDENTITY));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to create test keystores", e);
        }
    }

    @Override
    protected void initializeSchemaForTest()
    {
        Path clientKeystorePath = cassandraIdentityClientKeyStore();

        SSLOptions sslOptions = getSSLOptions(clientKeystorePath.toString(),
                                              mtlsTestHelper.clientKeyStorePassword(),
                                              mtlsTestHelper.trustStorePath(),
                                              mtlsTestHelper.trustStorePassword());
        withAuthenticatedSession(cluster.get(1), "cassandra", "cassandra", session -> {
            createTestKeyspace(session, "sidecar_internal", DC1_RF1);
            createRolesPermissionsTable(session);
            createTestRole(session);
        }, sslOptions);
    }

    @Test
    void testInvalidateIdentityToRoleCache(VertxTestContext testContext)
    {
        IdentityToRoleCache identityToRoleCache = serverWrapper.injector.getInstance(IdentityToRoleCache.class);
        verifyFullCacheInvalidation(IdentityToRoleCache.NAME,
                                    identityToRoleCache::getAll,
                                    testUserKeystorePath,
                                    TEST_USER_IDENTITY,
                                    1,
                                    TEST_SUPERUSER_IDENTITY,
                                    testContext);
    }

    @Test
    void testInvalidateIdentityToRoleCacheWithKeys(VertxTestContext testContext)
    {
        IdentityToRoleCache identityToRoleCache = serverWrapper.injector.getInstance(IdentityToRoleCache.class);
        verifySelectiveKeyInvalidation(IdentityToRoleCache.NAME,
                                       identityToRoleCache::getAll,
                                       List.of(testUserKeystorePath, testUser2KeystorePath),
                                       List.of(TEST_USER_IDENTITY),
                                       List.of(TEST_USER_IDENTITY),
                                       TEST_USER2_IDENTITY,
                                       testUserKeystorePath,
                                       TEST_USER_IDENTITY,
                                       testContext);
    }

    @Test
    void testInvalidateIdentityToRoleCacheWithMultipleKeys(VertxTestContext testContext)
    {
        IdentityToRoleCache identityToRoleCache = serverWrapper.injector.getInstance(IdentityToRoleCache.class);
        verifySelectiveKeyInvalidation(IdentityToRoleCache.NAME,
                                       identityToRoleCache::getAll,
                                       List.of(testUserKeystorePath, testUser2KeystorePath, superuserKeystorePath),
                                       List.of(TEST_USER_IDENTITY, TEST_USER2_IDENTITY),
                                       List.of(TEST_USER_IDENTITY, TEST_USER2_IDENTITY),
                                       TEST_SUPERUSER_IDENTITY,
                                       testUserKeystorePath,
                                       TEST_USER_IDENTITY,
                                       testContext);
    }

    @Test
    void testInvalidateRoleAuthorizationsCache(VertxTestContext testContext)
    {
        RoleAuthorizationsCache roleAuthorizationsCache = serverWrapper.injector.getInstance(RoleAuthorizationsCache.class);

        // Clear the cache first to ensure clean state
        String clearCacheRoute = String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE, RoleAuthorizationsCache.NAME);
        verifyAccess(HttpMethod.DELETE, clearCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));
        String endpointCacheRoute = String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE, CacheFactory.ENDPOINT_AUTHORIZATION_CACHE_NAME);
        verifyAccess(HttpMethod.DELETE, endpointCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));

        verifyAccess(HttpMethod.GET, SCHEMA_ROUTE, testUserKeystorePath, assertStatus(HttpResponseStatus.OK));

        // Verify cache is populated
        pollUntil(() -> roleAuthorizationsCache.get("unique_cache_entry_key")
                                               .map(entry -> entry != null && entry.get("test_role") != null), 3000)
        .compose(v -> {
            // Invalidate cache and verify its empty
            String invalidateCacheRoute = String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE, RoleAuthorizationsCache.NAME);
            verifyAccess(HttpMethod.DELETE, invalidateCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));
            return pollUntil(() -> roleAuthorizationsCache.getAll().map(java.util.Map::isEmpty), 3000);
        })
        .compose(v -> {
            // Re-populate cache with test user and verify test user is back in cache
            verifyAccess(HttpMethod.GET, SCHEMA_ROUTE, testUserKeystorePath, assertStatus(HttpResponseStatus.OK));
            return pollUntil(() -> roleAuthorizationsCache.get("unique_cache_entry_key")
                                                          .map(entry -> entry != null && entry.get("test_role") != null), 3000);
        })
        .onSuccess(v -> testContext.completeNow())
        .onFailure(testContext::failNow);
    }

    @Test
    void testInvalidateRoleAuthorizationsCacheWithKeys(VertxTestContext testContext)
    {
        RoleAuthorizationsCache roleAuthorizationsCache = serverWrapper.injector.getInstance(RoleAuthorizationsCache.class);
        verifyKeyBasedInvalidationNotSupported(RoleAuthorizationsCache.NAME,
                                               roleAuthorizationsCache::getAll,
                                               testUserKeystorePath,
                                               testContext);
    }

    @Test
    void testInvalidateSuperUserCache(VertxTestContext testContext)
    {
        SuperUserCache superUserCache = serverWrapper.injector.getInstance(SuperUserCache.class);
        verifyFullCacheInvalidation(SuperUserCache.NAME,
                                    superUserCache::getAll,
                                    superuser2KeystorePath,
                                    "test_superuser2_role",
                                    1,
                                    "test_superuser_role",
                                    testContext);
    }

    @Test
    void testInvalidateSuperUserCacheWithKeys(VertxTestContext testContext)
    {
        SuperUserCache superUserCache = serverWrapper.injector.getInstance(SuperUserCache.class);
        verifySelectiveKeyInvalidation(SuperUserCache.NAME,
                                       superUserCache::getAll,
                                       List.of(superuser2KeystorePath, superuser3KeystorePath),
                                       List.of("test_superuser2_role"),
                                       List.of("test_superuser2_role"),
                                       "test_superuser3_role",
                                       superuser2KeystorePath,
                                       "test_superuser2_role",
                                       testContext);
    }

    @Test
    void testInvalidateSuperUserCacheWithMultipleKeys(VertxTestContext testContext)
    {
        SuperUserCache superUserCache = serverWrapper.injector.getInstance(SuperUserCache.class);
        verifySelectiveKeyInvalidation(SuperUserCache.NAME,
                                       superUserCache::getAll,
                                       List.of(superuserKeystorePath, superuser2KeystorePath, superuser3KeystorePath),
                                       List.of("test_superuser2_role", "test_superuser3_role"),
                                       List.of("test_superuser2_role", "test_superuser3_role"),
                                       "test_superuser_role",
                                       superuser2KeystorePath,
                                       "test_superuser2_role",
                                       testContext);
    }

    @Test
    void testInvalidateEndpointAuthorizationCache(VertxTestContext testContext)
    {
        CacheFactory cacheFactory = serverWrapper.injector.getInstance(CacheFactory.class);
        Cache<AuthorizationCacheKey, Future<Boolean>> endpointAuthorizationCache = cacheFactory.endpointAuthorizationCache();

        // Clear the cache first to ensure clean state
        String clearCacheRoute = String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE, CacheFactory.ENDPOINT_AUTHORIZATION_CACHE_NAME);
        verifyAccess(HttpMethod.DELETE, clearCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));

        pollUntil(() -> Future.succeededFuture(endpointAuthorizationCache.asMap().isEmpty()), 3000)
        .compose(v -> {
            verifyAccess(HttpMethod.GET, SCHEMA_ROUTE, testUserKeystorePath, assertStatus(HttpResponseStatus.OK));
            return pollUntil(() -> Future.succeededFuture(!endpointAuthorizationCache.asMap().isEmpty()), 3000);
        })
        .compose(v -> {
            // Invalidate cache and verify
            String invalidateCacheRoute = String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE, CacheFactory.ENDPOINT_AUTHORIZATION_CACHE_NAME);
            verifyAccess(HttpMethod.DELETE, invalidateCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));
            return pollUntil(() -> Future.succeededFuture(endpointAuthorizationCache.asMap().isEmpty()), 3000);
        })
        .compose(v -> {
            // Re-populate cache with test user and verify test user is back in cache
            verifyAccess(HttpMethod.GET, SCHEMA_ROUTE, testUserKeystorePath, assertStatus(HttpResponseStatus.OK));
            return pollUntil(() -> Future.succeededFuture(!endpointAuthorizationCache.asMap().isEmpty()), 3000);
        })
        .onSuccess(v -> testContext.completeNow())
        .onFailure(testContext::failNow);
    }

    @Test
    void testInvalidateEndpointAuthorizationCacheWithKeys(VertxTestContext testContext)
    {
        CacheFactory cacheFactory = serverWrapper.injector.getInstance(CacheFactory.class);
        Cache<AuthorizationCacheKey, Future<Boolean>> endpointAuthorizationCache = cacheFactory.endpointAuthorizationCache();

        verifyKeyBasedInvalidationNotSupported(CacheFactory.ENDPOINT_AUTHORIZATION_CACHE_NAME,
                                              () -> Future.succeededFuture(endpointAuthorizationCache.asMap()),
                                              testUserKeystorePath,
                                              testContext);
    }

    @Test
    void testInvalidateUnknownCache()
    {
        String invalidateCacheRoute = "/api/v1/caches/unknown_cache/invalidate";
        verifyAccess(HttpMethod.DELETE, invalidateCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.NOT_FOUND));
    }

    @Test
    void testInvalidateCacheWithoutPermission()
    {
        // test_user does not have CACHE:INVALIDATE permission, should get 403
        String invalidateCacheRoute = String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE, IdentityToRoleCache.NAME);
        verifyAccess(HttpMethod.DELETE, invalidateCacheRoute, testUserKeystorePath, assertStatus(HttpResponseStatus.FORBIDDEN));

        // Verify with other cache types as well
        String roleAuthCacheRoute = String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE, RoleAuthorizationsCache.NAME);
        verifyAccess(HttpMethod.DELETE, roleAuthCacheRoute, testUserKeystorePath, assertStatus(HttpResponseStatus.FORBIDDEN));

        String superUserCacheRoute = String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE, SuperUserCache.NAME);
        verifyAccess(HttpMethod.DELETE, superUserCacheRoute, testUserKeystorePath, assertStatus(HttpResponseStatus.FORBIDDEN));

        String endpointAuthCacheRoute = String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE, CacheFactory.ENDPOINT_AUTHORIZATION_CACHE_NAME);
        verifyAccess(HttpMethod.DELETE, endpointAuthCacheRoute, testUserKeystorePath, assertStatus(HttpResponseStatus.FORBIDDEN));
    }

    private void createRolesPermissionsTable(Session session)
    {
        String statement = "CREATE TABLE IF NOT EXISTS sidecar_internal.role_permissions_v1 ("
                          + "role text,"
                          + "resource text,"
                          + "permissions set<text>,"
                          + "PRIMARY KEY(role, resource))";
        session.execute(statement);
    }

    private void createTestRole(Session session)
    {
        session.execute("CREATE ROLE IF NOT EXISTS \"test_role\" WITH SUPERUSER = false AND LOGIN = true");
        session.execute(String.format("ADD IDENTITY IF NOT EXISTS '%s' TO ROLE 'test_role'", TEST_USER_IDENTITY));

        session.execute("CREATE ROLE IF NOT EXISTS \"test_role2\" WITH SUPERUSER = false AND LOGIN = true");
        session.execute(String.format("ADD IDENTITY IF NOT EXISTS '%s' TO ROLE 'test_role2'", TEST_USER2_IDENTITY));

        // Create a test superuser role for testing SuperUserCache
        session.execute("CREATE ROLE IF NOT EXISTS \"test_superuser_role\" WITH SUPERUSER = true AND LOGIN = true");
        session.execute(String.format("ADD IDENTITY IF NOT EXISTS '%s' TO ROLE 'test_superuser_role'", TEST_SUPERUSER_IDENTITY));

        session.execute("CREATE ROLE IF NOT EXISTS \"test_superuser2_role\" WITH SUPERUSER = true AND LOGIN = true");
        session.execute(String.format("ADD IDENTITY IF NOT EXISTS '%s' TO ROLE 'test_superuser2_role'", TEST_SUPERUSER2_IDENTITY));

        session.execute("CREATE ROLE IF NOT EXISTS \"test_superuser3_role\" WITH SUPERUSER = true AND LOGIN = true");
        session.execute(String.format("ADD IDENTITY IF NOT EXISTS '%s' TO ROLE 'test_superuser3_role'", TEST_SUPERUSER3_IDENTITY));

        // Insert permissions into role_permissions_v1 to populate RoleAuthorizationsCache
        session.execute("INSERT INTO sidecar_internal.role_permissions_v1 (role, resource, permissions) " +
                       "VALUES ('test_role', 'cluster', {'SCHEMA:READ'})");
        session.execute("INSERT INTO sidecar_internal.role_permissions_v1 (role, resource, permissions) " +
                       "VALUES ('test_role', 'data', {'SNAPSHOT:CREATE'})");
        session.execute("INSERT INTO sidecar_internal.role_permissions_v1 (role, resource, permissions) " +
                       "VALUES ('test_role2', 'cluster', {'SCHEMA:READ'})");
        session.execute("INSERT INTO sidecar_internal.role_permissions_v1 (role, resource, permissions) " +
                       "VALUES ('test_role2', 'data', {'SNAPSHOT:CREATE'})");
    }

    private void configureAdminAndSidecarIdentity(IInstance instance)
    {
        for (int i = 0; i < 60; i++)
        {
            try
            {
                withAuthenticatedSession(instance, "cassandra", "cassandra", session -> {
                    session.execute("CREATE ROLE IF NOT EXISTS \"sidecar_role\" " +
                                    "WITH SUPERUSER = true " +
                                    "AND LOGIN = true");
                    session.execute(String.format("ADD IDENTITY IF NOT EXISTS '%s' TO ROLE 'cassandra'", CASSANDRA_IDENTITY));
                    session.execute(String.format("ADD IDENTITY IF NOT EXISTS '%s' TO ROLE 'sidecar_role'", SIDECAR_ROLE_IDENTITY));
                }, null);
                return;
            }
            catch (Exception e)
            {
                try
                {
                    TimeUnit.SECONDS.sleep(1);
                }
                catch (InterruptedException ie)
                {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }
    }

    private Path cassandraIdentityClientKeyStore()
    {
        try
        {
            return mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                     certificateBuilder.addSanUriName(CASSANDRA_IDENTITY));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private void verifyAccess(HttpMethod method, String testRoute, Path clientKeystorePath, Verifier<HttpResponse<Buffer>> assertions)
    {
        WebClient client = trustedClient(clientKeystorePath.toString(),
                                         mtlsTestHelper.clientKeyStorePassword(),
                                         mtlsTestHelper.trustStorePath(),
                                         mtlsTestHelper.trustStorePassword());
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

    /**
     * Verifies that attempting to invalidate a cache with keys returns BAD_REQUEST and leaves cache unchanged.
     * This is for caches that don't support key-based invalidation.
     *
     * @param cacheName the cache name
     * @param cacheSupplier supplier that returns a future of the cache map
     * @param populateKeystore keystore to use for populating the cache before the test
     * @param testContext the test context
     */
    private void verifyKeyBasedInvalidationNotSupported(String cacheName,
                                                       java.util.function.Supplier<? extends Future<? extends java.util.Map<?, ?>>> cacheSupplier,
                                                       Path populateKeystore,
                                                       VertxTestContext testContext)
    {
        // Clear cache and populate
        String clearCacheRoute = String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE, cacheName);
        verifyAccess(HttpMethod.DELETE, clearCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));
        String endpointCacheRoute = String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE, CacheFactory.ENDPOINT_AUTHORIZATION_CACHE_NAME);
        verifyAccess(HttpMethod.DELETE, endpointCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));
        verifyAccess(HttpMethod.GET, SCHEMA_ROUTE, populateKeystore, assertStatus(HttpResponseStatus.OK));

        pollUntilValue(() -> cacheSupplier.get(), cache -> !cache.isEmpty(), 3000)
        .compose(entriesBeforeAttempt -> {
            // Attempt to invalidate with specific keys - should return BAD_REQUEST
            String invalidateCacheRoute = String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE + "?keys=someKey", cacheName);
            verifyAccess(HttpMethod.DELETE, invalidateCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.BAD_REQUEST));

            // Verify cache contents are unchanged
            return cacheSupplier.get().map(entriesAfterAttempt -> {
                entriesBeforeAttempt.forEach((key, value) -> {
                    assertThat(entriesAfterAttempt.containsKey(key)).isTrue();
                    assertThat(entriesAfterAttempt.get(key)).isEqualTo(value);
                });
                return null;
            });
        })
        .onSuccess(v -> testContext.completeNow())
        .onFailure(testContext::failNow);
    }

    /**
     * Complete test flow for selective cache invalidation with specific keys.
     * Clears cache, populates with multiple entries, invalidates specific keys, verifies selective removal, and re-populates.
     *
     * @param cacheName the cache name to invalidate
     * @param cacheSupplier supplier that returns a future of the cache map
     * @param populateKeystores keystores to use for populating the cache
     * @param keysToInvalidate the keys to pass to the invalidation API (one or more)
     * @param verifyRemovedKeys the keys that should be removed from cache
     * @param verifyRemainingKey at least one key that should remain in cache
     * @param repopulateKeystore keystore to use for re-populating after invalidation
     * @param expectedRepopulatedKey the key expected to be back in cache after re-population
     * @param testContext the test context
     */
    private void verifySelectiveKeyInvalidation(String cacheName,
                                               java.util.function.Supplier<? extends Future<? extends java.util.Map<String, ?>>> cacheSupplier,
                                               List<Path> populateKeystores,
                                               List<String> keysToInvalidate,
                                               List<String> verifyRemovedKeys,
                                               String verifyRemainingKey,
                                               Path repopulateKeystore,
                                               String expectedRepopulatedKey,
                                               VertxTestContext testContext)
    {
        // Clear cache and populate
        String clearCacheRoute = String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE, cacheName);
        verifyAccess(HttpMethod.DELETE, clearCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));
        String endpointCacheRoute = String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE, CacheFactory.ENDPOINT_AUTHORIZATION_CACHE_NAME);
        verifyAccess(HttpMethod.DELETE, endpointCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));
        for (Path keystore : populateKeystores)
        {
            verifyAccess(HttpMethod.GET, SCHEMA_ROUTE, keystore, assertStatus(HttpResponseStatus.OK));
        }

        // Verify cache has multiple entries and get initial size
        pollUntilValue(() -> cacheSupplier.get(), cache -> cache.size() >= populateKeystores.size(), 3000)
        .compose(cache -> {
            // Verify removed keys are present before invalidation
            Future<Void> checksFuture = Future.succeededFuture();
            for (String removedKey : verifyRemovedKeys)
            {
                checksFuture = checksFuture.compose(unused ->
                    pollUntil(() -> cacheSupplier.get().map(c -> c.containsKey(removedKey)), 3000));
            }
            return checksFuture.map(cache);
        })
        .compose(cache -> {
            int initialSize = cache.size();

            // Build invalidation route with multiple keys
            StringBuilder routeBuilder = new StringBuilder(String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE, cacheName));
            for (int i = 0; i < keysToInvalidate.size(); i++)
            {
                routeBuilder.append(i == 0 ? "?keys=" : "&keys=").append(encode(keysToInvalidate.get(i), StandardCharsets.UTF_8));
            }
            String invalidateCacheRoute = routeBuilder.toString();

            verifyAccess(HttpMethod.DELETE, invalidateCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));

            // Verify selective invalidation
            return pollUntil(() -> cacheSupplier.get().map(c -> {
                if (c.isEmpty()) return false;
                if (c.size() != initialSize - keysToInvalidate.size()) return false;
                for (String removedKey : verifyRemovedKeys)
                {
                    if (c.containsKey(removedKey)) return false;
                }
                return c.containsKey(verifyRemainingKey);
            }), 3000);
        })
        .compose(v -> {
            // Re-populate and verify
            verifyAccess(HttpMethod.GET, SCHEMA_ROUTE, repopulateKeystore, assertStatus(HttpResponseStatus.OK));
            return pollUntil(() -> cacheSupplier.get().map(cache -> cache.containsKey(expectedRepopulatedKey)), 3000);
        })
        .onSuccess(v -> testContext.completeNow())
        .onFailure(testContext::failNow);
    }

    /**
     * Complete test flow for full cache invalidation (without keys).
     * Clears cache, populates it, verifies population, invalidates it, verifies expected state, and re-populates.
     *
     * @param cacheName the cache name to invalidate
     * @param cacheSupplier supplier that returns a future of the cache map
     * @param populateKeystore keystore to use for populating the cache
     * @param verifyKey key to verify exists in cache after population
     * @param expectedSizeAfterInvalidation expected cache size after invalidation (0 for empty, or typically 1 for superuser entry)
     * @param verifyRemainingKey key expected to remain after invalidation (can be null if expectedSizeAfterInvalidation is 0)
     * @param testContext the test context
     */
    private void verifyFullCacheInvalidation(String cacheName,
                                            java.util.function.Supplier<? extends Future<? extends java.util.Map<String, ?>>> cacheSupplier,
                                            Path populateKeystore,
                                            String verifyKey,
                                            int expectedSizeAfterInvalidation,
                                            String verifyRemainingKey,
                                            VertxTestContext testContext)
    {
        // Clear cache and populate
        String clearCacheRoute = String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE, cacheName);
        verifyAccess(HttpMethod.DELETE, clearCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));
        String endpointCacheRoute = String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE, CacheFactory.ENDPOINT_AUTHORIZATION_CACHE_NAME);
        verifyAccess(HttpMethod.DELETE, endpointCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));
        verifyAccess(HttpMethod.GET, SCHEMA_ROUTE, populateKeystore, assertStatus(HttpResponseStatus.OK));

        // Verify cache has the expected entry
        pollUntil(() -> cacheSupplier.get().map(cache -> cache.containsKey(verifyKey)), 3000)
        .compose(v -> {
            // Invalidate cache
            String invalidateCacheRoute = String.format(CACHE_INVALIDATE_ROUTE_TEMPLATE, cacheName);
            verifyAccess(HttpMethod.DELETE, invalidateCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));
            verifyAccess(HttpMethod.DELETE, endpointCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));

            // Verify expected state after invalidation
            if (expectedSizeAfterInvalidation == 0)
            {
                // Cache should be completely empty
                return pollUntil(() -> cacheSupplier.get().map(java.util.Map::isEmpty), 3000);
            }
            else
            {
                // Cache should have remaining entries
                return pollUntil(() -> cacheSupplier.get().map(cache ->
                    cache.size() == expectedSizeAfterInvalidation &&
                    !cache.containsKey(verifyKey) &&
                    cache.containsKey(verifyRemainingKey)
                ), 3000);
            }
        })
        .compose(v -> {
            // Re-populate and verify
            verifyAccess(HttpMethod.GET, SCHEMA_ROUTE, populateKeystore, assertStatus(HttpResponseStatus.OK));
            return pollUntil(() -> cacheSupplier.get().map(cache -> cache.containsKey(verifyKey)), 3000);
        })
        .onSuccess(v -> testContext.completeNow())
        .onFailure(testContext::failNow);
    }

    /**
     * Poll until a condition is met or timeout
     */
    private Future<Void> pollUntil(java.util.function.Supplier<Future<Boolean>> condition, long timeoutMs)
    {
        Promise<Void> promise = Promise.promise();
        long startTime = System.currentTimeMillis();
        Vertx vertx = serverWrapper.injector.getInstance(Vertx.class);

        vertx.setPeriodic(100, timerId -> {
            condition.get().onComplete(ar -> {
                if (ar.succeeded() && ar.result())
                {
                    vertx.cancelTimer(timerId);
                    promise.complete();
                }
                else if (System.currentTimeMillis() - startTime > timeoutMs)
                {
                    vertx.cancelTimer(timerId);
                    promise.fail("Timeout waiting for condition");
                }
            });
        });

        return promise.future();
    }

    /**
     * Poll until a future result meets a condition, and return the result
     */
    private <T> Future<T> pollUntilValue(java.util.function.Supplier<Future<T>> futureSupplier,
                                         java.util.function.Predicate<T> condition,
                                         long timeoutMs)
    {
        Promise<T> promise = Promise.promise();
        long startTime = System.currentTimeMillis();
        Vertx vertx = serverWrapper.injector.getInstance(Vertx.class);

        vertx.setPeriodic(100, timerId -> {
            futureSupplier.get().onComplete(ar -> {
                if (ar.succeeded() && condition.test(ar.result()))
                {
                    vertx.cancelTimer(timerId);
                    promise.complete(ar.result());
                }
                else if (System.currentTimeMillis() - startTime > timeoutMs)
                {
                    vertx.cancelTimer(timerId);
                    promise.fail("Timeout waiting for condition");
                }
            });
        });

        return promise.future();
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
            return new TemporaryCqlSessionProvider(buildContactPoints(cluster),
                                                   org.apache.cassandra.sidecar.testing.SharedExecutorNettyOptions.INSTANCE,
                                                   sslOptions);
        }
    }
}
