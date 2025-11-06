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

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.github.benmanes.caffeine.cache.AsyncCache;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
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

import static org.apache.cassandra.testing.DriverTestUtils.buildContactPoints;
import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TlsTestUtils.getSSLOptions;
import static org.apache.cassandra.testing.TlsTestUtils.withAuthenticatedSession;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration test for cache invalidation endpoints
 */
class InvalidateCacheIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    protected static final int MIN_VERSION_WITH_MTLS = 5;
    private static final String CASSANDRA_IDENTITY = "spiffe://cassandra/sidecar/cassandra_role";
    private static final String SIDECAR_ROLE_IDENTITY = "spiffe://cassandra/sidecar/sidecar_role";
    private static final String TEST_USER_IDENTITY = "spiffe://cassandra/sidecar/test_user";
    private static final String TEST_SUPERUSER_IDENTITY = "spiffe://cassandra/sidecar/test_superuser";
    private static final String SCHEMA_ROUTE = "/api/v1/cassandra/schema";

    private Path testUserKeystorePath;
    private Path superuserKeystorePath;

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
            superuserKeystorePath = mtlsTestHelper.issueClientKeyStore(certificateBuilder ->
                                                                       certificateBuilder.addSanUriName(TEST_SUPERUSER_IDENTITY));
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
    void testInvalidateIdentityToRoleCache()
    {
        IdentityToRoleCache identityToRoleCache = serverWrapper.injector.getInstance(IdentityToRoleCache.class);

        // Populate the cache by making a request with test user identity
        verifyAccess(HttpMethod.GET, SCHEMA_ROUTE, testUserKeystorePath, assertStatus(HttpResponseStatus.OK));

        // Verify cache has entries using loopAssert as cache operation is asynchronous
        loopAssert(3, () ->
            assertThat(identityToRoleCache.getAll()).isNotEmpty());

        // Invalidate the cache using superuser
        String invalidateCacheRoute = "/api/v1/caches/identity_to_role_cache/invalidate";
        verifyAccess(HttpMethod.DELETE, invalidateCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));

        // Verify cache is empty after invalidation using loopAssert as cache operation is asynchronous
        loopAssert(3, () ->
            assertThat(identityToRoleCache.getAll()).isEmpty());
    }

    @Test
    void testInvalidateRoleAuthorizationsCache()
    {
        RoleAuthorizationsCache roleAuthorizationsCache = serverWrapper.injector.getInstance(RoleAuthorizationsCache.class);

        // Populate the cache by making a request with test user identity
        verifyAccess(HttpMethod.GET, SCHEMA_ROUTE, testUserKeystorePath, assertStatus(HttpResponseStatus.OK));

        // Verify cache has entries using loopAssert as cache operation is asynchronous
        // RoleAuthorizationsCache stores all permissions under a single key
        loopAssert(3, () ->
            assertThat(roleAuthorizationsCache.getAll()).isNotEmpty());

        // Invalidate the cache using superuser
        String invalidateCacheRoute = "/api/v1/caches/RoleAuthorizationsCache/invalidate";
        verifyAccess(HttpMethod.DELETE, invalidateCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));

        // Verify cache is empty after invalidation using loopAssert as cache operation is asynchronous
        loopAssert(3, () ->
            assertThat(roleAuthorizationsCache.getAll()).isEmpty());
    }

    @Test
    void testInvalidateSuperUserCache()
    {
        SuperUserCache superUserCache = serverWrapper.injector.getInstance(SuperUserCache.class);

        // Populate the cache by making a request with the superuser identity
        verifyAccess(HttpMethod.GET, SCHEMA_ROUTE, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));

        // Verify cache has the entry using loopAssert as cache operation is asynchronous
        loopAssert(3, () ->
            assertThat(superUserCache.getAll()).isNotEmpty());

        // Invalidate the cache
        String invalidateCacheRoute = "/api/v1/caches/super_user_cache/invalidate";
        verifyAccess(HttpMethod.DELETE, invalidateCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));

        // Verify cache is empty after invalidation using loopAssert as cache operation is asynchronous
        loopAssert(3, () ->
            assertThat(superUserCache.getAll()).isEmpty());
    }

    @Test
    void testInvalidateEndpointAuthorizationCache()
    {
        CacheFactory cacheFactory = serverWrapper.injector.getInstance(CacheFactory.class);
        AsyncCache<AuthorizationCacheKey, Boolean> endpointAuthorizationCache = cacheFactory.endpointAuthorizationCache();

        // Populate the cache by making a request with test user identity
        verifyAccess(HttpMethod.GET, SCHEMA_ROUTE, testUserKeystorePath, assertStatus(HttpResponseStatus.OK));

        // Verify cache has entries using loopAssert as cache operation is asynchronous
        loopAssert(3, () ->
            assertThat(endpointAuthorizationCache.synchronous().asMap()).isNotEmpty());

        // Invalidate the cache using superuser
        String invalidateCacheRoute = "/api/v1/caches/endpoint_authorization_cache/invalidate";
        verifyAccess(HttpMethod.DELETE, invalidateCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));

        // Verify cache is empty after invalidation using loopAssert as cache operation is asynchronous
        loopAssert(3, () ->
            assertThat(endpointAuthorizationCache.synchronous().asMap()).isEmpty());
    }

    @Test
    void testInvalidateUnknownCache()
    {
        String invalidateCacheRoute = "/api/v1/caches/unknown_cache/invalidate";
        verifyAccess(HttpMethod.DELETE, invalidateCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.NOT_FOUND));
    }

    @Test
    void testInvalidateCacheCaseInsensitive()
    {
        IdentityToRoleCache identityToRoleCache = serverWrapper.injector.getInstance(IdentityToRoleCache.class);

        // Populate the cache with test user identity
        verifyAccess(HttpMethod.GET, SCHEMA_ROUTE, testUserKeystorePath, assertStatus(HttpResponseStatus.OK));

        // Verify cache has entries using loopAssert as cache operation is asynchronous
        loopAssert(3, () ->
            assertThat(identityToRoleCache.getAll()).isNotEmpty());

        // Test case-insensitive invalidation using superuser
        String invalidateCacheRoute = "/api/v1/caches/IDENTITY_TO_ROLE_CACHE/invalidate";
        verifyAccess(HttpMethod.DELETE, invalidateCacheRoute, superuserKeystorePath, assertStatus(HttpResponseStatus.OK));

        // Verify cache is empty after invalidation using loopAssert as cache operation is asynchronous
        loopAssert(3, () ->
            assertThat(identityToRoleCache.getAll()).isEmpty());
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

        // Create a test superuser role for testing SuperUserCache
        session.execute("CREATE ROLE IF NOT EXISTS \"test_superuser_role\" WITH SUPERUSER = true AND LOGIN = true");
        session.execute(String.format("ADD IDENTITY IF NOT EXISTS '%s' TO ROLE 'test_superuser_role'", TEST_SUPERUSER_IDENTITY));

        // Insert permissions into role_permissions_v1 to populate RoleAuthorizationsCache
        session.execute("INSERT INTO sidecar_internal.role_permissions_v1 (role, resource, permissions) " +
                       "VALUES ('test_role', 'cluster', {'SCHEMA:READ'})");
        session.execute("INSERT INTO sidecar_internal.role_permissions_v1 (role, resource, permissions) " +
                       "VALUES ('test_role', 'data', {'SNAPSHOT:CREATE'})");
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
