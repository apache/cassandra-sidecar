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

package org.apache.cassandra.sidecar.auth;

import java.io.File;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.auth.authorization.AndAuthorization;
import io.vertx.ext.auth.authorization.OrAuthorization;
import io.vertx.ext.auth.authorization.RoleBasedAuthorization;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.TestSslModule;
import org.apache.cassandra.sidecar.auth.authentication.AuthenticatorConfig;
import org.apache.cassandra.sidecar.auth.authentication.CertificateValidatorConfig;
import org.apache.cassandra.sidecar.auth.authentication.IdentityValidatorConfig;
import org.apache.cassandra.sidecar.auth.authorization.AuthorizerConfig;
import org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions;
import org.apache.cassandra.sidecar.auth.authorization.PermissionsAccessor;
import org.apache.cassandra.sidecar.auth.authorization.RequiredPermissionsProvider;
import org.apache.cassandra.sidecar.auth.authorization.SystemAuthDatabaseAccessor;
import org.apache.cassandra.sidecar.config.AuthenticatorConfiguration;
import org.apache.cassandra.sidecar.config.AuthorizerConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.yaml.AuthenticatorConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.AuthorizerConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.KeyStoreConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SslConfigurationImpl;
import org.apache.cassandra.sidecar.routes.MutualTlsAuthorizationHandler;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.utils.CacheFactory;
import org.apache.cassandra.sidecar.utils.CertificateBuilder;
import org.apache.cassandra.sidecar.utils.CertificateBundle;
import org.apache.cassandra.sidecar.utils.SSTableImporter;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests MutualTLS Authorization for {@link MutualTlsAuthorizationHandler}
 */
@ExtendWith(VertxExtension.class)
public class MutualTlsAuthorizationTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MutualTlsAuthorizationTest.class);

    @TempDir
    File tempDir;
    TestModule testModule;
    RequiredPermissionsProvider mockRequiredPermissionsProvider;
    private Server server;
    private Vertx vertx;
    private CertificateBundle ca;
    private CertificateBundle badCA;
    private Path truststorePath;
    private Path untrustedTruststorePath;

    TestModule testModule() throws Exception
    {
        ca = new CertificateBuilder().subject("CN=Apache cassandra Root CA, OU=Certification Authority, O=Unknown, C=Unknown")
                                     .alias("fakerootca")
                                     .isCertificateAuthority(true)
                                     .buildSelfSigned();

        badCA = new CertificateBuilder().subject("CN=Untrusted CA, OU=Certification Authority, O=Unknown, C=Unknown")
                                        .alias("fakerootca_bad")
                                        .isCertificateAuthority(true)
                                        .buildSelfSigned();

        truststorePath = ca.toTempKeyStorePath(tempDir.toPath(),
                                               "password".toCharArray(),
                                               "password".toCharArray());

        untrustedTruststorePath = badCA.toTempKeyStorePath(tempDir.toPath(),
                                                           "password".toCharArray(),
                                                           "password".toCharArray());


        CertificateBundle keystore =
        new CertificateBuilder().subject("CN=Apache Cassandra, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                .addSanDnsName(InetAddress.getLocalHost().getCanonicalHostName())
                                .addSanDnsName(InetAddress.getLocalHost().getHostName())
                                .addSanDnsName("localhost")
                                .buildIssuedBy(ca);

        Path serverKeystorePath = keystore.toTempKeyStorePath(tempDir.toPath(),
                                                              "password".toCharArray(),
                                                              "password".toCharArray());

        mockRequiredPermissionsProvider = mock(RequiredPermissionsProvider.class);

        return new TestMTLSModule(serverKeystorePath, truststorePath, mockRequiredPermissionsProvider);
    }

    @BeforeEach
    void setUp() throws Exception
    {
        testModule = testModule();

        Injector injector = Guice.createInjector(Modules.override(new MainModule())
                                                        .with(testModule));
        server = injector.getInstance(Server.class);
        vertx = injector.getInstance(Vertx.class);

        VertxTestContext context = new VertxTestContext();
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);

        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() throws InterruptedException
    {
        final CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    @Test
    void testSidecarHealthCheckReturnsOK(VertxTestContext testContext) throws Exception
    {
        AndAuthorization auth1 = AndAuthorization.create();
        OrAuthorization auth1Or = OrAuthorization.create();
        auth1Or.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.DROP.name())
                                                       .setResource("<ALL KEYSPACES>"));
        auth1Or.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.DROP.name())
                                                       .setResource("<keyspace system_auth>"));
        auth1.addAuthorization(auth1Or);

        when(mockRequiredPermissionsProvider.requiredPermissions("GET /api/v1/__health", new HashMap<>()))
        .thenReturn(auth1);

        Path clientKeystorePath = generateClientCertificate(null, ca, "spiffe://identity1");

        String url = "/api/v1/__health";

        WebClient client = client(clientKeystorePath, truststorePath);

        client.get(server.actualPort(), "localhost", url)
              .as(BodyCodec.string())
              .ssl(true)
              .send(testContext.succeeding(response -> testContext.verify(() ->
                                          {
                                              assertThat(response.statusCode()).isEqualTo(OK.code());
                                              assertThat(response.body()).isEqualTo("{\"status\":\"OK\"}");
                                              testContext.completeNow();
                                          })));
    }

    @Test
    void testWorksForAllPermissions(VertxTestContext testContext) throws Exception
    {
        AndAuthorization auth2 = AndAuthorization.create();
        OrAuthorization auth2Or = OrAuthorization.create();
        auth2Or.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.DROP.name())
                                                       .setResource("<ALL KEYSPACES>"));
        auth2Or.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.DROP.name())
                                                       .setResource("<keyspace system_auth>"));
        auth2.addAuthorization(auth2Or);

        when(mockRequiredPermissionsProvider.requiredPermissions("GET /api/v1/__health", new HashMap<>()))
        .thenReturn(auth2);

        Path clientKeystorePath = generateClientCertificate(null, ca, "spiffe://identity2");

        String url = "/api/v1/__health";

        WebClient client = client(clientKeystorePath, truststorePath);

        client.get(server.actualPort(), "localhost", url)
              .as(BodyCodec.string())
              .ssl(true)
              .send(testContext.succeeding(response -> testContext.verify(() ->
                                          {
                                              assertThat(response.statusCode()).isEqualTo(OK.code());
                                              assertThat(response.body()).isEqualTo("{\"status\":\"OK\"}");
                                              testContext.completeNow();
                                          })));
    }

    @Test
    void testIncorrectPermissions(VertxTestContext testContext) throws Exception
    {
        AndAuthorization auth3 = AndAuthorization.create();
        OrAuthorization auth3Or = OrAuthorization.create();
        auth3Or.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.CREATE.name())
                                                       .setResource("<ALL KEYSPACES>"));
        auth3Or.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.CREATE.name())
                                                       .setResource("<keyspace system_auth>"));
        auth3.addAuthorization(auth3Or);

        when(mockRequiredPermissionsProvider.requiredPermissions("GET /api/v1/__health", new HashMap<>()))
        .thenReturn(auth3);

        Path clientKeystorePath = generateClientCertificate(null, ca, "spiffe://identity3");

        String url = "/api/v1/__health";

        WebClient client = client(clientKeystorePath, truststorePath);

        client.get(server.actualPort(), "localhost", url)
              .as(BodyCodec.string())
              .ssl(true)
              .send(testContext.succeeding(response -> testContext.verify(() ->
                                          {
                                              assertThat(response.statusCode()).isEqualTo(UNAUTHORIZED.code());
                                              assertThat(response.body())
                                              .isEqualTo("{\"status\":\"Unauthorized\",\"code\":401,\"message\":\"Not Authorized\"}");
                                              testContext.completeNow();
                                          })));
    }

    @Test
    void testCorrectPermissionsWrongKeyspace(VertxTestContext testContext) throws Exception
    {
        AndAuthorization auth4 = AndAuthorization.create();
        OrAuthorization auth4Or1 = OrAuthorization.create();
        auth4Or1.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.CREATE.name())
                                                        .setResource("<ALL KEYSPACES>"));
        auth4Or1.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.CREATE.name())
                                                        .setResource("<keyspace system_auth>"));
        auth4.addAuthorization(auth4Or1);
        OrAuthorization auth4Or2 = OrAuthorization.create();
        auth4Or2.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.SELECT.name())
                                                        .setResource("<ALL KEYSPACES>"));
        auth4Or2.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.SELECT.name())
                                                        .setResource("<keyspace system_auth>"));
        auth4.addAuthorization(auth4Or2);
        when(mockRequiredPermissionsProvider.requiredPermissions("GET /api/v1/__health", new HashMap<>()))
        .thenReturn(auth4);

        Path clientKeystorePath = generateClientCertificate(null, ca, "spiffe://identity4");

        String url = "/api/v1/__health";

        WebClient client = client(clientKeystorePath, truststorePath);

        client.get(server.actualPort(), "localhost", url)
              .as(BodyCodec.string())
              .ssl(true)
              .send(testContext.succeeding(response -> testContext.verify(() ->
                                          {
                                              assertThat(response.statusCode()).isEqualTo(UNAUTHORIZED.code());
                                              assertThat(response.body())
                                              .isEqualTo("{\"status\":\"Unauthorized\",\"code\":401,\"message\":\"Not Authorized\"}");
                                              testContext.completeNow();
                                          })));
    }

    @Test
    void testWrongPermissionsCorrectKeyspace(VertxTestContext testContext) throws Exception
    {
        AndAuthorization auth5 = AndAuthorization.create();
        OrAuthorization auth5Or = OrAuthorization.create();
        auth5Or.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.CREATE.name())
                                                       .setResource("<ALL KEYSPACES>"));
        auth5Or.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.CREATE.name())
                                                       .setResource("<keyspace system_auth>"));
        auth5.addAuthorization(auth5Or);
        when(mockRequiredPermissionsProvider.requiredPermissions("GET /api/v1/__health", new HashMap<>()))
        .thenReturn(auth5);


        Path clientKeystorePath = generateClientCertificate(null, ca, "spiffe://identity5");

        String url = "/api/v1/__health";

        WebClient client = client(clientKeystorePath, truststorePath);

        client.get(server.actualPort(), "localhost", url)
              .as(BodyCodec.string())
              .ssl(true)
              .send(testContext.succeeding(response -> testContext.verify(() ->
                                          {
                                              assertThat(response.statusCode()).isEqualTo(UNAUTHORIZED.code());
                                              assertThat(response.body())
                                              .isEqualTo("{\"status\":\"Unauthorized\",\"code\":401,\"message\":\"Not Authorized\"}");
                                              testContext.completeNow();
                                          })));
    }

    @Test
    void testSuperUserStatus(VertxTestContext testContext) throws Exception
    {
        AndAuthorization auth6 = AndAuthorization.create();
        OrAuthorization auth6Or = OrAuthorization.create();
        auth6Or.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.CREATE.name())
                                                       .setResource("<ALL KEYSPACES>"));
        auth6Or.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.CREATE.name())
                                                       .setResource("<keyspace system_auth>"));
        auth6.addAuthorization(auth6Or);
        when(mockRequiredPermissionsProvider.requiredPermissions("GET /api/v1/__health", new HashMap<>()))
        .thenReturn(auth6);

        Path clientKeystorePath = generateClientCertificate(null, ca, "spiffe://adminID");

        String url = "/api/v1/__health";

        WebClient client = client(clientKeystorePath, truststorePath);

        client.get(server.actualPort(), "localhost", url)
              .as(BodyCodec.string())
              .ssl(true)
              .send(testContext.succeeding(response -> testContext.verify(() ->
                                          {
                                              assertThat(response.statusCode()).isEqualTo(OK.code());
                                              assertThat(response.body()).isEqualTo("{\"status\":\"OK\"}");
                                              testContext.completeNow();
                                          })));
    }

    @Test
    void testHasSomePermissions(VertxTestContext testContext) throws Exception
    {
        AndAuthorization auth7 = AndAuthorization.create();
        OrAuthorization auth7Or1 = OrAuthorization.create();
        auth7Or1.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.CREATE.name())
                                                        .setResource("<ALL KEYSPACES>"));
        auth7Or1.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.CREATE.name())
                                                        .setResource("<keyspace system_auth>"));
        auth7.addAuthorization(auth7Or1);
        OrAuthorization auth7Or2 = OrAuthorization.create();
        auth7Or2.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.SELECT.name())
                                                        .setResource("<ALL KEYSPACES>"));
        auth7Or2.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.SELECT.name())
                                                        .setResource("<keyspace system_auth>"));
        auth7.addAuthorization(auth7Or2);
        OrAuthorization auth7Or3 = OrAuthorization.create();
        auth7Or3.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.DROP.name())
                                                        .setResource("<ALL KEYSPACES>"));
        auth7Or3.addAuthorization(RoleBasedAuthorization.create(MutualTlsPermissions.DROP.name())
                                                        .setResource("<keyspace system_auth>"));
        auth7.addAuthorization(auth7Or3);
        when(mockRequiredPermissionsProvider.requiredPermissions("GET /api/v1/__health", new HashMap<>()))
        .thenReturn(auth7);

        Path clientKeystorePath = generateClientCertificate(null, ca, "spiffe://identity6");

        String url = "/api/v1/__health";

        WebClient client = client(clientKeystorePath, truststorePath);

        client.get(server.actualPort(), "localhost", url)
              .as(BodyCodec.string())
              .ssl(true)
              .send(testContext.succeeding(response -> testContext.verify(() ->
                                          {
                                              assertThat(response.statusCode()).isEqualTo(UNAUTHORIZED.code());
                                              assertThat(response.body())
                                              .isEqualTo("{\"status\":\"Unauthorized\",\"code\":401,\"message\":\"Not Authorized\"}");
                                              testContext.completeNow();
                                          })));
    }

    private WebClient client(Path clientKeystorePath, Path clientTruststorePath)
    {
        return WebClient.create(vertx, webClientOptions(clientKeystorePath, clientTruststorePath));
    }

    private WebClientOptions webClientOptions(Path clientKeystorePath, Path clientTruststorePath)
    {
        WebClientOptions options = new WebClientOptions();
        options.setKeyStoreOptions(new JksOptions().setPath(clientKeystorePath.toString())
                                                   .setPassword("cassandra"));
        options.setTrustStoreOptions(new JksOptions().setPath(clientTruststorePath.toString())
                                                     .setPassword("password"));
        options.setSsl(true);
        return options;
    }

    private Path generateClientCertificate(Function<CertificateBuilder, CertificateBuilder> customizeCertificate,
                                           CertificateBundle certificateAuthority,
                                           String identity) throws Exception
    {

        CertificateBuilder builder =
        new CertificateBuilder().subject("CN=Apache Cassandra, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                .notBefore(Instant.now().minus(1, ChronoUnit.DAYS))
                                .notAfter(Instant.now().plus(1, ChronoUnit.DAYS))
                                .alias("spiffecert")
                                .addSanUriName(identity)
                                .rsa2048Algorithm();
        if (customizeCertificate != null)
        {
            builder = customizeCertificate.apply(builder);
        }
        CertificateBundle ssc = builder.buildIssuedBy(certificateAuthority);

        return ssc.toTempKeyStorePath(tempDir.toPath(), "cassandra".toCharArray(), "cassandra".toCharArray());
    }

    @Test
    void testSidecarSpecificPermissions()
    {

    }

    @Singleton
    private static class MutualTLSAuthorizationTestCacheFactory extends CacheFactory
    {
        @Inject
        public MutualTLSAuthorizationTestCacheFactory(SidecarConfiguration sidecarConfiguration,
                                                      ServiceConfiguration configuration,
                                                      SSTableImporter ssTableImporter,
                                                      SystemAuthDatabaseAccessor systemAuthDatabaseAccessor)
        {
            super(sidecarConfiguration, configuration, ssTableImporter, systemAuthDatabaseAccessor);
        }

        @Override
        protected AsyncLoadingCache<String, Boolean> initRolesToSuperUserCache(CacheConfiguration cacheConfiguration, Ticker ticker)
        {
            assert systemAuthDatabaseAccessor != null;
            AsyncLoadingCache<String, Boolean> rolesToSuperUser =
            Caffeine.newBuilder()
                    .ticker(ticker)
                    .maximumSize(cacheConfiguration.maximumSize())
                    .expireAfterWrite(Duration.of(cacheConfiguration.expireAfterAccessMillis(), ChronoUnit.SECONDS))
                    .removalListener((key, value, cause) ->
                                     LOGGER.debug("Removed entry '{}' with key '{}' from {} with cause {}",
                                                  value, key, "roles", cause))
                    .buildAsync(systemAuthDatabaseAccessor::isSuperUser);

            CompletableFuture<Boolean> adminSuperUser = CompletableFuture.completedFuture(true);
            rolesToSuperUser.put("admin", adminSuperUser);
            CompletableFuture<Boolean> user1SuperUser = CompletableFuture.completedFuture(false);
            rolesToSuperUser.put("user1", user1SuperUser);
            CompletableFuture<Boolean> user2SuperUser = CompletableFuture.completedFuture(false);
            rolesToSuperUser.put("user2", user2SuperUser);
            CompletableFuture<Boolean> user3SuperUser = CompletableFuture.completedFuture(false);
            rolesToSuperUser.put("user3", user3SuperUser);
            CompletableFuture<Boolean> user4SuperUser = CompletableFuture.completedFuture(false);
            rolesToSuperUser.put("user4", user4SuperUser);
            CompletableFuture<Boolean> user5SuperUser = CompletableFuture.completedFuture(false);
            rolesToSuperUser.put("user5", user5SuperUser);
            CompletableFuture<Boolean> user6SuperUser = CompletableFuture.completedFuture(false);
            rolesToSuperUser.put("user6", user6SuperUser);

            return rolesToSuperUser;
        }

        @Override
        protected AsyncLoadingCache<String, String> initIdentityToRolesCache(CacheConfiguration cacheConfiguration, Ticker ticker)
        {
            LOGGER.info("Created cache here");
            assert systemAuthDatabaseAccessor != null;
            AsyncLoadingCache<String, String> identityToRoles =
            Caffeine.newBuilder()
                    .ticker(ticker)
                    .maximumSize(cacheConfiguration.maximumSize())
                    .expireAfterWrite(Duration.of(cacheConfiguration.expireAfterAccessMillis(), ChronoUnit.SECONDS))
                    .removalListener((key, value, cause) ->
                                     LOGGER.debug("Removed entry '{}' with key '{}' from {} with cause {}",
                                                  value, key, "identity_to_role", cause))
                    .buildAsync(systemAuthDatabaseAccessor::findRoleFromIdentity);

            CompletableFuture<String> adminRole = CompletableFuture.completedFuture("admin");
            identityToRoles.put("spiffe://adminID", adminRole);
            CompletableFuture<String> user1Role = CompletableFuture.completedFuture("user1");
            identityToRoles.put("spiffe://identity1", user1Role);
            CompletableFuture<String> user2Role = CompletableFuture.completedFuture("user2");
            identityToRoles.put("spiffe://identity2", user2Role);
            CompletableFuture<String> user3Role = CompletableFuture.completedFuture("user3");
            identityToRoles.put("spiffe://identity3", user3Role);
            CompletableFuture<String> user4Role = CompletableFuture.completedFuture("user4");
            identityToRoles.put("spiffe://identity4", user4Role);
            CompletableFuture<String> user5Role = CompletableFuture.completedFuture("user5");
            identityToRoles.put("spiffe://identity5", user5Role);
            CompletableFuture<String> user6Role = CompletableFuture.completedFuture("user6");
            identityToRoles.put("spiffe://identity6", user6Role);

            return identityToRoles;
        }

        @Override
        protected AsyncLoadingCache<String, AndAuthorization> initRoleToPermissionsCache(CacheConfiguration cacheConfiguration,
                                                                                         Ticker ticker)
        {
            assert systemAuthDatabaseAccessor != null;
            AsyncLoadingCache<String, AndAuthorization> roleToPermissions =
            Caffeine.newBuilder()
                    .ticker(ticker)
                    .maximumSize(cacheConfiguration.maximumSize())
                    .expireAfterWrite(Duration.of(cacheConfiguration.expireAfterAccessMillis(), ChronoUnit.SECONDS))
                    .removalListener((key, value, cause) ->
                                     LOGGER.debug("Removed entry '{}' with key '{}' from {} with cause {}",
                                                  value, key, "role_permissions", cause))
                    .buildAsync(systemAuthDatabaseAccessor::findPermissionsFromResourceRole);

            AndAuthorization adminAuth = AndAuthorization
                                         .create();
            for (MutualTlsPermissions perm : MutualTlsPermissions.ALL)
            {
                adminAuth.addAuthorization(RoleBasedAuthorization.create(perm.name()).setResource("<ALL KEYSPACES>"));
            }
            CompletableFuture<AndAuthorization> adminAuthFuture = CompletableFuture.completedFuture(adminAuth);
            roleToPermissions.put("admin", adminAuthFuture);

            AndAuthorization user1Auth = AndAuthorization
                                         .create()
                                         .addAuthorization(RoleBasedAuthorization
                                                           .create(MutualTlsPermissions.DROP.name())
                                                           .setResource("<keyspace system_auth>"));
            CompletableFuture<AndAuthorization> user1AuthFuture = CompletableFuture.completedFuture(user1Auth);
            roleToPermissions.put("user1", user1AuthFuture);

            AndAuthorization user2Auth = AndAuthorization
                                         .create()
                                         .addAuthorization(RoleBasedAuthorization
                                                           .create(MutualTlsPermissions.DROP.name())
                                                           .setResource("<ALL KEYSPACES>"));
            CompletableFuture<AndAuthorization> user2AuthFuture = CompletableFuture.completedFuture(user2Auth);
            roleToPermissions.put("user2", user2AuthFuture);

            AndAuthorization user3Auth = AndAuthorization
                                         .create()
                                         .addAuthorization(RoleBasedAuthorization
                                                           .create(MutualTlsPermissions.SELECT.name())
                                                           .setResource("<keyspace system_views>"));
            CompletableFuture<AndAuthorization> user3AuthFuture = CompletableFuture.completedFuture(user3Auth);
            roleToPermissions.put("user3", user3AuthFuture);

            AndAuthorization user4Auth = AndAuthorization
                                         .create()
                                         .addAuthorization(RoleBasedAuthorization
                                                           .create(MutualTlsPermissions.SELECT.name())
                                                           .setResource("<keyspace system_views>"))
                                         .addAuthorization(RoleBasedAuthorization
                                                           .create(MutualTlsPermissions.CREATE.name())
                                                           .setResource("<keyspace system_views>"));
            CompletableFuture<AndAuthorization> user4AuthFuture = CompletableFuture.completedFuture(user4Auth);
            roleToPermissions.put("user4", user4AuthFuture);

            AndAuthorization user5Auth = AndAuthorization
                                         .create()
                                         .addAuthorization(RoleBasedAuthorization
                                                           .create(MutualTlsPermissions.SELECT.name())
                                                           .setResource("<keyspace system_auth>"));
            CompletableFuture<AndAuthorization> user5AuthFuture = CompletableFuture.completedFuture(user5Auth);
            roleToPermissions.put("user5", user5AuthFuture);

            AndAuthorization user6Auth = AndAuthorization
                                         .create()
                                         .addAuthorization(RoleBasedAuthorization
                                                           .create(MutualTlsPermissions.SELECT.name())
                                                           .setResource("<keyspace system_auth>"))
                                         .addAuthorization(RoleBasedAuthorization
                                                           .create(MutualTlsPermissions.CREATE.name())
                                                           .setResource("<keyspace system_auth>"));
            CompletableFuture<AndAuthorization> user6AuthFuture = CompletableFuture.completedFuture(user6Auth);
            roleToPermissions.put("user6", user6AuthFuture);

            return roleToPermissions;
        }
    }

    private static class TestMTLSModule extends TestModule
    {
        private static final Logger logger = LoggerFactory.getLogger(TestSslModule.class);
        private final Path keystorePath;
        private final Path truststorePath;
        private final RequiredPermissionsProvider requiredPermissionsProvider;

        public TestMTLSModule(Path keystorePath, Path truststorePath, RequiredPermissionsProvider requiredPermissionsProvider)
        {
            this.keystorePath = keystorePath;
            this.truststorePath = truststorePath;
            this.requiredPermissionsProvider = requiredPermissionsProvider;
        }

        @Override
        public SidecarConfigurationImpl abstractConfig()
        {
            Path keyStorePath;
            Path trustStorePath;

            keyStorePath = this.keystorePath;
            String keyStorePassword = "password";

            trustStorePath = this.truststorePath;
            String trustStorePassword = "password";

            if (!Files.exists(keyStorePath))
            {
                logger.error("JMX password file not found in path={}", keyStorePath);
            }
            if (!Files.exists(trustStorePath))
            {
                logger.error("Trust Store file not found in path={}", trustStorePath);
            }

            SslConfiguration sslConfiguration =
            SslConfigurationImpl.builder()
                                .enabled(true)
                                .useOpenSsl(true)
                                .handshakeTimeoutInSeconds(10L)
                                .clientAuth("REQUIRED")
                                .keystore(new KeyStoreConfigurationImpl(keyStorePath.toAbsolutePath().toString(),
                                                                        keyStorePassword))
                                .truststore(new KeyStoreConfigurationImpl(trustStorePath.toAbsolutePath().toString(),
                                                                          trustStorePassword))
                                .build();

            AuthenticatorConfiguration authenticatorConfiguration =
            AuthenticatorConfigurationImpl.builder()
                                          .authorizedIdentities(Collections.singleton("spiffe://test.com/auth"))
                                          .authConfig(AuthenticatorConfig.MutualTlsAuthenticator)
                                          .certValidator(CertificateValidatorConfig.SpiffeCertificateValidator)
                                          .idValidator(IdentityValidatorConfig.MutualTlsIdentityValidator)
                                          .build();

            AuthorizerConfiguration authorizerConfiguration =
            AuthorizerConfigurationImpl.builder()
                                       .authConfig(AuthorizerConfig.MutualTlsAuthorizer)
                                       .build();

            return super.abstractConfig(sslConfiguration, authenticatorConfiguration, authorizerConfiguration);
        }

        @Provides
        @Singleton
        public PermissionsAccessor permissionsAccessor(MutualTLSAuthorizationTestCacheFactory cacheFactory)
        {
            return new PermissionsAccessor(cacheFactory);
        }

        @Provides
        @Singleton
        public RequiredPermissionsProvider requiredPermissionsProvider()
        {
            return requiredPermissionsProvider;
        }
    }
}
