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

package org.apache.cassandra.sidecar.acl.authentication;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.auth.mtls.utils.CertificateBuilder;
import io.vertx.ext.auth.mtls.utils.CertificateBundle;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.ParameterizedClassConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.yaml.AccessControlConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.CacheConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.KeyStoreConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ParameterizedClassConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SslConfigurationImpl;
import org.apache.cassandra.sidecar.db.SystemAuthDatabaseAccessor;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for Mutual TLS Authentication
 */
@ExtendWith(VertxExtension.class)
class MutualTLSAuthenticationHandlerTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MutualTLSAuthenticationHandlerTest.class);
    private static final String IDENTITY = "spiffe://cassandra/sidecar/test";
    private static final String ADMIN_IDENTITY = "spiffe://cassandra/sidecar/admin";

    @TempDir
    File tempDir;

    private Server server;
    private Vertx vertx;
    private CertificateBundle ca;
    private Path truststorePath;
    private final SystemAuthDatabaseAccessor mockSystemAuthDatabaseAccessor = mock(SystemAuthDatabaseAccessor.class);

    @BeforeEach
    void setUp() throws Exception
    {
        TestModule testModule = testModule();
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(testModule));
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
        CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    @Test
    void testValidRequestWithRole(VertxTestContext testContext) throws Exception
    {
        Path clientKeystorePath = generateClientCertificate(IDENTITY, ca);
        WebClient client = client(clientKeystorePath, truststorePath);

        when(mockSystemAuthDatabaseAccessor.findRoleFromIdentity(IDENTITY)).thenReturn("cassandra-role");
        when(mockSystemAuthDatabaseAccessor.findAllIdentityToRoles()).thenReturn(Collections.singletonMap(IDENTITY, "cassandra-role"));

        client.get(server.actualPort(), "localhost", "/api/v1/__health")
              .send(testContext.succeeding(response -> {
                  testContext.verify(() -> {
                      assertThat(response.statusCode()).isEqualTo(OK.code());
                      assertThat(response.bodyAsString()).isEqualTo("{\"status\":\"OK\"}");
                      testContext.completeNow();
                  });
              }));
    }

    @Test
    void testValidAdminRequestWithoutRole(VertxTestContext testContext) throws Exception
    {
        Path clientKeystorePath = generateClientCertificate(ADMIN_IDENTITY, ca);
        WebClient client = client(clientKeystorePath, truststorePath);

        client.get(server.actualPort(), "localhost", "/api/v1/__health")
              .send(testContext.succeeding(response -> {
                  testContext.verify(() -> {
                      assertThat(response.statusCode()).isEqualTo(OK.code());
                      assertThat(response.bodyAsString()).isEqualTo("{\"status\":\"OK\"}");
                      testContext.completeNow();
                  });
              }));
    }

    @Test
    void testValidAdminRequestWithRole(VertxTestContext testContext) throws Exception
    {
        Path clientKeystorePath = generateClientCertificate(ADMIN_IDENTITY, ca);
        WebClient client = client(clientKeystorePath, truststorePath);

        when(mockSystemAuthDatabaseAccessor.findRoleFromIdentity(ADMIN_IDENTITY)).thenReturn("cassandra-role");
        client.get(server.actualPort(), "localhost", "/api/v1/__health")
              .send(testContext.succeeding(response -> {
                  testContext.verify(() -> {
                      assertThat(response.statusCode()).isEqualTo(OK.code());
                      assertThat(response.bodyAsString()).isEqualTo("{\"status\":\"OK\"}");
                      testContext.completeNow();
                  });
              }));
    }

    @Test
    void testExpiredCertificate(VertxTestContext testContext) throws Exception
    {
        Path clientKeystorePath = generateClientCertificate(IDENTITY, true, ca);
        WebClient client = client(clientKeystorePath, truststorePath);

        when(mockSystemAuthDatabaseAccessor.findRoleFromIdentity(IDENTITY)).thenReturn("cassandra-role");
        client.get(server.actualPort(), "localhost", "/api/v1/__health")
              .send(testContext.failing(response -> {
                  testContext.verify(() -> {
                      assertThat(response.getCause()).isNotNull();
                      assertThat(response.getMessage()).contains("certificate_expired");
                      testContext.completeNow();
                  });
              }));
    }

    @Test
    void testInvalidIdentity(VertxTestContext testContext) throws Exception
    {
        Path clientKeystorePath = generateClientCertificate("invalid_spiffe_expected", ca);
        WebClient client = client(clientKeystorePath, truststorePath);

        client.get(server.actualPort(), "localhost", "/api/v1/__health")
              .send(testContext.succeeding(response -> {
                  testContext.verify(() -> {
                      assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.UNAUTHORIZED.code());
                      testContext.completeNow();
                  });
              }));
    }

    TestMTLSModule testModule() throws Exception
    {
        ca
        = CertificateBuilder.builder()
                            .subject("CN=Apache cassandra Root CA, OU=Certification Authority, O=Unknown, C=Unknown")
                            .addSanIpAddress("127.0.0.1")
                            .addSanDnsName("localhost")
                            .isCertificateAuthority(true)
                            .buildSelfSigned();

        truststorePath
        = ca.toTempKeyStorePath(tempDir.toPath(), "password".toCharArray(), "password".toCharArray());

        CertificateBundle keystore
        = CertificateBuilder.builder()
                            .subject("CN=Apache Cassandra, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                            .addSanDnsName("localhost")
                            .addSanIpAddress("127.0.0.1")
                            .buildIssuedBy(ca);

        Path serverKeystorePath = keystore.toTempKeyStorePath(tempDir.toPath(),
                                                              "password".toCharArray(),
                                                              "password".toCharArray());
        return new TestMTLSModule(serverKeystorePath, truststorePath, mockSystemAuthDatabaseAccessor);
    }

    private WebClient client(Path clientKeystorePath, Path clientTruststorePath)
    {
        return WebClient.create(vertx, webClientOptions(clientKeystorePath, clientTruststorePath));
    }

    private WebClientOptions webClientOptions(Path clientKeystorePath, Path clientTruststorePath)
    {
        WebClientOptions options = new WebClientOptions();
        options.setSsl(true);
        options.setKeyStoreOptions(new JksOptions().setPath(clientKeystorePath.toAbsolutePath().toString())
                                                   .setPassword("password"));
        options.setTrustStoreOptions(new JksOptions().setPath(clientTruststorePath.toAbsolutePath().toString())
                                                     .setPassword("password"));
        return options;
    }

    private Path generateClientCertificate(String identity, CertificateBundle ca) throws Exception
    {
        return generateClientCertificate(identity, false, ca);
    }

    private Path generateClientCertificate(String identity, boolean expired, CertificateBundle ca) throws Exception
    {
        CertificateBuilder builder
        = new CertificateBuilder().subject("CN=Apache Cassandra, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                  .alias("spiffecert")
                                  .addSanDnsName("localhost")
                                  .addSanIpAddress("127.0.0.1")
                                  .addSanUriName(identity);
        if (expired)
        {
            builder.notAfter(Date.from(Instant.now().minus(1, ChronoUnit.DAYS)));
        }
        CertificateBundle ssc = builder.buildIssuedBy(ca);
        return ssc.toTempKeyStorePath(tempDir.toPath(), "password".toCharArray(), "password".toCharArray());
    }

    /**
     * Override test module for mTLS test
     */
    public static class TestMTLSModule extends TestModule
    {
        private final Path keystorePath;
        private final Path truststorePath;
        private final SystemAuthDatabaseAccessor systemAuthDatabaseAccessor;

        public TestMTLSModule(Path keystorePath, Path truststorePath, SystemAuthDatabaseAccessor systemAuthDatabaseAccessor)
        {
            this.keystorePath = keystorePath;
            this.truststorePath = truststorePath;
            this.systemAuthDatabaseAccessor = systemAuthDatabaseAccessor;
        }

        @Override
        public SidecarConfigurationImpl abstractConfig()
        {
            if (!Files.exists(keystorePath))
            {
                LOGGER.error("Keystore file {} not found", keystorePath);
            }
            if (!Files.exists((truststorePath)))
            {
                LOGGER.error("Truststore file {} not found", keystorePath);
            }

            SslConfiguration sslConfiguration =
            SslConfigurationImpl.builder()
                                .enabled(true)
                                .useOpenSsl(true)
                                .handshakeTimeoutInSeconds(10L)
                                .clientAuth(ClientAuth.REQUEST.name())
                                .keystore(new KeyStoreConfigurationImpl(keystorePath.toAbsolutePath().toString(),
                                                                        "password"))
                                .truststore(new KeyStoreConfigurationImpl(truststorePath.toAbsolutePath().toString(),
                                                                          "password"))
                                .build();


            AccessControlConfiguration accessControlConfiguration
            = new AccessControlConfigurationImpl(true, authenticatorsConfiguration(), Collections.singleton(ADMIN_IDENTITY), new CacheConfigurationImpl());

            return super.abstractConfig(sslConfiguration, accessControlConfiguration);
        }

        @Provides
        @Singleton
        public SystemAuthDatabaseAccessor systemAuthDatabaseAccessor()
        {
            return systemAuthDatabaseAccessor;
        }

        private List<ParameterizedClassConfiguration> authenticatorsConfiguration()
        {
            Map<String, String> params = new HashMap<String, String>()
            {
                {
                    put("certificate_validator", "io.vertx.ext.auth.mtls.impl.CertificateValidatorImpl");
                    put("certificate_identity_extractor", "org.apache.cassandra.sidecar.acl.authentication.CassandraIdentityExtractor");
                }
            };
            ParameterizedClassConfiguration mTLSConfig
            = new ParameterizedClassConfigurationImpl("org.apache.cassandra.sidecar.acl.authentication.MutualTlsAuthenticationHandlerFactory",
                                                      params);
            return Collections.singletonList(mTLSConfig);
        }
    }
}
