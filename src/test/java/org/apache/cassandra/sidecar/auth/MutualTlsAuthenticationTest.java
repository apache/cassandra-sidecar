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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.core.net.JksOptions;
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
import org.apache.cassandra.sidecar.config.AuthenticatorConfiguration;
import org.apache.cassandra.sidecar.config.AuthorizerConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.yaml.AuthenticatorConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.AuthorizerConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.KeyStoreConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SslConfigurationImpl;
import org.apache.cassandra.sidecar.routes.MutualTlsAuthenticationHandler;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.utils.CertificateBuilder;
import org.apache.cassandra.sidecar.utils.CertificateBundle;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests MutualTLS Authentication for {@link MutualTlsAuthenticationHandler}
 */
@ExtendWith(VertxExtension.class)
public class MutualTlsAuthenticationTest
{
    private static final Logger logger = LoggerFactory.getLogger(MutualTlsAuthenticationTest.class);
    private static final String IDENTITY = "spiffe://test.com/auth";
    @TempDir
    File tempDir;
    TestModule testModule;
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


        CertificateBundle keystore = new CertificateBuilder().subject("CN=Apache Cassandra, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                                             .addSanDnsName(InetAddress.getLocalHost().getCanonicalHostName())
                                                             .addSanDnsName(InetAddress.getLocalHost().getHostName())
                                                             .addSanDnsName("localhost")
                                                             .buildIssuedBy(ca);

        Path serverKeystorePath = keystore.toTempKeyStorePath(tempDir.toPath(),
                                                              "password".toCharArray(),
                                                              "password".toCharArray());

        return new TestMTLSModule(serverKeystorePath, truststorePath);
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
            logger.info("Close event received before timeout.");
        else
            logger.error("Close event timed out.");
    }

    @DisplayName("Should return HTTP 200 OK if mTLS is working")
    @Test
    void testSidecarHealthCheckReturnsOK(VertxTestContext testContext) throws Exception
    {
        Path clientKeystorePath = generateClientCertificate(null, ca);

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

    @DisplayName("Should return HTTP 404 NOT FOUND with nonexistent endpoint")
    @Test
    void testIncorrectEndpoint(VertxTestContext testContext) throws Exception
    {
        Path clientKeystorePath = generateClientCertificate(null, ca);

        String url = "/test/test/not/real";

        WebClient client = client(clientKeystorePath, truststorePath);

        client.get(server.actualPort(), "localhost", url)
              .as(BodyCodec.string())
              .ssl(true)
              .send(testContext.succeeding(response -> testContext.verify(() ->
                                                                          {
                                                                              assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                                                                              testContext.completeNow();
                                                                          })));
    }

    @DisplayName("Should result in failure from expired certificate")
    @Test
    void testExpiredCert(VertxTestContext testContext) throws Exception
    {
        Path clientKeystorePath = generateClientCertificate(b -> b.notBefore(Instant.now().minus(2, ChronoUnit.DAYS))
                                                                  .notAfter(Instant.now().minus(1, ChronoUnit.DAYS)), ca);

        String url = "/api/v1/cassandra/__health";

        WebClient client = client(clientKeystorePath, truststorePath);

        client.get(server.actualPort(), "localhost", url)
              .as(BodyCodec.string())
              .send(testContext.failing(response -> testContext.verify(() ->
                                                                       {
                                                                           assertThat(response).hasMessageContaining("certificate_unknown");
                                                                           testContext.completeNow();
                                                                       })));
    }

    @DisplayName("Should result in failure from untrusted truststore")
    @Test
    void testFailWithUntrustedTruststore(VertxTestContext testContext) throws Exception
    {
        Path clientKeystorePath = generateClientCertificate(null, ca);

        String url = "/api/v1/cassandra/__health";

        WebClient client = client(clientKeystorePath, untrustedTruststorePath);

        client.get(server.actualPort(), "localhost", url)
              .as(BodyCodec.string())
              .send(testContext.failing(response -> testContext.verify(() ->
                                                                       {
                                                                           assertThat(response).hasMessageContaining("Failed to create SSL connection");
                                                                           testContext.completeNow();
                                                                       })));
    }

    @DisplayName("Should result in failure from certificate signed by untrusted CA")
    @Test
    void testFailWithUntrustedCertificate(VertxTestContext testContext) throws Exception
    {
        Path clientKeystorePath = generateClientCertificate(null, badCA);

        String url = "/api/v1/cassandra/__health";

        WebClient client = client(clientKeystorePath, untrustedTruststorePath);

        client.get(server.actualPort(), "localhost", url)
              .as(BodyCodec.string())
              .send(testContext.failing(response -> testContext.verify(() ->
                                                                       {
                                                                           assertThat(response).hasMessageContaining("Failed to create SSL connection");
                                                                           testContext.completeNow();
                                                                       })));
    }

    @DisplayName("Should result in success when Cassandra and Sidecar are running")
    @Test
    void testCassandraHealthCheck(VertxTestContext testContext) throws Exception
    {
        Path clientKeystorePath = generateClientCertificate(null, ca);

        WebClient client = client(clientKeystorePath, truststorePath);

        String url = "/api/v1/cassandra/__health";
        client.get(server.actualPort(), "localhost", url)
              .as(BodyCodec.string())
              .ssl(true)
              .send(testContext.succeeding(response -> testContext.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  assertThat(response.body()).isEqualTo("{\"status\":\"OK\"}");
                  testContext.completeNow();
              })));
    }

    @DisplayName("Should result in 404 NOT_FOUND")
    @Test
    void test404FailInstanceNotFound(VertxTestContext testContext) throws Exception
    {
        Path clientKeystorePath = generateClientCertificate(null, ca);

        WebClient client = client(clientKeystorePath, truststorePath);

        String url = "/api/v1/cassandra/__health?instanceId=500";
        client.get(server.actualPort(), "localhost", url)
              .as(BodyCodec.string())
              .ssl(true)
              .send(testContext.succeeding(response -> testContext.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  assertThat(response.body()).isEqualTo("{\"status\":\"Not Found\",\"code\":404,\"message\":\"Instance id '500' not found\"}");
                  testContext.completeNow();
              })));
    }

    @DisplayName("Should result in 503 NOT_OK")
    @Test
    void test503FailWithQueryParam(VertxTestContext testContext) throws Exception
    {
        testModule.delegate.setIsNativeUp(false);
        Path clientKeystorePath = generateClientCertificate(null, ca);

        WebClient client = client(clientKeystorePath, truststorePath);

        String url = "/api/v1/cassandra/__health?instanceId=2";
        client.get(server.actualPort(), "localhost", url)
              .as(BodyCodec.string())
              .ssl(true)
              .send(testContext.succeeding(response -> testContext.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(SERVICE_UNAVAILABLE.code());
                  assertThat(response.body()).isEqualTo("{\"status\":\"NOT_OK\"}");
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
                                           CertificateBundle certificateAuthority) throws Exception
    {

        CertificateBuilder builder = new CertificateBuilder().subject("CN=Apache Cassandra, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                                             .notBefore(Instant.now().minus(1, ChronoUnit.DAYS))
                                                             .notAfter(Instant.now().plus(1, ChronoUnit.DAYS))
                                                             .alias("spiffecert")
                                                             .addSanUriName(IDENTITY)
                                                             .rsa2048Algorithm();
        if (customizeCertificate != null)
        {
            builder = customizeCertificate.apply(builder);
        }
        CertificateBundle ssc = builder.buildIssuedBy(certificateAuthority);

        return ssc.toTempKeyStorePath(tempDir.toPath(), "cassandra".toCharArray(), "cassandra".toCharArray());
    }

    private static class TestMTLSModule extends TestModule
    {
        private static final Logger logger = LoggerFactory.getLogger(TestSslModule.class);
        private final Path keystorePath;
        private final Path truststorePath;

        public TestMTLSModule(Path keystorePath, Path truststorePath)
        {
            this.keystorePath = keystorePath;
            this.truststorePath = truststorePath;
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
                                          .authConfig(AuthenticatorConfig.MutualTls)
                                          .certValidator(CertificateValidatorConfig.Spiffe)
                                          .idValidator(IdentityValidatorConfig.MutualTls)
                                          .build();

            AuthorizerConfiguration authorizerConfiguration =
            AuthorizerConfigurationImpl.builder()
                                       .authConfig(AuthorizerConfig.AllowAll)
                                       .build();

            return super.abstractConfig(sslConfiguration, authenticatorConfiguration, authorizerConfiguration);
        }
    }
}
