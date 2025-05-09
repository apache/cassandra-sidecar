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

package org.apache.cassandra.sidecar.utils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.common.response.HealthResponse;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.SidecarClientConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.yaml.KeyStoreConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarClientConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SslConfigurationImpl;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.testing.utils.tls.CertificateBuilder;
import org.apache.cassandra.testing.utils.tls.CertificateBundle;

import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Unit test for the {@link SidecarClientProvider} class
 */
class SidecarClientProviderTest
{
    public static final char[] EMPTY_PASSWORD = new char[0];

    @TempDir
    static Path secretsPath;
    static Path truststorePath;
    static Path serverKeyStorePath;
    static Path validClientCertPath;
    static Path clientCertPath;

    Injector injector;
    private Vertx vertx;
    private Server server;

    SidecarClient client;
    TestModule testModule;

    private SidecarClientProvider provider;

    @BeforeAll
    static void configureCertificates() throws Exception
    {
        CertificateBundle certificateAuthority = new CertificateBuilder().subject("CN=Apache Cassandra Root CA, OU=Certification Authority, O=Unknown, C=Unknown")
                                                                         .alias("fakerootca")
                                                                         .isCertificateAuthority(true)
                                                                         .buildSelfSigned();
        truststorePath = certificateAuthority.toTempKeyStorePath(secretsPath, EMPTY_PASSWORD, EMPTY_PASSWORD);

        CertificateBuilder serverKeyStoreBuilder =
        new CertificateBuilder().subject("CN=Apache Cassandra, OU=mtls_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                .addSanDnsName("localhost");
        CertificateBundle serverKeyStore = serverKeyStoreBuilder.buildIssuedBy(certificateAuthority);
        serverKeyStorePath = serverKeyStore.toTempKeyStorePath(secretsPath, EMPTY_PASSWORD, EMPTY_PASSWORD);

        CertificateBundle expiredClientKeyStore = new CertificateBuilder().subject("CN=Apache Cassandra, OU=mtls_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                                                          .addSanDnsName("localhost")
                                                                          .notBefore(Instant.now().minus(7, ChronoUnit.DAYS))
                                                                          .notAfter(Instant.now().minus(1, ChronoUnit.DAYS))
                                                                          .buildIssuedBy(certificateAuthority);
        // Assign the expired client cert to the cert path
        clientCertPath = expiredClientKeyStore.toTempKeyStorePath(secretsPath, EMPTY_PASSWORD, EMPTY_PASSWORD);

        CertificateBundle validClientKeyStore = new CertificateBuilder().subject("CN=Apache Cassandra, OU=mtls_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                                                        .addSanDnsName("localhost")
                                                                        .buildIssuedBy(certificateAuthority);
        validClientCertPath = validClientKeyStore.toTempKeyStorePath(secretsPath, EMPTY_PASSWORD, EMPTY_PASSWORD);
    }

    @BeforeEach
    void setup()
    {
        testModule = new SidecarClientProviderModule();

        injector = Guice.createInjector(Modules.override(SidecarModules.all()).with(testModule));
        vertx = injector.getInstance(Vertx.class);
        server = getMTLSServerAndStart();
        provider = injector.getInstance(SidecarClientProvider.class);
        client = provider.get();
    }

    @AfterEach
    void cleanup()
    {
        if (server != null)
        {
            getBlocking(server.close(), 10, TimeUnit.SECONDS, "Close server");
        }
        getBlocking(vertx.close(), 10, TimeUnit.SECONDS, "Close vertx");
    }

    @Test
    void testSidecarClientIsSingleton()
    {
        SidecarClient client1 = provider.get();
        SidecarClient client2 = provider.get();

        assertThat(client1).isSameAs(client2);
    }

    @Test
    void testHotReloadOfClientCerts() throws Exception
    {
        // the certificate should be expired at the beginning of the test
        unsuccessfulClientRequest(client);

        // Replace the expired certificated with a good certificate we can use
        Files.copy(validClientCertPath, clientCertPath, StandardCopyOption.REPLACE_EXISTING);

        // Wait until the client reloads the certificate
        loopAssert(10, () -> successfulClientRequest(client));

        // Execute requests with the client. We should see successful requests go through now
        successfulClientRequest(client);
    }

    private void unsuccessfulClientRequest(SidecarClient client)
    {
        assertThatThrownBy(() -> client.sidecarHealth(new SidecarInstanceImpl("localhost", server.actualPort())).get(30, TimeUnit.SECONDS))
        .describedAs("Unsuccessful client requests are expected to fail")
        .isNotNull();
    }

    private void successfulClientRequest(SidecarClient client)
    {
        HealthResponse healthResponse = null;
        try
        {
            healthResponse = client.sidecarHealth(new SidecarInstanceImpl("localhost", server.actualPort())).get(30, TimeUnit.SECONDS);
        }
        catch (Exception exception)
        {
            fail("Client request was expected to succeed", exception);
        }
        assertThat(healthResponse).isNotNull();
        assertThat(healthResponse.isOk()).isTrue();
    }

    Server getMTLSServerAndStart()
    {
        // Start server and wait for it to be running
        Server server = injector.getInstance(Server.class);
        getBlocking(server.start(), 30, TimeUnit.SECONDS, "Server start");
        return server;
    }

    static class SidecarClientProviderModule extends TestModule
    {
        @Override
        public SidecarConfigurationImpl abstractConfig()
        {
            SslConfiguration serverSslConfiguration =
            SslConfigurationImpl.builder()
                                .enabled(true)
                                .useOpenSsl(true)
                                .handshakeTimeout(SecondBoundConfiguration.parse("10s"))
                                .clientAuth("REQUIRED")
                                .keystore(new KeyStoreConfigurationImpl(serverKeyStorePath.toAbsolutePath().toString(), ""))
                                .truststore(new KeyStoreConfigurationImpl(truststorePath.toAbsolutePath().toString(), ""))
                                .build();

            Function<SidecarConfigurationImpl.Builder, SidecarConfigurationImpl.Builder> configOverrides =
            builder -> {
                String type = "PKCS12";
                SecondBoundConfiguration checkInterval = SecondBoundConfiguration.ONE;

                SslConfiguration clientSslConfiguration =
                SslConfigurationImpl.builder()
                                    .enabled(true)
                                    .useOpenSsl(true)
                                    .keystore(new KeyStoreConfigurationImpl(clientCertPath.toAbsolutePath().toString(), "", type, checkInterval))
                                    .truststore(new KeyStoreConfigurationImpl(truststorePath.toAbsolutePath().toString(), "", type, checkInterval))
                                    .build();
                SidecarClientConfiguration sidecarClientConfiguration = new SidecarClientConfigurationImpl(clientSslConfiguration);
                return builder.sidecarClientConfiguration(sidecarClientConfiguration);
            };
            return super.abstractConfig(serverSslConfiguration, configOverrides);
        }
    }
}
