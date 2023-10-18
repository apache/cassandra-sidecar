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

package org.apache.cassandra.sidecar.server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLHandshakeException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.PfxOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.KeyStoreConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SslConfigurationImpl;
import org.assertj.core.api.InstanceOfAssertFactories;

import static org.apache.cassandra.sidecar.common.ResourceUtils.writeResourceToPath;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatRuntimeException;
import static org.assertj.core.api.Assertions.from;

/**
 * Unit test for server with different SSL configurations
 */
@ExtendWith(VertxExtension.class)
class ServerSSLTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerSSLTest.class);
    public static final String DEFAULT_PASSWORD = "password";

    @TempDir
    private Path certPath;
    SidecarConfigurationImpl.Builder builder = SidecarConfigurationImpl.builder();
    Injector injector;
    Vertx vertx;
    Server server;
    Path serverKeyStoreP12Path;
    Path clientKeyStoreP12Path;
    Path expiredServerKeyStoreP12Path;
    Path trustStoreP12Path;
    Path serverKeyStoreJksPath;
    Path trustStoreJksPath;
    private KeyStoreConfigurationImpl p12TrustStore;
    private KeyStoreConfigurationImpl p12KeyStore;

    @BeforeEach
    void setup()
    {
        ClassLoader classLoader = ServerSSLTest.class.getClassLoader();
        serverKeyStoreP12Path = writeResourceToPath(classLoader, certPath, "certs/server_keystore.p12");
        clientKeyStoreP12Path = writeResourceToPath(classLoader, certPath, "certs/client_keystore.p12");
        expiredServerKeyStoreP12Path = writeResourceToPath(classLoader, certPath, "certs/expired_server_keystore.p12");
        trustStoreP12Path = writeResourceToPath(classLoader, certPath, "certs/truststore.p12");
        serverKeyStoreJksPath = writeResourceToPath(classLoader, certPath, "certs/server_keystore.jks");
        trustStoreJksPath = writeResourceToPath(classLoader, certPath, "certs/truststore.jks");

        p12TrustStore = new KeyStoreConfigurationImpl(trustStoreP12Path.toString(), DEFAULT_PASSWORD, "PKCS12", -1);
        p12KeyStore = new KeyStoreConfigurationImpl(serverKeyStoreP12Path.toString(), DEFAULT_PASSWORD, "PKCS12", -1);

        injector = Guice.createInjector(Modules.override(new MainModule())
                                               .with(Modules.override(new TestModule())
                                                            .with(new ServerSSLTestModule(builder))));
        vertx = injector.getInstance(Vertx.class);
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
    void failsWhenKeyStoreIsNotConfigured()
    {
        builder.sslConfiguration(SslConfigurationImpl.builder().enabled(true).build());
        server = server();

        assertThatRuntimeException()
        .isThrownBy(() -> server.start())
        .withMessage("Invalid keystore parameters for SSL")
        .withRootCauseInstanceOf(IllegalArgumentException.class)
        .extracting(from(t -> t.getCause().getMessage()), as(InstanceOfAssertFactories.STRING))
        .contains("keyStorePath and keyStorePassword must be set if ssl enabled");
    }

    @Test
    void failsWhenKeyStorePasswordIsIncorrect()
    {
        SslConfigurationImpl ssl =
        SslConfigurationImpl.builder()
                            .enabled(true)
                            .keystore(new KeyStoreConfigurationImpl(serverKeyStoreP12Path.toString(), "badpassword"))
                            .build();
        builder.sslConfiguration(ssl);
        server = server();

        assertThatRuntimeException()
        .isThrownBy(() -> server.start())
        .withMessage("Invalid keystore parameters for SSL")
        .extracting(from(t -> t.getCause().getMessage()), as(InstanceOfAssertFactories.STRING))
        .contains("keystore password was incorrect");
    }

    @Test
    void failsWhenTrustStorePasswordIsIncorrect()
    {
        SslConfigurationImpl ssl =
        SslConfigurationImpl.builder()
                            .enabled(true)
                            .keystore(new KeyStoreConfigurationImpl(serverKeyStoreP12Path.toString(), DEFAULT_PASSWORD))
                            .truststore(new KeyStoreConfigurationImpl(trustStoreP12Path.toString(), "badpassword"))
                            .build();
        builder.sslConfiguration(ssl);
        server = server();

        assertThatRuntimeException()
        .isThrownBy(() -> server.start())
        .withMessage("Invalid keystore parameters for SSL")
        .extracting(from(t -> t.getCause().getMessage()), as(InstanceOfAssertFactories.STRING))
        .contains("keystore password was incorrect");
    }

    @Test
    void testSSLWithPkcs12Succeeds(VertxTestContext context)
    {
        SslConfigurationImpl ssl =
        SslConfigurationImpl.builder()
                            .enabled(true)
                            .keystore(p12KeyStore)
                            .truststore(p12TrustStore)
                            .build();

        builder.sslConfiguration(ssl)
               .serviceConfiguration(ServiceConfigurationImpl.builder()
                                                             .host("127.0.0.1")
                                                             .port(0)
                                                             .build());
        server = server();

        server.start()
              .compose(s -> validateHealthEndpoint(clientWithP12Keystore(true, false)))
              .onComplete(context.succeedingThenComplete());
    }

    @Test
    void testSSLWithJksSucceeds(VertxTestContext context)
    {
        SslConfigurationImpl ssl =
        SslConfigurationImpl.builder()
                            .enabled(true)
                            .keystore(new KeyStoreConfigurationImpl(serverKeyStoreJksPath.toString(), DEFAULT_PASSWORD))
                            .truststore(new KeyStoreConfigurationImpl(trustStoreJksPath.toString(), DEFAULT_PASSWORD))
                            .build();

        builder.sslConfiguration(ssl)
               .serviceConfiguration(ServiceConfigurationImpl.builder()
                                                             .host("127.0.0.1")
                                                             .port(0)
                                                             .build());
        server = server();

        server.start()
              .compose(s -> validateHealthEndpoint(clientWithJksTrustStore()))
              .onComplete(context.succeedingThenComplete());
    }

    @Test
    void testOpenSSLSucceeds(VertxTestContext context)
    {
        SslConfigurationImpl ssl =
        SslConfigurationImpl.builder()
                            .enabled(true)
                            .useOpenSsl(true)
                            .keystore(p12KeyStore)
                            .truststore(p12TrustStore)
                            .build();

        builder.sslConfiguration(ssl)
               .serviceConfiguration(ServiceConfigurationImpl.builder()
                                                             .host("127.0.0.1")
                                                             .port(9043)
                                                             .build());

        server = server();

        server.start()
              .compose(s -> validateHealthEndpoint(clientWithP12Keystore(true, false)))
              .onComplete(context.succeedingThenComplete());
    }

    @Test
    void testTwoWaySSLSucceeds(VertxTestContext context)
    {
        SslConfigurationImpl ssl =
        SslConfigurationImpl.builder()
                            .enabled(true)
                            .useOpenSsl(true)
                            .keystore(p12KeyStore)
                            .truststore(p12TrustStore)
                            .clientAuth("REQUIRED")
                            .build();

        builder.sslConfiguration(ssl)
               .serviceConfiguration(ServiceConfigurationImpl.builder()
                                                             .host("127.0.0.1")
                                                             .port(0)
                                                             .build());
        server = server();

        server.start()
              .compose(s -> validateHealthEndpoint(clientWithP12Keystore(true, true)))
              .onComplete(context.succeedingThenComplete());
    }

    @Test
    void failsOnMissingClientKeystore(VertxTestContext context)
    {
        SslConfigurationImpl ssl =
        SslConfigurationImpl.builder()
                            .enabled(true)
                            .useOpenSsl(true)
                            .keystore(p12KeyStore)
                            .truststore(p12TrustStore)
                            .clientAuth("REQUIRED")
                            .build();

        builder.sslConfiguration(ssl)
               .serviceConfiguration(ServiceConfigurationImpl.builder()
                                                             .host("127.0.0.1")
                                                             .port(0)
                                                             .build());
        server = server();

        server.start()
              .compose(s -> validateHealthEndpoint(clientWithP12Keystore(true, false)))
              .onComplete(context.failing(throwable -> {
                  assertThat(throwable).isNotNull()
                                       .hasMessageContaining("Received fatal alert: bad_certificate");
                  context.completeNow();
              }));
    }

    @Test
    void testTwoWaySSLSucceedsWithOptionalClientAuth(VertxTestContext context)
    {
        SslConfigurationImpl ssl =
        SslConfigurationImpl.builder()
                            .enabled(true)
                            .keystore(p12KeyStore)
                            .truststore(p12TrustStore)
                            .clientAuth("REQUEST") // client auth is optional
                            .build();

        builder.sslConfiguration(ssl)
               .serviceConfiguration(ServiceConfigurationImpl.builder()
                                                             .host("127.0.0.1")
                                                             .port(0)
                                                             .build());
        server = server();

        server.start()
              .compose(s -> validateHealthEndpoint(clientWithP12Keystore(true, false)))
              .onComplete(context.succeedingThenComplete());
    }

    @Test
    void failsOnMissingClientTrustStore(VertxTestContext context)
    {
        SslConfigurationImpl ssl =
        SslConfigurationImpl.builder()
                            .enabled(true)
                            .keystore(p12KeyStore)
                            .truststore(p12TrustStore)
                            .build();

        builder.sslConfiguration(ssl)
               .serviceConfiguration(ServiceConfigurationImpl.builder()
                                                             .host("127.0.0.1")
                                                             .port(0)
                                                             .build());
        server = server();

        server.start()
              .compose(s -> validateHealthEndpoint(clientWithP12Keystore(false, false)))
              .onComplete(context.failing(throwable -> {
                  assertThat(throwable).isNotNull()
                                       .isInstanceOf(SSLHandshakeException.class)
                                       .hasMessageContaining("Failed to create SSL connection");
                  context.completeNow();
              }));
    }

    @Test
    void testHotReloadOfServerCertificates(VertxTestContext context)
    {
        KeyStoreConfigurationImpl expiredP12KeyStore =
        new KeyStoreConfigurationImpl(expiredServerKeyStoreP12Path.toString(), DEFAULT_PASSWORD, "PKCS12", -1);
        SslConfigurationImpl ssl =
        SslConfigurationImpl.builder()
                            .enabled(true)
                            .keystore(expiredP12KeyStore)
                            .truststore(p12TrustStore)
                            .build();

        int serverVerticleInstances = 16;
        builder.sslConfiguration(ssl)
               .serviceConfiguration(ServiceConfigurationImpl.builder()
                                                             .host("127.0.0.1")
                                                             // > 1 to ensure that hot reloading works for all
                                                             // the deployed servers on each verticle instance
                                                             .serverVerticleInstances(serverVerticleInstances)
                                                             .port(9043)
                                                             .build());

        server = server();
        WebClient client = clientWithP12Keystore(true, false);

        server.start()
              .compose(s -> {
                  // Access the health endpoint, all the request are expected to fail with SSL connection errors
                  // Try to hit all the deployed server verticles by iterating over the number of deployed instances
                  List<Future<Void>> futureList = new ArrayList<>();
                  for (int i = 0; i < serverVerticleInstances; i++)
                  {
                      futureList.add(validateHealthEndpoint(client)
                                     .onComplete(context.failing(throwable -> {
                                         assertThat(throwable).isNotNull()
                                                              .isInstanceOf(SSLHandshakeException.class)
                                                              .hasMessageContaining("Failed to create SSL connection");
                                     })));
                  }
                  return Future.all(futureList)
                               .onSuccess(v -> context.failNow("Success is not expected when the keystore is expired"))
                               .recover(t -> Future.succeededFuture());
              })
              .compose(v -> {
                  try
                  {
                      // Override the expired certificate with a valid certificate
                      Files.copy(serverKeyStoreP12Path, expiredServerKeyStoreP12Path,
                                 StandardCopyOption.REPLACE_EXISTING);
                  }
                  catch (IOException e)
                  {
                      throw new RuntimeException(e);
                  }
                  // Force a reload of certificates in the server
                  return server.updateSSLOptions(System.currentTimeMillis());
              })
              .compose(s -> {
                  // Access the health endpoint, all the request are expected to succeed now that a valid
                  // certificate has been loaded into the server. Try to hit all the deployed server verticles
                  // by iterating over the number of deployed instances
                  List<Future<Void>> futureList = new ArrayList<>();
                  for (int i = 0; i < serverVerticleInstances; i++)
                  {
                      futureList.add(validateHealthEndpoint(client));
                  }
                  return Future.all(futureList);
              })
              .onComplete(context.succeedingThenComplete());
    }

    /**
     * @return the server using the per-test SSL configuration
     */
    Server server()
    {
        return injector.getInstance(Server.class);
    }

    Future<Void> validateHealthEndpoint(WebClient client)
    {
        LOGGER.info("Checking server health localhost:{}/api/v1/__health", server.actualPort());
        return client.get(server.actualPort(), "localhost", "/api/v1/__health")
                     .send()
                     .compose(response -> {
                         assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                         assertThat(response.bodyAsJsonObject().getString("status")).isEqualTo("OK");
                         return Future.succeededFuture();
                     });
    }

    WebClient clientWithP12Keystore(boolean includeTrustStore, boolean includeClientKeyStore)
    {
        WebClientOptions options = new WebClientOptions().setSsl(true);
        if (includeTrustStore)
        {
            options.setTrustOptions(new PfxOptions().setPath(trustStoreP12Path.toString())
                                                    .setPassword(DEFAULT_PASSWORD));
        }
        if (includeClientKeyStore)
        {
            options.setKeyCertOptions(new PfxOptions().setPath(clientKeyStoreP12Path.toString())
                                                      .setPassword(DEFAULT_PASSWORD));
        }
        return WebClient.create(vertx, options);
    }

    WebClient clientWithJksTrustStore()
    {
        JksOptions trustOptions = new JksOptions().setPath(trustStoreJksPath.toString()).setPassword(DEFAULT_PASSWORD);
        return WebClient.create(vertx, new WebClientOptions().setSsl(true).setTrustOptions(trustOptions));
    }

    static class ServerSSLTestModule extends AbstractModule
    {
        final SidecarConfigurationImpl.Builder builder;

        ServerSSLTestModule(SidecarConfigurationImpl.Builder builder)
        {
            this.builder = builder;
        }

        @Provides
        @Singleton
        public SidecarConfiguration configuration()
        {
            return builder.build();
        }
    }
}
