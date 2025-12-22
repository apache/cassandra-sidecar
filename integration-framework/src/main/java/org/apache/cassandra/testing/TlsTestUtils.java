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

package org.apache.cassandra.testing;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.shared.Uninterruptibles;

import static org.apache.cassandra.testing.utils.IInstanceUtils.tryGetIntConfig;

/**
 * Test utility class for all TLS-things related
 */
public class TlsTestUtils
{
    public static SSLOptions getSSLOptions(String keystorePath, String keystorePassword,
                                           String trustStorePath, String trustStorePassword) throws RuntimeException
    {
        try
        {
            SslContext context = buildSSLContext(keystorePath, keystorePassword, trustStorePath, trustStorePassword);
            return new RemoteEndpointAwareNettySSLOptions(context);
        }
        catch (GeneralSecurityException | IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void withAuthenticatedSession(IInstance instance,
                                                String username,
                                                String password,
                                                Consumer<Session> consumer,
                                                SSLOptions sslOptions)
    {
        withAuthenticatedSession(instance, username, password, (builder) -> builder, consumer, sslOptions);
    }

    public static void withAuthenticatedSession(IInstance instance,
                                                String username,
                                                String password,
                                                Function<Cluster.Builder, Cluster.Builder> builderCustomizer,
                                                Consumer<Session> consumer,
                                                SSLOptions sslOptions)
    {
        InetSocketAddress nativeInetSocketAddress = new InetSocketAddress(instance.config().broadcastAddress().getAddress(),
                                                                          tryGetIntConfig(instance.config(), "native_transport_port", 9042));
        InetAddress address = nativeInetSocketAddress.getAddress();

        com.datastax.driver.core.Cluster.Builder builder = com.datastax.driver.core.Cluster.builder()
                                                                                           .withLoadBalancingPolicy(new DCAwareRoundRobinPolicy.Builder().build())
                                                                                           .withSSL(sslOptions)
                                                                                           .withoutJMXReporting()
                                                                                           .withAuthProvider(new PlainTextAuthProvider(username, password))
                                                                                           .addContactPoint(address.getHostAddress())
                                                                                           .withPort(nativeInetSocketAddress.getPort());

        if (builderCustomizer != null)
        {
            builder = builderCustomizer.apply(builder);
        }

        try (com.datastax.driver.core.Cluster c = builder.build(); Session session = c.connect())
        {
            consumer.accept(session);
        }
    }

    /**
     * Waits until the existing role is initialized and attempts to run the {@code runnable}.
     *
     * @param runnable the code to run
     */
    public static void waitForExistingRoles(Runnable runnable)
    {
        for (int i = 0; i < 60; i++)
        {
            try
            {
                runnable.run();
                return;
            }
            catch (AuthenticationException exception)
            {
                if (exception.getMessage() != null &&
                    exception.getMessage().contains("Provided username cassandra and/or password are incorrect"))
                {
                    // Wait until the default username/password is created
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                }
                else
                {
                    throw exception;
                }
            }
        }
    }

    public static SslContext buildSSLContext(String keystorePath, String keystorePassword,
                                             String trustStorePath, String trustStorePassword) throws GeneralSecurityException, IOException
    {

        SslContextBuilder sslContextBuilder;
        try
        {
            sslContextBuilder = SslContextBuilder.forClient()
                                                 .protocols(List.of("TLSv1.2", "TLSv1.3"));

            if (keystorePath != null && keystorePassword != null)
            {
                KeyStore keyStore = createKeystore(keystorePath, keystorePassword);
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keyStore, keystorePassword.toCharArray());
                sslContextBuilder.keyManager(kmf);
            }

            // We set the truststore only if it is configured. For an SSL connection, if the truststore is required
            // but the user has not provided one, the default Java truststore is used.
            KeyStore truststore = createKeystore(trustStorePath, trustStorePassword);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(truststore);
            sslContextBuilder.trustManager(tmf);
            return sslContextBuilder.build();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error creating SsLContext for Cassandra connections", e);
        }
    }

    static KeyStore createKeystore(String path, String password) throws GeneralSecurityException, IOException
    {
        KeyStore keystore = KeyStore.getInstance("PKCS12");
        try (InputStream inputStream = Files.newInputStream(Paths.get(path)))
        {
            keystore.load(inputStream, password.toCharArray());
        }
        return keystore;
    }
}
