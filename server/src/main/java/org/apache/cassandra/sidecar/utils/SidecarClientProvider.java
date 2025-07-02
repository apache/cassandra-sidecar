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

import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.net.OpenSSLEngineOptions;
import io.vertx.core.net.SSLOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.apache.cassandra.sidecar.client.HttpClientConfig;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarClientConfig;
import org.apache.cassandra.sidecar.client.SidecarClientConfigImpl;
import org.apache.cassandra.sidecar.client.SidecarClientVertxRequestExecutor;
import org.apache.cassandra.sidecar.client.SidecarInstancesProvider;
import org.apache.cassandra.sidecar.client.VertxHttpClient;
import org.apache.cassandra.sidecar.client.VertxRequestExecutor;
import org.apache.cassandra.sidecar.client.retry.ExponentialBackoffRetryPolicy;
import org.apache.cassandra.sidecar.client.retry.RetryPolicy;
import org.apache.cassandra.sidecar.common.server.utils.SidecarVersionProvider;
import org.apache.cassandra.sidecar.common.server.utils.ThrowableUtils;
import org.apache.cassandra.sidecar.config.SidecarClientConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;

/**
 * Provider class for retrieving the singleton {@link SidecarClient} instance
 */
@Singleton
public class SidecarClientProvider implements Provider<SidecarClient>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarClientProvider.class);
    private final Vertx vertx;
    private final SidecarInstancesProvider sidecarInstancesProvider;
    private final SidecarVersionProvider sidecarVersionProvider;
    private final SidecarClient client;
    private final WebClient webClient;

    private final AtomicBoolean isClosing = new AtomicBoolean(false);
    private final WebClientOptions webClientOptions;
    private final SidecarClientConfiguration sidecarClientConfiguration;

    @Inject
    public SidecarClientProvider(Vertx vertx,
                                 SidecarConfiguration sidecarConfiguration,
                                 SidecarInstancesProvider sidecarInstancesProvider,
                                 SidecarVersionProvider sidecarVersionProvider)
    {
        this.vertx = vertx;
        this.sidecarInstancesProvider = sidecarInstancesProvider;
        this.sidecarVersionProvider = sidecarVersionProvider;
        this.sidecarClientConfiguration = sidecarConfiguration.sidecarClientConfiguration();
        this.webClientOptions = webClientOptions(sidecarClientConfiguration);
        this.webClient = WebClient.create(vertx, webClientOptions);
        this.client = initializeSidecarClient(sidecarClientConfiguration);
    }

    @Override
    public SidecarClient get()
    {
        return client;
    }

    /**
     * Updates the SSL Options for the client
     *
     * @param lastModifiedTime the time of last modification for the file
     * @return a future with the result of the update
     */
    public Future<Boolean> updateSSLOptions(long lastModifiedTime)
    {
        SSLOptions sslOptions = webClientOptions.getSslOptions();
        configureSSLOptions(sslOptions, sidecarClientConfiguration.sslConfiguration(), lastModifiedTime);
        return webClient.updateSSLOptions(sslOptions);
    }

    public void close()
    {
        if (isClosing.compareAndSet(false, true))
        {
            LOGGER.info("Closing Sidecar Client...");
            ThrowableUtils.propagate(client::close);
        }
    }

    private SidecarClient initializeSidecarClient(SidecarClientConfiguration clientConfig)
    {
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder<>().ssl(webClientOptions.isSsl())
                                                                            .timeoutMillis(clientConfig.requestTimeout()
                                                                                                       .toMillis())
                                                                            .idleTimeoutMillis(clientConfig.requestIdleTimeout()
                                                                                                           .toIntMillis())
                                                                            .userAgent("cassandra-sidecar/" + sidecarVersionProvider.sidecarVersion())
                                                                            .build();

        VertxHttpClient vertxHttpClient = new VertxHttpClient(vertx, webClient, httpClientConfig);
        RetryPolicy defaultRetryPolicy = new ExponentialBackoffRetryPolicy(clientConfig.maxRetries(), clientConfig.retryDelay()
                                                                                                                  .toMillis(),
                clientConfig.maxRetryDelay()
                            .toMillis());
        VertxRequestExecutor requestExecutor = new SidecarClientVertxRequestExecutor(vertxHttpClient);

        SidecarClientConfig config = SidecarClientConfigImpl.builder()
                                                            .retryDelayMillis(clientConfig.retryDelay()
                                                                                          .toMillis())
                                                            .maxRetryDelayMillis(clientConfig.maxRetryDelay()
                                                                                             .toMillis())
                                                            .maxRetries(clientConfig.maxRetries())
                                                            .build();
        return new SidecarClient(sidecarInstancesProvider, requestExecutor, config, defaultRetryPolicy);
    }

    static WebClientOptions webClientOptions(SidecarClientConfiguration clientConfig)
    {
        WebClientOptions options = new WebClientOptions();
        options.getPoolOptions()
               .setCleanerPeriod(clientConfig.connectionPoolCleanerPeriod()
                                             .toIntMillis())
               .setEventLoopSize(clientConfig.connectionPoolEventLoopSize())
               .setHttp1MaxSize(clientConfig.connectionPoolMaxSize())
               .setMaxWaitQueueSize(clientConfig.connectionPoolMaxWaitQueueSize());

        SslConfiguration ssl = clientConfig.sslConfiguration();
        if (ssl != null && ssl.enabled())
        {
            options.setSsl(true);

            if (!ssl.secureTransportProtocols()
                    .isEmpty())
            {
                // Use LinkedHashSet to preserve input order
                options.setEnabledSecureTransportProtocols(new LinkedHashSet<>(ssl.secureTransportProtocols()));
            }

            for (String cipherSuite : ssl.cipherSuites())
            {
                options.addEnabledCipherSuite(cipherSuite);
            }

            if (ssl.preferOpenSSL() && OpenSSLEngineOptions.isAvailable())
            {
                LOGGER.info("Using OpenSSL for encryption in Webclient Options");
                options.setSslEngineOptions(new OpenSSLEngineOptions().setSessionCacheEnabled(true));
            }
            else
            {
                LOGGER.warn("OpenSSL not enabled, using JDK for TLS in Webclient Options");
            }

            configureSSLOptions(options.getSslOptions(), ssl, 0);
        }
        return options;
    }

    static void configureSSLOptions(SSLOptions options,
                                    SslConfiguration ssl,
                                    long timestamp)
    {
        options.setSslHandshakeTimeout(ssl.handshakeTimeout()
                                          .quantity())
               .setSslHandshakeTimeoutUnit(ssl.handshakeTimeout()
                                              .unit());

        if (ssl.isKeystoreConfigured())
        {
            SslUtils.setKeyStoreConfiguration(options, ssl.keystore(), timestamp);
        }
        if (ssl.isTrustStoreConfigured())
        {
            SslUtils.setTrustStoreConfiguration(options, ssl.truststore());
        }
    }
}
