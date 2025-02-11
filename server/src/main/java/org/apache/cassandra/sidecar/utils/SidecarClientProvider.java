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

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.OpenSSLEngineOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.apache.cassandra.sidecar.client.HttpClientConfig;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.client.SimpleSidecarInstancesProvider;
import org.apache.cassandra.sidecar.client.VertxHttpClient;
import org.apache.cassandra.sidecar.client.VertxRequestExecutor;
import org.apache.cassandra.sidecar.client.retry.ExponentialBackoffRetryPolicy;
import org.apache.cassandra.sidecar.client.retry.RetryPolicy;
import org.apache.cassandra.sidecar.common.server.utils.SidecarVersionProvider;
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
    private final SidecarClientConfiguration clientConfig;
    private final SslConfiguration sslConfig;
    private final SidecarVersionProvider sidecarVersionProvider;
    private final SidecarClient client;

    @Inject
    public SidecarClientProvider(Vertx vertx,
                                 SidecarConfiguration sidecarConfiguration,
                                 SidecarVersionProvider sidecarVersionProvider)
    {
        this.vertx = vertx;
        this.clientConfig = sidecarConfiguration.sidecarClientConfiguration();
        this.sslConfig = sidecarConfiguration.sslConfiguration();
        this.sidecarVersionProvider = sidecarVersionProvider;
        this.client = initializeSidecarClient();
    }

    @Override
    public SidecarClient get()
    {
        return client;
    }

    private SidecarClient initializeSidecarClient()
    {
        WebClientOptions webClientOptions = webClientOptions();
        HttpClient httpClient = vertx.createHttpClient(webClientOptions);
        WebClient webClient = WebClient.wrap(httpClient, webClientOptions);

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder<>()
                                            .ssl(webClientOptions().isSsl())
                                            .timeoutMillis(clientConfig.requestTimeout().toMillis())
                                            .idleTimeoutMillis(clientConfig.requestIdleTimeout().toIntMillis())
                                            .userAgent("cassandra-sidecar/" + sidecarVersionProvider.sidecarVersion())
                                            .build();

        VertxHttpClient vertxHttpClient = new VertxHttpClient(vertx, webClient, httpClientConfig);
        RetryPolicy defaultRetryPolicy = new ExponentialBackoffRetryPolicy(clientConfig.maxRetries(),
                                                                           clientConfig.retryDelayMillis(),
                                                                           clientConfig.maxRetryDelayMillis());
        VertxRequestExecutor requestExecutor = new VertxRequestExecutor(vertxHttpClient);
        SidecarInstance instance = new SidecarInstanceImpl(webClientOptions.getDefaultHost(), webClientOptions.getDefaultPort());
        ArrayList<SidecarInstance> instances = new ArrayList<>();
        instances.add(instance);
        SimpleSidecarInstancesProvider instancesProvider = new SimpleSidecarInstancesProvider(instances);
        return new SidecarClient(instancesProvider,
                                 requestExecutor,
                                 clientConfig,
                                 defaultRetryPolicy);
    }

    private WebClientOptions webClientOptions()
    {
        WebClientOptions options = new WebClientOptions();
        options.getPoolOptions()
               .setCleanerPeriod(clientConfig.connectionPoolCleanerPeriod().toIntMillis())
               .setEventLoopSize(clientConfig.connectionPoolEventLoopSize())
               .setHttp1MaxSize(clientConfig.connectionPoolMaxSize())
               .setMaxWaitQueueSize(clientConfig.connectionPoolMaxWaitQueueSize());

        boolean useSsl = clientConfig.useSsl();
        if (sslConfig.isKeystoreConfigured())
        {
            options.setKeyStoreOptions(new JksOptions().setPath(sslConfig.keystore().path())
                                                       .setPassword(sslConfig.keystore().password()));
            if (sslConfig.preferOpenSSL() && OpenSSLEngineOptions.isAvailable())
            {
                LOGGER.info("Using OpenSSL for encryption in Webclient Options");
                useSsl = true;
                options.setSslEngineOptions(new OpenSSLEngineOptions().setSessionCacheEnabled(true));
            }
            else
            {
                LOGGER.warn("OpenSSL not enabled, using JDK for TLS in Webclient Options");
            }
        }

        if (sslConfig.truststore() != null && sslConfig.truststore().isConfigured())
        {
            options.setTrustStoreOptions(new JksOptions().setPath(sslConfig.truststore().path())
                                                         .setPassword(sslConfig.truststore().password()));
        }

        options.setSsl(useSsl);
        return options;
    }
}
