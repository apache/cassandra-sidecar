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

import java.util.function.Function;

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Singleton;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.net.OpenSSLEngineOptions;
import io.vertx.core.net.SSLOptions;
import io.vertx.core.net.TrafficShapingOptions;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.TrafficShapingConfiguration;
import org.apache.cassandra.sidecar.utils.SslUtils;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A provider that takes the {@link SidecarConfiguration} and builds {@link HttpServerOptions} from the configured
 * values
 */
@Singleton
public class HttpServerOptionsProvider implements Function<SidecarConfiguration, HttpServerOptions>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerOptionsProvider.class);

    /**
     * @return the {@link HttpServerOptions} built from the provided {@link SidecarConfiguration}
     */
    @Override
    public HttpServerOptions apply(SidecarConfiguration configuration)
    {
        HttpServerOptions options = new HttpServerOptions().setLogActivity(true);
        ServiceConfiguration serviceConf = configuration.serviceConfiguration();
        options.setIdleTimeoutUnit(MILLISECONDS)
               .setIdleTimeout(serviceConf.requestIdleTimeoutMillis())
               .setTcpKeepAlive(serviceConf.tcpKeepAlive())
               .setAcceptBacklog(serviceConf.acceptBacklog());

        if (SystemUtils.IS_OS_LINUX)
        {
            options.setTcpFastOpen(true);
            options.setTcpCork(true);
        }

        SslConfiguration ssl = configuration.sslConfiguration();
        if (ssl != null && ssl.enabled())
        {
            options.setClientAuth(ClientAuth.valueOf(ssl.clientAuth()))
                   .setSsl(true);

            if (ssl.preferOpenSSL() && OpenSSLEngineOptions.isAvailable())
            {
                LOGGER.info("Using OpenSSL for encryption");
                options.setSslEngineOptions(new OpenSSLEngineOptions().setSessionCacheEnabled(true));
            }
            else
            {
                LOGGER.warn("OpenSSL not enabled, using JDK for TLS");
            }

            configureSSLOptions(options.getSslOptions(), ssl, 0);
        }

        options.setTrafficShapingOptions(buildTrafficShapingOptions(serviceConf.trafficShapingConfiguration()));
        return options;
    }

    /**
     * Configures the SSL options for the server
     *
     * @param options   the SSL options
     * @param ssl       the SSL configuration
     * @param timestamp a timestamp for the keystore file for when the file was last changed, or 0 for the startup value
     */
    protected void configureSSLOptions(SSLOptions options, SslConfiguration ssl, long timestamp)
    {
        options.setSslHandshakeTimeout(ssl.handshakeTimeoutInSeconds())
               .setSslHandshakeTimeoutUnit(SECONDS);

        configureKeyStore(options, ssl, timestamp);
        configureTrustStore(options, ssl);
    }

    /**
     * Configures the key store
     *
     * @param options   the SSL options
     * @param ssl       the SSL configuration
     * @param timestamp a timestamp for the keystore file for when the file was last changed, or 0 for the startup value
     */
    protected void configureKeyStore(SSLOptions options, SslConfiguration ssl, long timestamp)
    {
        SslUtils.setKeyStoreConfiguration(options, ssl.keystore(), timestamp);
    }

    /**
     * Configures the trust store if provided
     *
     * @param options the SSL options
     * @param ssl     the SSL configuration
     */
    protected void configureTrustStore(SSLOptions options, SslConfiguration ssl)
    {
        if (ssl.isTrustStoreConfigured())
        {
            SslUtils.setTrustStoreConfiguration(options, ssl.truststore());
        }
    }

    /**
     * Returns the built {@link TrafficShapingOptions} that are going to be applied to the server.
     *
     * @param trafficShapingConfig the configuration for the traffic shaping options.
     * @return the built {@link TrafficShapingOptions} from the {@link TrafficShapingConfiguration}
     */
    protected TrafficShapingOptions buildTrafficShapingOptions(TrafficShapingConfiguration trafficShapingConfig)
    {
        return new TrafficShapingOptions()
               .setInboundGlobalBandwidth(trafficShapingConfig.inboundGlobalBandwidthBytesPerSecond())
               .setOutboundGlobalBandwidth(trafficShapingConfig.outboundGlobalBandwidthBytesPerSecond())
               .setPeakOutboundGlobalBandwidth(trafficShapingConfig.peakOutboundGlobalBandwidthBytesPerSecond())
               .setCheckIntervalForStats(trafficShapingConfig.checkIntervalForStatsMillis())
               .setCheckIntervalForStatsTimeUnit(MILLISECONDS)
               .setMaxDelayToWait(trafficShapingConfig.maxDelayToWaitMillis())
               .setMaxDelayToWaitUnit(MILLISECONDS);
    }
}
