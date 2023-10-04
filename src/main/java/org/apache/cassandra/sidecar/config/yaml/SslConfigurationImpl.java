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

package org.apache.cassandra.sidecar.config.yaml;

import java.util.Arrays;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.core.http.ClientAuth;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;

/**
 * Encapsulates SSL Configuration
 */
public class SslConfigurationImpl implements SslConfiguration
{
    public static final boolean DEFAULT_SSL_ENABLED = false;
    public static final boolean DEFAULT_USE_OPEN_SSL = true;
    public static final long DEFAULT_HANDSHAKE_TIMEOUT_SECONDS = 10L;
    public static final String DEFAULT_CLIENT_AUTH = "NONE";


    @JsonProperty("enabled")
    protected final boolean enabled;

    @JsonProperty(value = "use_openssl", defaultValue = "true")
    protected final boolean useOpenSsl;

    @JsonProperty(value = "handshake_timeout_sec", defaultValue = "10")
    protected final long handshakeTimeoutInSeconds;

    protected String clientAuth;

    @JsonProperty("keystore")
    protected final KeyStoreConfiguration keystore;

    @JsonProperty("truststore")
    protected final KeyStoreConfiguration truststore;

    public SslConfigurationImpl()
    {
        this(DEFAULT_SSL_ENABLED,
             DEFAULT_USE_OPEN_SSL,
             DEFAULT_HANDSHAKE_TIMEOUT_SECONDS,
             DEFAULT_CLIENT_AUTH,
             null,
             null);
    }

    public SslConfigurationImpl(boolean enabled,
                                boolean useOpenSsl,
                                long handshakeTimeoutInSeconds,
                                String clientAuth,
                                KeyStoreConfiguration keystore,
                                KeyStoreConfiguration truststore)
    {
        this.enabled = enabled;
        this.useOpenSsl = useOpenSsl;
        this.handshakeTimeoutInSeconds = handshakeTimeoutInSeconds;
        this.clientAuth = clientAuth;
        this.keystore = keystore;
        this.truststore = truststore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("enabled")
    public boolean enabled()
    {
        return enabled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "use_openssl", defaultValue = "true")
    public boolean useOpenSSL()
    {
        return useOpenSsl;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "handshake_timeout_sec", defaultValue = "10")
    public long handshakeTimeoutInSeconds()
    {
        return handshakeTimeoutInSeconds;
    }

    @Override
    @JsonProperty(value = "client_auth", defaultValue = "NONE")
    public String clientAuth()
    {
        return clientAuth;
    }

    @JsonProperty(value = "client_auth", defaultValue = "NONE")
    public void setClientAuth(String clientAuth)
    {
        this.clientAuth = clientAuth;
        try
        {
            // forces a validation of the input
            this.clientAuth = ClientAuth.valueOf(clientAuth).name();
        }
        catch (IllegalArgumentException exception)
        {
            String errorMessage = String.format("Invalid client_auth configuration=\"%s\", valid values are (%s)",
                                                clientAuth,
                                                Arrays.stream(ClientAuth.values())
                                                      .map(Enum::name)
                                                      .collect(Collectors.joining(",")));
            throw new IllegalArgumentException(errorMessage);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("keystore")
    public KeyStoreConfiguration keystore()
    {
        return keystore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTrustStoreConfigured()
    {
        return truststore != null && truststore.isConfigured();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("truststore")
    public KeyStoreConfiguration truststore()
    {
        return truststore;
    }
}
