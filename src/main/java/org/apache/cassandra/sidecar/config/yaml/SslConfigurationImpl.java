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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.core.http.ClientAuth;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
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
    public static final List<String> DEFAULT_SECURE_TRANSPORT_PROTOCOLS
    = Collections.unmodifiableList(Arrays.asList("TLSv1.2", "TLSv1.3"));


    @JsonProperty("enabled")
    protected final boolean enabled;

    @JsonProperty(value = "use_openssl", defaultValue = "true")
    protected final boolean useOpenSsl;

    @JsonProperty(value = "handshake_timeout_sec", defaultValue = "10")
    protected final long handshakeTimeoutInSeconds;

    protected String clientAuth;

    @JsonProperty(value = "cipher_suites")
    protected final List<String> cipherSuites;

    @JsonProperty(value = "accepted_protocols")
    protected final List<String> secureTransportProtocols;

    @JsonProperty("keystore")
    protected final KeyStoreConfiguration keystore;

    @JsonProperty("truststore")
    protected final KeyStoreConfiguration truststore;

    public SslConfigurationImpl()
    {
        this(builder());
    }

    protected SslConfigurationImpl(Builder builder)
    {
        enabled = builder.enabled;
        useOpenSsl = builder.useOpenSsl;
        handshakeTimeoutInSeconds = builder.handshakeTimeoutInSeconds;
        setClientAuth(builder.clientAuth);
        keystore = builder.keystore;
        truststore = builder.truststore;
        cipherSuites = builder.cipherSuites;
        secureTransportProtocols = builder.secureTransportProtocols;
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
    public boolean preferOpenSSL()
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

    /**
     * {@inheritDoc}
     */
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
    @JsonProperty(value = "cipher_suites")
    public List<String> cipherSuites()
    {
        return cipherSuites;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "accepted_protocols")
    public List<String> secureTransportProtocols()
    {
        return secureTransportProtocols;
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

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code SslConfigurationImpl} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, SslConfigurationImpl>
    {
        protected boolean enabled = DEFAULT_SSL_ENABLED;
        protected boolean useOpenSsl = DEFAULT_USE_OPEN_SSL;
        protected long handshakeTimeoutInSeconds = DEFAULT_HANDSHAKE_TIMEOUT_SECONDS;
        protected String clientAuth = DEFAULT_CLIENT_AUTH;
        protected List<String> cipherSuites = Collections.emptyList();
        protected List<String> secureTransportProtocols = DEFAULT_SECURE_TRANSPORT_PROTOCOLS;
        protected KeyStoreConfiguration keystore = null;
        protected KeyStoreConfiguration truststore = null;

        protected Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code enabled} and returns a reference to this Builder enabling method chaining.
         *
         * @param enabled the {@code enabled} to set
         * @return a reference to this Builder
         */
        public Builder enabled(boolean enabled)
        {
            return update(b -> b.enabled = enabled);
        }

        /**
         * Sets the {@code useOpenSsl} and returns a reference to this Builder enabling method chaining.
         *
         * @param useOpenSsl the {@code useOpenSsl} to set
         * @return a reference to this Builder
         */
        public Builder useOpenSsl(boolean useOpenSsl)
        {
            return update(b -> b.useOpenSsl = useOpenSsl);
        }

        /**
         * Sets the {@code handshakeTimeoutInSeconds} and returns a reference to this Builder enabling method chaining.
         *
         * @param handshakeTimeoutInSeconds the {@code handshakeTimeoutInSeconds} to set
         * @return a reference to this Builder
         */
        public Builder handshakeTimeoutInSeconds(long handshakeTimeoutInSeconds)
        {
            return update(b -> b.handshakeTimeoutInSeconds = handshakeTimeoutInSeconds);
        }

        /**
         * Sets the {@code clientAuth} and returns a reference to this Builder enabling method chaining.
         *
         * @param clientAuth the {@code clientAuth} to set
         * @return a reference to this Builder
         */
        public Builder clientAuth(String clientAuth)
        {
            return update(b -> b.clientAuth = clientAuth);
        }

        /**
         * Sets the {@code cipherSuites} and returns a reference to this Builder enabling method chaining.
         *
         * @param cipherSuites the {@code cipherSuites} to set
         * @return a reference to this Builder
         */
        public Builder cipherSuites(List<String> cipherSuites)
        {
            return update(b -> b.cipherSuites = new ArrayList<>(cipherSuites));
        }

        /**
         * Sets the {@code secureTransportProtocols} and returns a reference to this Builder enabling method chaining.
         *
         * @param secureTransportProtocols the {@code secureTransportProtocols} to set
         * @return a reference to this Builder
         */
        public Builder secureTransportProtocols(List<String> secureTransportProtocols)
        {
            return update(b -> b.secureTransportProtocols = new ArrayList<>(secureTransportProtocols));
        }

        /**
         * Sets the {@code keystore} and returns a reference to this Builder enabling method chaining.
         *
         * @param keystore the {@code keystore} to set
         * @return a reference to this Builder
         */
        public Builder keystore(KeyStoreConfiguration keystore)
        {
            return update(b -> b.keystore = keystore);
        }

        /**
         * Sets the {@code truststore} and returns a reference to this Builder enabling method chaining.
         *
         * @param truststore the {@code truststore} to set
         * @return a reference to this Builder
         */
        public Builder truststore(KeyStoreConfiguration truststore)
        {
            return update(b -> b.truststore = truststore);
        }

        /**
         * Returns a {@code SslConfigurationImpl} built from the parameters previously set.
         *
         * @return a {@code SslConfigurationImpl} built with parameters of this {@code SslConfigurationImpl.Builder}
         */
        @Override
        public SslConfigurationImpl build()
        {
            return new SslConfigurationImpl(this);
        }
    }
}
