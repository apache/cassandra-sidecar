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

package org.apache.cassandra.sidecar.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;

/**
 * Encapsulates SSL Configuration
 */
public class SslConfiguration
{
    @JsonProperty("enabled")
    private final boolean enabled;

    @JsonProperty("keystore")
    private final KeyStoreConfiguration keystore;

    @JsonProperty("truststore")
    private final KeyStoreConfiguration truststore;

    public SslConfiguration()
    {
        this.enabled = false;
        this.keystore = null;
        this.truststore = null;
    }

    protected SslConfiguration(Builder<?> builder)
    {
        enabled = builder.enabled;
        keystore = builder.keystore;
        truststore = builder.truststore;
    }

    /**
     * @return {@code true} if SSL is enabled, {@code false} otherwise
     */
    @JsonProperty("enabled")
    public boolean enabled()
    {
        return enabled;
    }

    /**
     * @return {@code true} if the keystore is configured, and the {@link KeyStoreConfiguration#path()} and
     * {@link KeyStoreConfiguration#password()} parameters are provided
     */
    public boolean isKeystoreConfigured()
    {
        return keystore != null && keystore.isConfigured();
    }

    /**
     * @return the configuration for the keystore
     */
    @JsonProperty("keystore")
    public KeyStoreConfiguration keystore()
    {
        return keystore;
    }

    /**
     * @return {@code true} if the truststore is configured, and the {@link KeyStoreConfiguration#path()} and
     * {@link KeyStoreConfiguration#password()} parameters are provided
     */
    public boolean isTruststoreConfigured()
    {
        return truststore != null && truststore.isConfigured();
    }

    /**
     * @return the configuration for the truststore
     */
    @JsonProperty("truststore")
    public KeyStoreConfiguration truststore()
    {
        return truststore;
    }

    public static Builder<?> builder()
    {
        return new Builder<>();
    }

    /**
     * {@code SslConfiguration} builder static inner class.
     */
    public static class Builder<T extends Builder<?>> implements DataObjectBuilder<T, SslConfiguration>
    {
        protected boolean enabled = false;
        protected KeyStoreConfiguration keystore;
        protected KeyStoreConfiguration truststore;

        protected Builder()
        {
        }

        /**
         * Sets the {@code enabled} and returns a reference to this Builder enabling method chaining.
         *
         * @param enabled the {@code enabled} to set
         * @return a reference to this Builder
         */
        public T enabled(boolean enabled)
        {
            return override(b -> b.enabled = enabled);
        }

        /**
         * Sets the {@code keystore} and returns a reference to this Builder enabling method chaining.
         *
         * @param keystore the {@code keystore} to set
         * @return a reference to this Builder
         */
        public T keystore(KeyStoreConfiguration keystore)
        {
            return override(b -> b.keystore = keystore);
        }

        /**
         * Sets the {@code truststore} and returns a reference to this Builder enabling method chaining.
         *
         * @param truststore the {@code truststore} to set
         * @return a reference to this Builder
         */
        public T truststore(KeyStoreConfiguration truststore)
        {
            return override((T b) -> b.truststore = truststore);
        }

        /**
         * Returns a {@code SslConfiguration} built from the parameters previously set.
         *
         * @return a {@code SslConfiguration} built with parameters of this {@code SslConfiguration.Builder}
         */
        @Override
        public SslConfiguration build()
        {
            return new SslConfiguration(this);
        }
    }
}
