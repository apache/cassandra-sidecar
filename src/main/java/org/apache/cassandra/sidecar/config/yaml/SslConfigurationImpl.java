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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;

/**
 * Encapsulates SSL Configuration
 */
public class SslConfigurationImpl implements SslConfiguration
{
    @JsonProperty("enabled")
    protected final boolean enabled;

    @JsonProperty("keystore")
    protected final KeyStoreConfiguration keystore;

    @JsonProperty("truststore")
    protected final KeyStoreConfiguration truststore;

    public SslConfigurationImpl()
    {
        this(false, null, null);
    }

    public SslConfigurationImpl(boolean enabled,
                                KeyStoreConfiguration keystore,
                                KeyStoreConfiguration truststore)
    {
        this.enabled = enabled;
        this.keystore = keystore;
        this.truststore = truststore;
    }

    /**
     * @return {@code true} if SSL is enabled, {@code false} otherwise
     */
    @Override
    @JsonProperty("enabled")
    public boolean enabled()
    {
        return enabled;
    }

    /**
     * @return {@code true} if the keystore is configured, and the {@link KeyStoreConfiguration#path()} and
     * {@link KeyStoreConfiguration#password()} parameters are provided
     */
    @Override
    public boolean isKeystoreConfigured()
    {
        return keystore != null && keystore.isConfigured();
    }

    /**
     * @return the configuration for the keystore
     */
    @Override
    @JsonProperty("keystore")
    public KeyStoreConfiguration keystore()
    {
        return keystore;
    }

    /**
     * @return {@code true} if the truststore is configured, and the {@link KeyStoreConfiguration#path()} and
     * {@link KeyStoreConfiguration#password()} parameters are provided
     */
    @Override
    public boolean isTruststoreConfigured()
    {
        return truststore != null && truststore.isConfigured();
    }

    /**
     * @return the configuration for the truststore
     */
    @Override
    @JsonProperty("truststore")
    public KeyStoreConfiguration truststore()
    {
        return truststore;
    }
}
