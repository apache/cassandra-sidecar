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

/**
 * Encapsulates SSL Configuration
 */
public interface SslConfiguration
{
    /**
     * @return {@code true} if SSL is enabled, {@code false} otherwise
     */
    boolean enabled();

    /**
     * Returns {@code true} if the OpenSSL engine should be used, {@code false} otherwise. The OpenSSL engine
     * will only be enabled if the native libraries for OpenSSL have been loaded correctly.
     *
     * @return {@code true} if the OpenSSL engine should be used, {@code false} otherwise
     */
    boolean useOpenSSL();

    /**
     * @return the configuration for the SSL handshake timeout in seconds
     */
    long handshakeTimeoutInSeconds();

    /**
     * Returns the client authentication mode. Valid values are {@code NONE}, {@code REQUEST}, and {@code REQUIRED}.
     * When the authentication mode is set to {@code REQUIRED} then server will require the SSL certificate to be
     * presented, otherwise it won't accept the request. When the authentication mode is set to {@code REQUEST}, the
     * certificate is optional.
     *
     * @return the client authentication mode
     */
    String clientAuth();

    /**
     * @return {@code true} if the keystore is configured, and the {@link KeyStoreConfiguration#path()} and
     * {@link KeyStoreConfiguration#password()} parameters are provided
     */
    default boolean isKeystoreConfigured()
    {
        return keystore() != null && keystore().isConfigured();
    }

    /**
     * @return the configuration for the keystore
     */
    KeyStoreConfiguration keystore();

    /**
     * @return {@code true} if the truststore is configured, and the {@link KeyStoreConfiguration#path()} and
     * {@link KeyStoreConfiguration#password()} parameters are provided
     */
    default boolean isTrustStoreConfigured()
    {
        return truststore() != null && truststore().isConfigured();
    }

    /**
     * @return the configuration for the truststore
     */
    KeyStoreConfiguration truststore();
}
