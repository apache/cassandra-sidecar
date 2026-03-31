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

package org.apache.cassandra.sidecar.cdc;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.secrets.SecretsProvider;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SidecarClientSecretsProvider}
 */
public class SidecarClientSecretsProviderTests
{
    @Test
    void testSecretsProviderWithSslEnabledNoKeystoreNoTruststore()
    {
        SslConfiguration sslConfig = mockSslConfiguration(
            true,
            true,
            "REQUIRED",
            Arrays.asList("TLS_RSA_128"),
            Arrays.asList("TLSv1.2"),
            "10s",
            false,
            false
        );

        SidecarConfiguration sidecarConfiguration = mockSidecarConfiguration(sslConfig);

        SecretsProvider result = new SidecarClientSecretsProvider(sidecarConfiguration);

        assertThat(result).isNotNull();
    }

    @Test
    void testSecretsProviderWithKeystoreOnly()
    {
        KeyStoreConfiguration keystoreConfig = mockKeystoreConfiguration(
            "/path/to/keystore.jks",
            "keystorePassword",
            "JKS"
        );

        SslConfiguration sslConfig = mockSslConfiguration(
            true,
            false,
            "OPTIONAL",
            Arrays.asList("TLS_RSA_256"),
            Arrays.asList("TLSv1.3"),
            "15s",
            true,
            false
        );

        when(sslConfig.keystore()).thenReturn(keystoreConfig);

        SidecarConfiguration sidecarConfiguration = mockSidecarConfiguration(sslConfig);

        SecretsProvider result = new SidecarClientSecretsProvider(sidecarConfiguration);

        assertThat(result).isNotNull();
        assertThat(result.keyStoreType()).isEqualTo("JKS");
        assertThat(result.keyStorePassword()).isEqualTo("keystorePassword".toCharArray());
    }

    @Test
    void testSecretsProviderWithTruststoreOnly()
    {
        // SslConfig validation requires keystore password when any SSL config is provided
        // This test validates that truststore-only configuration is rejected
        KeyStoreConfiguration truststoreConfig = mockKeystoreConfiguration(
            "/path/to/truststore.jks",
            "truststorePassword",
            "PKCS12"
        );

        SslConfiguration sslConfig = mockSslConfiguration(
            true,
            true,
            "NONE",
            Collections.emptyList(),
            Arrays.asList("TLSv1.2", "TLSv1.3"),
            "20s",
            false,
            true
        );

        when(sslConfig.truststore()).thenReturn(truststoreConfig);

        SidecarConfiguration sidecarConfiguration = mockSidecarConfiguration(sslConfig);

        IllegalArgumentException exception = org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new SidecarClientSecretsProvider(sidecarConfiguration)
        );

        assertThat(exception.getMessage()).contains("KEYSTORE_PASSWORD");
    }

    @Test
    void testSecretsProviderWithBothKeystoreAndTruststore()
    {
        KeyStoreConfiguration keystoreConfig = mockKeystoreConfiguration(
            "/path/to/keystore.p12",
            "keystorePass123",
            "PKCS12"
        );

        KeyStoreConfiguration truststoreConfig = mockKeystoreConfiguration(
            "/path/to/truststore.p12",
            "truststorePass456",
            "PKCS12"
        );

        SslConfiguration sslConfig = mockSslConfiguration(
            true,
            true,
            "REQUIRED",
            Arrays.asList("TLS_ECDHE_RSA", "TLS_AES_256"),
            Arrays.asList("TLSv1.2", "TLSv1.3"),
            "30s",
            true,
            true
        );

        when(sslConfig.keystore()).thenReturn(keystoreConfig);
        when(sslConfig.truststore()).thenReturn(truststoreConfig);

        SidecarConfiguration sidecarConfiguration = mockSidecarConfiguration(sslConfig);

        SecretsProvider result = new SidecarClientSecretsProvider(sidecarConfiguration);

        assertThat(result).isNotNull();
        assertThat(result.keyStoreType()).isEqualTo("PKCS12");
        assertThat(result.keyStorePassword()).isEqualTo("keystorePass123".toCharArray());
        assertThat(result.trustStoreType()).isEqualTo("PKCS12");
        assertThat(result.trustStorePassword()).isEqualTo("truststorePass456".toCharArray());
    }

    @Test
    void testSecretsProviderUsesCorrectSslConfigKeys()
    {
        KeyStoreConfiguration keystoreConfig = mockKeystoreConfiguration(
            "/path/to/keystore.jks",
            "keystorePassword",
            "JKS"
        );

        KeyStoreConfiguration truststoreConfig = mockKeystoreConfiguration(
            "/path/to/truststore.jks",
            "truststorePassword",
            "PKCS12"
        );

        SslConfiguration sslConfig = mockSslConfiguration(
            true,
            false,
            "REQUIRED",
            Collections.emptyList(),
            Arrays.asList("TLSv1.2"),
            "10s",
            true,
            true
        );

        when(sslConfig.keystore()).thenReturn(keystoreConfig);
        when(sslConfig.truststore()).thenReturn(truststoreConfig);

        SidecarConfiguration sidecarConfiguration = mockSidecarConfiguration(sslConfig);

        SecretsProvider result = new SidecarClientSecretsProvider(sidecarConfiguration);

        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(SidecarClientSecretsProvider.class);

        assertThat(result.keyStoreType()).isEqualTo("JKS");
        assertThat(result.keyStorePassword()).isEqualTo("keystorePassword".toCharArray());

        assertThat(result.trustStoreType()).isEqualTo("PKCS12");
        assertThat(result.trustStorePassword()).isEqualTo("truststorePassword".toCharArray());
    }

    private SidecarConfiguration mockSidecarConfiguration(SslConfiguration sslConfiguration)
    {
        SidecarConfiguration sidecarConfiguration = mock(SidecarConfiguration.class, RETURNS_DEEP_STUBS);
        when(sidecarConfiguration.sidecarClientConfiguration().sslConfiguration()).thenReturn(sslConfiguration);
        return sidecarConfiguration;
    }

    private SslConfiguration mockSslConfiguration(boolean enabled,
                                                   boolean preferOpenSSL,
                                                   String clientAuth,
                                                   java.util.List<String> cipherSuites,
                                                   java.util.List<String> secureTransportProtocols,
                                                   String handshakeTimeout,
                                                   boolean keystoreConfigured,
                                                   boolean truststoreConfigured)
    {
        SslConfiguration sslConfig = mock(SslConfiguration.class, RETURNS_DEEP_STUBS);
        when(sslConfig.enabled()).thenReturn(enabled);
        when(sslConfig.preferOpenSSL()).thenReturn(preferOpenSSL);
        when(sslConfig.clientAuth()).thenReturn(clientAuth);
        when(sslConfig.cipherSuites()).thenReturn(cipherSuites);
        when(sslConfig.secureTransportProtocols()).thenReturn(secureTransportProtocols);

        SecondBoundConfiguration durationSpec = mock(SecondBoundConfiguration.class);
        when(durationSpec.toString()).thenReturn(handshakeTimeout);
        when(sslConfig.handshakeTimeout()).thenReturn(durationSpec);

        when(sslConfig.isKeystoreConfigured()).thenReturn(keystoreConfigured);
        when(sslConfig.isTrustStoreConfigured()).thenReturn(truststoreConfigured);

        return sslConfig;
    }

    private KeyStoreConfiguration mockKeystoreConfiguration(String path, String password, String type)
    {
        KeyStoreConfiguration config = mock(KeyStoreConfiguration.class);
        when(config.path()).thenReturn(path);
        when(config.password()).thenReturn(password);
        when(config.type()).thenReturn(type);
        return config;
    }
}
