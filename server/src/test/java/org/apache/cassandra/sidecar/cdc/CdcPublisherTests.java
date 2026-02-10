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
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.inject.Provider;
import io.vertx.core.Vertx;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.sidecar.CdcSidecarInstancesProvider;
import org.apache.cassandra.cdc.sidecar.ClusterConfigProvider;
import org.apache.cassandra.cdc.sidecar.SidecarCdcClient;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.secrets.SecretsProvider;
import org.apache.cassandra.secrets.SslConfigSecretsProvider;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.coordination.RangeManager;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.db.VirtualTablesDatabaseAccessor;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.kafka.common.serialization.Serializer;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for CdcPublisher
 */
public class CdcPublisherTests
{
    @Mock
    private Vertx vertx;
    @Mock
    private ExecutorPools executorPools;
    @Mock
    private TaskExecutorPool taskExecutorPool;
    @Mock
    private ClusterConfigProvider clusterConfigProvider;
    @Mock
    private SchemaSupplier schemaSupplier;
    @Mock
    private CdcSidecarInstancesProvider sidecarInstancesProvider;
    @Mock
    private SidecarCdcClient.ClientConfig clientConfig;
    @Mock
    private InstanceMetadataFetcher instanceMetadataFetcher;
    @Mock
    private CdcDatabaseAccessor databaseAccessor;
    @Mock
    private ICdcStats cdcStats;
    @Mock
    private VirtualTablesDatabaseAccessor virtualTables;
    @Mock
    private SidecarCdcStats sidecarCdcStats;
    @Mock
    private Serializer<CdcEvent> avroSerializer;
    @Mock
    private Provider<RangeManager> rangeManager;

    private SidecarConfiguration sidecarConfiguration;
    private CdcConfig cdcConfig;
    private CdcPublisher cdcPublisher;

    @BeforeEach
    void setUp()
    {
        MockitoAnnotations.openMocks(this);

        // Mock deep stubs for complex configuration objects
        sidecarConfiguration = mock(SidecarConfiguration.class, RETURNS_DEEP_STUBS);
        cdcConfig = mock(CdcConfig.class, RETURNS_DEEP_STUBS);

        // Mock ExecutorPools behavior
        when(executorPools.internal()).thenReturn(taskExecutorPool);

        // Mock Vertx EventBus for event listeners
        when(vertx.eventBus()).thenReturn(mock(io.vertx.core.eventbus.EventBus.class, RETURNS_DEEP_STUBS));

        cdcPublisher = new CdcPublisher(
            vertx,
            sidecarConfiguration,
            executorPools,
            clusterConfigProvider,
            schemaSupplier,
            sidecarInstancesProvider,
            clientConfig,
            instanceMetadataFetcher,
            cdcConfig,
            databaseAccessor,
            cdcStats,
            virtualTables,
            sidecarCdcStats,
            avroSerializer,
            rangeManager
        );
    }


    @Test
    void testSecretsProviderReturnsNullWhenSslDisabled()
    {
        SslConfiguration sslConfig = mock(SslConfiguration.class);
        when(sidecarConfiguration.sidecarClientConfiguration().sslConfiguration()).thenReturn(sslConfig);
        when(sslConfig.enabled()).thenReturn(false);

        SecretsProvider result = cdcPublisher.secretsProvider();

        assertThat(result).isNull();
    }

    @Test
    void testSecretsProviderWithSslEnabledNoKeystoreNoTruststore()
    {
        SslConfiguration sslConfig = mockSslConfiguration(
            true,                           // enabled
            true,                           // preferOpenSSL
            "REQUIRED",                     // clientAuth
            Arrays.asList("TLS_RSA_128"),  // cipherSuites
            Arrays.asList("TLSv1.2"),      // secureTransportProtocols
            "10s",                          // handshakeTimeout
            false,                          // keystoreConfigured
            false                           // truststoreConfigured
        );

        when(sidecarConfiguration.sidecarClientConfiguration().sslConfiguration()).thenReturn(sslConfig);

        SecretsProvider result = cdcPublisher.secretsProvider();

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
        when(sidecarConfiguration.sidecarClientConfiguration().sslConfiguration()).thenReturn(sslConfig);

        SecretsProvider result = cdcPublisher.secretsProvider();

        assertThat(result).isNotNull();
        assertThat(result.keyStoreType()).isEqualTo("JKS");
        assertThat(result.keyStorePassword()).isEqualTo("keystorePassword".toCharArray());
    }

    @Test
    void testSecretsProviderWithTruststoreOnly()
    {
        // SslConfig validation requires keystore password to always be provided
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
        when(sidecarConfiguration.sidecarClientConfiguration().sslConfiguration()).thenReturn(sslConfig);

        // SslConfig.create() validates and requires keystore password when any SSL config is provided
        IllegalArgumentException exception = org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> cdcPublisher.secretsProvider()
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
        when(sidecarConfiguration.sidecarClientConfiguration().sslConfiguration()).thenReturn(sslConfig);

        SecretsProvider result = cdcPublisher.secretsProvider();

        assertThat(result).isNotNull();
        assertThat(result.keyStoreType()).isEqualTo("PKCS12");
        assertThat(result.keyStorePassword()).isEqualTo("keystorePass123".toCharArray());
        assertThat(result.trustStoreType()).isEqualTo("PKCS12");
        assertThat(result.trustStorePassword()).isEqualTo("truststorePass456".toCharArray());
    }

    @Test
    void testSecretsProviderUsesCorrectSslConfigKeys()
    {
        // This test validates that CdcPublisher uses SslConfig constants with MapUtils.lowerCaseKey()
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
        when(sidecarConfiguration.sidecarClientConfiguration().sslConfiguration()).thenReturn(sslConfig);

        SecretsProvider result = cdcPublisher.secretsProvider();

        // Validate that the SecretsProvider was created successfully using the correct keys
        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(SslConfigSecretsProvider.class);

        // Verify keystore configuration is accessible
        assertThat(result.keyStoreType()).isEqualTo("JKS");
        assertThat(result.keyStorePassword()).isEqualTo("keystorePassword".toCharArray());

        // Verify truststore configuration is accessible
        assertThat(result.trustStoreType()).isEqualTo("PKCS12");
        assertThat(result.trustStorePassword()).isEqualTo("truststorePassword".toCharArray());
    }

    @Test
    void testEventConsumerCreatesValidConsumer()
    {
        Map<String, Object> kafkaConfigs = new HashMap<>();
        kafkaConfigs.put("bootstrap.servers", "localhost:9092");
        kafkaConfigs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaConfigs.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        when(cdcConfig.kafkaConfigs()).thenReturn(kafkaConfigs);
        when(cdcConfig.kafkaTopic()).thenReturn("test-cdc-topic");
        when(cdcConfig.maxRecordSizeBytes()).thenReturn(1048576); // 1MB
        when(cdcConfig.failOnRecordTooLargeError()).thenReturn(false);
        when(cdcConfig.failOnKafkaError()).thenReturn(true);

        EventConsumer result = cdcPublisher.eventConsumer(cdcConfig, avroSerializer);

        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(CdcEventConsumer.class);
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
