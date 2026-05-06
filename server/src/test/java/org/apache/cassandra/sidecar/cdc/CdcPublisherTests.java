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
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.sidecar.CdcSidecarInstancesProvider;
import org.apache.cassandra.cdc.sidecar.ClusterConfigProvider;
import org.apache.cassandra.cdc.sidecar.ReplicationFactorSupplier;
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
import org.apache.cassandra.sidecar.db.SidecarRegistryCache;
import org.apache.cassandra.sidecar.db.VirtualTablesDatabaseAccessor;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.kafka.common.serialization.Serializer;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
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
    @Mock
    private SidecarRegistryCache sidecarRegistryCache;

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
            rangeManager,
            sidecarRegistryCache
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
    void testCreateReplicationFactorSupplierReturnsSidecarImpl()
    {
        ReplicationFactorSupplier supplier = cdcPublisher.createReplicationFactorSupplier();
        assertThat(supplier).isInstanceOf(SidecarReplicationFactorSupplier.class);
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
        when(instanceMetadataFetcher.callOnFirstAvailableInstance(any())).thenReturn("4.1.11");

        EventConsumer result = cdcPublisher.eventConsumer(cdcConfig, avroSerializer);

        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(CdcEventConsumer.class);
    }

    @Test
    void testResolveSidecarPortReturnsCachedPortWhenPresent()
    {
        // Arrange: registry cache has an entry for the requested host.
        String host = "10.0.0.1";
        int cachedPort = 28747;
        int defaultPort = 9043;
        CassandraInstance instance = mock(CassandraInstance.class);
        when(instance.nodeName()).thenReturn(host);
        when(sidecarRegistryCache.getPort(host)).thenReturn(cachedPort);

        // Act
        Integer resolved = cdcPublisher.resolveSidecarPort(instance, defaultPort);

        // Assert: cached value wins; default port must not be returned.
        assertThat(resolved).isEqualTo(cachedPort);
    }

    @Test
    void testResolveSidecarPortFallsBackToDefaultWhenCacheMisses()
    {
        // Arrange: registry cache has no entry for the requested host.
        String host = "10.0.0.2";
        int defaultPort = 9043;
        CassandraInstance instance = mock(CassandraInstance.class);
        when(instance.nodeName()).thenReturn(host);
        when(sidecarRegistryCache.getPort(host)).thenReturn(null);
        when(sidecarRegistryCache.size()).thenReturn(0L);

        // Act
        Integer resolved = cdcPublisher.resolveSidecarPort(instance, defaultPort);

        // Assert: caller-provided default port is returned on miss.
        assertThat(resolved).isEqualTo(defaultPort);
    }

    @Test
    void testResolveSidecarPortLooksUpUsingNodeName()
    {
        // Arrange: ensure the lookup key is the instance's nodeName(), not some other field.
        String host = "cassandra-host-3.dc1.example.com";
        int cachedPort = 30001;
        CassandraInstance instance = mock(CassandraInstance.class);
        when(instance.nodeName()).thenReturn(host);
        when(sidecarRegistryCache.getPort(host)).thenReturn(cachedPort);

        // Act
        Integer resolved = cdcPublisher.resolveSidecarPort(instance, 9999);

        // Assert
        assertThat(resolved).isEqualTo(cachedPort);
        verify(instance).nodeName();
        verify(sidecarRegistryCache).getPort(host);
    }

    @Test
    void testResolveSidecarPortReturnsZeroFromCacheWhenExplicitlyCached()
    {
        // Edge case: a cached entry of 0 (autoboxed Integer, not null) is still "present" and must win
        // over the default port. This guards against accidentally treating 0 as "missing".
        String host = "10.0.0.3";
        CassandraInstance instance = mock(CassandraInstance.class);
        when(instance.nodeName()).thenReturn(host);
        when(sidecarRegistryCache.getPort(host)).thenReturn(0);

        Integer resolved = cdcPublisher.resolveSidecarPort(instance, 9043);

        assertThat(resolved).isEqualTo(0);
    }

    @Test
    void testResolveSidecarPortDoesNotCallSizeOnCacheHit()
    {
        // size() is only consulted for the fallback log line; on a cache hit it must not be called.
        String host = "10.0.0.4";
        CassandraInstance instance = mock(CassandraInstance.class);
        when(instance.nodeName()).thenReturn(host);
        when(sidecarRegistryCache.getPort(host)).thenReturn(28747);

        cdcPublisher.resolveSidecarPort(instance, 9043);

        verify(sidecarRegistryCache).getPort(host);
        verify(sidecarRegistryCache, never()).size();
    }

    @Test
    void testResolveSidecarPortConsultsCacheSizeOnFallback()
    {
        // On a cache miss the publisher logs cacheSize for diagnostics; verify size() is queried.
        String host = "10.0.0.5";
        CassandraInstance instance = mock(CassandraInstance.class);
        when(instance.nodeName()).thenReturn(host);
        when(sidecarRegistryCache.getPort(host)).thenReturn(null);
        when(sidecarRegistryCache.size()).thenReturn(7L);

        Integer resolved = cdcPublisher.resolveSidecarPort(instance, 9043);

        assertThat(resolved).isEqualTo(9043);
        verify(sidecarRegistryCache).size();
    }

    // ---- portResolver() — the Function<CassandraInstance,Integer> handed to SidecarCdcClient ----

    @Test
    void testPortResolverIsNotNull()
    {
        // Smoke-check: the builder must always return a usable Function reference;
        // SidecarCdcClient construction would NPE otherwise.
        java.util.function.Function<CassandraInstance, Integer> resolver = cdcPublisher.portResolver(9043);
        assertThat(resolver).isNotNull();
    }

    @Test
    void testPortResolverReturnsCachedPortFromRegistry()
    {
        // Verifies the Function delegates to resolveSidecarPort(): a cache hit returns the cached port.
        String host = "10.0.0.10";
        int cachedPort = 28747;
        int defaultPort = 9043;
        CassandraInstance instance = mock(CassandraInstance.class);
        when(instance.nodeName()).thenReturn(host);
        when(sidecarRegistryCache.getPort(host)).thenReturn(cachedPort);

        Integer resolved = cdcPublisher.portResolver(defaultPort).apply(instance);

        assertThat(resolved).isEqualTo(cachedPort);
    }

    @Test
    void testPortResolverFallsBackToCapturedDefaultPort()
    {
        // The lambda must close over the defaultPort passed to portResolver(...), not some other value.
        String host = "10.0.0.11";
        int defaultPort = 12345;
        CassandraInstance instance = mock(CassandraInstance.class);
        when(instance.nodeName()).thenReturn(host);
        when(sidecarRegistryCache.getPort(host)).thenReturn(null);
        when(sidecarRegistryCache.size()).thenReturn(0L);

        Integer resolved = cdcPublisher.portResolver(defaultPort).apply(instance);

        assertThat(resolved).isEqualTo(defaultPort);
    }

    @Test
    void testPortResolverEachCallReevaluatesCache()
    {
        // The same Function instance must re-query the cache on every call so that
        // newly-registered peers become visible without rebuilding the SidecarCdcClient.
        String host = "10.0.0.12";
        int defaultPort = 9043;
        CassandraInstance instance = mock(CassandraInstance.class);
        when(instance.nodeName()).thenReturn(host);

        // First call: cache miss -> returns default port.
        when(sidecarRegistryCache.getPort(host)).thenReturn(null);
        when(sidecarRegistryCache.size()).thenReturn(0L);
        java.util.function.Function<CassandraInstance, Integer> resolver = cdcPublisher.portResolver(defaultPort);
        assertThat(resolver.apply(instance)).isEqualTo(defaultPort);

        // Cache is updated with a real port, then call again with the same Function.
        when(sidecarRegistryCache.getPort(host)).thenReturn(28747);
        assertThat(resolver.apply(instance)).isEqualTo(28747);
    }

    @Test
    void testPortResolverInvokesCacheGetPortPerApply()
    {
        // The Function is called once per outbound HTTP request to a peer; verify no caching/memoization
        // hides that fact (otherwise stale ports would be sent).
        String host = "10.0.0.13";
        int defaultPort = 9043;
        CassandraInstance instance = mock(CassandraInstance.class);
        when(instance.nodeName()).thenReturn(host);
        when(sidecarRegistryCache.getPort(host)).thenReturn(28747);

        java.util.function.Function<CassandraInstance, Integer> resolver = cdcPublisher.portResolver(defaultPort);
        resolver.apply(instance);
        resolver.apply(instance);
        resolver.apply(instance);

        verify(sidecarRegistryCache, org.mockito.Mockito.times(3)).getPort(host);
    }

    @Test
    void testPortResolverSupportsDifferentDefaultPortsAcrossInvocations()
    {
        // Two SidecarCdcClient instances with different default ports must not cross-contaminate state.
        String host = "10.0.0.14";
        CassandraInstance instance = mock(CassandraInstance.class);
        when(instance.nodeName()).thenReturn(host);
        when(sidecarRegistryCache.getPort(host)).thenReturn(null);
        when(sidecarRegistryCache.size()).thenReturn(0L);

        java.util.function.Function<CassandraInstance, Integer> resolverA = cdcPublisher.portResolver(1111);
        java.util.function.Function<CassandraInstance, Integer> resolverB = cdcPublisher.portResolver(2222);

        assertThat(resolverA.apply(instance)).isEqualTo(1111);
        assertThat(resolverB.apply(instance)).isEqualTo(2222);
    }

    // ---- createSidecarCdcClient() — covers the variable declaration + construction at run() lines 232+ ----

    @Test
    void testCreateSidecarCdcClientReturnsClientFromConstructor()
    {
        // Stub effectivePort() so portResolver(defaultPort) closes over a known value.
        when(clientConfig.effectivePort()).thenReturn(9043);
        // SSL disabled so secretsProvider() returns null (already covered above).
        SslConfiguration sslConfig = mock(SslConfiguration.class);
        when(sidecarConfiguration.sidecarClientConfiguration().sslConfiguration()).thenReturn(sslConfig);
        when(sslConfig.enabled()).thenReturn(false);

        // Mock the SidecarCdcClient constructor itself so we don't open real sockets.
        try (org.mockito.MockedConstruction<SidecarCdcClient> ignored =
                 org.mockito.Mockito.mockConstruction(SidecarCdcClient.class))
        {
            SidecarCdcClient client = cdcPublisher.createSidecarCdcClient();

            assertThat(client).isNotNull();
            assertThat(ignored.constructed()).hasSize(1);
        }
    }

    @Test
    void testCreateSidecarCdcClientReadsDefaultPortFromClientConfig()
    {
        // Verifies the line `int defaultPort = clientConfig.effectivePort();` is exercised
        // and the value flows into the constructor (via portResolver).
        when(clientConfig.effectivePort()).thenReturn(28747);
        SslConfiguration sslConfig = mock(SslConfiguration.class);
        when(sidecarConfiguration.sidecarClientConfiguration().sslConfiguration()).thenReturn(sslConfig);
        when(sslConfig.enabled()).thenReturn(false);

        try (org.mockito.MockedConstruction<SidecarCdcClient> ignored =
                 org.mockito.Mockito.mockConstruction(SidecarCdcClient.class))
        {
            cdcPublisher.createSidecarCdcClient();

            verify(clientConfig).effectivePort();
        }
    }


    /**
     * Covers line 243 ({@code SidecarCdcClient sidecarCdcClient = createSidecarCdcClient();} inside
     * {@code run()}).  The method is {@code private}, so it is reached indirectly via
     * {@link CdcPublisher#execute(Promise)}.
     *
     * <p>The test subclass overrides {@link CdcPublisher#eventConsumer} to prevent real
     * {@code KafkaProducer} construction.  {@link org.mockito.MockedConstruction} is used for
     * {@link SidecarCdcClient} and {@link CdcManager} so neither real I/O nor a live Cassandra
     * cluster is required.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testExecuteRunsHappyPathAndCoversLine243()
    {
        EventConsumer mockConsumer = mock(EventConsumer.class);

        CdcPublisher testPublisher = new CdcPublisher(
            vertx, sidecarConfiguration, executorPools, clusterConfigProvider,
            schemaSupplier, sidecarInstancesProvider, clientConfig,
            instanceMetadataFetcher, cdcConfig, databaseAccessor,
            cdcStats, virtualTables, sidecarCdcStats, avroSerializer,
            rangeManager, sidecarRegistryCache)
        {
            @Override
            public EventConsumer eventConsumer(CdcConfig conf, Serializer<CdcEvent> serializer)
            {
                return mockConsumer;
            }
        };

        when(clientConfig.effectivePort()).thenReturn(9043);
        SslConfiguration sslConfig = mock(SslConfiguration.class);
        when(sidecarConfiguration.sidecarClientConfiguration().sslConfiguration()).thenReturn(sslConfig);
        when(sslConfig.enabled()).thenReturn(false);

        try (org.mockito.MockedConstruction<SidecarCdcClient> mockedClient =
                 org.mockito.Mockito.mockConstruction(SidecarCdcClient.class);
             org.mockito.MockedConstruction<CdcManager> mockedManager =
                 org.mockito.Mockito.mockConstruction(CdcManager.class, (m, ctx) ->
                     when(m.buildCdcConsumers()).thenReturn(Collections.emptyList())))
        {
            Promise<Void> promise = mock(Promise.class);
            testPublisher.execute(promise);

            // createSidecarCdcClient() (line 243) was reached: exactly one SidecarCdcClient was built
            assertThat(mockedClient.constructed()).hasSize(1);
            // new CdcManager(...) (line 244) was also reached
            assertThat(mockedManager.constructed()).hasSize(1);
            verify(promise).complete();
        }
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
