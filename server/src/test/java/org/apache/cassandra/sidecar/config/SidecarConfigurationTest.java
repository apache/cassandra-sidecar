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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.cassandra.sidecar.config.yaml.MetricsFilteringConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.assertj.core.api.Condition;

import static org.apache.cassandra.sidecar.common.ResourceUtils.writeResourceToPath;
import static org.apache.cassandra.sidecar.config.yaml.MetricsFilteringConfigurationImpl.EQUALS_TYPE;
import static org.apache.cassandra.sidecar.config.yaml.MetricsFilteringConfigurationImpl.REGEX_TYPE;
import static org.apache.cassandra.sidecar.config.yaml.VertxMetricsConfigurationImpl.DEFAULT_JMX_DOMAIN_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

/**
 * Tests reading Sidecar {@link SidecarConfiguration} from YAML files
 */
class SidecarConfigurationTest
{
    @TempDir
    private Path configPath;

    @Test
    void testSidecarConfiguration() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_multiple_instances.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        validateMultipleInstancesSidecarConfiguration(config, false);
    }

    @Test
    void testLegacySidecarYAMLFormatWithSingleInstance() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_single_instance.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        validateSingleInstanceSidecarConfiguration(config);
    }

    @Test
    void testReadAllowableTimeSkew() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_custom_allowable_time_skew.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        assertThat(config.serviceConfiguration()).isNotNull();
        assertThat(config.serviceConfiguration().allowableSkewInMinutes()).isEqualTo(1);
    }

    @Test
    void testReadingSingleInstanceSectionOverMultipleInstances() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_with_single_multiple_instances.yaml");
        SidecarConfiguration configuration = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        assertThat(configuration.cassandraInstances()).isNotNull().hasSize(1);

        InstanceConfiguration i1 = configuration.cassandraInstances().get(0);
        assertThat(i1.host()).isEqualTo("localhost");
        assertThat(i1.port()).isEqualTo(9042);
    }

    @Test
    void testReadingCassandraInputValidation() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_validation_configuration.yaml");
        SidecarConfiguration configuration = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        CassandraInputValidationConfiguration validationConfiguration =
        configuration.cassandraInputValidationConfiguration();

        assertThat(validationConfiguration.forbiddenKeyspaces()).contains("a", "b", "c");
        assertThat(validationConfiguration.allowedPatternForName()).isEqualTo("[a-z]+");
        assertThat(validationConfiguration.allowedPatternForQuotedName()).isEqualTo("[A-Z]+");
        assertThat(validationConfiguration.allowedPatternForComponentName())
        .isEqualTo("(.db|.cql|.json|.crc32|TOC.txt)");
        assertThat(validationConfiguration.allowedPatternForRestrictedComponentName())
        .isEqualTo("(.db|TOC.txt)");
    }

    @Test
    void testReadingJmxConfiguration() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_multiple_instances.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        assertThat(config.serviceConfiguration().jmxConfiguration()).isNotNull();
        JmxConfiguration jmxConfiguration = config.serviceConfiguration().jmxConfiguration();
        assertThat(jmxConfiguration.maxRetries()).isEqualTo(42);
        assertThat(jmxConfiguration.retryDelayMillis()).isEqualTo(1234L);
    }

    @Test
    void testReadingBlankJmxConfigurationReturnsDefaults() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_missing_jmx.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        assertThat(config.serviceConfiguration().jmxConfiguration()).isNotNull();
        JmxConfiguration jmxConfiguration = config.serviceConfiguration().jmxConfiguration();
        assertThat(jmxConfiguration.maxRetries()).isEqualTo(3);
        assertThat(jmxConfiguration.retryDelayMillis()).isEqualTo(200L);
    }

    @Test
    void testUploadsConfiguration() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_multiple_instances.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);

        assertThat(config.serviceConfiguration()).isNotNull();
        SSTableUploadConfiguration uploadConfiguration = config.serviceConfiguration()
                                                               .sstableUploadConfiguration();
        assertThat(uploadConfiguration).isNotNull();

        assertThat(uploadConfiguration.concurrentUploadsLimit()).isEqualTo(80);
        assertThat(uploadConfiguration.minimumSpacePercentageRequired()).isEqualTo(10);
    }

    @Test
    void testSslConfiguration() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_ssl.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        validateMultipleInstancesSidecarConfiguration(config, true);
    }

    @Test
    void testFilePermissions() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_file_permissions.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);

        assertThat(config).isNotNull();
        assertThat(config.serviceConfiguration()).isNotNull();
        assertThat(config.serviceConfiguration().sstableUploadConfiguration()).isNotNull();
        assertThat(config.serviceConfiguration().sstableUploadConfiguration().filePermissions()).isEqualTo("rw-rw-rw-");
    }

    @Test
    void testInvalidFilePermissions()
    {
        Path yamlPath = yaml("config/sidecar_invalid_file_permissions.yaml");
        assertThatExceptionOfType(JsonMappingException.class)
        .isThrownBy(() -> SidecarConfigurationImpl.readYamlConfiguration(yamlPath))
        .withRootCauseInstanceOf(IllegalArgumentException.class)
        .withMessageContaining("Invalid file_permissions configuration=\"not-valid\"");
    }

    @Test
    void testInvalidClientAuth()
    {
        Path yamlPath = yaml("config/sidecar_invalid_client_auth.yaml");
        assertThatExceptionOfType(JsonMappingException.class)
        .isThrownBy(() -> SidecarConfigurationImpl.readYamlConfiguration(yamlPath))
        .withRootCauseInstanceOf(IllegalArgumentException.class)
        .withMessageContaining("Invalid client_auth configuration=\"notvalid\", " +
                               "valid values are (NONE,REQUEST,REQUIRED)");
    }

    @Test
    void testDriverParameters() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_driver_params.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);

        DriverConfiguration driverConfiguration = config.driverConfiguration();
        assertThat(driverConfiguration).isNotNull();
        assertThat(driverConfiguration.localDc()).isEqualTo("dc1");
        List<InetSocketAddress> endpoints = Arrays.asList(new InetSocketAddress("127.0.0.1", 9042),
                                                          new InetSocketAddress("127.0.0.2", 9042));
        assertThat(driverConfiguration.contactPoints()).isEqualTo(endpoints);
        assertThat(driverConfiguration.numConnections()).isEqualTo(6);
    }

    @Test
    void testReadCustomSchemaKeyspaceConfiguration() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_schema_keyspace_configuration.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);

        SchemaKeyspaceConfiguration configuration = config.serviceConfiguration().schemaKeyspaceConfiguration();
        assertThat(configuration).isNotNull();
        assertThat(configuration.isEnabled()).isTrue();
        assertThat(configuration.keyspace()).isEqualTo("sidecar_internal");
        assertThat(configuration.replicationStrategy()).isEqualTo("SimpleStrategy");
        assertThat(configuration.replicationFactor()).isEqualTo(3);
        assertThat(configuration.createReplicationStrategyString())
        .isEqualTo("{'class':'SimpleStrategy', 'replication_factor':'3'}");
    }

    @Test
    void testMissingSSTableSnapshotSection() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_missing_sstable_snapshot.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);

        assertThat(config.serviceConfiguration()).isNotNull();
        assertThat(config.serviceConfiguration().sstableSnapshotConfiguration()).isNull();
    }

    @Test
    void testMetricOptionsParsedFromYaml() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_metrics.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);

        MetricsConfiguration configuration = config.metricsConfiguration();
        assertThat(configuration.registryName()).isEqualTo("cassandra_sidecar_metrics");
        VertxMetricsConfiguration vertxMetricsConfiguration = configuration.vertxConfiguration();
        assertThat(vertxMetricsConfiguration.enabled()).isTrue();
        assertThat(vertxMetricsConfiguration.exposeViaJMX()).isFalse();
        assertThat(vertxMetricsConfiguration.jmxDomainName()).isEqualTo(DEFAULT_JMX_DOMAIN_NAME);
        List<MetricsFilteringConfiguration> includeConfigurations = configuration.includeConfigurations();
        List<MetricsFilteringConfiguration> excludeConfigurations = configuration.excludeConfigurations();
        assertThat(includeConfigurations.size()).isEqualTo(1);
        assertThat(excludeConfigurations.size()).isEqualTo(2);
        assertThat(includeConfigurations.get(0).type()).isEqualTo("regex");
        assertThat(includeConfigurations.get(0).value()).isEqualTo(".*");
        if (excludeConfigurations.get(0).type().equals("regex"))
        {
            assertThat(excludeConfigurations.get(0).value()).isEqualTo("vertx.eventbus.*");
            assertThat(excludeConfigurations.get(1).type()).isEqualTo("equals");
            assertThat(excludeConfigurations.get(1).value()).isEqualTo("instances_up");
        }
        else
        {
            assertThat(excludeConfigurations.get(1).value()).isEqualTo("vertx.eventbus.*");
            assertThat(excludeConfigurations.get(1).type()).isEqualTo("regex");
            assertThat(excludeConfigurations.get(0).value()).isEqualTo("instances_up");
        }
    }

    @Test
    void testInvalidMetricOptions()
    {
        Path yamlPath = yaml("config/sidecar_invalid_metrics.yaml");
        assertThatExceptionOfType(JsonMappingException.class)
        .isThrownBy(() -> SidecarConfigurationImpl.readYamlConfiguration(yamlPath))
        .withRootCauseInstanceOf(IllegalArgumentException.class)
        .withMessageContaining("contains passed for metric filtering is not recognized. Expected types are "
                               + REGEX_TYPE + " or " + EQUALS_TYPE);
    }

    @Test
    void testMetricsAllowedWithDefaultRegexFilter()
    {
        Pattern pattern = Pattern.compile(MetricsFilteringConfigurationImpl.DEFAULT_VALUE);
        assertThat(pattern.matcher("vertx.http.servers.localhost:0.responses-5xx")).matches();
        assertThat(pattern.matcher("sidecar.schema.cassandra_instances_up")).matches();
        assertThat(pattern.matcher("vertx.eventbus.messages.bytes-read")).matches();
        assertThat(pattern.matcher("throttled_429")).matches();
    }

    @Test
    void testVertxFilesystemOptionsConfiguration() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_vertx_filesystem_options.yaml");
        SidecarConfigurationImpl sidecarConfiguration = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        assertThat(sidecarConfiguration).isNotNull();
        assertThat(sidecarConfiguration.serviceConfiguration()).isNotNull();
        VertxFilesystemOptionsConfiguration vertxFsOptions = sidecarConfiguration.serviceConfiguration()
                                                                                 .vertxFilesystemOptionsConfiguration();
        assertThat(vertxFsOptions).isNotNull();
        assertThat(vertxFsOptions.fileCachingEnabled()).isTrue();
        assertThat(vertxFsOptions.fileCacheDir()).isEqualTo("/path/to/vertx/cache");
        assertThat(vertxFsOptions.classPathResolvingEnabled()).isTrue();
    }

    void validateSingleInstanceSidecarConfiguration(SidecarConfiguration config)
    {
        assertThat(config.cassandraInstances()).isNotNull().hasSize(1);

        InstanceConfiguration i1 = config.cassandraInstances().get(0);

        // instance 1
        assertThat(i1.id()).isEqualTo(0);
        assertThat(i1.host()).isEqualTo("localhost");
        assertThat(i1.port()).isEqualTo(9042);
        assertThat(i1.username()).isEqualTo("cassandra");
        assertThat(i1.password()).isEqualTo("cassandra");
        assertThat(i1.dataDirs()).containsExactly("/ccm/test/node1/data0", "/ccm/test/node1/data1");
        assertThat(i1.stagingDir()).isEqualTo("/ccm/test/node1/sstable-staging");
        assertThat(i1.jmxHost()).isEqualTo("127.0.0.1");
        assertThat(i1.jmxPort()).isEqualTo(7199);
        assertThat(i1.jmxSslEnabled()).isTrue();
        assertThat(i1.jmxRole()).isEqualTo("controlRole");
        assertThat(i1.jmxRolePassword()).isEqualTo("controlPassword");

        // service configuration
        validateServiceConfigurationFromYaml(config.serviceConfiguration());

        // ssl configuration
        assertThat(config.sslConfiguration()).isNull();

        // health check configuration
        validateHealthCheckConfigurationFromYaml(config.healthCheckConfiguration());

        // metrics configuration
        validateMetricsConfiguration(config.metricsConfiguration());

        // cassandra input validation configuration
        validateCassandraInputValidationConfigurationFromYaml(config.cassandraInputValidationConfiguration());
    }

    void validateMultipleInstancesSidecarConfiguration(SidecarConfiguration config, boolean withSslConfiguration)
    {
        // instances configuration
        assertThat(config.cassandraInstances()).isNotNull().hasSize(3);

        InstanceConfiguration i1 = config.cassandraInstances().get(0);
        InstanceConfiguration i2 = config.cassandraInstances().get(1);
        InstanceConfiguration i3 = config.cassandraInstances().get(2);

        // instance 1
        assertThat(i1.id()).isEqualTo(1);
        assertThat(i1.host()).isEqualTo("localhost1");
        assertThat(i1.port()).isEqualTo(9042);
        assertThat(i1.username()).isEqualTo("cassandra");
        assertThat(i1.password()).isEqualTo("cassandra");
        assertThat(i1.dataDirs()).containsExactly("/ccm/test/node1/data0", "/ccm/test/node1/data1");
        assertThat(i1.stagingDir()).isEqualTo("/ccm/test/node1/sstable-staging");
        assertThat(i1.jmxHost()).isEqualTo("127.0.0.1");
        assertThat(i1.jmxPort()).isEqualTo(7100);
        assertThat(i1.jmxSslEnabled()).isFalse();

        // instance 2
        assertThat(i2.id()).isEqualTo(2);
        assertThat(i2.host()).isEqualTo("localhost2");
        assertThat(i2.port()).isEqualTo(9042);
        assertThat(i2.username()).isEqualTo("cassandra");
        assertThat(i2.password()).isEqualTo("cassandra");
        assertThat(i2.dataDirs()).containsExactly("/ccm/test/node2/data0", "/ccm/test/node2/data1");
        assertThat(i2.stagingDir()).isEqualTo("/ccm/test/node2/sstable-staging");
        assertThat(i2.jmxHost()).isEqualTo("127.0.0.1");
        assertThat(i2.jmxPort()).isEqualTo(7200);
        assertThat(i2.jmxSslEnabled()).isFalse();

        // instance 3
        assertThat(i3.id()).isEqualTo(3);
        assertThat(i3.host()).isEqualTo("localhost3");
        assertThat(i3.port()).isEqualTo(9042);
        assertThat(i3.username()).isEqualTo("cassandra");
        assertThat(i3.password()).isEqualTo("cassandra");
        assertThat(i3.dataDirs()).containsExactly("/ccm/test/node3/data0", "/ccm/test/node3/data1");
        assertThat(i3.stagingDir()).isEqualTo("/ccm/test/node3/sstable-staging");
        assertThat(i3.jmxHost()).isEqualTo("127.0.0.1");
        assertThat(i3.jmxPort()).isEqualTo(7300);
        assertThat(i3.jmxSslEnabled()).isFalse();

        // service configuration
        validateServiceConfigurationFromYaml(config.serviceConfiguration());

        // ssl configuration
        if (withSslConfiguration)
        {
            validateSslConfigurationFromYaml(config.sslConfiguration());
        }
        else
        {
            assertThat(config.sslConfiguration()).isNull();
        }

        // health check configuration
        validateHealthCheckConfigurationFromYaml(config.healthCheckConfiguration());

        // metrics configuration
        validateMetricsConfiguration(config.metricsConfiguration());

        // cassandra input validation configuration
        validateCassandraInputValidationConfigurationFromYaml(config.cassandraInputValidationConfiguration());
    }

    void validateServiceConfigurationFromYaml(ServiceConfiguration serviceConfiguration)
    {
        assertThat(serviceConfiguration).isNotNull();
        assertThat(serviceConfiguration.host()).isEqualTo("0.0.0.0");
        assertThat(serviceConfiguration.port()).is(new Condition<>(port -> port == 9043 || port == 0, "port"));
        assertThat(serviceConfiguration.requestIdleTimeoutMillis()).isEqualTo(300000);
        assertThat(serviceConfiguration.requestTimeoutMillis()).isEqualTo(300000);
        assertThat(serviceConfiguration.allowableSkewInMinutes()).isEqualTo(60);
        assertThat(serviceConfiguration.tcpKeepAlive()).isFalse();
        assertThat(serviceConfiguration.acceptBacklog()).isEqualTo(1024);

        // service configuration throttling
        ThrottleConfiguration throttle = serviceConfiguration.throttleConfiguration();

        assertThat(throttle).isNotNull();
        assertThat(throttle.rateLimitStreamRequestsPerSecond()).isEqualTo(5000);
        assertThat(throttle.timeoutInSeconds()).isEqualTo(10);

        // validate traffic shaping options
        TrafficShapingConfiguration trafficShaping = serviceConfiguration.trafficShapingConfiguration();
        assertThat(trafficShaping).isNotNull();
        assertThat(trafficShaping.inboundGlobalBandwidthBytesPerSecond()).isEqualTo(500L);
        assertThat(trafficShaping.outboundGlobalBandwidthBytesPerSecond()).isEqualTo(1500L);
        assertThat(trafficShaping.peakOutboundGlobalBandwidthBytesPerSecond()).isEqualTo(2000L);
        assertThat(trafficShaping.maxDelayToWaitMillis()).isEqualTo(2500L);
        assertThat(trafficShaping.checkIntervalForStatsMillis()).isEqualTo(3000L);

        // SSTable snapshot configuration section
        SSTableSnapshotConfiguration snapshotConfig = serviceConfiguration.sstableSnapshotConfiguration();

        assertThat(snapshotConfig).isNotNull();

        assertThat(snapshotConfig.snapshotListCacheConfiguration().enabled()).isTrue();
        assertThat(snapshotConfig.snapshotListCacheConfiguration().maximumSize()).isEqualTo(450);
        assertThat(snapshotConfig.snapshotListCacheConfiguration().expireAfterAccessMillis()).isEqualTo(350);

        // vertx FileSystemOptions
        validateVertxFilesystemOptionsConfiguration(serviceConfiguration.vertxFilesystemOptionsConfiguration());
    }

    private void validateHealthCheckConfigurationFromYaml(HealthCheckConfiguration config)
    {
        assertThat(config).isNotNull();
        assertThat(config.initialDelayMillis()).isEqualTo(100);
        assertThat(config.checkIntervalMillis()).isEqualTo(30_000);
    }

    void validateCassandraInputValidationConfigurationFromYaml(CassandraInputValidationConfiguration config)
    {
        assertThat(config).isNotNull();
        assertThat(config.forbiddenKeyspaces()).containsExactlyInAnyOrder("system_schema",
                                                                          "system_traces",
                                                                          "system_distributed",
                                                                          "system",
                                                                          "system_auth",
                                                                          "system_views",
                                                                          "system_virtual_schema");
        assertThat(config.allowedPatternForName()).isEqualTo("[a-zA-Z][a-zA-Z0-9_]{0,47}");
        assertThat(config.allowedPatternForQuotedName()).isEqualTo("[a-zA-Z_0-9]{1,48}");
        assertThat(config.allowedPatternForComponentName())
        .isEqualTo("[a-zA-Z0-9_-]+(.db|.cql|.json|.crc32|TOC.txt)");
        assertThat(config.allowedPatternForRestrictedComponentName()).isEqualTo("[a-zA-Z0-9_-]+(.db|TOC.txt)");
    }

    void validateVertxFilesystemOptionsConfiguration(VertxFilesystemOptionsConfiguration config)
    {
        assertThat(config).isNotNull();
        assertThat(config.classPathResolvingEnabled()).isFalse();
        assertThat(config.fileCacheDir()).isNotNull();
        assertThat(config.fileCachingEnabled()).isNotNull();
    }

    void validateSslConfigurationFromYaml(SslConfiguration config)
    {
        assertThat(config).isNotNull();
        assertThat(config.enabled()).isTrue();
        assertThat(config.preferOpenSSL()).isFalse();
        assertThat(config.handshakeTimeoutInSeconds()).isEqualTo(25L);
        assertThat(config.clientAuth()).isEqualTo("REQUEST");
        assertThat(config.keystore()).isNotNull();
        assertThat(config.keystore().type()).isEqualTo("PKCS12");
        assertThat(config.keystore().path()).isEqualTo("path/to/keystore.p12");
        assertThat(config.keystore().password()).isEqualTo("password");
        assertThat(config.keystore().reloadStore()).isTrue();
        assertThat(config.keystore().checkIntervalInSeconds()).isEqualTo(300);
        assertThat(config.truststore()).isNotNull();
        assertThat(config.truststore().path()).isEqualTo("path/to/truststore.p12");
        assertThat(config.truststore().password()).isEqualTo("password");
        assertThat(config.truststore().reloadStore()).isFalse();
        assertThat(config.truststore().checkIntervalInSeconds()).isEqualTo(-1);
        assertThat(config.secureTransportProtocols()).containsExactly("TLSv1.3");
        assertThat(config.cipherSuites()).contains("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
                                                   "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
                                                   "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                                                   "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
                                                   "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
                                                   "TLS_RSA_WITH_AES_128_GCM_SHA256",
                                                   "TLS_RSA_WITH_AES_128_CBC_SHA",
                                                   "TLS_RSA_WITH_AES_256_CBC_SHA");
    }

    void validateMetricsConfiguration(MetricsConfiguration config)
    {
        assertThat(config.registryName()).isNotEmpty();
        assertThat(config.vertxConfiguration()).isNotNull();
        assertThat(config.includeConfigurations()).isNotNull();
        assertThat(config.excludeConfigurations()).isNotNull();
    }

    private Path yaml(String resourceName)
    {
        ClassLoader classLoader = this.getClass().getClassLoader();
        return writeResourceToPath(classLoader, configPath, resourceName);
    }
}
