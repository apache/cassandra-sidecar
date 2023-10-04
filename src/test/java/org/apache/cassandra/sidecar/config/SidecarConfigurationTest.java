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
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.assertj.core.api.Condition;

import static org.apache.cassandra.sidecar.common.ResourceUtils.writeResourceToPath;
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
        assertThat(validationConfiguration.allowedPatternForDirectory()).isEqualTo("[a-z]+");
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
                                                               .ssTableUploadConfiguration();
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
        assertThat(config.serviceConfiguration().ssTableUploadConfiguration()).isNotNull();
        assertThat(config.serviceConfiguration().ssTableUploadConfiguration().filePermissions()).isEqualTo("rw-rw-rw-");
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
        validateDefaultServiceConfiguration(config.serviceConfiguration());

        // ssl configuration
        assertThat(config.sslConfiguration()).isNull();

        // health check configuration
        validateHealthCheckConfiguration(config.healthCheckConfiguration());

        // cassandra input validation configuration
        validateDefaultCassandraInputValidationConfiguration(config.cassandraInputValidationConfiguration());
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
        validateDefaultServiceConfiguration(config.serviceConfiguration());

        // ssl configuration
        if (withSslConfiguration)
        {
            validateDefaultSslConfiguration(config.sslConfiguration());
        }
        else
        {
            assertThat(config.sslConfiguration()).isNull();
        }

        // health check configuration
        validateHealthCheckConfiguration(config.healthCheckConfiguration());

        // cassandra input validation configuration
        validateDefaultCassandraInputValidationConfiguration(config.cassandraInputValidationConfiguration());
    }

    void validateDefaultServiceConfiguration(ServiceConfiguration serviceConfiguration)
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
        assertThat(throttle.delayInSeconds()).isEqualTo(5);
        assertThat(throttle.timeoutInSeconds()).isEqualTo(10);

        // validate traffic shaping options
        TrafficShapingConfiguration trafficShaping = serviceConfiguration.trafficShapingConfiguration();
        assertThat(trafficShaping).isNotNull();
        assertThat(trafficShaping.inboundGlobalBandwidthBytesPerSecond()).isEqualTo(500L);
        assertThat(trafficShaping.outboundGlobalBandwidthBytesPerSecond()).isEqualTo(1500L);
        assertThat(trafficShaping.peakOutboundGlobalBandwidthBytesPerSecond()).isEqualTo(2000L);
        assertThat(trafficShaping.maxDelayToWaitMillis()).isEqualTo(2500L);
        assertThat(trafficShaping.checkIntervalForStatsMillis()).isEqualTo(3000L);
    }

    private void validateHealthCheckConfiguration(HealthCheckConfiguration config)
    {
        assertThat(config).isNotNull();
        assertThat(config.initialDelayMillis()).isEqualTo(100);
        assertThat(config.checkIntervalMillis()).isEqualTo(30_000);
    }

    void validateDefaultCassandraInputValidationConfiguration(CassandraInputValidationConfiguration config)
    {
        assertThat(config).isNotNull();
        assertThat(config.forbiddenKeyspaces()).containsExactlyInAnyOrder("system_schema",
                                                                          "system_traces",
                                                                          "system_distributed",
                                                                          "system",
                                                                          "system_auth",
                                                                          "system_views",
                                                                          "system_virtual_schema");
        assertThat(config.allowedPatternForDirectory()).isEqualTo("[a-zA-Z0-9_-]+");
        assertThat(config.allowedPatternForComponentName())
        .isEqualTo("[a-zA-Z0-9_-]+(.db|.cql|.json|.crc32|TOC.txt)");
        assertThat(config.allowedPatternForRestrictedComponentName()).isEqualTo("[a-zA-Z0-9_-]+(.db|TOC.txt)");
    }

    void validateDefaultSslConfiguration(SslConfiguration config)
    {
        assertThat(config).isNotNull();
        assertThat(config.enabled()).isTrue();
        assertThat(config.preferOpenSSL()).isFalse();
        assertThat(config.handshakeTimeoutInSeconds()).isEqualTo(25L);
        assertThat(config.clientAuth()).isEqualTo("REQUEST");
        assertThat(config.keystore()).isNotNull();
        assertThat(config.keystore().path()).isEqualTo("path/to/keystore.p12");
        assertThat(config.keystore().password()).isEqualTo("password");
        assertThat(config.keystore().reloadStore()).isTrue();
        assertThat(config.keystore().checkIntervalInSeconds()).isEqualTo(300);
        assertThat(config.truststore()).isNotNull();
        assertThat(config.truststore().path()).isEqualTo("path/to/truststore.p12");
        assertThat(config.truststore().password()).isEqualTo("password");
        assertThat(config.truststore().reloadStore()).isFalse();
        assertThat(config.truststore().checkIntervalInSeconds()).isEqualTo(-1);
    }

    private Path yaml(String resourceName)
    {
        ClassLoader classLoader = this.getClass().getClassLoader();
        return writeResourceToPath(classLoader, configPath, resourceName);
    }
}
