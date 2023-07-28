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

package org.apache.cassandra.sidecar;

import java.io.IOException;
import java.net.URL;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.utils.ValidationConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests reading Sidecar {@link Configuration} from YAML files
 */
class YAMLSidecarConfigurationTest
{
    private static final String SIDECAR_VERSION = "unknown";

    private final CassandraVersionProvider versionProvider = mock(CassandraVersionProvider.class);

    @Test
    public void testSidecarConfiguration() throws IOException
    {
        String confPath1 = confPath("sidecar_multiple_instances.yaml");
        Configuration multipleInstancesConfig = YAMLSidecarConfiguration.of(confPath1, versionProvider, SIDECAR_VERSION, DnsResolver.DEFAULT);
        validateSidecarConfiguration(multipleInstancesConfig);

        String confPath = confPath("sidecar_single_instance.yaml");
        Configuration singleInstanceConfig = YAMLSidecarConfiguration.of(confPath, versionProvider, SIDECAR_VERSION, DnsResolver.DEFAULT);
        validateSidecarConfiguration(singleInstanceConfig);
    }

    @Test
    public void testLegacySidecarYAMLFormatWithSingleInstance() throws IOException
    {
        String confPath = confPath("sidecar_single_instance.yaml");
        Configuration configuration = YAMLSidecarConfiguration.of(confPath, versionProvider, SIDECAR_VERSION, DnsResolver.DEFAULT);
        InstancesConfig instancesConfig = configuration.getInstancesConfig();
        assertThat(instancesConfig.instances().size()).isEqualTo(1);
        InstanceMetadata instanceMetadata = instancesConfig.instances().get(0);
        assertThat(instanceMetadata.host()).isEqualTo("localhost");
        assertThat(instanceMetadata.port()).isEqualTo(9042);
    }

    @Test
    public void testReadAllowableTimeSkew() throws IOException
    {
        String confPath1 = confPath("sidecar_single_instance.yaml");
        Configuration configuration = YAMLSidecarConfiguration.of(confPath1, versionProvider, SIDECAR_VERSION, DnsResolver.DEFAULT);
        assertThat(configuration.allowableSkewInMinutes()).isEqualTo(89);

        String confPath = confPath("sidecar_custom_allowable_time_skew.yaml");
        configuration = YAMLSidecarConfiguration.of(confPath, versionProvider, SIDECAR_VERSION, DnsResolver.DEFAULT);
        assertThat(configuration.allowableSkewInMinutes()).isEqualTo(1);
    }

    @Test
    public void testReadingSingleInstanceSectionOverMultipleInstances() throws IOException
    {
        String confPath = confPath("sidecar_with_single_multiple_instances.yaml");
        Configuration configuration = YAMLSidecarConfiguration.of(confPath, versionProvider, SIDECAR_VERSION, DnsResolver.DEFAULT);
        InstancesConfig instancesConfig = configuration.getInstancesConfig();
        assertThat(instancesConfig.instances().size()).isEqualTo(1);
        InstanceMetadata instanceMetadata = instancesConfig.instances().get(0);
        assertThat(instanceMetadata.host()).isEqualTo("localhost");
        assertThat(instanceMetadata.port()).isEqualTo(9042);
    }

    @Test
    public void testReadingMultipleInstances() throws IOException
    {
        String confPath = confPath("sidecar_multiple_instances.yaml");
        Configuration configuration = YAMLSidecarConfiguration.of(confPath, versionProvider, SIDECAR_VERSION, DnsResolver.DEFAULT);
        InstancesConfig instancesConfig = configuration.getInstancesConfig();
        assertThat(instancesConfig.instances().size()).isEqualTo(2);
    }

    @Test
    public void testReadingCassandraInputValidation() throws IOException
    {
        String confPath = confPath("sidecar_validation_configuration.yaml");
        Configuration configuration = YAMLSidecarConfiguration.of(confPath, versionProvider, SIDECAR_VERSION, DnsResolver.DEFAULT);
        ValidationConfiguration validationConfiguration = configuration.getValidationConfiguration();

        assertThat(validationConfiguration.forbiddenKeyspaces()).contains("a", "b", "c");
        assertThat(validationConfiguration.allowedPatternForDirectory()).isEqualTo("[a-z]+");
        assertThat(validationConfiguration.allowedPatternForComponentName())
        .isEqualTo("(.db|.cql|.json|.crc32|TOC.txt)");
        assertThat(validationConfiguration.allowedPatternForRestrictedComponentName())
        .isEqualTo("(.db|TOC.txt)");
    }

    @Test
    public void testUploadsConfiguration() throws IOException
    {
        String confPath = confPath("sidecar_multiple_instances.yaml");
        Configuration configuration = YAMLSidecarConfiguration.of(confPath, versionProvider, SIDECAR_VERSION, DnsResolver.DEFAULT);

        assertThat(configuration.getConcurrentUploadsLimit()).isEqualTo(80);
        assertThat(configuration.getMinSpacePercentRequiredForUpload()).isEqualTo(10);
    }

    private void validateSidecarConfiguration(Configuration config)
    {
        assertThat(config.getHost()).isEqualTo("0.0.0.0");
        assertThat(config.getPort()).isEqualTo(9043);
        assertThat(config.getRequestIdleTimeoutMillis()).isEqualTo(500_000);
        assertThat(config.getRequestTimeoutMillis()).isEqualTo(1_200_000L);
        assertThat(config.getRateLimitStreamRequestsPerSecond()).isEqualTo(80);
        assertThat(config.getThrottleDelayInSeconds()).isEqualTo(7);
        assertThat(config.getThrottleTimeoutInSeconds()).isEqualTo(21);
        assertThat(config.allowableSkewInMinutes()).isEqualTo(89);
        assertThat(config.getSSTableImportPollIntervalMillis()).isEqualTo(50);
        assertThat(config.ssTableImportCacheConfiguration()).isNotNull();
        assertThat(config.ssTableImportCacheConfiguration().expireAfterAccessMillis()).isEqualTo(1000L);
        assertThat(config.ssTableImportCacheConfiguration().maximumSize()).isEqualTo(100);
    }

    private String confPath(String resourceName)
    {
        URL resource = getClass().getClassLoader().getResource(resourceName);
        return Objects.requireNonNull(resource).toString();
    }
}
