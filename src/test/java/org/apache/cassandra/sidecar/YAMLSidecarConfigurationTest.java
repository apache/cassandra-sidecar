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
import org.apache.cassandra.sidecar.common.utils.ValidationConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests reading Sidecar {@link Configuration} from YAML files
 */
class YAMLSidecarConfigurationTest
{
    CassandraVersionProvider versionProvider = mock(CassandraVersionProvider.class);

    @Test
    public void testSidecarConfiguration() throws IOException
    {
        Configuration multipleInstancesConfig =
        YAMLSidecarConfiguration.of(confPath("sidecar_multiple_instances.yaml"),
                                    versionProvider);
        validateSidecarConfiguration(multipleInstancesConfig);

        Configuration singleInstanceConfig =
        YAMLSidecarConfiguration.of(confPath("sidecar_single_instance.yaml"), versionProvider);
        validateSidecarConfiguration(singleInstanceConfig);
    }

    @Test
    public void testLegacySidecarYAMLFormatWithSingleInstance() throws IOException
    {
        Configuration configuration =
        YAMLSidecarConfiguration.of(confPath("sidecar_single_instance.yaml"), versionProvider);
        InstancesConfig instancesConfig = configuration.getInstancesConfig();
        assertThat(instancesConfig.instances().size()).isEqualTo(1);
        InstanceMetadata instanceMetadata = instancesConfig.instances().get(0);
        assertThat(instanceMetadata.host()).isEqualTo("localhost");
        assertThat(instanceMetadata.port()).isEqualTo(9042);
    }

    @Test
    public void testReadAllowableTimeSkew() throws IOException
    {
        Configuration configuration =
        YAMLSidecarConfiguration.of(confPath("sidecar_single_instance.yaml"), versionProvider);
        assertThat(configuration.allowableSkewInMinutes()).isEqualTo(89);

        configuration =
        YAMLSidecarConfiguration.of(confPath("sidecar_custom_allowable_time_skew.yaml"), versionProvider);
        assertThat(configuration.allowableSkewInMinutes()).isEqualTo(1);
    }

    @Test
    public void testReadingSingleInstanceSectionOverMultipleInstances() throws IOException
    {
        Configuration configuration =
        YAMLSidecarConfiguration.of(confPath("sidecar_with_single_multiple_instances.yaml"),
                                    versionProvider);
        InstancesConfig instancesConfig = configuration.getInstancesConfig();
        assertThat(instancesConfig.instances().size()).isEqualTo(1);
        InstanceMetadata instanceMetadata = instancesConfig.instances().get(0);
        assertThat(instanceMetadata.host()).isEqualTo("localhost");
        assertThat(instanceMetadata.port()).isEqualTo(9042);
    }

    @Test
    public void testReadingMultipleInstances() throws IOException
    {
        Configuration configuration =
        YAMLSidecarConfiguration.of(confPath("sidecar_multiple_instances.yaml"),
                                    versionProvider);
        InstancesConfig instancesConfig = configuration.getInstancesConfig();
        assertThat(instancesConfig.instances().size()).isEqualTo(2);
    }

    @Test
    public void testReadingCassandraInputValidation() throws IOException
    {
        Configuration configuration =
        YAMLSidecarConfiguration.of(confPath("sidecar_validation_configuration.yaml"),
                                    versionProvider);
        ValidationConfiguration validationConfiguration = configuration.getValidationConfiguration();

        assertThat(validationConfiguration.getForbiddenKeyspaces()).contains("a", "b", "c");
        assertThat(validationConfiguration.getAllowedPatternForDirectory()).isEqualTo("[a-z]+");
        assertThat(validationConfiguration.getAllowedPatternForComponentName())
        .isEqualTo("(.db|.cql|.json|.crc32|TOC.txt)");
        assertThat(validationConfiguration.getAllowedPatternForRestrictedComponentName())
        .isEqualTo("(.db|TOC.txt)");
    }

    @Test
    public void testUploadsConfiguration() throws IOException
    {
        Configuration configuration =
        YAMLSidecarConfiguration.of(confPath("sidecar_multiple_instances.yaml"),
                                    versionProvider);

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
