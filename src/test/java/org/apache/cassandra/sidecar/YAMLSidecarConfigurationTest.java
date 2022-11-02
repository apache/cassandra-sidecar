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
    public void testLegacySidecarYAMLFormatWithSingleInstance() throws IOException
    {
        Configuration configuration =
        YAMLSidecarConfiguration.of(confPath("sidecar_single_instance.yaml"), versionProvider);
        InstancesConfig instancesConfig = configuration.getInstancesConfig();
        assertThat(instancesConfig.instances().size()).isEqualTo(1);
        InstanceMetadata instanceMetadata = instancesConfig.instances().get(0);
        assertThat(instanceMetadata.host()).isEqualTo("localhost");
        assertThat(instanceMetadata.port()).isEqualTo(9042);
        assertThat(instanceMetadata.session()).isNotNull();
        assertThat(instanceMetadata.jmxClient()).isNotNull();
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
        assertThat(instanceMetadata.session()).isNotNull();
        assertThat(instanceMetadata.jmxClient()).isNotNull();
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

    private String confPath(String resourceName)
    {
        URL resource = getClass().getClassLoader().getResource(resourceName);
        return Objects.requireNonNull(resource).toString();
    }
}
