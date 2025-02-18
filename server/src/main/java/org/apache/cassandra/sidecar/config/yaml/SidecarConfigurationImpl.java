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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.google.common.reflect.ClassPath;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.CassandraInputValidationConfiguration;
import org.apache.cassandra.sidecar.config.DriverConfiguration;
import org.apache.cassandra.sidecar.config.InstanceConfiguration;
import org.apache.cassandra.sidecar.config.MetricsConfiguration;
import org.apache.cassandra.sidecar.config.PeriodicTaskConfiguration;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.config.S3ClientConfiguration;
import org.apache.cassandra.sidecar.config.SchemaReportingConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarClientConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SidecarPeerHealthConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.VertxConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Configuration for this Sidecar process
 */
@SuppressWarnings("unused")
public class SidecarConfigurationImpl implements SidecarConfiguration
{
    @Deprecated
    @JsonProperty(value = "cassandra")
    protected final InstanceConfiguration cassandraInstance;

    @JsonProperty(value = "cassandra_instances")
    protected final List<InstanceConfiguration> cassandraInstances;

    @JsonProperty(value = "driver_parameters")
    protected final DriverConfiguration driverConfiguration;

    @JsonProperty(value = "sidecar")
    protected final ServiceConfiguration serviceConfiguration;

    @JsonProperty("ssl")
    protected final SslConfiguration sslConfiguration;

    @JsonProperty("access_control")
    protected final AccessControlConfiguration accessControlConfiguration;

    @JsonProperty("sidecar_peer_health")
    protected final SidecarPeerHealthConfiguration sidecarPeerHealthConfiguration;

    @JsonProperty("sidecar_client")
    protected final SidecarClientConfiguration sidecarClientConfiguration;

    @JsonProperty("healthcheck")
    protected final PeriodicTaskConfiguration healthCheckConfiguration;

    @JsonProperty("metrics")
    protected final MetricsConfiguration metricsConfiguration;

    @JsonProperty("cassandra_input_validation")
    protected final CassandraInputValidationConfiguration cassandraInputValidationConfiguration;

    @JsonProperty("blob_restore")
    protected final RestoreJobConfiguration restoreJobConfiguration;

    @JsonProperty("s3_client")
    protected final S3ClientConfiguration s3ClientConfiguration;

    @JsonProperty("vertx")
    @Nullable
    protected final VertxConfiguration vertxConfiguration;

    @JsonProperty("schema_reporting")
    @NotNull
    protected final SchemaReportingConfiguration schemaReportingConfiguration;

    public SidecarConfigurationImpl()
    {
        this(builder());
    }

    protected SidecarConfigurationImpl(Builder builder)
    {
        cassandraInstance = builder.cassandraInstance;
        cassandraInstances = builder.cassandraInstances;
        serviceConfiguration = builder.serviceConfiguration;
        sslConfiguration = builder.sslConfiguration;
        accessControlConfiguration = builder.accessControlConfiguration;
        healthCheckConfiguration = builder.healthCheckConfiguration;
        sidecarPeerHealthConfiguration = builder.sidecarPeerHealthConfiguration;
        sidecarClientConfiguration = builder.sidecarClientConfiguration;
        metricsConfiguration = builder.metricsConfiguration;
        cassandraInputValidationConfiguration = builder.cassandraInputValidationConfiguration;
        driverConfiguration = builder.driverConfiguration;
        restoreJobConfiguration = builder.restoreJobConfiguration;
        s3ClientConfiguration = builder.s3ClientConfiguration;
        vertxConfiguration = builder.vertxConfiguration;
        schemaReportingConfiguration = builder.schemaReportingConfiguration;
    }

    /**
     * @return a single configured cassandra instance
     * @deprecated in favor of configuring multiple instances in the yaml under cassandra_instances
     */
    @Override
    @JsonProperty(value = "cassandra")
    @Deprecated
    public InstanceConfiguration cassandra()
    {
        return cassandraInstance;
    }

    /**
     * @return the configured Cassandra instances that this Sidecar manages
     */
    @Override
    @JsonProperty(value = "cassandra_instances")
    public List<InstanceConfiguration> cassandraInstances()
    {
        if (cassandraInstance != null)
        {
            return Collections.singletonList(cassandraInstance);
        }
        else if (cassandraInstances != null && !cassandraInstances.isEmpty())
        {
            return Collections.unmodifiableList(cassandraInstances);
        }
        return Collections.emptyList();
    }

    /**
     * @return the configuration of the REST Services
     */
    @Override
    @JsonProperty(value = "sidecar", required = true)
    public ServiceConfiguration serviceConfiguration()
    {
        return serviceConfiguration;
    }

    /**
     * @return the SSL configuration
     */
    @Override
    @JsonProperty("ssl")
    public SslConfiguration sslConfiguration()
    {
        return sslConfiguration;
    }

    /**
     * @return configuration needed for setting up access control in Sidecar
     */
    @Override
    @JsonProperty("access_control")
    public AccessControlConfiguration accessControlConfiguration()
    {
        return accessControlConfiguration;
    }

    @Override
    public SidecarClientConfiguration sidecarClientConfiguration()
    {
        return sidecarClientConfiguration;
    }

    /**
     * @return the configuration for the health check service
     */
    @Override
    @JsonProperty("healthcheck")
    public PeriodicTaskConfiguration healthCheckConfiguration()
    {
        return healthCheckConfiguration;
    }

    /**
     * @return the configuration for the down detector service
     */
    @Override
    @JsonProperty("sidecar_peer_health")
    public SidecarPeerHealthConfiguration sidecarPeerHealthConfiguration()
    {
        return sidecarPeerHealthConfiguration;
    }

    /**
     * @return the configuration needed for metrics capture
     */
    @Override
    @JsonProperty("metrics")
    public MetricsConfiguration metricsConfiguration()
    {
        return metricsConfiguration;
    }

    @Override
    @JsonProperty("driver_parameters")
    public DriverConfiguration driverConfiguration()
    {
        return driverConfiguration;
    }

    /**
     * @return the configuration for Cassandra input validation
     */
    @Override
    @JsonProperty("cassandra_input_validation")
    public CassandraInputValidationConfiguration cassandraInputValidationConfiguration()
    {
        return cassandraInputValidationConfiguration;
    }

    /**
     * @return the configuration for restore jobs
     */
    @Override
    @JsonProperty("blob_restore")
    public RestoreJobConfiguration restoreJobConfiguration()
    {
        return restoreJobConfiguration;
    }

    /**
     * @return the configuration for Amazon S3 client
     */
    @Override
    @JsonProperty("s3_client")
    public S3ClientConfiguration s3ClientConfiguration()
    {
        return s3ClientConfiguration;
    }

    /**
     * @return the configuration for vert.x
     */
    @Override
    @JsonProperty("vertx")
    @Nullable
    public VertxConfiguration vertxConfiguration()
    {
        return vertxConfiguration;
    }

    /**
     * @return the configuration for Schema Reporting
     */
    @Override
    @JsonProperty("schema_reporting")
    @NotNull
    public SchemaReportingConfiguration schemaReportingConfiguration()
    {
        return schemaReportingConfiguration;
    }

    public static SidecarConfigurationImpl readYamlConfiguration(String yamlConfigurationPath) throws IOException
    {
        try
        {
            return readYamlConfiguration(Paths.get(new URI(yamlConfigurationPath)));
        }
        catch (URISyntaxException e)
        {
            throw new IOException("Invalid URI: " + yamlConfigurationPath, e);
        }
    }

    public static SidecarConfigurationImpl readYamlConfiguration(Path yamlConfigurationPath) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
                              .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
                              .configure(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS, true)
                              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                              .registerModule(resolveYamlTypeMappings());

        return mapper.readValue(yamlConfigurationPath.toFile(), SidecarConfigurationImpl.class);
    }

    @VisibleForTesting
    public static SidecarConfigurationImpl fromYamlString(String yaml) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
                              .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
                              .configure(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS, true)
                              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                              .registerModule(resolveYamlTypeMappings());

        return mapper.readValue(yaml, SidecarConfigurationImpl.class);
    }

    private static SimpleModule resolveYamlTypeMappings() throws IOException
    {
        String packageName = SidecarConfigurationImpl.class.getPackage().getName();
        String outerPackageName = SidecarConfiguration.class.getPackage().getName();
        SimpleModule module = new SimpleModule();
        ClassPath path = ClassPath.from(ClassLoader.getSystemClassLoader());
        Set<Class> declared = path.getTopLevelClasses(outerPackageName)
                                  .stream()
                                  .filter(c -> c.getName().endsWith("Configuration"))
                                  .map(ClassPath.ClassInfo::load)
                                  .collect(Collectors.toSet());
        Set<Class> implemented = new HashSet<>();
        ClassPath.from(ClassLoader.getSystemClassLoader()).getTopLevelClasses(packageName)
                 .stream()
                 .map(ClassPath.ClassInfo::load)
                 .forEach(clazz -> {
                     if (clazz.isInterface())
                     {
                         return;
                     }
                     // find the configuration interface it implements
                     // note: it assumes that the concrete implementation implement one and only one
                     //       configuration interface, and the name of the configuration interface ends
                     //       with "Configuration"
                     Class[] interfaces = clazz.getInterfaces();
                     Class configurationInterface = null;
                     for (Class c : interfaces)
                     {
                         if (c.getPackage().getName().equals(outerPackageName) && c.getName().endsWith("Configuration"))
                         {
                             configurationInterface = c;
                             if (!implemented.add(configurationInterface))
                             {
                                 throw new IllegalStateException("Multiple implementations found for " +
                                                                 "configuration interface: " + configurationInterface);
                             }
                             break;
                         }
                     }
                     // it does not implement any configuration interface
                     if (configurationInterface == null)
                     {
                         return;
                     }

                     module.addAbstractTypeMapping(configurationInterface, clazz);
                 });

        Set<Class> unimplemented = Sets.difference(declared, implemented);
        if (!unimplemented.isEmpty())
        {
            throw new IllegalStateException("Found unimplemented configuration class(es): " + unimplemented);
        }
        return module;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code SidecarConfigurationImpl} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, SidecarConfigurationImpl>
    {
        private InstanceConfiguration cassandraInstance;
        private List<InstanceConfiguration> cassandraInstances;
        private ServiceConfiguration serviceConfiguration = new ServiceConfigurationImpl();
        private SslConfiguration sslConfiguration = null;
        private AccessControlConfiguration accessControlConfiguration = new AccessControlConfigurationImpl();
        private PeriodicTaskConfiguration healthCheckConfiguration
        = new PeriodicTaskConfigurationImpl(true,
                                            MillisecondBoundConfiguration.ZERO,
                                            MillisecondBoundConfiguration.parse("30s"));
        private SidecarPeerHealthConfiguration sidecarPeerHealthConfiguration = new SidecarPeerHealthConfigurationImpl();
        private SidecarClientConfiguration sidecarClientConfiguration = new SidecarClientConfigurationImpl();
        private MetricsConfiguration metricsConfiguration = new MetricsConfigurationImpl();
        private CassandraInputValidationConfiguration cassandraInputValidationConfiguration = new CassandraInputValidationConfigurationImpl();
        private DriverConfiguration driverConfiguration = new DriverConfigurationImpl();
        private RestoreJobConfiguration restoreJobConfiguration = new RestoreJobConfigurationImpl();
        private S3ClientConfiguration s3ClientConfiguration = new S3ClientConfigurationImpl();
        private VertxConfiguration vertxConfiguration = new VertxConfigurationImpl();
        private SchemaReportingConfiguration schemaReportingConfiguration = new SchemaReportingConfigurationImpl();

        protected Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code cassandraInstance} and returns a reference to this Builder enabling method chaining.
         *
         * @param cassandraInstance the {@code cassandraInstance} to set
         * @return a reference to this Builder
         */
        public Builder cassandraInstance(InstanceConfiguration cassandraInstance)
        {
            return update(b -> b.cassandraInstance = cassandraInstance);
        }

        /**
         * Sets the {@code cassandraInstances} and returns a reference to this Builder enabling method chaining.
         *
         * @param cassandraInstances the {@code cassandraInstances} to set
         * @return a reference to this Builder
         */
        public Builder cassandraInstances(List<InstanceConfiguration> cassandraInstances)
        {
            return update(b -> b.cassandraInstances = cassandraInstances);
        }

        /**
         * Sets the {@code serviceConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param serviceConfiguration the {@code serviceConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder serviceConfiguration(ServiceConfiguration serviceConfiguration)
        {
            return update(b -> b.serviceConfiguration = serviceConfiguration);
        }

        /**
         * Sets the {@code sslConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param sslConfiguration the {@code sslConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder sslConfiguration(SslConfiguration sslConfiguration)
        {
            return update(b -> b.sslConfiguration = sslConfiguration);
        }

        /**
         * Sets the {@code downDetectorConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param sidecarPeerHealthConfiguration the {@code downDetectorConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder downDetectorConfiguration(SidecarPeerHealthConfiguration sidecarPeerHealthConfiguration)
        {
            return update(b -> b.sidecarPeerHealthConfiguration = sidecarPeerHealthConfiguration);
        }

        /**
         * Sets the {@code accessControlConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param accessControlConfiguration the {@code accessControlConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder accessControlConfiguration(AccessControlConfiguration accessControlConfiguration)
        {
            return update(b -> b.accessControlConfiguration = accessControlConfiguration);
        }

        /**
         * Sets the {@code healthCheckConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param healthCheckConfiguration the {@code healthCheckConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder healthCheckConfiguration(PeriodicTaskConfiguration healthCheckConfiguration)
        {
            return update(b -> b.healthCheckConfiguration = healthCheckConfiguration);
        }

        /**
         * Sets the {@code healthCheckConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param sidecarPeerHealthConfiguration the {@code healthCheckConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder sidecarPeerHealthConfiguration(SidecarPeerHealthConfiguration sidecarPeerHealthConfiguration)
        {
            return update(b -> b.sidecarPeerHealthConfiguration = sidecarPeerHealthConfiguration);
        }

        /**
         * Sets the {@code sidecarClientConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param sidecarClientConfiguration the {@code sidecarClientConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder sidecarClientConfiguration(SidecarClientConfiguration sidecarClientConfiguration)
        {
            return update(b -> b.sidecarClientConfiguration = sidecarClientConfiguration);
        }

        /**
         * Sets the {@code metricsConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param metricsConfiguration the {@code metricsConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder metricsConfiguration(MetricsConfiguration metricsConfiguration)
        {
            return update(b -> b.metricsConfiguration = metricsConfiguration);
        }

        /**
         * Sets the {@code driverConfiguration} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param driverConfiguration the {@code driverConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder driverConfiguration(DriverConfiguration driverConfiguration)
        {
            return update(b -> b.driverConfiguration = driverConfiguration);
        }

        /**
         * Sets the {@code cassandraInputValidationConfiguration} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param configuration the {@code cassandraInputValidationConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder cassandraInputValidationConfiguration(CassandraInputValidationConfiguration configuration)
        {
            return update(b -> b.cassandraInputValidationConfiguration = configuration);
        }

        /**
         * Sets the {@code restoreJobConfiguration} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param configuration the {@code restoreJobConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder restoreJobConfiguration(RestoreJobConfiguration configuration)
        {
            return update(b -> b.restoreJobConfiguration = configuration);
        }

        /**
         * Sets the {@code s3ClientConfiguration} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param configuration the {@code s3ClientConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder s3ClientConfiguration(S3ClientConfiguration configuration)
        {
            return update(b -> b.s3ClientConfiguration = configuration);
        }

        /**
         * Sets the {@code vertxConfiguration} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param configuration the {@code vertxConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder vertxConfiguration(VertxConfiguration configuration)
        {
            return update(b -> b.vertxConfiguration = configuration);
        }

        /**
         * Sets the {@code schemaReportingConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param configuration the {@code schemaReportingConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder schemaReportingConfiguration(SchemaReportingConfiguration configuration)
        {
            return update(builder -> builder.schemaReportingConfiguration = configuration);
        }

        /**
         * Returns a {@code SidecarConfigurationImpl} built from the parameters previously set.
         *
         * @return a {@code SidecarConfigurationImpl} built with parameters of this
         * {@code SidecarConfigurationImpl.Builder}
         */
        @Override
        public SidecarConfigurationImpl build()
        {
            return new SidecarConfigurationImpl(this);
        }
    }
}
