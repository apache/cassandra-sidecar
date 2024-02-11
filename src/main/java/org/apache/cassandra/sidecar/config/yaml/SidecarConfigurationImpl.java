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
import java.util.List;

import com.google.common.reflect.ClassPath;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.config.CassandraInputValidationConfiguration;
import org.apache.cassandra.sidecar.config.DriverConfiguration;
import org.apache.cassandra.sidecar.config.HealthCheckConfiguration;
import org.apache.cassandra.sidecar.config.InstanceConfiguration;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.config.S3ClientConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;

/**
 * Configuration for this Sidecar process
 */
public class SidecarConfigurationImpl implements SidecarConfiguration
{
    @Deprecated
    @JsonProperty(value = "cassandra")
    protected final InstanceConfiguration cassandraInstance;

    @JsonProperty(value = "cassandra_instances")
    protected final List<InstanceConfiguration> cassandraInstances;

    @JsonProperty(value = "driver_parameters")
    protected final DriverConfiguration driverConfiguration;

    @JsonProperty(value = "sidecar", required = true)
    protected final ServiceConfiguration serviceConfiguration;
    @JsonProperty("ssl")
    protected final SslConfiguration sslConfiguration;

    @JsonProperty("healthcheck")
    protected final HealthCheckConfiguration healthCheckConfiguration;

    @JsonProperty("cassandra_input_validation")
    protected final CassandraInputValidationConfiguration cassandraInputValidationConfiguration;

    @JsonProperty("blob_restore")
    protected final RestoreJobConfiguration restoreJobConfiguration;

    @JsonProperty("s3_client")
    protected final S3ClientConfiguration s3ClientConfiguration;

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
        healthCheckConfiguration = builder.healthCheckConfiguration;
        cassandraInputValidationConfiguration = builder.cassandraInputValidationConfiguration;
        driverConfiguration = builder.driverConfiguration;
        restoreJobConfiguration = builder.restoreJobConfiguration;
        s3ClientConfiguration = builder.s3ClientConfiguration;
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
     * @return the configuration for the health check service
     */
    @Override
    @JsonProperty("healthcheck")
    public HealthCheckConfiguration healthCheckConfiguration()
    {
        return healthCheckConfiguration;
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

    private static SimpleModule resolveYamlTypeMappings() throws IOException
    {
        String packageName = SidecarConfigurationImpl.class.getPackage().getName();
        String outerPackageName = SidecarConfiguration.class.getPackage().getName();
        SimpleModule module = new SimpleModule();
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
        private HealthCheckConfiguration healthCheckConfiguration = new HealthCheckConfigurationImpl();
        private CassandraInputValidationConfiguration cassandraInputValidationConfiguration
        = new CassandraInputValidationConfigurationImpl();
        private DriverConfiguration driverConfiguration = new DriverConfigurationImpl();
        private RestoreJobConfiguration restoreJobConfiguration = new RestoreJobConfigurationImpl();
        private S3ClientConfiguration s3ClientConfiguration = new S3ClientConfigurationImpl();

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
         * Sets the {@code healthCheckConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param healthCheckConfiguration the {@code healthCheckConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder healthCheckConfiguration(HealthCheckConfiguration healthCheckConfiguration)
        {
            return update(b -> b.healthCheckConfiguration = healthCheckConfiguration);
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
