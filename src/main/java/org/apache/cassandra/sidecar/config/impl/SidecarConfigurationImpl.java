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

package org.apache.cassandra.sidecar.config.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.CassandraInputValidationConfiguration;
import org.apache.cassandra.sidecar.config.HealthCheckConfiguration;
import org.apache.cassandra.sidecar.config.InstanceConfiguration;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;
import org.apache.cassandra.sidecar.config.SSTableImportConfiguration;
import org.apache.cassandra.sidecar.config.SSTableUploadConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.ThrottleConfiguration;
import org.apache.cassandra.sidecar.config.WorkerPoolConfiguration;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Configuration for this Sidecar process
 */
public class SidecarConfigurationImpl implements SidecarConfiguration
{
    @Deprecated
    @JsonProperty(value = "cassandra")
    private final InstanceConfiguration cassandraInstance;

    @JsonProperty(value = "cassandra_instances")
    private final List<InstanceConfiguration> cassandraInstances;

    @JsonProperty(value = "sidecar", required = true)
    private final ServiceConfiguration serviceConfiguration;

    @JsonProperty("ssl")
    private final SslConfiguration sslConfiguration;

    @JsonProperty("healthcheck")
    private final HealthCheckConfiguration healthCheckConfiguration;

    @JsonProperty("cassandra_input_validation")
    private final CassandraInputValidationConfiguration cassandraInputValidationConfiguration;

    public SidecarConfigurationImpl()
    {
        this.cassandraInstance = null;
        this.cassandraInstances = Collections.emptyList();
        this.serviceConfiguration = new ServiceConfigurationImpl();
        this.sslConfiguration = null;
        this.healthCheckConfiguration = new HealthCheckConfigurationImpl();
        this.cassandraInputValidationConfiguration = new CassandraInputValidationConfigurationImpl();
    }

    protected SidecarConfigurationImpl(Builder<?> builder)
    {
        cassandraInstance = null;
        cassandraInstances = Collections.unmodifiableList(builder.cassandraInstances);
        serviceConfiguration = builder.serviceConfiguration;
        sslConfiguration = builder.sslConfiguration;
        healthCheckConfiguration = builder.healthCheckConfiguration;
        cassandraInputValidationConfiguration = builder.cassandraInputValidationConfiguration;
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

    /**
     * @return the configuration for Cassandra input validation
     */
    @Override
    @JsonProperty("cassandra_input_validation")
    public CassandraInputValidationConfiguration cassandraInputValidationConfiguration()
    {
        return cassandraInputValidationConfiguration;
    }

    @VisibleForTesting
    public Builder<?> unbuild()
    {
        return new Builder<>(this);
    }

    public static Builder<?> builder()
    {
        return new Builder<>();
    }

    public static SidecarConfigurationImpl readYamlConfiguration(String yamlConfigurationPath) throws IOException
    {
        return readYamlConfiguration(Paths.get(yamlConfigurationPath));
    }

    public static SidecarConfigurationImpl readYamlConfiguration(Path yamlConfigurationPath) throws IOException
    {
        SimpleModule simpleModule = new SimpleModule()
                                    .addAbstractTypeMapping(CacheConfiguration.class,
                                                            CacheConfigurationImpl.class)
                                    .addAbstractTypeMapping(CassandraInputValidationConfiguration.class,
                                                            CassandraInputValidationConfigurationImpl.class)
                                    .addAbstractTypeMapping(HealthCheckConfiguration.class,
                                                            HealthCheckConfigurationImpl.class)
                                    .addAbstractTypeMapping(InstanceConfiguration.class,
                                                            InstanceConfigurationImpl.class)
                                    .addAbstractTypeMapping(KeyStoreConfiguration.class,
                                                            KeyStoreConfigurationImpl.class)
                                    .addAbstractTypeMapping(SSTableImportConfiguration.class,
                                                            SSTableImportConfigurationImpl.class)
                                    .addAbstractTypeMapping(SSTableUploadConfiguration.class,
                                                            SSTableUploadConfigurationImpl.class)
                                    .addAbstractTypeMapping(ServiceConfiguration.class,
                                                            ServiceConfigurationImpl.class)
                                    .addAbstractTypeMapping(SidecarConfiguration.class,
                                                            SidecarConfigurationImpl.class)
                                    .addAbstractTypeMapping(SslConfiguration.class,
                                                            SslConfigurationImpl.class)
                                    .addAbstractTypeMapping(ThrottleConfiguration.class,
                                                            ThrottleConfigurationImpl.class)
                                    .addAbstractTypeMapping(WorkerPoolConfiguration.class,
                                                            WorkerPoolConfigurationImpl.class);

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
                              .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
                              .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
                              .configure(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS, true)
                              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                              .registerModule(simpleModule);

        return mapper.readValue(yamlConfigurationPath.toFile(), SidecarConfigurationImpl.class);
    }

    /**
     * {@code SidecarConfigurationImpl} builder static inner class.
     *
     * @param <T> the builder type
     */
    public static class Builder<T extends Builder<?>> implements DataObjectBuilder<T, SidecarConfigurationImpl>
    {
        protected List<InstanceConfiguration> cassandraInstances = new ArrayList<>();
        protected ServiceConfiguration serviceConfiguration;
        protected SslConfiguration sslConfiguration;
        protected HealthCheckConfiguration healthCheckConfiguration = new HealthCheckConfigurationImpl();
        protected CassandraInputValidationConfiguration cassandraInputValidationConfiguration
        = new CassandraInputValidationConfigurationImpl();

        protected Builder()
        {
        }

        protected Builder(SidecarConfigurationImpl configuration)
        {
            cassandraInstances = configuration.cassandraInstances;
            serviceConfiguration = configuration.serviceConfiguration;
            sslConfiguration = configuration.sslConfiguration;
            healthCheckConfiguration = configuration.healthCheckConfiguration;
            cassandraInputValidationConfiguration = configuration.cassandraInputValidationConfiguration;
        }

        /**
         * Sets the {@code cassandraInstances} and returns a reference to this Builder enabling method chaining.
         *
         * @param cassandraInstances the {@code cassandraInstances} to set
         * @return a reference to this Builder
         */
        public T cassandraInstances(InstanceConfiguration... cassandraInstances)
        {
            return update(b -> b.cassandraInstances = Arrays.asList(cassandraInstances));
        }

        /**
         * Sets the {@code cassandraInstances} and returns a reference to this Builder enabling method chaining.
         *
         * @param cassandraInstances the {@code cassandraInstances} to set
         * @return a reference to this Builder
         */
        public T cassandraInstances(List<InstanceConfiguration> cassandraInstances)
        {
            return update(b -> b.cassandraInstances = cassandraInstances);
        }

        /**
         * Sets the {@code serviceConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param serviceConfiguration the {@code serviceConfiguration} to set
         * @return a reference to this Builder
         */
        public T serviceConfiguration(ServiceConfiguration serviceConfiguration)
        {
            return update(b -> b.serviceConfiguration = serviceConfiguration);
        }

        /**
         * Sets the {@code sslConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param sslConfiguration the {@code sslConfiguration} to set
         * @return a reference to this Builder
         */
        public T sslConfiguration(SslConfiguration sslConfiguration)
        {
            return update(b -> b.sslConfiguration = sslConfiguration);
        }

        /**
         * Sets the {@code healthCheckConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param healthCheckConfiguration the {@code healthCheckConfiguration} to set
         * @return a reference to this Builder
         */
        public T healthCheckConfiguration(HealthCheckConfiguration healthCheckConfiguration)
        {
            return update(b -> b.healthCheckConfiguration = healthCheckConfiguration);
        }

        /**
         * Sets the {@code cassandraInputValidationConfiguration} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param cassandraInputValidationConfiguration the {@code cassandraInputValidationConfiguration} to set
         * @return a reference to this Builder
         */
        public T cassandraInputValidationConfiguration(CassandraInputValidationConfiguration
                                                       cassandraInputValidationConfiguration)
        {
            return update(b -> b.cassandraInputValidationConfiguration = cassandraInputValidationConfiguration);
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
