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
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Configuration for this Sidecar process
 */
public class SidecarConfiguration
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

    public SidecarConfiguration()
    {
        this.cassandraInstance = null;
        this.cassandraInstances = Collections.emptyList();
        this.serviceConfiguration = new ServiceConfiguration();
        this.sslConfiguration = null;
        this.healthCheckConfiguration = new HealthCheckConfiguration();
        this.cassandraInputValidationConfiguration = new CassandraInputValidationConfiguration();
    }

    protected SidecarConfiguration(Builder builder)
    {
        cassandraInstance = null;
        cassandraInstances = builder.cassandraInstances;
        serviceConfiguration = builder.serviceConfiguration;
        sslConfiguration = builder.sslConfiguration;
        healthCheckConfiguration = builder.healthCheckConfiguration;
        cassandraInputValidationConfiguration = builder.cassandraInputValidationConfiguration;
    }

    /**
     * @return a single configured cassandra instance
     * @deprecated in favor of configuring multiple instances in the yaml under cassandra_instances
     */
    @JsonProperty(value = "cassandra")
    @Deprecated
    InstanceConfiguration cassandra()
    {
        return cassandraInstance;
    }

    /**
     * @return the configured Cassandra instances that this Sidecar manages
     */
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
    @JsonProperty(value = "sidecar", required = true)
    public ServiceConfiguration serviceConfiguration()
    {
        return serviceConfiguration;
    }

    /**
     * @return the SSL configuration
     */
    @JsonProperty("ssl")
    public SslConfiguration sslConfiguration()
    {
        return sslConfiguration;
    }

    /**
     * @return the configuration for the health check service
     */
    @JsonProperty("healthcheck")
    public HealthCheckConfiguration healthCheckConfiguration()
    {
        return healthCheckConfiguration;
    }

    /**
     * @return the configuration for Cassandra input validation
     */
    @JsonProperty("cassandra_input_validation")
    public CassandraInputValidationConfiguration cassandraInputValidationConfiguration()
    {
        return cassandraInputValidationConfiguration;
    }

    @VisibleForTesting
    public Builder unbuild()
    {
        return new Builder(this);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static SidecarConfiguration readYamlConfiguration(String yamlConfigurationPath) throws IOException
    {
        return readYamlConfiguration(Paths.get(yamlConfigurationPath));
    }

    public static SidecarConfiguration readYamlConfiguration(Path yamlConfigurationPath) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
                              .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
                              .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
                              .configure(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS, true)
                              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        return mapper.readValue(yamlConfigurationPath.toFile(), SidecarConfiguration.class);
    }

    /**
     * {@code SidecarConfiguration} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, SidecarConfiguration>
    {
        private List<InstanceConfiguration> cassandraInstances;
        private ServiceConfiguration serviceConfiguration;
        private SslConfiguration sslConfiguration;
        private HealthCheckConfiguration healthCheckConfiguration = new HealthCheckConfiguration();
        private CassandraInputValidationConfiguration cassandraInputValidationConfiguration
        = new CassandraInputValidationConfiguration();

        protected Builder()
        {
        }

        protected Builder(SidecarConfiguration configuration)
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
        public Builder cassandraInstances(InstanceConfiguration... cassandraInstances)
        {
            return override(b -> b.cassandraInstances = Arrays.asList(cassandraInstances));
        }

        /**
         * Sets the {@code cassandraInstances} and returns a reference to this Builder enabling method chaining.
         *
         * @param cassandraInstances the {@code cassandraInstances} to set
         * @return a reference to this Builder
         */
        public Builder cassandraInstances(List<InstanceConfiguration> cassandraInstances)
        {
            return override(b -> b.cassandraInstances = cassandraInstances);
        }

        /**
         * Sets the {@code serviceConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param serviceConfiguration the {@code serviceConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder serviceConfiguration(ServiceConfiguration serviceConfiguration)
        {
            return override(b -> b.serviceConfiguration = serviceConfiguration);
        }

        /**
         * Sets the {@code sslConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param sslConfiguration the {@code sslConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder sslConfiguration(SslConfiguration sslConfiguration)
        {
            return override(b -> b.sslConfiguration = sslConfiguration);
        }

        /**
         * Sets the {@code healthCheckConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param healthCheckConfiguration the {@code healthCheckConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder healthCheckConfiguration(HealthCheckConfiguration healthCheckConfiguration)
        {
            return override(b -> b.healthCheckConfiguration = healthCheckConfiguration);
        }

        /**
         * Sets the {@code cassandraInputValidationConfiguration} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param cassandraInputValidationConfiguration the {@code cassandraInputValidationConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder cassandraInputValidationConfiguration(
        CassandraInputValidationConfiguration cassandraInputValidationConfiguration)
        {
            return override(b -> b.cassandraInputValidationConfiguration = cassandraInputValidationConfiguration);
        }

        /**
         * Returns a {@code SidecarConfiguration} built from the parameters previously set.
         *
         * @return a {@code SidecarConfiguration} built with parameters of this {@code SidecarConfiguration.Builder}
         */
        public SidecarConfiguration build()
        {
            return new SidecarConfiguration(this);
        }
    }
}
