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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.CassandraInputValidationConfiguration;
import org.apache.cassandra.sidecar.config.HealthCheckConfiguration;
import org.apache.cassandra.sidecar.config.InstanceConfiguration;
import org.apache.cassandra.sidecar.config.JmxConfiguration;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;
import org.apache.cassandra.sidecar.config.SSTableImportConfiguration;
import org.apache.cassandra.sidecar.config.SSTableUploadConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.ThrottleConfiguration;
import org.apache.cassandra.sidecar.config.TrafficShapingConfiguration;
import org.apache.cassandra.sidecar.config.WorkerPoolConfiguration;

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

    @JsonProperty(value = "sidecar", required = true)
    protected final ServiceConfiguration serviceConfiguration;

    @JsonProperty("ssl")
    protected final SslConfiguration sslConfiguration;

    @JsonProperty("healthcheck")
    protected final HealthCheckConfiguration healthCheckConfiguration;

    @JsonProperty("cassandra_input_validation")
    protected final CassandraInputValidationConfiguration cassandraInputValidationConfiguration;

    public SidecarConfigurationImpl()
    {
        this(Collections.emptyList(),
             new ServiceConfigurationImpl(),
             null /* sslConfiguration */,
             new HealthCheckConfigurationImpl(),
             new CassandraInputValidationConfigurationImpl());
    }

    public SidecarConfigurationImpl(ServiceConfiguration serviceConfiguration)
    {
        this(Collections.emptyList(),
             serviceConfiguration,
             null /* sslConfiguration */,
             new HealthCheckConfigurationImpl(),
             new CassandraInputValidationConfigurationImpl());
    }

    public SidecarConfigurationImpl(ServiceConfiguration serviceConfiguration,
                                    SslConfiguration sslConfiguration,
                                    HealthCheckConfiguration healthCheckConfiguration)
    {
        this(Collections.emptyList(),
             serviceConfiguration,
             sslConfiguration,
             healthCheckConfiguration,
             new CassandraInputValidationConfigurationImpl());
    }

    public SidecarConfigurationImpl(List<InstanceConfiguration> cassandraInstances,
                                    ServiceConfiguration serviceConfiguration,
                                    SslConfiguration sslConfiguration,
                                    HealthCheckConfiguration healthCheckConfiguration,
                                    CassandraInputValidationConfiguration cassandraInputValidationConfiguration)
    {
        this.cassandraInstance = null;
        this.cassandraInstances = Collections.unmodifiableList(cassandraInstances);
        this.serviceConfiguration = serviceConfiguration;
        this.sslConfiguration = sslConfiguration;
        this.healthCheckConfiguration = healthCheckConfiguration;
        this.cassandraInputValidationConfiguration = cassandraInputValidationConfiguration;
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
                                                            WorkerPoolConfigurationImpl.class)
                                    .addAbstractTypeMapping(JmxConfiguration.class,
                                                            JmxConfigurationImpl.class)
                                    .addAbstractTypeMapping(TrafficShapingConfiguration.class,
                                                            TrafficShapingConfigurationImpl.class);

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
                              .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
                              .configure(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS, true)
                              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                              .registerModule(simpleModule);

        return mapper.readValue(yamlConfigurationPath.toFile(), SidecarConfigurationImpl.class);
    }
}
