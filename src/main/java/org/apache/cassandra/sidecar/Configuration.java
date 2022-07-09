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

import javax.annotation.Nullable;

import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.common.utils.ValidationConfiguration;

/**
 * Sidecar configuration
 */
public class Configuration
{
    /* Cassandra Instances Config */
    private final InstancesConfig instancesConfig;

    /* Sidecar's HTTP REST API port */
    private final Integer port;

    /* Sidecar's listen address */
    private String host;

    /* Healthcheck frequency in miilis */
    private final Integer healthCheckFrequencyMillis;

    /* SSL related settings */
    @Nullable
    private final String keyStorePath;

    @Nullable
    private final String keyStorePassword;

    @Nullable
    private final String trustStorePath;

    @Nullable
    private final String trustStorePassword;

    private final boolean isSslEnabled;

    private final long rateLimitStreamRequestsPerSecond;

    private final long throttleTimeoutInSeconds;

    private final long throttleDelayInSeconds;

    private final ValidationConfiguration validationConfiguration;

    public Configuration(InstancesConfig instancesConfig, String host, Integer port, Integer healthCheckFrequencyMillis,
                         boolean isSslEnabled, @Nullable String keyStorePath, @Nullable String keyStorePassword,
                         @Nullable String trustStorePath, @Nullable String trustStorePassword,
                         long rateLimitStreamRequestsPerSecond, long throttleTimeoutInSeconds,
                         long throttleDelayInSeconds, ValidationConfiguration validationConfiguration)
    {
        this.instancesConfig = instancesConfig;
        this.host = host;
        this.port = port;
        this.healthCheckFrequencyMillis = healthCheckFrequencyMillis;

        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
        this.isSslEnabled = isSslEnabled;
        this.rateLimitStreamRequestsPerSecond = rateLimitStreamRequestsPerSecond;
        this.throttleTimeoutInSeconds = throttleTimeoutInSeconds;
        this.throttleDelayInSeconds = throttleDelayInSeconds;
        this.validationConfiguration = validationConfiguration;
    }

    /**
     * Get Cassandra Instances config
     *
     * @return
     */
    public InstancesConfig getInstancesConfig()
    {
        return instancesConfig;
    }

    /**
     *  Sidecar's listen address
     *
     * @return
     */
    public String getHost()
    {
        return host;
    }

    /**
     * Get the Sidecar's REST HTTP API port
     *
     * @return
     */
    public Integer getPort()
    {
        return port;
    }

    /**
     * Get the health check frequency in millis
     *
     * @return
     */
    public Integer getHealthCheckFrequencyMillis()
    {
        return healthCheckFrequencyMillis;
    }

    /**
     * Get the SSL status
     *
     * @return
     */
    public boolean isSslEnabled()
    {
        return isSslEnabled;
    }

    /**
     * Get the Keystore Path
     *
     * @return
     */
    @Nullable
    public String getKeyStorePath()
    {
        return keyStorePath;
    }

    /**
     * Get the Keystore password
     *
     * @return
     */
    @Nullable
    public String getKeystorePassword()
    {
        return keyStorePassword;
    }

    /**
     * Get the Truststore Path
     *
     * @return
     */
    @Nullable
    public String getTrustStorePath()
    {
        return trustStorePath;
    }

    /**
     * Get the Truststore password
     *
     * @return
     */
    @Nullable
    public String getTruststorePassword()
    {
        return trustStorePassword;
    }

    /**
     * Get number of stream requests accepted per second
     *
     * @return
     */
    public long getRateLimitStreamRequestsPerSecond()
    {
        return rateLimitStreamRequestsPerSecond;
    }

    public long getThrottleTimeoutInSeconds()
    {
        return throttleTimeoutInSeconds;
    }

    public long getThrottleDelayInSeconds()
    {
        return throttleDelayInSeconds;
    }

    /**
     * Configuration Builder
     */
    public static class Builder
    {
        private InstancesConfig instancesConfig;
        private String host;
        private Integer port;
        private Integer healthCheckFrequencyMillis;
        private String keyStorePath;
        private String keyStorePassword;
        private String trustStorePath;
        private String trustStorePassword;
        private boolean isSslEnabled;
        private long rateLimitStreamRequestsPerSecond;
        private long throttleTimeoutInSeconds;
        private long throttleDelayInSeconds;

        private ValidationConfiguration validationConfiguration;

        public Builder setInstancesConfig(InstancesConfig instancesConfig)
        {
            this.instancesConfig = instancesConfig;
            return this;
        }

        public Builder setHost(String host)
        {
            this.host = host;
            return this;
        }

        public Builder setPort(Integer port)
        {
            this.port = port;
            return this;
        }

        public Builder setHealthCheckFrequency(Integer freqMillis)
        {
            this.healthCheckFrequencyMillis = freqMillis;
            return this;
        }

        public Builder setKeyStorePath(String path)
        {
            this.keyStorePath = path;
            return this;
        }

        public Builder setKeyStorePassword(String password)
        {
            this.keyStorePassword = password;
            return this;
        }

        public Builder setTrustStorePath(String path)
        {
            this.trustStorePath = path;
            return this;
        }

        public Builder setTrustStorePassword(String password)
        {
            this.trustStorePassword = password;
            return this;
        }

        public Builder setSslEnabled(boolean enabled)
        {
            this.isSslEnabled = enabled;
            return this;
        }

        public Builder setRateLimitStreamRequestsPerSecond(long rateLimitStreamRequestsPerSecond)
        {
            this.rateLimitStreamRequestsPerSecond = rateLimitStreamRequestsPerSecond;
            return this;
        }

        public Builder setThrottleTimeoutInSeconds(long throttleTimeoutInSeconds)
        {
            this.throttleTimeoutInSeconds = throttleTimeoutInSeconds;
            return this;
        }

        public Builder setThrottleDelayInSeconds(long throttleDelayInSeconds)
        {
            this.throttleDelayInSeconds = throttleDelayInSeconds;
            return this;
        }

        public Builder setValidationConfiguration(ValidationConfiguration validationConfiguration)
        {
            this.validationConfiguration = validationConfiguration;
            return this;
        }

        public Configuration build()
        {
            return new Configuration(instancesConfig, host, port, healthCheckFrequencyMillis, isSslEnabled,
                                     keyStorePath, keyStorePassword, trustStorePath, trustStorePassword,
                                     rateLimitStreamRequestsPerSecond, throttleTimeoutInSeconds,
                                     throttleDelayInSeconds, validationConfiguration);
        }
    }
}
