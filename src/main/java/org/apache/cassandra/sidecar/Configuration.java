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
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.WorkerPoolConfiguration;

/**
 * Sidecar configuration
 */
public class Configuration
{
    /* Cassandra Instances Config */
    private final InstancesConfig instancesConfig;

    /* Sidecar's HTTP REST API port */
    private final int port;

    /* Sidecar's listen address */
    private final String host;

    /* Healthcheck frequency in millis */
    private final int healthCheckFrequencyMillis;

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

    private final int allowableSkewInMinutes;

    private final int requestIdleTimeoutMillis;

    private final long requestTimeoutMillis;
    private final int ssTableImportPollIntervalMillis;

    private final float minSpacePercentRequiredForUpload;

    private final int concurrentUploadsLimit;

    private final ValidationConfiguration validationConfiguration;
    private final CacheConfiguration ssTableImportCacheConfiguration;
    private final WorkerPoolConfiguration serverWorkerPoolConfiguration;
    private final WorkerPoolConfiguration serverInternalWorkerPoolConfiguration;

    public Configuration(InstancesConfig instancesConfig,
                         String host,
                         int port,
                         int healthCheckFrequencyMillis,
                         boolean isSslEnabled,
                         @Nullable String keyStorePath,
                         @Nullable String keyStorePassword,
                         @Nullable String trustStorePath,
                         @Nullable String trustStorePassword,
                         long rateLimitStreamRequestsPerSecond,
                         long throttleTimeoutInSeconds,
                         long throttleDelayInSeconds,
                         int allowableSkewInMinutes,
                         int requestIdleTimeoutMillis,
                         long requestTimeoutMillis,
                         float minSpacePercentRequiredForUpload,
                         int concurrentUploadsLimit,
                         int ssTableImportPollIntervalMillis,
                         ValidationConfiguration validationConfiguration,
                         CacheConfiguration ssTableImportCacheConfiguration,
                         WorkerPoolConfiguration serverWorkerPoolConfiguration,
                         WorkerPoolConfiguration serverInternalWorkerPoolConfiguration)
    {
        this.instancesConfig = instancesConfig;
        this.host = host;
        this.port = port;
        this.healthCheckFrequencyMillis = healthCheckFrequencyMillis;
        this.ssTableImportPollIntervalMillis = ssTableImportPollIntervalMillis;
        this.ssTableImportCacheConfiguration = ssTableImportCacheConfiguration;

        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
        this.isSslEnabled = isSslEnabled;
        this.rateLimitStreamRequestsPerSecond = rateLimitStreamRequestsPerSecond;
        this.throttleTimeoutInSeconds = throttleTimeoutInSeconds;
        this.throttleDelayInSeconds = throttleDelayInSeconds;
        this.allowableSkewInMinutes = allowableSkewInMinutes;
        this.requestIdleTimeoutMillis = requestIdleTimeoutMillis;
        this.requestTimeoutMillis = requestTimeoutMillis;
        this.minSpacePercentRequiredForUpload = minSpacePercentRequiredForUpload;
        this.concurrentUploadsLimit = concurrentUploadsLimit;
        this.validationConfiguration = validationConfiguration;
        this.serverWorkerPoolConfiguration = serverWorkerPoolConfiguration;
        this.serverInternalWorkerPoolConfiguration = serverInternalWorkerPoolConfiguration;
    }

    /**
     * Constructs a new configuration object with the given {@link Builder}
     *
     * @param builder the configuration builder
     */
    protected Configuration(Builder builder)
    {
        instancesConfig = builder.instancesConfig;
        port = builder.port;
        host = builder.host;
        healthCheckFrequencyMillis = builder.healthCheckFrequencyMillis;
        keyStorePath = builder.keyStorePath;
        keyStorePassword = builder.keyStorePassword;
        trustStorePath = builder.trustStorePath;
        trustStorePassword = builder.trustStorePassword;
        isSslEnabled = builder.isSslEnabled;
        rateLimitStreamRequestsPerSecond = builder.rateLimitStreamRequestsPerSecond;
        throttleTimeoutInSeconds = builder.throttleTimeoutInSeconds;
        throttleDelayInSeconds = builder.throttleDelayInSeconds;
        allowableSkewInMinutes = builder.allowableSkewInMinutes;
        requestIdleTimeoutMillis = builder.requestIdleTimeoutMillis;
        requestTimeoutMillis = builder.requestTimeoutMillis;
        ssTableImportPollIntervalMillis = builder.ssTableImportPollIntervalMillis;
        minSpacePercentRequiredForUpload = builder.minSpacePercentRequiredForUploads;
        concurrentUploadsLimit = builder.concurrentUploadsLimit;
        validationConfiguration = builder.validationConfiguration;
        ssTableImportCacheConfiguration = builder.ssTableImportCacheConfiguration;
        serverWorkerPoolConfiguration = builder.serverWorkerPoolConfiguration;
        serverInternalWorkerPoolConfiguration = builder.serverInternalWorkerPoolConfiguration;
    }

    /**
     * @return the Cassandra Instances config
     */
    public InstancesConfig getInstancesConfig()
    {
        return instancesConfig;
    }

    /**
     * @return the Cassandra validation configuration
     */
    public ValidationConfiguration getValidationConfiguration()
    {
        return validationConfiguration;
    }

    /**
     * @return the listen address for Sidecar
     */
    public String getHost()
    {
        return host;
    }

    /**
     * @return the Sidecar's REST HTTP API port
     */
    public Integer getPort()
    {
        return port;
    }

    /**
     * @return the health check frequency in milliseconds
     */
    public int getHealthCheckFrequencyMillis()
    {
        return healthCheckFrequencyMillis;
    }

    /**
     * @return the SSTable import poll interval in milliseconds
     */
    public int getSSTableImportPollIntervalMillis()
    {
        return ssTableImportPollIntervalMillis;
    }

    /**
     * @return true if SSL is enabled, false otherwise
     */
    public boolean isSslEnabled()
    {
        return isSslEnabled;
    }

    /**
     * @return the Keystore Path
     */
    @Nullable
    public String getKeyStorePath()
    {
        return keyStorePath;
    }

    /**
     * @return the Keystore password
     */
    @Nullable
    public String getKeystorePassword()
    {
        return keyStorePassword;
    }

    /**
     * @return the Truststore Path
     */
    @Nullable
    public String getTrustStorePath()
    {
        return trustStorePath;
    }

    /**
     * @return the Truststore password
     */
    @Nullable
    public String getTruststorePassword()
    {
        return trustStorePassword;
    }

    /**
     * @return the number of stream requests accepted per second
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

    public int allowableSkewInMinutes()
    {
        return allowableSkewInMinutes;
    }

    public int getRequestIdleTimeoutMillis()
    {
        return this.requestIdleTimeoutMillis;
    }

    public long getRequestTimeoutMillis()
    {
        return this.requestTimeoutMillis;
    }

    public float getMinSpacePercentRequiredForUpload()
    {
        return this.minSpacePercentRequiredForUpload;
    }

    public int getConcurrentUploadsLimit()
    {
        return this.concurrentUploadsLimit;
    }

    public CacheConfiguration ssTableImportCacheConfiguration()
    {
        return ssTableImportCacheConfiguration;
    }

    public WorkerPoolConfiguration serverWorkerPoolConfiguration()
    {
        return serverWorkerPoolConfiguration;
    }

    public WorkerPoolConfiguration serverInternalWorkerPoolConfiguration()
    {
        return serverInternalWorkerPoolConfiguration;
    }

    /**
     * {@code Configuration} builder static inner class.
     * @param <T> the type that extends Builder
     */
    public static class Builder<T extends Builder<T>>
    {
        protected InstancesConfig instancesConfig;
        protected String host;
        protected int port;
        protected int healthCheckFrequencyMillis;
        protected String keyStorePath;
        protected String keyStorePassword;
        protected String trustStorePath;
        protected String trustStorePassword;
        protected boolean isSslEnabled;
        protected long rateLimitStreamRequestsPerSecond;
        protected long throttleTimeoutInSeconds;
        protected long throttleDelayInSeconds;
        protected int allowableSkewInMinutes;
        protected int requestIdleTimeoutMillis;
        protected long requestTimeoutMillis;
        protected int ssTableImportPollIntervalMillis = 100;
        protected float minSpacePercentRequiredForUploads;
        protected int concurrentUploadsLimit;
        protected ValidationConfiguration validationConfiguration;
        protected CacheConfiguration ssTableImportCacheConfiguration;
        protected WorkerPoolConfiguration serverWorkerPoolConfiguration;
        protected WorkerPoolConfiguration serverInternalWorkerPoolConfiguration;

        protected T self()
        {
            //noinspection unchecked
            return (T) this;
        }

        /**
         * Sets the {@code instancesConfig} and returns a reference to this Builder enabling method chaining.
         *
         * @param instancesConfig the {@code instancesConfig} to set
         * @return a reference to this Builder
         */
        public T setInstancesConfig(InstancesConfig instancesConfig)
        {
            this.instancesConfig = instancesConfig;
            return self();
        }

        /**
         * Sets the {@code host} and returns a reference to this Builder enabling method chaining.
         *
         * @param host the {@code host} to set
         * @return a reference to this Builder
         */
        public T setHost(String host)
        {
            this.host = host;
            return self();
        }

        /**
         * Sets the {@code port} and returns a reference to this Builder enabling method chaining.
         *
         * @param port the {@code port} to set
         * @return a reference to this Builder
         */
        public T setPort(int port)
        {
            this.port = port;
            return self();
        }

        /**
         * Sets the {@code healthCheckFrequencyMillis} and returns a reference to this Builder enabling method chaining.
         *
         * @param freqMillis the {@code healthCheckFrequencyMillis} to set
         * @return a reference to this Builder
         */
        public T setHealthCheckFrequency(int freqMillis)
        {
            healthCheckFrequencyMillis = freqMillis;
            return self();
        }

        /**
         * Sets the {@code keyStorePath} and returns a reference to this Builder enabling method chaining.
         *
         * @param path the {@code keyStorePath} to set
         * @return a reference to this Builder
         */
        public T setKeyStorePath(String path)
        {
            keyStorePath = path;
            return self();
        }

        /**
         * Sets the {@code keyStorePassword} and returns a reference to this Builder enabling method chaining.
         *
         * @param password the {@code keyStorePassword} to set
         * @return a reference to this Builder
         */
        public T setKeyStorePassword(String password)
        {
            keyStorePassword = password;
            return self();
        }

        /**
         * Sets the {@code trustStorePath} and returns a reference to this Builder enabling method chaining.
         *
         * @param path the {@code trustStorePath} to set
         * @return a reference to this Builder
         */
        public T setTrustStorePath(String path)
        {
            trustStorePath = path;
            return self();
        }

        /**
         * Sets the {@code trustStorePassword} and returns a reference to this Builder enabling method chaining.
         *
         * @param password the {@code trustStorePassword} to set
         * @return a reference to this Builder
         */
        public T setTrustStorePassword(String password)
        {
            trustStorePassword = password;
            return self();
        }

        /**
         * Sets the {@code isSslEnabled} and returns a reference to this Builder enabling method chaining.
         *
         * @param enabled the {@code isSslEnabled} to set
         * @return a reference to this Builder
         */
        public T setSslEnabled(boolean enabled)
        {
            isSslEnabled = enabled;
            return self();
        }

        /**
         * Sets the {@code rateLimitStreamRequestsPerSecond} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param rateLimitStreamRequestsPerSecond the {@code rateLimitStreamRequestsPerSecond} to set
         * @return a reference to this Builder
         */
        public T setRateLimitStreamRequestsPerSecond(long rateLimitStreamRequestsPerSecond)
        {
            this.rateLimitStreamRequestsPerSecond = rateLimitStreamRequestsPerSecond;
            return self();
        }

        /**
         * Sets the {@code throttleTimeoutInSeconds} and returns a reference to this Builder enabling method chaining.
         *
         * @param throttleTimeoutInSeconds the {@code throttleTimeoutInSeconds} to set
         * @return a reference to this Builder
         */
        public T setThrottleTimeoutInSeconds(long throttleTimeoutInSeconds)
        {
            this.throttleTimeoutInSeconds = throttleTimeoutInSeconds;
            return self();
        }

        /**
         * Sets the {@code throttleDelayInSeconds} and returns a reference to this Builder enabling method chaining.
         *
         * @param throttleDelayInSeconds the {@code throttleDelayInSeconds} to set
         * @return a reference to this Builder
         */
        public T setThrottleDelayInSeconds(long throttleDelayInSeconds)
        {
            this.throttleDelayInSeconds = throttleDelayInSeconds;
            return self();
        }

        /**
         * Sets the {@code allowableSkewInMinutes} and returns a reference to this Builder enabling method chaining.
         *
         * @param allowableSkewInMinutes the {@code allowableSkewInMinutes} to set
         * @return a reference to this Builder
         */
        public T setAllowableSkewInMinutes(int allowableSkewInMinutes)
        {
            this.allowableSkewInMinutes = allowableSkewInMinutes;
            return self();
        }

        /**
         * Sets the {@code requestIdleTimeoutMillis} and returns a reference to this Builder enabling method chaining.
         *
         * @param requestIdleTimeoutMillis the {@code requestIdleTimeoutMillis} to set
         * @return a reference to this Builder
         */
        public T setRequestIdleTimeoutMillis(int requestIdleTimeoutMillis)
        {
            this.requestIdleTimeoutMillis = requestIdleTimeoutMillis;
            return self();
        }

        /**
         * Sets the {@code requestTimeoutMillis} and returns a reference to this Builder enabling method chaining.
         *
         * @param requestTimeoutMillis the {@code requestTimeoutMillis} to set
         * @return a reference to this Builder
         */
        public T setRequestTimeoutMillis(long requestTimeoutMillis)
        {
            this.requestTimeoutMillis = requestTimeoutMillis;
            return self();
        }

        /**
         * Sets the {@code ssTableImportPollIntervalMillis} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param ssTableImportPollIntervalMillis the {@code ssTableImportPollIntervalMillis} to set
         * @return a reference to this Builder
         */
        public T setSSTableImportPollIntervalMillis(int ssTableImportPollIntervalMillis)
        {
            this.ssTableImportPollIntervalMillis = ssTableImportPollIntervalMillis;
            return self();
        }

        /**
         * Sets the {@code minSpacePercentRequiredForUpload} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param minSpacePercentRequiredForUploads the {@code minSpacePercentRequiredForUpload} to set
         * @return a reference to this Builder
         */
        public T setMinSpacePercentRequiredForUploads(float minSpacePercentRequiredForUploads)
        {
            this.minSpacePercentRequiredForUploads = minSpacePercentRequiredForUploads;
            return self();
        }

        /**
         * Sets the {@code concurrentUploadsLimit} and returns a reference to this Builder enabling method chaining.
         *
         * @param concurrentUploadsLimit the {@code concurrentUploadsLimit} to set
         * @return a reference to this Builder
         */
        public T setConcurrentUploadsLimit(int concurrentUploadsLimit)
        {
            this.concurrentUploadsLimit = concurrentUploadsLimit;
            return self();
        }

        /**
         * Sets the {@code validationConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param validationConfiguration the {@code validationConfiguration} to set
         * @return a reference to this Builder
         */
        public T setValidationConfiguration(ValidationConfiguration validationConfiguration)
        {
            this.validationConfiguration = validationConfiguration;
            return self();
        }

        /**
         * Sets the {@code ssTableImportCacheConfiguration} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param cacheConfiguration the {@code ssTableImportCacheConfiguration} to set
         * @return a reference to this Builder
         */
        public T setSSTableImportCacheConfiguration(CacheConfiguration cacheConfiguration)
        {
            ssTableImportCacheConfiguration = cacheConfiguration;
            return self();
        }

        /**
         * Sets the {@code serverWorkerPoolConfiguration} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param workerPoolConfiguration the {@code serverWorkerPoolConfiguration} to set
         * @return a reference to this Builder
         */
        public T setServerWorkerPoolConfiguration(WorkerPoolConfiguration workerPoolConfiguration)
        {
            serverWorkerPoolConfiguration = workerPoolConfiguration;
            return self();
        }

        /**
         * Sets the {@code serverInternalWorkerPoolConfiguration} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param workerPoolConfiguration the {@code serverInternalWorkerPoolConfiguration} to set
         * @return a reference to this Builder
         */
        public T setServerInternalWorkerPoolConfiguration(WorkerPoolConfiguration workerPoolConfiguration)
        {
            serverInternalWorkerPoolConfiguration = workerPoolConfiguration;
            return self();
        }

        /**
         * Returns a {@code Configuration} built from the parameters previously set.
         *
         * @return a {@code Configuration} built with parameters of this {@code Configuration.Builder}
         */
        public Configuration build()
        {
            return new Configuration(this);
        }
    }
}
