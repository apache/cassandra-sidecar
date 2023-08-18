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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Configuration for the Sidecar Service and configuration of the REST endpoints in the service
 */
public class ServiceConfiguration
{
    public static final String HOST_PROPERTY = "host";
    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final String PORT_PROPERTY = "port";
    public static final int DEFAULT_PORT = 9043;
    public static final String REQUEST_IDLE_TIMEOUT_MILLIS_PROPERTY = "request_idle_timeout_millis";
    public static final int DEFAULT_REQUEST_IDLE_TIMEOUT_MILLIS = 300000;
    public static final String REQUEST_TIMEOUT_MILLIS_PROPERTY = "request_timeout_millis";
    public static final long DEFAULT_REQUEST_TIMEOUT_MILLIS = 300000L;
    public static final String ALLOWABLE_SKEW_IN_MINUTES_PROPERTY = "allowable_time_skew_in_minutes";
    public static final int DEFAULT_ALLOWABLE_SKEW_IN_MINUTES = 60;
    public static final String THROTTLE_PROPERTY = "throttle";
    public static final String SSTABLE_UPLOAD_PROPERTY = "sstable_upload";
    public static final String SSTABLE_IMPORT_PROPERTY = "sstable_import";
    public static final String WORKER_POOLS_PROPERTY = "worker_pools";

    public static final String SERVICE_POOL = "service";
    public static final String INTERNAL_POOL = "internal";
    protected static final Map<String, WorkerPoolConfiguration> DEFAULT_WORKER_POOLS_CONFIGURATION
    = Collections.unmodifiableMap(new HashMap<String, WorkerPoolConfiguration>()
    {{
        put(SERVICE_POOL, WorkerPoolConfiguration.builder()
                                                 .workerPoolName("sidecar-worker-pool")
                                                 .workerPoolSize(20)
                                                 .workerMaxExecutionTimeMillis(TimeUnit.SECONDS.toMillis(60))
                                                 .build());


        put(INTERNAL_POOL, WorkerPoolConfiguration.builder()
                                                  .workerPoolName("sidecar-internal-worker-pool")
                                                  .workerPoolSize(20)
                                                  .workerMaxExecutionTimeMillis(TimeUnit.MINUTES.toMillis(15))
                                                  .build());
    }});


    @JsonProperty(value = HOST_PROPERTY, defaultValue = DEFAULT_HOST)
    protected final String host;

    @JsonProperty(value = PORT_PROPERTY, defaultValue = DEFAULT_PORT + "")
    private final int port;

    @JsonProperty(value = REQUEST_IDLE_TIMEOUT_MILLIS_PROPERTY, defaultValue = DEFAULT_REQUEST_IDLE_TIMEOUT_MILLIS + "")
    private final int requestIdleTimeoutMillis;

    @JsonProperty(value = REQUEST_TIMEOUT_MILLIS_PROPERTY, defaultValue = DEFAULT_REQUEST_TIMEOUT_MILLIS + "")
    private final long requestTimeoutMillis;

    @JsonProperty(value = ALLOWABLE_SKEW_IN_MINUTES_PROPERTY, defaultValue = DEFAULT_ALLOWABLE_SKEW_IN_MINUTES + "")
    private final int allowableSkewInMinutes;

    @JsonProperty(value = THROTTLE_PROPERTY, required = true)
    private final ThrottleConfiguration throttleConfiguration;

    @JsonProperty(value = SSTABLE_UPLOAD_PROPERTY, required = true)
    private final SSTableUploadConfiguration ssTableUploadConfiguration;

    @JsonProperty(value = SSTABLE_IMPORT_PROPERTY, required = true)
    private final SSTableImportConfiguration ssTableImportConfiguration;

    @JsonProperty(value = WORKER_POOLS_PROPERTY, required = true)
    private final Map<String, ? extends WorkerPoolConfiguration> workerPoolsConfiguration;

    public ServiceConfiguration()
    {
        host = DEFAULT_HOST;
        port = DEFAULT_PORT;
        requestIdleTimeoutMillis = DEFAULT_REQUEST_IDLE_TIMEOUT_MILLIS;
        requestTimeoutMillis = DEFAULT_REQUEST_TIMEOUT_MILLIS;
        allowableSkewInMinutes = DEFAULT_ALLOWABLE_SKEW_IN_MINUTES;
        throttleConfiguration = new ThrottleConfiguration();
        ssTableUploadConfiguration = new SSTableUploadConfiguration();
        ssTableImportConfiguration = new SSTableImportConfiguration();
        workerPoolsConfiguration = DEFAULT_WORKER_POOLS_CONFIGURATION;
    }

    private ServiceConfiguration(Builder builder)
    {
        host = builder.host;
        port = builder.port;
        requestIdleTimeoutMillis = builder.requestIdleTimeoutMillis;
        requestTimeoutMillis = builder.requestTimeoutMillis;
        allowableSkewInMinutes = builder.allowableSkewInMinutes;
        throttleConfiguration = builder.throttleConfiguration;
        ssTableUploadConfiguration = builder.ssTableUploadConfiguration;
        ssTableImportConfiguration = builder.ssTableImportConfiguration;
        if (builder.workerPoolsConfiguration == null || builder.workerPoolsConfiguration.isEmpty())
        {
            workerPoolsConfiguration = DEFAULT_WORKER_POOLS_CONFIGURATION;
        }
        else
        {
            workerPoolsConfiguration = builder.workerPoolsConfiguration;
        }
    }

    /**
     * Sidecar's HTTP REST API listen address
     */
    @JsonProperty(value = HOST_PROPERTY, defaultValue = DEFAULT_HOST)
    public String host()
    {
        return host;
    }

    /**
     * @return Sidecar's HTTP REST API port
     */
    @JsonProperty(value = PORT_PROPERTY, defaultValue = DEFAULT_PORT + "")
    public int port()
    {
        return port;
    }

    /**
     * Determines if a connection will timeout and be closed if no data is received nor sent within the timeout.
     * Zero means don't timeout.
     *
     * @return the configured idle timeout value
     */
    @JsonProperty(value = REQUEST_IDLE_TIMEOUT_MILLIS_PROPERTY, defaultValue = DEFAULT_REQUEST_IDLE_TIMEOUT_MILLIS + "")
    public int requestIdleTimeoutMillis()
    {
        return requestIdleTimeoutMillis;
    }

    /**
     * Determines if a response will timeout if the response has not been written after a certain time.
     */
    @JsonProperty(value = REQUEST_TIMEOUT_MILLIS_PROPERTY, defaultValue = DEFAULT_REQUEST_TIMEOUT_MILLIS + "")
    public long requestTimeoutMillis()
    {
        return requestTimeoutMillis;
    }

    /**
     * @return the maximum time skew allowed between the server and the client
     */
    @JsonProperty(value = ALLOWABLE_SKEW_IN_MINUTES_PROPERTY, defaultValue = DEFAULT_ALLOWABLE_SKEW_IN_MINUTES + "")
    public int allowableSkewInMinutes()
    {
        return allowableSkewInMinutes;
    }

    /**
     * @return the throttling configuration
     */
    @JsonProperty(value = THROTTLE_PROPERTY, required = true)
    public ThrottleConfiguration throttleConfiguration()
    {
        return throttleConfiguration;
    }

    /**
     * @return the configuration for SSTable component uploads on this service
     */
    @JsonProperty(value = SSTABLE_UPLOAD_PROPERTY, required = true)
    public SSTableUploadConfiguration ssTableUploadConfiguration()
    {
        return ssTableUploadConfiguration;
    }

    /**
     * @return the configuration for the SSTable Import functionality
     */
    @JsonProperty(value = SSTABLE_IMPORT_PROPERTY, required = true)
    public SSTableImportConfiguration ssTableImportConfiguration()
    {
        return ssTableImportConfiguration;
    }

    /**
     * @return the configured worker pools for the service
     */
    @JsonProperty(value = WORKER_POOLS_PROPERTY, required = true)
    public Map<String, ? extends WorkerPoolConfiguration> workerPoolsConfiguration()
    {
        return workerPoolsConfiguration;
    }

    /**
     * @return the configuration for the {@link #SERVICE_POOL}
     */
    public WorkerPoolConfiguration serverWorkerPoolConfiguration()
    {
        return workerPoolsConfiguration.get(SERVICE_POOL);
    }

    /**
     * @return the configuration for the {@link #INTERNAL_POOL}
     */
    public WorkerPoolConfiguration serverInternalWorkerPoolConfiguration()
    {
        return workerPoolsConfiguration.get(INTERNAL_POOL);
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

    /**
     * {@code ServiceConfiguration} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, ServiceConfiguration>
    {
        private String host = DEFAULT_HOST;
        private int port = DEFAULT_PORT;
        private int requestIdleTimeoutMillis = DEFAULT_REQUEST_IDLE_TIMEOUT_MILLIS;
        private long requestTimeoutMillis = DEFAULT_REQUEST_TIMEOUT_MILLIS;
        private int allowableSkewInMinutes = DEFAULT_ALLOWABLE_SKEW_IN_MINUTES;
        private ThrottleConfiguration throttleConfiguration = new ThrottleConfiguration();
        private SSTableUploadConfiguration ssTableUploadConfiguration = new SSTableUploadConfiguration();
        private SSTableImportConfiguration ssTableImportConfiguration = new SSTableImportConfiguration();
        private Map<String, WorkerPoolConfiguration> workerPoolsConfiguration = new HashMap<>();

        protected Builder()
        {
        }

        @SuppressWarnings("unchecked")
        protected Builder(ServiceConfiguration serviceConfiguration)
        {
            host = serviceConfiguration.host;
            port = serviceConfiguration.port;
            requestIdleTimeoutMillis = serviceConfiguration.requestIdleTimeoutMillis;
            requestTimeoutMillis = serviceConfiguration.requestTimeoutMillis;
            allowableSkewInMinutes = serviceConfiguration.allowableSkewInMinutes;
            throttleConfiguration = serviceConfiguration.throttleConfiguration;
            ssTableUploadConfiguration = serviceConfiguration.ssTableUploadConfiguration;
            ssTableImportConfiguration = serviceConfiguration.ssTableImportConfiguration;
            workerPoolsConfiguration = (Map<String, WorkerPoolConfiguration>)
                                       serviceConfiguration.workerPoolsConfiguration;
        }

        /**
         * Sets the {@code host} and returns a reference to this Builder enabling method chaining.
         *
         * @param host the {@code host} to set
         * @return a reference to this Builder
         */
        public Builder host(String host)
        {
            return override(b -> b.host = host);
        }

        /**
         * Sets the {@code port} and returns a reference to this Builder enabling method chaining.
         *
         * @param port the {@code port} to set
         * @return a reference to this Builder
         */
        public Builder port(int port)
        {
            return override(b -> b.port = port);
        }

        /**
         * Sets the {@code requestIdleTimeoutMillis} and returns a reference to this Builder enabling method chaining.
         *
         * @param requestIdleTimeoutMillis the {@code requestIdleTimeoutMillis} to set
         * @return a reference to this Builder
         */
        public Builder requestIdleTimeoutMillis(int requestIdleTimeoutMillis)
        {
            return override(b -> b.requestIdleTimeoutMillis = requestIdleTimeoutMillis);
        }

        /**
         * Sets the {@code requestTimeoutMillis} and returns a reference to this Builder enabling method chaining.
         *
         * @param requestTimeoutMillis the {@code requestTimeoutMillis} to set
         * @return a reference to this Builder
         */
        public Builder requestTimeoutMillis(long requestTimeoutMillis)
        {
            return override(b -> b.requestTimeoutMillis = requestTimeoutMillis);
        }

        /**
         * Sets the {@code allowableSkewInMinutes} and returns a reference to this Builder enabling method chaining.
         *
         * @param allowableSkewInMinutes the {@code allowableSkewInMinutes} to set
         * @return a reference to this Builder
         */
        public Builder allowableSkewInMinutes(int allowableSkewInMinutes)
        {
            return override(b -> b.allowableSkewInMinutes = allowableSkewInMinutes);
        }

        /**
         * Sets the {@code throttleConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param throttleConfiguration the {@code throttleConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder throttleConfiguration(ThrottleConfiguration throttleConfiguration)
        {
            return override(b -> b.throttleConfiguration = throttleConfiguration);
        }

        /**
         * Sets the {@code ssTableUploadConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param ssTableUploadConfiguration the {@code ssTableUploadConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder ssTableUploadConfiguration(SSTableUploadConfiguration ssTableUploadConfiguration)
        {
            return override(b -> b.ssTableUploadConfiguration = ssTableUploadConfiguration);
        }

        /**
         * Sets the {@code ssTableImportConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param ssTableImportConfiguration the {@code ssTableImportConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder ssTableImportConfiguration(SSTableImportConfiguration ssTableImportConfiguration)
        {
            return override(b -> b.ssTableImportConfiguration = ssTableImportConfiguration);
        }

        /**
         * Sets the {@code workerPoolsConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param workerPoolsConfiguration the {@code workerPoolsConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder workerPoolsConfiguration(Map<String, WorkerPoolConfiguration> workerPoolsConfiguration)
        {
            return override(b -> b.workerPoolsConfiguration = workerPoolsConfiguration);
        }


        /**
         * Sets the {@link #SERVICE_POOL} configuration to {@code workerPoolConfiguration} and returns a
         * reference to this Builder enabling method chaining.
         *
         * @param workerPoolConfiguration the {@code workerPoolConfiguration} to set for the {@link #SERVICE_POOL}
         * @return a reference to this Builder
         */
        public Builder servicePoolConfiguration(WorkerPoolConfiguration workerPoolConfiguration)
        {
            workerPoolsConfiguration.put(SERVICE_POOL, workerPoolConfiguration);
            return self();
        }

        /**
         * Sets the {@link #INTERNAL_POOL} configuration to {@code workerPoolConfiguration} and returns a
         * reference to this Builder enabling method chaining.
         *
         * @param workerPoolConfiguration the {@code workerPoolConfiguration} to set for the {@link #INTERNAL_POOL}
         * @return a reference to this Builder
         */
        public Builder internalPoolConfiguration(WorkerPoolConfiguration workerPoolConfiguration)
        {
            workerPoolsConfiguration.put(INTERNAL_POOL, workerPoolConfiguration);
            return self();
        }

        /**
         * Returns a {@code ServiceConfiguration} built from the parameters previously set.
         *
         * @return a {@code ServiceConfiguration} built with parameters of this {@code ServiceConfiguration.Builder}
         */
        @Override
        public ServiceConfiguration build()
        {
            return new ServiceConfiguration(this);
        }
    }
}
