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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.JmxConfiguration;
import org.apache.cassandra.sidecar.config.SSTableImportConfiguration;
import org.apache.cassandra.sidecar.config.SSTableUploadConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.ThrottleConfiguration;
import org.apache.cassandra.sidecar.config.WorkerPoolConfiguration;

/**
 * Configuration for the Sidecar Service and configuration of the REST endpoints in the service
 */
public class ServiceConfigurationImpl implements ServiceConfiguration
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
    protected static final Map<String, WorkerPoolConfiguration> DEFAULT_WORKER_POOLS_CONFIGURATION
    = Collections.unmodifiableMap(new HashMap<String, WorkerPoolConfiguration>()
    {{
        put(SERVICE_POOL, new WorkerPoolConfigurationImpl("sidecar-worker-pool", 20,
                                                          TimeUnit.SECONDS.toMillis(60)));

        put(INTERNAL_POOL, new WorkerPoolConfigurationImpl("sidecar-internal-worker-pool", 20,
                                                           TimeUnit.MINUTES.toMillis(15)));
    }});


    @JsonProperty(value = HOST_PROPERTY, defaultValue = DEFAULT_HOST)
    protected final String host;

    @JsonProperty(value = PORT_PROPERTY, defaultValue = DEFAULT_PORT + "")
    protected final int port;

    @JsonProperty(value = REQUEST_IDLE_TIMEOUT_MILLIS_PROPERTY, defaultValue = DEFAULT_REQUEST_IDLE_TIMEOUT_MILLIS + "")
    protected final int requestIdleTimeoutMillis;

    @JsonProperty(value = REQUEST_TIMEOUT_MILLIS_PROPERTY, defaultValue = DEFAULT_REQUEST_TIMEOUT_MILLIS + "")
    protected final long requestTimeoutMillis;

    @JsonProperty(value = ALLOWABLE_SKEW_IN_MINUTES_PROPERTY, defaultValue = DEFAULT_ALLOWABLE_SKEW_IN_MINUTES + "")
    protected final int allowableSkewInMinutes;

    @JsonProperty(value = THROTTLE_PROPERTY, required = true)
    protected final ThrottleConfiguration throttleConfiguration;

    @JsonProperty(value = SSTABLE_UPLOAD_PROPERTY, required = true)
    protected final SSTableUploadConfiguration ssTableUploadConfiguration;

    @JsonProperty(value = SSTABLE_IMPORT_PROPERTY, required = true)
    protected final SSTableImportConfiguration ssTableImportConfiguration;

    @JsonProperty(value = WORKER_POOLS_PROPERTY, required = true)
    protected final Map<String, ? extends WorkerPoolConfiguration> workerPoolsConfiguration;

    @JsonProperty("jmx")
    protected final JmxConfiguration jmxConfiguration;

    public ServiceConfigurationImpl()
    {
        this(DEFAULT_HOST,
             DEFAULT_PORT,
             DEFAULT_REQUEST_IDLE_TIMEOUT_MILLIS,
             DEFAULT_REQUEST_TIMEOUT_MILLIS,
             DEFAULT_ALLOWABLE_SKEW_IN_MINUTES,
             new ThrottleConfigurationImpl(),
             new SSTableUploadConfigurationImpl(),
             new SSTableImportConfigurationImpl(),
             DEFAULT_WORKER_POOLS_CONFIGURATION,
             new JmxConfigurationImpl());
    }

    public ServiceConfigurationImpl(SSTableImportConfiguration ssTableImportConfiguration)
    {
        this(DEFAULT_HOST,
             DEFAULT_PORT,
             DEFAULT_REQUEST_IDLE_TIMEOUT_MILLIS,
             DEFAULT_REQUEST_TIMEOUT_MILLIS,
             DEFAULT_ALLOWABLE_SKEW_IN_MINUTES,
             new ThrottleConfigurationImpl(),
             new SSTableUploadConfigurationImpl(),
             ssTableImportConfiguration,
             DEFAULT_WORKER_POOLS_CONFIGURATION,
             new JmxConfigurationImpl());
    }

    public ServiceConfigurationImpl(String host,
                                    ThrottleConfiguration throttleConfiguration,
                                    SSTableUploadConfiguration ssTableUploadConfiguration,
                                    JmxConfiguration jmxConfiguration)
    {
        this(host,
             DEFAULT_PORT,
             DEFAULT_REQUEST_IDLE_TIMEOUT_MILLIS,
             DEFAULT_REQUEST_TIMEOUT_MILLIS,
             DEFAULT_ALLOWABLE_SKEW_IN_MINUTES,
             throttleConfiguration,
             ssTableUploadConfiguration,
             new SSTableImportConfigurationImpl(),
             DEFAULT_WORKER_POOLS_CONFIGURATION,
             jmxConfiguration);
    }

    public ServiceConfigurationImpl(int requestIdleTimeoutMillis,
                                    long requestTimeoutMillis,
                                    SSTableUploadConfiguration ssTableUploadConfiguration)
    {

        this(DEFAULT_HOST,
             DEFAULT_PORT,
             requestIdleTimeoutMillis,
             requestTimeoutMillis,
             DEFAULT_ALLOWABLE_SKEW_IN_MINUTES,
             new ThrottleConfigurationImpl(),
             ssTableUploadConfiguration,
             new SSTableImportConfigurationImpl(),
             DEFAULT_WORKER_POOLS_CONFIGURATION,
             new JmxConfigurationImpl());
    }

    public ServiceConfigurationImpl(String host,
                                    int port,
                                    int requestIdleTimeoutMillis,
                                    long requestTimeoutMillis,
                                    int allowableSkewInMinutes,
                                    ThrottleConfiguration throttleConfiguration,
                                    SSTableUploadConfiguration ssTableUploadConfiguration,
                                    SSTableImportConfiguration ssTableImportConfiguration,
                                    Map<String, ? extends WorkerPoolConfiguration> workerPoolsConfiguration,
                                    JmxConfiguration jmxConfiguration)
    {
        this.host = host;
        this.port = port;
        this.requestIdleTimeoutMillis = requestIdleTimeoutMillis;
        this.requestTimeoutMillis = requestTimeoutMillis;
        this.allowableSkewInMinutes = allowableSkewInMinutes;
        this.throttleConfiguration = throttleConfiguration;
        this.ssTableUploadConfiguration = ssTableUploadConfiguration;
        this.ssTableImportConfiguration = ssTableImportConfiguration;
        this.jmxConfiguration = jmxConfiguration;
        if (workerPoolsConfiguration == null || workerPoolsConfiguration.isEmpty())
        {
            this.workerPoolsConfiguration = DEFAULT_WORKER_POOLS_CONFIGURATION;
        }
        else
        {
            this.workerPoolsConfiguration = workerPoolsConfiguration;
        }
    }

    public ServiceConfigurationImpl(String host)
    {
        this(host,
             DEFAULT_PORT,
             DEFAULT_REQUEST_IDLE_TIMEOUT_MILLIS,
             DEFAULT_REQUEST_TIMEOUT_MILLIS,
             DEFAULT_ALLOWABLE_SKEW_IN_MINUTES,
             new ThrottleConfigurationImpl(),
             new SSTableUploadConfigurationImpl(),
             new SSTableImportConfigurationImpl(),
             DEFAULT_WORKER_POOLS_CONFIGURATION,
             new JmxConfigurationImpl());
    }

    /**
     * Sidecar's HTTP REST API listen address
     */
    @Override
    @JsonProperty(value = HOST_PROPERTY, defaultValue = DEFAULT_HOST)
    public String host()
    {
        return host;
    }

    /**
     * @return Sidecar's HTTP REST API port
     */
    @Override
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
    @Override
    @JsonProperty(value = REQUEST_IDLE_TIMEOUT_MILLIS_PROPERTY, defaultValue = DEFAULT_REQUEST_IDLE_TIMEOUT_MILLIS + "")
    public int requestIdleTimeoutMillis()
    {
        return requestIdleTimeoutMillis;
    }

    /**
     * Determines if a response will timeout if the response has not been written after a certain time.
     */
    @Override
    @JsonProperty(value = REQUEST_TIMEOUT_MILLIS_PROPERTY, defaultValue = DEFAULT_REQUEST_TIMEOUT_MILLIS + "")
    public long requestTimeoutMillis()
    {
        return requestTimeoutMillis;
    }

    /**
     * @return the maximum time skew allowed between the server and the client
     */
    @Override
    @JsonProperty(value = ALLOWABLE_SKEW_IN_MINUTES_PROPERTY, defaultValue = DEFAULT_ALLOWABLE_SKEW_IN_MINUTES + "")
    public int allowableSkewInMinutes()
    {
        return allowableSkewInMinutes;
    }

    /**
     * @return the throttling configuration
     */
    @Override
    @JsonProperty(value = THROTTLE_PROPERTY, required = true)
    public ThrottleConfiguration throttleConfiguration()
    {
        return throttleConfiguration;
    }

    /**
     * @return the configuration for SSTable component uploads on this service
     */
    @Override
    @JsonProperty(value = SSTABLE_UPLOAD_PROPERTY, required = true)
    public SSTableUploadConfiguration ssTableUploadConfiguration()
    {
        return ssTableUploadConfiguration;
    }

    /**
     * @return the configuration for the SSTable Import functionality
     */
    @Override
    @JsonProperty(value = SSTABLE_IMPORT_PROPERTY, required = true)
    public SSTableImportConfiguration ssTableImportConfiguration()
    {
        return ssTableImportConfiguration;
    }

    /**
     * @return the configured worker pools for the service
     */
    @Override
    @JsonProperty(value = WORKER_POOLS_PROPERTY, required = true)
    public Map<String, ? extends WorkerPoolConfiguration> workerPoolsConfiguration()
    {
        return workerPoolsConfiguration;
    }

    /**
     * @return the general JMX configuration options (not per-instance)
     */
    @Override
    @JsonProperty("jmx")
    public JmxConfiguration jmxConfiguration()
    {
        return jmxConfiguration;
    }

}
