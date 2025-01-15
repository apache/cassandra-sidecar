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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.MinuteBoundConfiguration;
import org.apache.cassandra.sidecar.config.CdcConfiguration;
import org.apache.cassandra.sidecar.config.CoordinationConfiguration;
import org.apache.cassandra.sidecar.config.JmxConfiguration;
import org.apache.cassandra.sidecar.config.SSTableImportConfiguration;
import org.apache.cassandra.sidecar.config.SSTableSnapshotConfiguration;
import org.apache.cassandra.sidecar.config.SSTableUploadConfiguration;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.ThrottleConfiguration;
import org.apache.cassandra.sidecar.config.TrafficShapingConfiguration;
import org.apache.cassandra.sidecar.config.WorkerPoolConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.jetbrains.annotations.Nullable;

/**
 * Configuration for the Sidecar Service and configuration of the REST endpoints in the service
 */
public class ServiceConfigurationImpl implements ServiceConfiguration
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceConfigurationImpl.class);
    public static final String HOST_PROPERTY = "host";
    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final String PORT_PROPERTY = "port";
    public static final int DEFAULT_PORT = 9043;
    public static final String REQUEST_IDLE_TIMEOUT_PROPERTY = "request_idle_timeout";
    public static final MillisecondBoundConfiguration DEFAULT_REQUEST_IDLE_TIMEOUT = MillisecondBoundConfiguration.parse("5m");
    public static final String REQUEST_TIMEOUT_PROPERTY = "request_timeout";
    public static final MillisecondBoundConfiguration DEFAULT_REQUEST_TIMEOUT = MillisecondBoundConfiguration.parse("5m");
    public static final String TCP_KEEP_ALIVE_PROPERTY = "tcp_keep_alive";
    public static final boolean DEFAULT_TCP_KEEP_ALIVE = false;
    public static final String ACCEPT_BACKLOG_PROPERTY = "accept_backlog";
    public static final int DEFAULT_ACCEPT_BACKLOG = 1024;
    public static final String ALLOWABLE_TIME_SKEW_PROPERTY = "allowable_time_skew";
    public static final MinuteBoundConfiguration DEFAULT_ALLOWABLE_TIME_SKEW = MinuteBoundConfiguration.parse("1h");
    private static final String SERVER_VERTICLE_INSTANCES_PROPERTY = "server_verticle_instances";
    private static final String OPERATIONAL_JOB_TRACKER_SIZE_PROPERTY = "operations_job_tracker_size";
    private static final String OPERATIONAL_JOB_EXECUTION_MAX_WAIT_TIME_PROPERTY = "operations_job_sync_response_timeout";
    private static final int DEFAULT_SERVER_VERTICLE_INSTANCES = 1;
    private static final int DEFAULT_OPERATIONAL_JOB_TRACKER_SIZE = 64;
    private static final MillisecondBoundConfiguration DEFAULT_OPERATIONAL_JOB_EXECUTION_MAX_WAIT_TIME =
    MillisecondBoundConfiguration.parse("5s");
    public static final String THROTTLE_PROPERTY = "throttle";
    public static final String SSTABLE_UPLOAD_PROPERTY = "sstable_upload";
    public static final String SSTABLE_IMPORT_PROPERTY = "sstable_import";
    public static final String SSTABLE_SNAPSHOT_PROPERTY = "sstable_snapshot";
    public static final String WORKER_POOLS_PROPERTY = "worker_pools";
    private static final String JMX_PROPERTY = "jmx";
    private static final String TRAFFIC_SHAPING_PROPERTY = "traffic_shaping";
    private static final String SCHEMA = "schema";
    private static final String CDC = "cdc";
    private static final String COORDINATION = "coordination";
    protected static final Map<String, WorkerPoolConfiguration> DEFAULT_WORKER_POOLS_CONFIGURATION
    = Collections.unmodifiableMap(new HashMap<String, WorkerPoolConfiguration>()
    {{
        put(SERVICE_POOL, new WorkerPoolConfigurationImpl("sidecar-worker-pool", 20,
                                                          MillisecondBoundConfiguration.parse("60s")));

        put(INTERNAL_POOL, new WorkerPoolConfigurationImpl("sidecar-internal-worker-pool", 20,
                                                           MillisecondBoundConfiguration.parse("15m")));
    }});


    @JsonProperty(value = HOST_PROPERTY, defaultValue = DEFAULT_HOST)
    protected final String host;

    @JsonProperty(value = PORT_PROPERTY, defaultValue = DEFAULT_PORT + "")
    protected final int port;

    protected MillisecondBoundConfiguration requestIdleTimeout;

    protected MillisecondBoundConfiguration requestTimeout;

    @JsonProperty(value = TCP_KEEP_ALIVE_PROPERTY, defaultValue = DEFAULT_TCP_KEEP_ALIVE + "")
    protected final boolean tcpKeepAlive;

    @JsonProperty(value = ACCEPT_BACKLOG_PROPERTY, defaultValue = DEFAULT_ACCEPT_BACKLOG + "")
    protected final int acceptBacklog;

    protected MinuteBoundConfiguration allowableTimeSkew;

    @JsonProperty(value = SERVER_VERTICLE_INSTANCES_PROPERTY, defaultValue = DEFAULT_SERVER_VERTICLE_INSTANCES + "")
    protected final int serverVerticleInstances;

    @JsonProperty(value = OPERATIONAL_JOB_TRACKER_SIZE_PROPERTY, defaultValue = DEFAULT_OPERATIONAL_JOB_TRACKER_SIZE + "")
    protected final int operationalJobTrackerSize;

    @JsonProperty(value = OPERATIONAL_JOB_EXECUTION_MAX_WAIT_TIME_PROPERTY)
    protected final MillisecondBoundConfiguration operationalJobExecutionMaxWaitTime;

    @JsonProperty(value = THROTTLE_PROPERTY)
    protected final ThrottleConfiguration throttleConfiguration;

    @JsonProperty(value = SSTABLE_UPLOAD_PROPERTY)
    protected final SSTableUploadConfiguration sstableUploadConfiguration;

    @JsonProperty(value = SSTABLE_IMPORT_PROPERTY)
    protected final SSTableImportConfiguration sstableImportConfiguration;

    @JsonProperty(value = SSTABLE_SNAPSHOT_PROPERTY)
    protected final SSTableSnapshotConfiguration sstableSnapshotConfiguration;

    @JsonProperty(value = WORKER_POOLS_PROPERTY)
    protected final Map<String, ? extends WorkerPoolConfiguration> workerPoolsConfiguration;

    @JsonProperty(value = JMX_PROPERTY)
    protected final JmxConfiguration jmxConfiguration;

    @JsonProperty(value = TRAFFIC_SHAPING_PROPERTY)
    protected final TrafficShapingConfiguration trafficShapingConfiguration;

    @JsonProperty(value = SCHEMA)
    protected final SchemaKeyspaceConfiguration schemaKeyspaceConfiguration;

    @JsonProperty(value = CDC)
    protected final CdcConfiguration cdcConfiguration;

    @JsonProperty(value = COORDINATION)
    protected final CoordinationConfiguration coordinationConfiguration;

    /**
     * Constructs a new {@link ServiceConfigurationImpl} with the default values
     */
    public ServiceConfigurationImpl()
    {
        this(builder());
    }

    /**
     * Constructs a new {@link ServiceConfigurationImpl} with the configured {@link Builder}
     *
     * @param builder the builder object
     */
    protected ServiceConfigurationImpl(Builder builder)
    {
        host = builder.host;
        port = builder.port;
        requestIdleTimeout = builder.requestIdleTimeout;
        requestTimeout = builder.requestTimeout;
        tcpKeepAlive = builder.tcpKeepAlive;
        acceptBacklog = builder.acceptBacklog;
        allowableTimeSkew = builder.allowableTimeSkew;
        serverVerticleInstances = builder.serverVerticleInstances;
        operationalJobTrackerSize = builder.operationalJobTrackerSize;
        operationalJobExecutionMaxWaitTime = builder.operationalJobExecutionMaxWaitTime;
        throttleConfiguration = builder.throttleConfiguration;
        sstableUploadConfiguration = builder.sstableUploadConfiguration;
        sstableImportConfiguration = builder.sstableImportConfiguration;
        sstableSnapshotConfiguration = builder.sstableSnapshotConfiguration;
        workerPoolsConfiguration = builder.workerPoolsConfiguration;
        jmxConfiguration = builder.jmxConfiguration;
        trafficShapingConfiguration = builder.trafficShapingConfiguration;
        schemaKeyspaceConfiguration = builder.schemaKeyspaceConfiguration;
        cdcConfiguration = builder.cdcConfiguration;
        coordinationConfiguration = builder.coordinationConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = HOST_PROPERTY)
    public String host()
    {
        return host;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = PORT_PROPERTY)
    public int port()
    {
        return port;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = REQUEST_IDLE_TIMEOUT_PROPERTY)
    public MillisecondBoundConfiguration requestIdleTimeout()
    {
        return requestIdleTimeout;
    }

    @JsonProperty(value = REQUEST_IDLE_TIMEOUT_PROPERTY)
    public void setRequestIdleTimeout(MillisecondBoundConfiguration requestIdleTimeout)
    {
        this.requestIdleTimeout = requestIdleTimeout;
    }

    /**
     * Legacy property {@code request_idle_timeout_millis}
     *
     * @param requestIdleTimeoutMillis idle timeout in milliseconds
     * @deprecated in favor of {@link #REQUEST_IDLE_TIMEOUT_PROPERTY}
     */
    @JsonProperty(value = "request_idle_timeout_millis")
    @Deprecated
    public void setRequestIdleTimeoutMillis(long requestIdleTimeoutMillis)
    {
        LOGGER.warn("'request_idle_timeout_millis' is deprecated, use '{}' instead", REQUEST_IDLE_TIMEOUT_PROPERTY);
        setRequestIdleTimeout(new MillisecondBoundConfiguration(requestIdleTimeoutMillis, TimeUnit.MILLISECONDS));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = REQUEST_TIMEOUT_PROPERTY)
    public MillisecondBoundConfiguration requestTimeout()
    {
        return requestTimeout;
    }

    @JsonProperty(value = REQUEST_TIMEOUT_PROPERTY)
    public void setRequestTimeout(MillisecondBoundConfiguration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
    }

    /**
     * Legacy property {@code request_timeout_millis}
     *
     * @param requestTimeoutMillis request timeout in milliseconds
     * @deprecated in favor of {@link #REQUEST_TIMEOUT_PROPERTY}
     */
    @JsonProperty(value = "request_timeout_millis")
    @Deprecated
    public void setRequestTimeoutMillis(long requestTimeoutMillis)
    {
        LOGGER.warn("'request_timeout_millis' is deprecated, use '{}' instead", REQUEST_TIMEOUT_PROPERTY);
        setRequestTimeout(new MillisecondBoundConfiguration(requestTimeoutMillis, TimeUnit.MILLISECONDS));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = TCP_KEEP_ALIVE_PROPERTY)
    public boolean tcpKeepAlive()
    {
        return tcpKeepAlive;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = ACCEPT_BACKLOG_PROPERTY)
    public int acceptBacklog()
    {
        return acceptBacklog;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = ALLOWABLE_TIME_SKEW_PROPERTY)
    public MinuteBoundConfiguration allowableTimeSkew()
    {
        return allowableTimeSkew;
    }

    @JsonProperty(value = ALLOWABLE_TIME_SKEW_PROPERTY)
    public void setAllowableTimeSkew(MinuteBoundConfiguration allowableTimeSkew)
    {
        if (allowableTimeSkew.compareTo(MinuteBoundConfiguration.parse("1m")) < 0)
        {
            throw new ConfigurationException(String.format("Invalid %s value (%s). The minimum allowed value is 1 minute (1m)",
                                                           ALLOWABLE_TIME_SKEW_PROPERTY, allowableTimeSkew));
        }
        this.allowableTimeSkew = allowableTimeSkew;
    }

    /**
     * Legacy property {@code allowable_time_skew_in_minutes}
     *
     * @param allowableTimeSkewInMinutes allowable time skew in minutes
     * @deprecated in favor of {@link #ALLOWABLE_TIME_SKEW_PROPERTY}
     */
    @JsonProperty(value = "allowable_time_skew_in_minutes")
    @Deprecated
    public void setAllowableTimeSkewInMinutes(long allowableTimeSkewInMinutes)
    {
        LOGGER.warn("'allowable_time_skew_in_minutes' is deprecated, use '{}' instead", ALLOWABLE_TIME_SKEW_PROPERTY);
        setAllowableTimeSkew(new MinuteBoundConfiguration(allowableTimeSkewInMinutes, TimeUnit.MINUTES));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = SERVER_VERTICLE_INSTANCES_PROPERTY)
    public int serverVerticleInstances()
    {
        return serverVerticleInstances;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = OPERATIONAL_JOB_TRACKER_SIZE_PROPERTY)
    public int operationalJobTrackerSize()
    {
        return operationalJobTrackerSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = OPERATIONAL_JOB_EXECUTION_MAX_WAIT_TIME_PROPERTY)
    public MillisecondBoundConfiguration operationalJobExecutionMaxWaitTime()
    {
        return operationalJobExecutionMaxWaitTime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = THROTTLE_PROPERTY)
    public ThrottleConfiguration throttleConfiguration()
    {
        return throttleConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = SSTABLE_UPLOAD_PROPERTY)
    public SSTableUploadConfiguration sstableUploadConfiguration()
    {
        return sstableUploadConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = SSTABLE_IMPORT_PROPERTY)
    public SSTableImportConfiguration sstableImportConfiguration()
    {
        return sstableImportConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = SSTABLE_SNAPSHOT_PROPERTY)
    public SSTableSnapshotConfiguration sstableSnapshotConfiguration()
    {
        return sstableSnapshotConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = WORKER_POOLS_PROPERTY)
    public Map<String, ? extends WorkerPoolConfiguration> workerPoolsConfiguration()
    {
        return workerPoolsConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = JMX_PROPERTY)
    public JmxConfiguration jmxConfiguration()
    {
        return jmxConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = TRAFFIC_SHAPING_PROPERTY)
    public TrafficShapingConfiguration trafficShapingConfiguration()
    {
        return trafficShapingConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = SCHEMA)
    public SchemaKeyspaceConfiguration schemaKeyspaceConfiguration()
    {
        return schemaKeyspaceConfiguration;
    }

    /**
     * @return the configuration for cdc
     */
    @Override
    @JsonProperty(value = CDC)
    @Nullable
    public CdcConfiguration cdcConfiguration()
    {
        return cdcConfiguration;
    }

    @Override
    @JsonProperty(value = COORDINATION)
    public CoordinationConfiguration coordinationConfiguration()
    {
        return coordinationConfiguration;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code ServiceConfigurationImpl} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, ServiceConfigurationImpl>
    {
        protected String host = DEFAULT_HOST;
        protected int port = DEFAULT_PORT;
        protected MillisecondBoundConfiguration requestIdleTimeout = DEFAULT_REQUEST_IDLE_TIMEOUT;
        protected MillisecondBoundConfiguration requestTimeout = DEFAULT_REQUEST_TIMEOUT;
        protected boolean tcpKeepAlive = DEFAULT_TCP_KEEP_ALIVE;
        protected int acceptBacklog = DEFAULT_ACCEPT_BACKLOG;
        protected MinuteBoundConfiguration allowableTimeSkew = DEFAULT_ALLOWABLE_TIME_SKEW;
        protected int serverVerticleInstances = DEFAULT_SERVER_VERTICLE_INSTANCES;
        protected int operationalJobTrackerSize = DEFAULT_OPERATIONAL_JOB_TRACKER_SIZE;
        protected MillisecondBoundConfiguration operationalJobExecutionMaxWaitTime = DEFAULT_OPERATIONAL_JOB_EXECUTION_MAX_WAIT_TIME;
        protected ThrottleConfiguration throttleConfiguration = new ThrottleConfigurationImpl();
        protected SSTableUploadConfiguration sstableUploadConfiguration = new SSTableUploadConfigurationImpl();
        protected SSTableImportConfiguration sstableImportConfiguration = new SSTableImportConfigurationImpl();
        protected SSTableSnapshotConfiguration sstableSnapshotConfiguration = new SSTableSnapshotConfigurationImpl();
        protected Map<String, ? extends WorkerPoolConfiguration> workerPoolsConfiguration =
        DEFAULT_WORKER_POOLS_CONFIGURATION;
        protected JmxConfiguration jmxConfiguration = new JmxConfigurationImpl();
        protected TrafficShapingConfiguration trafficShapingConfiguration = new TrafficShapingConfigurationImpl();
        protected SchemaKeyspaceConfiguration schemaKeyspaceConfiguration = new SchemaKeyspaceConfigurationImpl();
        protected CdcConfiguration cdcConfiguration = new CdcConfigurationImpl();
        protected CoordinationConfiguration coordinationConfiguration = new CoordinationConfigurationImpl();

        private Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code host} and returns a reference to this Builder enabling method chaining.
         *
         * @param host the {@code host} to set
         * @return a reference to this Builder
         */
        public Builder host(String host)
        {
            return update(b -> b.host = host);
        }

        /**
         * Sets the {@code port} and returns a reference to this Builder enabling method chaining.
         *
         * @param port the {@code port} to set
         * @return a reference to this Builder
         */
        public Builder port(int port)
        {
            return update(b -> b.port = port);
        }

        /**
         * Sets the {@code requestIdleTimeout} and returns a reference to this Builder enabling method chaining.
         *
         * @param requestIdleTimeout the {@code requestIdleTimeout} to set
         * @return a reference to this Builder
         */
        public Builder requestIdleTimeout(MillisecondBoundConfiguration requestIdleTimeout)
        {
            return update(b -> b.requestIdleTimeout = requestIdleTimeout);
        }

        /**
         * Sets the {@code requestTimeout} and returns a reference to this Builder enabling method chaining.
         *
         * @param requestTimeout the {@code requestTimeout} to set
         * @return a reference to this Builder
         */
        public Builder requestTimeout(MillisecondBoundConfiguration requestTimeout)
        {
            return update(b -> b.requestTimeout = requestTimeout);
        }

        /**
         * Sets the {@code tcpKeepAlive} and returns a reference to this Builder enabling method chaining.
         *
         * @param tcpKeepAlive the {@code tcpKeepAlive} to set
         * @return a reference to this Builder
         */
        public Builder tcpKeepAlive(boolean tcpKeepAlive)
        {
            return update(b -> b.tcpKeepAlive = tcpKeepAlive);
        }

        /**
         * Sets the {@code acceptBacklog} and returns a reference to this Builder enabling method chaining.
         *
         * @param acceptBacklog the {@code acceptBacklog} to set
         * @return a reference to this Builder
         */
        public Builder acceptBacklog(int acceptBacklog)
        {
            return update(b -> b.acceptBacklog = acceptBacklog);
        }

        /**
         * Sets the {@code allowableTimeSkew} and returns a reference to this Builder enabling method chaining.
         *
         * @param allowableTimeSkew the {@code allowableTimeSkew} to set
         * @return a reference to this Builder
         */
        public Builder allowableTimeSkew(MinuteBoundConfiguration allowableTimeSkew)
        {
            return update(b -> b.allowableTimeSkew = allowableTimeSkew);
        }

        /**
         * Sets the {@code serverVerticleInstances} and returns a reference to this Builder enabling method chaining.
         *
         * @param serverVerticleInstances the {@code serverVerticleInstances} to set
         * @return a reference to this Builder
         */
        public Builder serverVerticleInstances(int serverVerticleInstances)
        {
            return update(b -> b.serverVerticleInstances = serverVerticleInstances);
        }

        /**
         * Sets the {@code operationalJobTrackerSize} and returns a reference to this Builder enabling method chaining.
         *
         * @param operationalJobTrackerSize the {@code operationalJobTrackerSize} to set
         * @return a reference to this Builder
         */
        public Builder operationalJobTrackerSize(int operationalJobTrackerSize)
        {
            return update(b -> b.operationalJobTrackerSize = operationalJobTrackerSize);
        }

        /**
         * Sets the {@code operationalJobExecutionMaxWaitTime} and returns a reference to this Builder
         * enabling method chaining.
         *
         * @param operationalJobExecutionMaxWaitTime the {@code operationalJobExecutionMaxWaitTime} to set
         * @return a reference to this Builder
         */
        public Builder operationalJobExecutionMaxWaitTime(MillisecondBoundConfiguration operationalJobExecutionMaxWaitTime)
        {
            return update(b -> b.operationalJobExecutionMaxWaitTime = operationalJobExecutionMaxWaitTime);
        }

        /**
         * Sets the {@code throttleConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param throttleConfiguration the {@code throttleConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder throttleConfiguration(ThrottleConfiguration throttleConfiguration)
        {
            return update(b -> b.throttleConfiguration = throttleConfiguration);
        }

        /**
         * Sets the {@code sstableUploadConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param sstableUploadConfiguration the {@code sstableUploadConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder sstableUploadConfiguration(SSTableUploadConfiguration sstableUploadConfiguration)
        {
            return update(b -> b.sstableUploadConfiguration = sstableUploadConfiguration);
        }

        /**
         * Sets the {@code sstableImportConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param sstableImportConfiguration the {@code sstableImportConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder sstableImportConfiguration(SSTableImportConfiguration sstableImportConfiguration)
        {
            return update(b -> b.sstableImportConfiguration = sstableImportConfiguration);
        }

        /**
         * Sets the {@code sstableSnapshotConfiguration} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param sstableSnapshotConfiguration the {@code sstableSnapshotConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder sstableSnapshotConfiguration(SSTableSnapshotConfiguration sstableSnapshotConfiguration)
        {
            return update(b -> b.sstableSnapshotConfiguration = sstableSnapshotConfiguration);
        }

        /**
         * Sets the {@code workerPoolsConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param workerPoolsConfiguration the {@code workerPoolsConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder workerPoolsConfiguration(Map<String, ? extends WorkerPoolConfiguration> workerPoolsConfiguration)
        {
            return update(b -> b.workerPoolsConfiguration = workerPoolsConfiguration);
        }

        /**
         * Sets the {@code jmxConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param jmxConfiguration the {@code jmxConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder jmxConfiguration(JmxConfiguration jmxConfiguration)
        {
            return update(b -> b.jmxConfiguration = jmxConfiguration);
        }

        /**
         * Sets the {@code trafficShapingConfiguration} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param trafficShapingConfiguration the {@code trafficShapingConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder trafficShapingConfiguration(TrafficShapingConfiguration trafficShapingConfiguration)
        {
            return update(b -> b.trafficShapingConfiguration = trafficShapingConfiguration);
        }

        /**
         * Sets the {@code schemaKeyspaceConfiguration} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param schemaKeyspaceConfiguration the {@code schemaKeyspaceConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder schemaKeyspaceConfiguration(SchemaKeyspaceConfiguration schemaKeyspaceConfiguration)
        {
            return update(b -> b.schemaKeyspaceConfiguration = schemaKeyspaceConfiguration);
        }

        /**
         * Set the {@code cdcConfiguration} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param configuration th {@code cdcConfiguration} to set
         * @return a reference to the Builder
         */
        public Builder cdcConfiguration(CdcConfiguration configuration)
        {
            return update(b -> b.cdcConfiguration = configuration);
        }

        /**
         * Sets the {@code coordinationConfiguration} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param coordinationConfiguration the {@code coordinationConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder coordinationConfiguration(CoordinationConfiguration coordinationConfiguration)
        {
            return update(b -> b.coordinationConfiguration = coordinationConfiguration);
        }

        /**
         * Returns a {@code ServiceConfigurationImpl} built from the parameters previously set.
         *
         * @return a {@code ServiceConfigurationImpl} built with parameters of this
         * {@code ServiceConfigurationImpl.Builder}
         */
        @Override
        public ServiceConfigurationImpl build()
        {
            return new ServiceConfigurationImpl(this);
        }
    }
}
