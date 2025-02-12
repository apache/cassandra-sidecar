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

import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.SidecarClientConfiguration;


/**
 * Configuration for Sidecar client
 */
public class SidecarClientConfigurationImpl implements SidecarClientConfiguration
{
    public static final String USE_SSL = "use_ssl";
    public static final String KEYSTORE_CONFIGURATION = "keystore_configuration";
    public static final String TRUSTSTORE_CONFIGURATION = "truststore_configuration";
    public static final String REQUEST_TIMEOUT = "request_timeout";
    public static final String REQUEST_IDLE_TIMEOUT = "request_idle_timeout";
    public static final String CONNECTION_POOL_MAX_SIZE = "connection_pool_max_size";
    public static final String CONNECTION_POOL_CLEARING_PERIOD = "connection_pool_clearing_period";
    public static final String CONNECTION_POOL_EVENT_LOOP_SIZE = "connection_pool_event_loop_size";
    public static final String CONNECTION_POOL_MAX_WAIT_QUEUE_SIZE = "connection_pool_max_wait_queue_size";
    public static final String MAX_RETRIES = "max_retries";
    public static final String RETRY_DELAY = "retry_delay";
    public static final String MAX_RETRY_DELAY = "max_retry_delay";

    public static final boolean DEFAULT_USE_SSL = false;
    public static final MillisecondBoundConfiguration DEFAULT_REQUEST_TIMEOUT = MillisecondBoundConfiguration.parse("60s");
    public static final MillisecondBoundConfiguration DEFAULT_REQUEST_IDLE_TIMEOUT = MillisecondBoundConfiguration.parse("60s");
    public static final int DEFAULT_CONNECTION_POOL_MAX_SIZE = 10;
    public static final MillisecondBoundConfiguration DEFAULT_CONNECTION_POOL_CLEARING_PERIOD = MillisecondBoundConfiguration.parse("60s");
    public static final int DEFAULT_CONNECTION_POOL_EVENT_LOOP_SIZE = 10;
    public static final int DEFAULT_CONNECTION_POOL_MAX_WAIT_QUEUE_SIZE = 10;
    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final MillisecondBoundConfiguration DEFAULT_RETRY_DELAY = MillisecondBoundConfiguration.parse("1s");
    public static final MillisecondBoundConfiguration DEFAULT_MAX_RETRY_DELAY = MillisecondBoundConfiguration.parse("10s");


    @JsonProperty(value = USE_SSL)
    protected final boolean useSsl;

    @JsonProperty(value = REQUEST_TIMEOUT)
    protected final MillisecondBoundConfiguration requestTimeout;

    @JsonProperty(value = REQUEST_IDLE_TIMEOUT)
    protected final MillisecondBoundConfiguration requestIdleTimeout;

    @JsonProperty(value = CONNECTION_POOL_MAX_SIZE)
    protected final int connectionPoolMaxSize;

    @JsonProperty(value = CONNECTION_POOL_CLEARING_PERIOD)
    protected final MillisecondBoundConfiguration connectionPoolClearingPeriod;

    @JsonProperty(value = CONNECTION_POOL_EVENT_LOOP_SIZE)
    protected final int connectionPoolEventLoopSize;

    @JsonProperty(value = CONNECTION_POOL_MAX_WAIT_QUEUE_SIZE)
    protected final int connectionPoolEventMaxWaitQueueSize;

    @JsonProperty(value = MAX_RETRIES)
    protected final int maxRetries;

    @JsonProperty(value = RETRY_DELAY)
    protected final MillisecondBoundConfiguration retryDelay;

    @JsonProperty(value = MAX_RETRY_DELAY)
    protected final MillisecondBoundConfiguration maxRetryDelay;

    public SidecarClientConfigurationImpl()
    {

        this(DEFAULT_USE_SSL,
             DEFAULT_REQUEST_TIMEOUT,
             DEFAULT_REQUEST_IDLE_TIMEOUT,
             DEFAULT_CONNECTION_POOL_MAX_SIZE,
             DEFAULT_CONNECTION_POOL_CLEARING_PERIOD,
             DEFAULT_CONNECTION_POOL_EVENT_LOOP_SIZE,
             DEFAULT_CONNECTION_POOL_MAX_WAIT_QUEUE_SIZE,
             DEFAULT_MAX_RETRIES,
             DEFAULT_RETRY_DELAY,
             DEFAULT_MAX_RETRY_DELAY
        );
    }

    public SidecarClientConfigurationImpl(boolean useSsl, MillisecondBoundConfiguration requestTimeout, MillisecondBoundConfiguration requestIdleTimeout, int connectionPoolMaxSize, MillisecondBoundConfiguration connectionPoolClearingPeriod, int connectionPoolEventLoopSize, int connectionPoolEventMaxWaitQueueSize, int maxRetries, MillisecondBoundConfiguration retryDelay, MillisecondBoundConfiguration maxRetryDelay)
    {
        this.useSsl = useSsl;
        this.requestTimeout = requestTimeout;
        this.requestIdleTimeout = requestIdleTimeout;
        this.connectionPoolMaxSize = connectionPoolMaxSize;
        this.connectionPoolClearingPeriod = connectionPoolClearingPeriod;
        this.connectionPoolEventLoopSize = connectionPoolEventLoopSize;
        this.connectionPoolEventMaxWaitQueueSize = connectionPoolEventMaxWaitQueueSize;
        this.maxRetries = maxRetries;
        this.retryDelay = retryDelay;
        this.maxRetryDelay = maxRetryDelay;
    }

    @Override
    public boolean useSsl()
    {
        return useSsl;
    }

    @Override
    public MillisecondBoundConfiguration requestTimeout()
    {
        return requestTimeout;
    }

    @Override
    public MillisecondBoundConfiguration requestIdleTimeout()
    {
        return requestIdleTimeout;
    }

    @Override
    public int connectionPoolMaxSize()
    {
        return connectionPoolMaxSize;
    }

    @Override
    public MillisecondBoundConfiguration connectionPoolCleanerPeriod()
    {
        return connectionPoolClearingPeriod;
    }

    @Override
    public int connectionPoolEventLoopSize()
    {
        return connectionPoolEventLoopSize;
    }

    @Override
    public int connectionPoolMaxWaitQueueSize()
    {
        return connectionPoolEventMaxWaitQueueSize;
    }

    @Override
    public int maxRetries()
    {
        return maxRetries;
    }

    @Override
    public MillisecondBoundConfiguration retryDelay()
    {
        return retryDelay;
    }

    @Override
    public MillisecondBoundConfiguration maxRetryDelay()
    {
        return maxRetryDelay;
    }

}
