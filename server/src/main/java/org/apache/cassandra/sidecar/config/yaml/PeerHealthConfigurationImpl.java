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

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.PeerHealthConfiguration;


/**
 * Configuration for Peer Health checks
 */
public class PeerHealthConfigurationImpl extends PeriodicTaskConfigurationImpl implements PeerHealthConfiguration
{
    public static final boolean DEFAULT_ENABLED = false;
    public static final MillisecondBoundConfiguration DEFAULT_FREQUENCY = new MillisecondBoundConfiguration(30, TimeUnit.SECONDS);
    public static final int DEFAULT_SIDECAR_CLIENT_HEALTH_CHECK_RETRIES = 5;
    public static final MillisecondBoundConfiguration DEFAULT_SIDECAR_CLIENT_HEALTH_CHECK_RETRY_DELAY
    = new MillisecondBoundConfiguration(10, TimeUnit.SECONDS);

    @JsonProperty(value = "sidecar_client_health_check_retries")
    private final int sidecarClientHealthCheckRetries;
    @JsonProperty(value = "sidecar_client_health_check_retry_delay")
    private final MillisecondBoundConfiguration sidecarClientHealthCheckRetryDelay;

    /**
     * Constructs a new {@link PeerHealthConfigurationImpl} instance with the default configuration
     * values.
     */
    public PeerHealthConfigurationImpl()
    {
        super(DEFAULT_ENABLED, DEFAULT_FREQUENCY, DEFAULT_FREQUENCY);
        this.sidecarClientHealthCheckRetries = DEFAULT_SIDECAR_CLIENT_HEALTH_CHECK_RETRIES;
        this.sidecarClientHealthCheckRetryDelay = DEFAULT_SIDECAR_CLIENT_HEALTH_CHECK_RETRY_DELAY;
    }

    public PeerHealthConfigurationImpl(boolean enabled,
                                       MillisecondBoundConfiguration frequency,
                                       int sidecarClientHealthCheckRetries,
                                       MillisecondBoundConfiguration sidecarClientHealthCheckRetryDelay)
    {
        super(enabled, frequency, frequency);
        this.sidecarClientHealthCheckRetries = sidecarClientHealthCheckRetries;
        this.sidecarClientHealthCheckRetryDelay = sidecarClientHealthCheckRetryDelay;
    }

    /**
     *  {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "sidecar_client_health_check_retries")
    public int sidecarClientHealthCheckRetries()
    {
        return sidecarClientHealthCheckRetries;
    }

    /**
     *  {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "sidecar_client_health_check_retry_delay")
    public MillisecondBoundConfiguration sidecarClientHealthCheckRetryDelay()
    {
        return sidecarClientHealthCheckRetryDelay;
    }
}
