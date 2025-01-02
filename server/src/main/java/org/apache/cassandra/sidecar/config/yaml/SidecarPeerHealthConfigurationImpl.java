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
import org.apache.cassandra.sidecar.config.SidecarPeerHealthConfiguration;


/**
 * Configuration for Peer Health checks
 */
public class SidecarPeerHealthConfigurationImpl extends PeriodicTaskConfigurationImpl implements SidecarPeerHealthConfiguration
{
    public static final boolean DEFAULT_ENABLED = true;
    public static final MillisecondBoundConfiguration DEFAULT_FREQUENCY = new MillisecondBoundConfiguration(30, TimeUnit.SECONDS);
    public static final int DEFAULT_HEALTH_CHECK_RETRIES = 5;
    public static final MillisecondBoundConfiguration DEFAULT_HEALTH_CHECK_RETRY_DELAY = new MillisecondBoundConfiguration(10, TimeUnit.SECONDS);;

    @JsonProperty(value = "health_check_retries")
    private final int healthCheckRetries;
    @JsonProperty(value = "health_check_retry_delay")
    private final MillisecondBoundConfiguration healthCheckRetryDelay;

    /**
     * Constructs a new {@link SidecarPeerHealthConfigurationImpl} instance with the default configuration
     * values.
     */
    public SidecarPeerHealthConfigurationImpl()
    {
        super(DEFAULT_ENABLED, DEFAULT_FREQUENCY, DEFAULT_FREQUENCY);
        this.healthCheckRetries = DEFAULT_HEALTH_CHECK_RETRIES;
        this.healthCheckRetryDelay = DEFAULT_HEALTH_CHECK_RETRY_DELAY;
    }

    public SidecarPeerHealthConfigurationImpl(boolean enabled,
                                              MillisecondBoundConfiguration frequency,
                                              int healthCheckRetries,
                                              MillisecondBoundConfiguration healthCheckRetryDelay)
    {
        super(enabled, frequency, frequency);
        this.healthCheckRetries = healthCheckRetries;
        this.healthCheckRetryDelay = healthCheckRetryDelay;
    }

    /**
     * @return the number of maximum retries to be performed during a Sidecar peer health check
     */
    @Override
    @JsonProperty(value = "health_check_retries")
    public int healthCheckRetries()
    {
        return healthCheckRetries;
    }

    /**
     * @return the delay between Sidecar peer health checks retries in milliseconds
     */
    @Override
    @JsonProperty(value = "health_check_retry_delay")
    public MillisecondBoundConfiguration healthCheckRetryDelay()
    {
        return healthCheckRetryDelay;
    }
}
