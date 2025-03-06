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
    public static final boolean DEFAULT_ENABLED = false;
    public static final MillisecondBoundConfiguration DEFAULT_EXECUTE_INTERVAL = new MillisecondBoundConfiguration(30, TimeUnit.SECONDS);
    public static final int DEFAULT_MAX_RETRIES = 5;
    public static final MillisecondBoundConfiguration DEFAULT_RETRY_DELAY = new MillisecondBoundConfiguration(10, TimeUnit.SECONDS);

    @JsonProperty(value = "max_retries")
    private final int maxRetries;
    @JsonProperty(value = "retry_delay")
    private final MillisecondBoundConfiguration retryDelay;

    /**
     * Constructs a new {@link SidecarPeerHealthConfigurationImpl} instance with the default configuration
     * values.
     */
    public SidecarPeerHealthConfigurationImpl()
    {
        super(DEFAULT_ENABLED, DEFAULT_EXECUTE_INTERVAL, DEFAULT_EXECUTE_INTERVAL);
        this.maxRetries = DEFAULT_MAX_RETRIES;
        this.retryDelay = DEFAULT_RETRY_DELAY;
    }

    public SidecarPeerHealthConfigurationImpl(boolean enabled,
                                              MillisecondBoundConfiguration frequency,
                                              int maxRetries,
                                              MillisecondBoundConfiguration retryDelay)
    {
        super(enabled, frequency, frequency);
        this.maxRetries = maxRetries;
        this.retryDelay = retryDelay;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "max_retries")
    public int maxRetries()
    {
        return maxRetries;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "retry_delay")
    public MillisecondBoundConfiguration retryDelay()
    {
        return retryDelay;
    }
}
