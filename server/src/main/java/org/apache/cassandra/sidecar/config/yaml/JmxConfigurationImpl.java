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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.JmxConfiguration;

/**
 * General JMX connectivity configuration that is not instance-specific.
 */
public class JmxConfigurationImpl implements JmxConfiguration
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JmxConfigurationImpl.class);

    @JsonProperty("max_retries")
    protected final int maxRetries;

    protected MillisecondBoundConfiguration retryDelay;

    public JmxConfigurationImpl()
    {
        this(3, MillisecondBoundConfiguration.parse("200ms"));
    }

    public JmxConfigurationImpl(int maxRetries, MillisecondBoundConfiguration retryDelay)
    {
        this.maxRetries = maxRetries;
        this.retryDelay = retryDelay;
    }

    /**
     * @return the maximum number of connection retry attempts to make before failing
     */
    @Override
    @JsonProperty("max_retries")
    public int maxRetries()
    {
        return maxRetries;
    }

    /**
     * @return the delay, in milliseconds, between retry attempts
     */
    @Override
    @JsonProperty("retry_delay")
    public MillisecondBoundConfiguration retryDelay()
    {
        return retryDelay;
    }

    @JsonProperty("retry_delay")
    public void setRetryDelay(MillisecondBoundConfiguration retryDelay)
    {
        this.retryDelay = retryDelay;
    }

    /**
     * Legacy property {@code retry_delay_millis}
     *
     * @param retryDelayMillis retry in milliseconds
     * @deprecated in favor of {@code retry_delay}
     */
    @JsonProperty("retry_delay_millis")
    @Deprecated
    public void setRetryDelayMillis(long retryDelayMillis)
    {
        LOGGER.warn("'retry_delay_millis' is deprecated, use 'retry_delay' instead");
        setRetryDelay(new MillisecondBoundConfiguration(retryDelayMillis, TimeUnit.MILLISECONDS));
    }
}
