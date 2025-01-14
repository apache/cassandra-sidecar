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
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.ThrottleConfiguration;

/**
 * The traffic shaping configuration options for the service
 */
public class ThrottleConfigurationImpl implements ThrottleConfiguration
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ThrottleConfigurationImpl.class);
    public static final long DEFAULT_STREAM_REQUESTS_PER_SEC = 5000;
    public static final SecondBoundConfiguration DEFAULT_TIMEOUT = SecondBoundConfiguration.parse("10s");
    public static final String STREAM_REQUESTS_PER_SEC_PROPERTY = "stream_requests_per_sec";
    public static final String TIMEOUT_PROPERTY = "timeout";

    @JsonProperty(value = STREAM_REQUESTS_PER_SEC_PROPERTY)
    protected final long rateLimitStreamRequestsPerSecond;
    protected SecondBoundConfiguration timeout;

    public ThrottleConfigurationImpl()
    {
        this(DEFAULT_STREAM_REQUESTS_PER_SEC,
             DEFAULT_TIMEOUT);
    }

    public ThrottleConfigurationImpl(long rateLimitStreamRequestsPerSecond)
    {
        this(rateLimitStreamRequestsPerSecond,
             DEFAULT_TIMEOUT);
    }

    public ThrottleConfigurationImpl(long rateLimitStreamRequestsPerSecond,
                                     SecondBoundConfiguration timeout)
    {
        this.rateLimitStreamRequestsPerSecond = rateLimitStreamRequestsPerSecond;
        this.timeout = timeout;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = STREAM_REQUESTS_PER_SEC_PROPERTY)
    public long rateLimitStreamRequestsPerSecond()
    {
        return rateLimitStreamRequestsPerSecond;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = TIMEOUT_PROPERTY)
    public SecondBoundConfiguration timeout()
    {
        return timeout;
    }

    @JsonProperty(value = TIMEOUT_PROPERTY)
    public void setTimeout(SecondBoundConfiguration timeout)
    {
        this.timeout = timeout;
    }

    /**
     * Legacy property {@code timeout_sec}
     *
     * @param timeoutInSeconds timeout in seconds
     * @deprecated in favor of {@link #TIMEOUT_PROPERTY}
     */
    @JsonProperty(value = "timeout_sec")
    @Deprecated
    public void setTimeoutInSeconds(long timeoutInSeconds)
    {
        LOGGER.warn("'timeout_sec' is deprecated, use '{}' instead", TIMEOUT_PROPERTY);
        setTimeout(new SecondBoundConfiguration(timeoutInSeconds, TimeUnit.SECONDS));
    }
}
