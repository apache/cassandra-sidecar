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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.ThrottleConfiguration;

/**
 * The traffic shaping configuration options for the service
 */
public class ThrottleConfigurationImpl implements ThrottleConfiguration
{
    public static final long DEFAULT_STREAM_REQUESTS_PER_SEC = 5000;
    public static final long DEFAULT_TIMEOUT_SEC = 10;
    public static final long DEFAULT_DELAY_SEC = 5;
    public static final String STREAM_REQUESTS_PER_SEC_PROPERTY = "stream_requests_per_sec";
    public static final String TIMEOUT_SEC_PROPERTY = "timeout_sec";
    public static final String DELAY_SEC_PROPERTY = "delay_sec";

    @JsonProperty(value = STREAM_REQUESTS_PER_SEC_PROPERTY)
    protected final long rateLimitStreamRequestsPerSecond;
    @JsonProperty(value = TIMEOUT_SEC_PROPERTY)
    protected final long timeoutInSeconds;
    @JsonProperty(value = DELAY_SEC_PROPERTY)
    protected final long delayInSeconds;

    public ThrottleConfigurationImpl()
    {
        this(DEFAULT_STREAM_REQUESTS_PER_SEC,
             DEFAULT_TIMEOUT_SEC,
             DEFAULT_DELAY_SEC);
    }

    public ThrottleConfigurationImpl(long rateLimitStreamRequestsPerSecond)
    {
        this(rateLimitStreamRequestsPerSecond,
             DEFAULT_TIMEOUT_SEC,
             DEFAULT_DELAY_SEC);
    }

    public ThrottleConfigurationImpl(long rateLimitStreamRequestsPerSecond,
                                     long timeoutInSeconds,
                                     long delayInSeconds)
    {
        this.rateLimitStreamRequestsPerSecond = rateLimitStreamRequestsPerSecond;
        this.timeoutInSeconds = timeoutInSeconds;
        this.delayInSeconds = delayInSeconds;
    }

    @Override
    @JsonProperty(value = STREAM_REQUESTS_PER_SEC_PROPERTY)
    public long rateLimitStreamRequestsPerSecond()
    {
        return rateLimitStreamRequestsPerSecond;
    }

    @Override
    @JsonProperty(value = TIMEOUT_SEC_PROPERTY)
    public long timeoutInSeconds()
    {
        return timeoutInSeconds;
    }

    @Override
    @JsonProperty(value = DELAY_SEC_PROPERTY)
    public long delayInSeconds()
    {
        return delayInSeconds;
    }
}
