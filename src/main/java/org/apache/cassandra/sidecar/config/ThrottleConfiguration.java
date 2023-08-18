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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;

/**
 * The traffic shaping configuration options for the service
 */
public class ThrottleConfiguration
{
    public static final long DEFAULT_STREAM_REQUESTS_PER_SEC = 5000;
    public static final long DEFAULT_TIMEOUT_SEC = 10;
    public static final long DEFAULT_DELAY_SEC = 5;
    public static final String STREAM_REQUESTS_PER_SEC_PROPERTY = "stream_requests_per_sec";
    public static final String TIMEOUT_SEC_PROPERTY = "timeout_sec";
    public static final String DELAY_SEC_PROPERTY = "delay_sec";

    @JsonProperty(value = STREAM_REQUESTS_PER_SEC_PROPERTY, defaultValue = DEFAULT_STREAM_REQUESTS_PER_SEC + "")
    protected final long rateLimitStreamRequestsPerSecond;
    @JsonProperty(value = TIMEOUT_SEC_PROPERTY, defaultValue = DEFAULT_TIMEOUT_SEC + "")
    protected final long timeoutInSeconds;
    @JsonProperty(value = DELAY_SEC_PROPERTY, defaultValue = DEFAULT_DELAY_SEC + "")
    protected final long delayInSeconds;

    public ThrottleConfiguration()
    {
        this.rateLimitStreamRequestsPerSecond = DEFAULT_STREAM_REQUESTS_PER_SEC;
        this.timeoutInSeconds = DEFAULT_TIMEOUT_SEC;
        this.delayInSeconds = DEFAULT_DELAY_SEC;
    }

    protected ThrottleConfiguration(Builder builder)
    {
        rateLimitStreamRequestsPerSecond = builder.rateLimitStreamRequestsPerSecond;
        timeoutInSeconds = builder.timeoutInSeconds;
        delayInSeconds = builder.delayInSeconds;
    }

    @JsonProperty(value = STREAM_REQUESTS_PER_SEC_PROPERTY, defaultValue = DEFAULT_STREAM_REQUESTS_PER_SEC + "")
    public long rateLimitStreamRequestsPerSecond()
    {
        return rateLimitStreamRequestsPerSecond;
    }

    @JsonProperty(value = TIMEOUT_SEC_PROPERTY, defaultValue = DEFAULT_TIMEOUT_SEC + "")
    public long timeoutInSeconds()
    {
        return timeoutInSeconds;
    }

    @JsonProperty(value = DELAY_SEC_PROPERTY, defaultValue = DEFAULT_DELAY_SEC + "")
    public long delayInSeconds()
    {
        return delayInSeconds;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code ThrottleConfiguration} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, ThrottleConfiguration>
    {
        private long rateLimitStreamRequestsPerSecond = DEFAULT_STREAM_REQUESTS_PER_SEC;
        private long timeoutInSeconds = DEFAULT_TIMEOUT_SEC;
        private long delayInSeconds = DEFAULT_DELAY_SEC;

        protected Builder()
        {
        }

        /**
         * Sets the {@code rateLimitStreamRequestsPerSecond} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param rateLimitStreamRequestsPerSecond the {@code rateLimitStreamRequestsPerSecond} to set
         * @return a reference to this Builder
         */
        public Builder rateLimitStreamRequestsPerSecond(long rateLimitStreamRequestsPerSecond)
        {
            return override(b -> b.rateLimitStreamRequestsPerSecond = rateLimitStreamRequestsPerSecond);
        }

        /**
         * Sets the {@code timeoutInSeconds} and returns a reference to this Builder enabling method chaining.
         *
         * @param timeoutInSeconds the {@code timeoutInSeconds} to set
         * @return a reference to this Builder
         */
        public Builder timeoutInSeconds(long timeoutInSeconds)
        {
            return override(b -> b.timeoutInSeconds = timeoutInSeconds);
        }

        /**
         * Sets the {@code delayInSeconds} and returns a reference to this Builder enabling method chaining.
         *
         * @param delayInSeconds the {@code delayInSeconds} to set
         * @return a reference to this Builder
         */
        public Builder delayInSeconds(long delayInSeconds)
        {
            return override(b -> b.delayInSeconds = delayInSeconds);
        }

        /**
         * Returns a {@code ThrottleConfiguration} built from the parameters previously set.
         *
         * @return a {@code ThrottleConfiguration} built with parameters of this {@code ThrottleConfiguration.Builder}
         */
        public ThrottleConfiguration build()
        {
            return new ThrottleConfiguration(this);
        }
    }
}
