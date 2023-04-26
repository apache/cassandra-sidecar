/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.client;

/**
 * Encapsulates configurations for the {@link SidecarClient}
 */
public class SidecarConfig
{
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_RETRY_DELAY_MILLIS = 500L;
    private static final long DEFAULT_MAX_RETRY_DELAY_MILLIS = 60_000L;

    private final int maxRetries;
    private final long retryDelayMillis;
    private final long maxRetryDelayMillis;

    private SidecarConfig(Builder builder)
    {
        maxRetries = builder.maxRetries;
        retryDelayMillis = builder.retryDelayMillis;
        maxRetryDelayMillis = builder.maxRetryDelayMillis;
    }

    /**
     * @return the maximum number of times to retry a request
     */
    public int maxRetries()
    {
        return maxRetries;
    }

    /**
     * @return the initial amount of time to wait before retrying a failed request
     */
    public long retryDelayMillis()
    {
        return retryDelayMillis;
    }

    /**
     * @return the maximum amount of time to wait before retrying a failed request
     */
    public long maxRetryDelayMillis()
    {
        return maxRetryDelayMillis;
    }

    /**
     * {@code SidecarConfig} builder static inner class.
     */
    public static final class Builder
    {
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private long retryDelayMillis = DEFAULT_RETRY_DELAY_MILLIS;
        private long maxRetryDelayMillis = DEFAULT_MAX_RETRY_DELAY_MILLIS;

        public Builder()
        {
        }

        /**
         * Sets the {@code maxRetries} and returns a reference to this Builder enabling method chaining.
         *
         * @param maxRetries the {@code maxRetries} to set
         * @return a reference to this Builder
         */
        public Builder maxRetries(int maxRetries)
        {
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * Sets the {@code retryDelayMillis} and returns a reference to this Builder enabling method chaining.
         *
         * @param retryDelayMillis the {@code retryDelayMillis} to set
         * @return a reference to this Builder
         */
        public Builder retryDelayMillis(long retryDelayMillis)
        {
            this.retryDelayMillis = retryDelayMillis;
            return this;
        }

        /**
         * Sets the {@code maxRetryDelayMillis} and returns a reference to this Builder enabling method chaining.
         *
         * @param maxRetryDelayMillis the {@code maxRetryDelayMillis} to set
         * @return a reference to this Builder
         */
        public Builder maxRetryDelayMillis(long maxRetryDelayMillis)
        {
            this.maxRetryDelayMillis = maxRetryDelayMillis;
            return this;
        }

        /**
         * Returns a {@code SidecarConfig} built from the parameters previously set.
         *
         * @return a {@code SidecarConfig} built with parameters of this {@code SidecarConfig.Builder}
         */
        public SidecarConfig build()
        {
            return new SidecarConfig(this);
        }
    }
}
