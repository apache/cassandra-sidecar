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

import org.apache.cassandra.sidecar.common.DataObjectBuilder;

/**
 * Encapsulates configurations for the {@link SidecarClient}
 */
public class SidecarClientConfigImpl implements SidecarClientConfig
{
    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final long DEFAULT_RETRY_DELAY_MILLIS = 500L;
    public static final long DEFAULT_MAX_RETRY_DELAY_MILLIS = 60_000L;

    protected final int maxRetries;
    protected final long retryDelayMillis;
    protected final long maxRetryDelayMillis;

    private SidecarClientConfigImpl(Builder<?> builder)
    {
        maxRetries = builder.maxRetries;
        retryDelayMillis = builder.retryDelayMillis;
        maxRetryDelayMillis = builder.maxRetryDelayMillis;
    }

    /**
     * @return the maximum number of times to retry a request
     */
    @Override
    public int maxRetries()
    {
        return maxRetries;
    }

    /**
     * @return the initial amount of time to wait before retrying a failed request
     */
    @Override
    public long retryDelayMillis()
    {
        return retryDelayMillis;
    }

    /**
     * @return the maximum amount of time to wait before retrying a failed request
     */
    @Override
    public long maxRetryDelayMillis()
    {
        return maxRetryDelayMillis;
    }

    public static Builder<?> builder()
    {
        return new Builder<>();
    }

    /**
     * {@code SidecarConfig} builder static inner class.
     *
     * @param <T> the type of the Builder class
     */
    public static class Builder<T extends Builder<?>> implements DataObjectBuilder<T, SidecarClientConfig>
    {
        protected int maxRetries = DEFAULT_MAX_RETRIES;
        protected long retryDelayMillis = DEFAULT_RETRY_DELAY_MILLIS;
        protected long maxRetryDelayMillis = DEFAULT_MAX_RETRY_DELAY_MILLIS;

        protected Builder()
        {
        }

        /**
         * Sets the {@code maxRetries} and returns a reference to this Builder enabling method chaining.
         *
         * @param maxRetries the {@code maxRetries} to set
         * @return a reference to this Builder
         */
        public T maxRetries(int maxRetries)
        {
            return update(b -> b.maxRetries = maxRetries);
        }

        /**
         * Sets the {@code retryDelayMillis} and returns a reference to this Builder enabling method chaining.
         *
         * @param retryDelayMillis the {@code retryDelayMillis} to set
         * @return a reference to this Builder
         */
        public T retryDelayMillis(long retryDelayMillis)
        {
            return update(b -> b.retryDelayMillis = retryDelayMillis);
        }

        /**
         * Sets the {@code maxRetryDelayMillis} and returns a reference to this Builder enabling method chaining.
         *
         * @param maxRetryDelayMillis the {@code maxRetryDelayMillis} to set
         * @return a reference to this Builder
         */
        public T maxRetryDelayMillis(long maxRetryDelayMillis)
        {
            return update(b -> b.maxRetryDelayMillis = maxRetryDelayMillis);
        }

        /**
         * Returns a {@code SidecarConfig} built from the parameters previously set.
         *
         * @return a {@code SidecarConfig} built with parameters of this {@code SidecarConfig.Builder}
         */
        @Override
        public SidecarClientConfig build()
        {
            return new SidecarClientConfigImpl(this);
        }
    }
}
