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

package org.apache.cassandra.sidecar.client.retry;

/**
 * A retry policy that will perform an exponential backoff. The backoff period increases for each retry attempt.
 *
 * <p>Example: For a configured {@code maxRetries} of {@code 10}, {@code retryDelayMillis} of {@code 200}
 * milliseconds, and {@code maxRetryDelayMillis} of {@code 20,000} milliseconds, the sequence is as follows:
 *
 * <pre>
 *     request       backoff
 *
 *     1                200
 *     2                400
 *     3                800
 *     4               1600
 *     5               3200
 *     6               6400
 *     7              12800
 *     8              20000
 *     9              20000
 *     10             20000
 * </pre>
 */
public class ExponentialBackoffRetryPolicy extends BasicRetryPolicy
{
    private final long maxRetryDelayMillis;
    private final long maxAttemptsBeforeOverflow;

    /**
     * Constructs an exponential backoff retry policy unlimited number of retries and no delay between retries.
     */
    public ExponentialBackoffRetryPolicy()
    {
        super();
        this.maxRetryDelayMillis = 0;
        this.maxAttemptsBeforeOverflow = computeMaxAttemptsBeforeOverflow();
    }

    /**
     * Constructs an exponential backoff retry policy with {@code maxRetries} number of retries,
     * {@code retryDelayMillis} delay between retries, and {@code maxRetryDelayMillis} maximum delay for the
     * exponential backoff.
     *
     * @param maxRetries          the maximum number of retries
     * @param retryDelayMillis    the delay between retries in milliseconds
     * @param maxRetryDelayMillis the maximum retry delay in milliseconds
     */
    public ExponentialBackoffRetryPolicy(int maxRetries, long retryDelayMillis, long maxRetryDelayMillis)
    {
        super(maxRetries, retryDelayMillis);
        this.maxRetryDelayMillis = maxRetryDelayMillis;
        this.maxAttemptsBeforeOverflow = computeMaxAttemptsBeforeOverflow();
    }

    /**
     * Returns the number of milliseconds to wait before attempting the next request. This value is upper-bounded
     * by {@code maxRetryDelayMillis} if configured. The delay increases exponentially based on the number of
     * attempts already performed.
     *
     * @param attempts the number of attempts already performed for this request
     * @return the number of milliseconds to wait before attempting the next request
     */
    @Override
    protected long retryDelayMillis(int attempts)
    {
        long retryDelay;
        if (attempts >= maxAttemptsBeforeOverflow)
        {
            // the number of attempts in the method will overflow, so just use the Long.MAX_VALUE
            retryDelay = Long.MAX_VALUE;
        }
        else
        {
            retryDelay = (long) Math.pow(2, attempts - 1)  * retryDelayMillis;
        }
        if (maxRetryDelayMillis > 0)
        {
            return Math.min(maxRetryDelayMillis, retryDelay);
        }
        return retryDelay;
    }

    /**
     * Computes the maximum number of attempts possible before overflowing. The value is computed from:
     *
     * <pre>
     *                             (   Long.MAX_VALUE  )
     *     max_attempts = 1 + log2 ( ----------------- )
     *                             (  retryDelayMills  )
     * </pre>
     *
     * Which is deduced from the {@code retryDelay} calculation in the {@link #retryDelayMillis(int)} method:
     *
     * <pre>
     *     retryDelay = 2^(attempts - 1) * retryDelayMillis
     * </pre>
     *
     * Where the {@code retryDelay} is the maximum allowed value before overflowing, {@code Long.MAX_VALUE}.
     *
     * @return the maximum number of attempts before overflowing
     */
    private long computeMaxAttemptsBeforeOverflow()
    {
        return 1L + (long) (Math.log((double) Long.MAX_VALUE / (double) retryDelayMillis) / Math.log(2));
    }
}
