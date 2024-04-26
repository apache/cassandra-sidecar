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

package com.google.common.util.concurrent;

import java.util.concurrent.atomic.AtomicReference;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

/**
 * Wrapper class over guava Rate Limiter, uses SmoothBursty Ratelimiter. This class mainly exists to expose
 * package protected method queryEarliestAvailable of guava RateLimiter.
 * <p>
 * In addition to Guava's Rate Limiter functionality, it adds support for disabling rate-limiting.
 */
public class SidecarRateLimiter
{
    private final AtomicReference<RateLimiterWrapper> ref = new AtomicReference<>(null);

    private SidecarRateLimiter(final double permitsPerSecond)
    {
        if (permitsPerSecond > 0)
        {
            RateLimiterWrapper rateLimiterWrapper = RateLimiterWrapper.create(permitsPerSecond);
            ref.set(rateLimiterWrapper);
        }
    }

    /**
     * Creates a new {@link SidecarRateLimiter} with the configured {@code permitsPerSecond}. When the
     * {@code permitsPerSecond} is less than or equal to zero, the rate-limiter is disabled (unthrottled).
     *
     * @param permitsPerSecond the rate of the returned {@code RateLimiter}, measured in how many
     *                         permits become available per second
     * @return the new instance of the rate limiter
     */
    public static SidecarRateLimiter create(final double permitsPerSecond)
    {
        return new SidecarRateLimiter(permitsPerSecond);
    }

    /**
     * Returns calculated wait time in micros for next available permit. Permit is not reserved during calculation,
     * this wait time is an approximation.
     *
     * @return approx wait time in micros for next available permit
     */
    public long queryWaitTimeInMicros()
    {
        RateLimiterWrapper rateLimiterWrapper = ref.get();
        return rateLimiterWrapper != null ? rateLimiterWrapper.queryWaitTimeInMicros() : 0;
    }

    /**
     * Acquires a permit if it can be acquired immediately without delay.
     *
     * @return {@code true} if the permit was acquired or rate limiting is disabled, {@code false} otherwise
     */
    public boolean tryAcquire()
    {
        RateLimiterWrapper rateLimiterWrapper = ref.get();
        return rateLimiterWrapper == null || rateLimiterWrapper.rateLimiter.tryAcquire();
    }

    /**
     * Updates the stable rate of the internal {@code RateLimiter}, that is, the {@code permitsPerSecond}
     * argument provided in the factory method that constructed the {@code RateLimiter}. Setting the rate to any
     * value less than or equal to {@code 0}, will disable rate limiting.
     *
     * @param permitsPerSecond the new stable rate of this {@code RateLimiter}
     * @throws IllegalArgumentException if {@code permitsPerSecond} is negative or zero
     */
    public void rate(double permitsPerSecond)
    {
        RateLimiterWrapper rateLimiterWrapper = ref.get();

        if (permitsPerSecond > 0.0)
        {
            if (rateLimiterWrapper == null)
            {
                ref.compareAndSet(null, RateLimiterWrapper.create(permitsPerSecond));
            }
            else
            {
                rateLimiterWrapper.rateLimiter.setRate(permitsPerSecond);
            }
        }
        else
        {
            ref.set(null);
        }
    }

    /**
     * Returns the stable rate (as {@code permits per seconds}) with which this {@code SidecarRateLimiter} is
     * configured with. The initial value of this is the same as the {@code permitsPerSecond} argument passed in
     * the factory method that produced this {@code SidecarRateLimiter}, and it is only updated after invocations
     * to {@linkplain #rate(double)}. If rate-limiting has been disabled, then returns {@code 0} to indicate that
     * no rate limiting has been configured.
     *
     * @return the stable rate configured in the rate limiter, or {@code 0} when rate limiting is disabled
     */
    public double rate()
    {
        RateLimiterWrapper rateLimiterWrapper = ref.get();
        return rateLimiterWrapper != null ? rateLimiterWrapper.rateLimiter.getRate() : 0;
    }

    /**
     * Acquires a single permit from this {@code SidecarRateLimiter}, blocking until the request can be
     * granted. Tells the amount of time slept, if any. When rate-limiting is disabled, it will return {@code 0.0}
     * to indicate that no amount of time was spent sleeping.
     *
     * <p>This method is equivalent to {@code acquire(1)}.
     *
     * @return time spent sleeping to enforce rate, in seconds; 0.0 if not rate-limited
     */
    @CanIgnoreReturnValue
    public double acquire()
    {
        RateLimiterWrapper rateLimiterWrapper = ref.get();
        return rateLimiterWrapper != null ? rateLimiterWrapper.rateLimiter.acquire() : 0;
    }

    /**
     * Acquires the given number of permits from this {@code RateLimiter}, blocking until the request
     * can be granted. Tells the amount of time slept, if any. When rate-limiting is disabled, it will return
     * {@code 0.0} to indicate that no amount of time was spent sleeping. As opposed to the delegating class,
     * {@code 0} or negative are allowed permit values, and it will result in essentially disabling rate-limiting
     * and the method will return {@code 0.0} to indicate that no amount of time was spent sleeping.
     *
     * @param permits the number of permits to acquire
     * @return time spent sleeping to enforce rate, in seconds; 0.0 if not rate-limited
     */
    @CanIgnoreReturnValue
    public double acquire(int permits)
    {
        RateLimiterWrapper rateLimiterWrapper = ref.get();
        return rateLimiterWrapper != null && permits > 0 ? rateLimiterWrapper.rateLimiter.acquire(permits) : 0;
    }

    // Attention: Hack to expose the package private method queryEarliestAvailable and RateLimiter.SleepingStopwatch
    @SuppressWarnings("UnstableApiUsage")
    private static class RateLimiterWrapper
    {
        private final RateLimiter rateLimiter;
        private final RateLimiter.SleepingStopwatch stopwatch;

        private RateLimiterWrapper(RateLimiter rateLimiter, RateLimiter.SleepingStopwatch stopwatch)
        {
            this.rateLimiter = rateLimiter;
            this.stopwatch = stopwatch;
        }

        static RateLimiterWrapper create(double permitsPerSecond)
        {
            RateLimiter.SleepingStopwatch stopwatch = RateLimiter.SleepingStopwatch.createFromSystemTimer();
            RateLimiter rateLimiter = RateLimiter.create(permitsPerSecond, stopwatch);
            return new RateLimiterWrapper(rateLimiter, stopwatch);
        }

        public long queryWaitTimeInMicros()
        {
            long earliestAvailableMicros = rateLimiter.queryEarliestAvailable(0);
            long nowMicros = stopwatch.readMicros();
            return Math.max(earliestAvailableMicros - nowMicros, 0);
        }
    }
}
