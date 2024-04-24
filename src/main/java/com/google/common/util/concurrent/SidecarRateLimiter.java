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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Stopwatch;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

/**
 * Wrapper class over guava Rate Limiter, uses SmoothBursty Ratelimiter. This class mainly exists to expose
 * package protected method queryEarliestAvailable of guava RateLimiter.
 * <p>
 * In addition to Guava's Rate Limiter functionality, it adds support for disabling rate-limiting.
 */
@SuppressWarnings("UnstableApiUsage")
public class SidecarRateLimiter
{
    private final AtomicReference<RateLimiter> ref = new AtomicReference<>(null);
    // Clock synced with RateLimiter's creation. Used to measure elapsed time as close as possible to measurements by
    // SleepingStopwatch maintained by SmoothRateLimiter.
    private Stopwatch clock;

    private SidecarRateLimiter(final double permitsPerSecond)
    {
        if (permitsPerSecond > 0)
        {
            RateLimiter rateLimiter = RateLimiter.create(permitsPerSecond);
            clock = Stopwatch.createStarted();
            ref.set(rateLimiter);
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

    // Attention: Hack to expose the package private method queryEarliestAvailable

    /**
     * Returns calculated wait time in micros for next available permit. Permit is not reserved during calculation,
     * this wait time is an approximation.
     *
     * @return approx wait time in micros for next available permit
     */
    public long queryWaitTimeInMicros()
    {
        long earliestAvailable = queryEarliestAvailable(0);
        long now = clock != null ? clock.elapsed(TimeUnit.MICROSECONDS) : 0;
        return Math.max(earliestAvailable - now, 0);
    }


    /**
     * Returns the earliest time permits will become available. Returns 0 if disabled.
     *
     * <br><b>Note:</b> this is a hack to expose the package private method
     * {@link RateLimiter#queryEarliestAvailable(long)}
     *
     * @param nowMicros current time in micros
     * @return earliest time permits will become available
     */
    public long queryEarliestAvailable(final long nowMicros)
    {
        RateLimiter rateLimiter = ref.get();
        return rateLimiter != null ? rateLimiter.queryEarliestAvailable(nowMicros) : 0;
    }

    /**
     * Acquires a permit if it can be acquired immediately without delay.
     *
     * @return {@code true} if the permit was acquired or rate limiting is disabled, {@code false} otherwise
     */
    public boolean tryAcquire()
    {
        RateLimiter rateLimiter = ref.get();
        return rateLimiter == null || rateLimiter.tryAcquire();
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
        RateLimiter rateLimiter = ref.get();

        if (permitsPerSecond > 0.0)
        {
            if (rateLimiter == null)
            {
                ref.compareAndSet(null, RateLimiter.create(permitsPerSecond));
                if (clock != null)
                {
                    clock.reset();
                    clock.start();
                    return;
                }
                clock = Stopwatch.createStarted();
            }
            else
            {
                rateLimiter.setRate(permitsPerSecond);
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
        RateLimiter rateLimiter = ref.get();
        return rateLimiter != null ? rateLimiter.getRate() : 0;
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
        RateLimiter rateLimiter = ref.get();
        return rateLimiter != null ? rateLimiter.acquire() : 0;
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
        RateLimiter rateLimiter = ref.get();
        return rateLimiter != null && permits > 0 ? rateLimiter.acquire(permits) : 0;
    }
}
