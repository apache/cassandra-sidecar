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

    private SidecarRateLimiter(final double permitsPerSecond)
    {
        if (permitsPerSecond > 0)
        {
            RateLimiter rateLimiter = RateLimiter.create(permitsPerSecond);
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

    /**
     * @return {@code true} if rate-limiting is enabled, {@code false} otherwise
     */
    public boolean isRateLimited()
    {
        return ref.get() != null;
    }

    /**
     * Disables rate limiting
     */
    public void disableRateLimiting()
    {
        ref.set(null);
    }

    // Delegated methods

    /**
     * Returns the earliest time permits will become available. Returns 0 if disabled
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
     * Tries to reserve 1 permit, if not available immediately returns false. Returns true if rate-limiting is disabled
     *
     * @return {@code true} if the permit was acquired, {@code false} otherwise
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
            }
            else
            {
                rateLimiter.setRate(permitsPerSecond);
            }
        }
        else
        {
            disableRateLimiting();
        }
    }

    public void doSetRate(double permitsPerSecond, long nowMicros)
    {
        RateLimiter rateLimiter = ref.get();

        if (permitsPerSecond > 0.0)
        {
            if (rateLimiter == null)
            {
                RateLimiter limiter = RateLimiter.create(permitsPerSecond);
                limiter.doSetRate(permitsPerSecond, nowMicros);
                ref.compareAndSet(null, limiter);
            }
            else
            {
                rateLimiter.doSetRate(permitsPerSecond, nowMicros);
            }
        }
        else
        {
            disableRateLimiting();
        }
    }

    public double rate()
    {
        RateLimiter rateLimiter = ref.get();
        return rateLimiter != null ? rateLimiter.getRate() : 0;
    }

    public double doGetRate()
    {
        RateLimiter rateLimiter = ref.get();
        return rateLimiter != null ? rateLimiter.doGetRate() : 0;
    }

    @CanIgnoreReturnValue
    public double acquire()
    {
        RateLimiter rateLimiter = ref.get();
        return rateLimiter != null ? rateLimiter.acquire() : 0;
    }

    @CanIgnoreReturnValue
    public double acquire(int permits)
    {
        RateLimiter rateLimiter = ref.get();
        return rateLimiter != null && permits > 0 ? rateLimiter.acquire(permits) : 0;
    }

    public long reserve(int permits)
    {
        RateLimiter rateLimiter = ref.get();
        return rateLimiter != null && permits > 0 ? rateLimiter.reserve(permits) : 0;
    }

    public boolean tryAcquire(long timeout, TimeUnit unit)
    {
        RateLimiter rateLimiter = ref.get();
        return rateLimiter == null || rateLimiter.tryAcquire(timeout, unit);
    }

    public boolean tryAcquire(int permits)
    {
        RateLimiter rateLimiter = ref.get();
        return rateLimiter == null || rateLimiter.tryAcquire(permits);
    }

    public boolean tryAcquire(int permits, long timeout, TimeUnit unit)
    {
        RateLimiter rateLimiter = ref.get();
        return rateLimiter == null || rateLimiter.tryAcquire(permits, timeout, unit);
    }

    public long reserveAndGetWaitLength(int permits, long nowMicros)
    {
        RateLimiter rateLimiter = ref.get();
        return rateLimiter != null ? rateLimiter.reserveAndGetWaitLength(permits, nowMicros) : 0;
    }

    public long reserveEarliestAvailable(int permits, long nowMicros)
    {
        RateLimiter rateLimiter = ref.get();
        return rateLimiter != null ? rateLimiter.reserveEarliestAvailable(permits, nowMicros) : 0;
    }
}
