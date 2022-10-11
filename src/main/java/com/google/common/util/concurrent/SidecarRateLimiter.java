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

import com.google.errorprone.annotations.CanIgnoreReturnValue;

/**
 * Wrapper class over guava Rate Limiter, uses SmoothBursty Ratelimiter. This class mainly exists to expose
 * package protected method queryEarliestAvailable of guava RateLimiter
 */
public class SidecarRateLimiter
{
    private final RateLimiter rateLimiter;

    private SidecarRateLimiter(final double permitsPerSecond)
    {
        this.rateLimiter = RateLimiter.create(permitsPerSecond);
    }

    public static SidecarRateLimiter create(final double permitsPerSecond)
    {
        return new SidecarRateLimiter(permitsPerSecond);
    }

    // Delegated methods

    /**
     * Returns earliest time permits will become available
     */
    public long queryEarliestAvailable(final long nowMicros)
    {
        return this.rateLimiter.queryEarliestAvailable(nowMicros);
    }

    /**
     * Tries to reserve 1 permit, if not available immediately returns false
     */
    public boolean tryAcquire()
    {
        return this.rateLimiter.tryAcquire();
    }

    /**
     * Updates the stable rate of the internal {@code RateLimiter}, that is, the {@code permitsPerSecond}
     * argument provided in the factory method that constructed the {@code RateLimiter}.
     *
     * @param permitsPerSecond the new stable rate of this {@code RateLimiter}
     * @throws IllegalArgumentException if {@code permitsPerSecond} is negative or zero
     */
    public void setRate(double permitsPerSecond)
    {
        this.rateLimiter.setRate(permitsPerSecond);
    }

    public void doSetRate(double permitsPerSecond, long nowMicros)
    {
        rateLimiter.doSetRate(permitsPerSecond, nowMicros);
    }

    public double getRate()
    {
        return rateLimiter.getRate();
    }

    public double doGetRate()
    {
        return rateLimiter.doGetRate();
    }

    @CanIgnoreReturnValue
    public double acquire()
    {
        return rateLimiter.acquire();
    }

    @CanIgnoreReturnValue
    public double acquire(int permits)
    {
        return rateLimiter.acquire(permits);
    }

    public long reserve(int permits)
    {
        return rateLimiter.reserve(permits);
    }

    public boolean tryAcquire(long timeout, TimeUnit unit)
    {
        return rateLimiter.tryAcquire(timeout, unit);
    }

    public boolean tryAcquire(int permits)
    {
        return rateLimiter.tryAcquire(permits);
    }

    public boolean tryAcquire(int permits, long timeout, TimeUnit unit)
    {
        return rateLimiter.tryAcquire(permits, timeout, unit);
    }

    public long reserveAndGetWaitLength(int permits, long nowMicros)
    {
        return rateLimiter.reserveAndGetWaitLength(permits, nowMicros);
    }

    public long reserveEarliestAvailable(int permits, long nowMicros)
    {
        return rateLimiter.reserveEarliestAvailable(permits, nowMicros);
    }
}
