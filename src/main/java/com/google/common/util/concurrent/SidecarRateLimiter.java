package com.google.common.util.concurrent;

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
}
