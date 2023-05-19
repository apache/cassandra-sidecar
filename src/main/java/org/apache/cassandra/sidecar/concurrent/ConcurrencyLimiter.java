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

package org.apache.cassandra.sidecar.concurrent;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

/**
 * A class that provides functionality for a concurrency limiter where implementing consumers can try to acquire a
 * permit before executing an operation, and later releasing the permit when the operation is completed. This
 * implementation relies on clients playing fairly and not acquiring more permits or releasing more permits than they
 * are allowed. This class is intended to be used on constrained resources, for example uploads to the server, where
 * we want to limit the amount of concurrent uploads to the server, and if we are too busy, we want to provide a
 * mechanism to deny new uploads until the server has been freed up.
 *
 * <p>The intended usage of this class is as follows:
 *
 * <pre>
 *     if (limiter.tryAcquire()) {
 *         try {
 *             // .. some expensive operation
 *         } finally {
 *             limiter.releasePermit();
 *         }
 *     } else {
 *         // .. handle the case where the limit has been reached
 *     }
 * </pre>
 *
 * <p>In a vertx {@link io.vertx.core.Handler}, this can be implemented as follows:
 *
 * <pre>
 *     public void handle(RoutingContext context) {
 *         if (!limiter.tryAcquire()) {
 *             // handle the case where the limit has been reached
 *             return;
 *         }
 *         // to make sure that permit is always released
 *         context.addEndHandler(v -&gt; limiter.releasePermit());
 *         // .. some expensive operation
 *     }
 * </pre>
 */
public class ConcurrencyLimiter
{
    private final AtomicInteger permits = new AtomicInteger(0);
    private final IntSupplier concurrencyLimitSupplier;

    public ConcurrencyLimiter(IntSupplier concurrencyLimitSupplier)
    {
        this.concurrencyLimitSupplier = concurrencyLimitSupplier;
    }

    /**
     * @return true if we successfully acquire the permit, false otherwise
     */
    public boolean tryAcquire()
    {
        int current;
        int next;
        int limit = concurrencyLimitSupplier.getAsInt();
        do
        {
            current = permits.get();
            next = current + 1;
            if (next > limit)
            {
                return false;
            }
        }
        while (!permits.compareAndSet(current, next));
        return true;
    }

    /**
     * Releases the permit that we held
     */
    public void releasePermit()
    {
        permits.updateAndGet(current -> {
            int next = current - 1;
            return Math.max(next, 0);
        });
    }

    /**
     * @return the configured limit for this instance
     */
    public int limit()
    {
        return concurrencyLimitSupplier.getAsInt();
    }

    /**
     * @return the number of acquired permits at this moment
     */
    public int acquiredPermits()
    {
        return permits.get();
    }
}
