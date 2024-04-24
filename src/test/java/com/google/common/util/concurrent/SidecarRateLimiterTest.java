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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link SidecarRateLimiter} class
 */
class SidecarRateLimiterTest
{
    @Test
    void testCreation()
    {
        // Creates a SidecarRateLimiter that is enabled
        SidecarRateLimiter enabledRateLimiter = SidecarRateLimiter.create(100);
        assertThat(enabledRateLimiter).isNotNull();
        assertThat(enabledRateLimiter.rate()).isEqualTo(100);
        assertThat(enabledRateLimiter.tryAcquire()).isTrue();
        enabledRateLimiter.rate(150);
        assertThat(enabledRateLimiter.rate()).isEqualTo(150);
        assertThat(enabledRateLimiter.queryEarliestAvailable(0)).isGreaterThan(0);

        // Creates a SidecarRateLimiter that is disabled
        SidecarRateLimiter disabledRateLimiter = SidecarRateLimiter.create(-1);
        assertThat(disabledRateLimiter).isNotNull();
        assertThat(disabledRateLimiter.rate()).isEqualTo(0);
        assertThat(disabledRateLimiter.queryEarliestAvailable(1000L)).isEqualTo(0);
    }

    @Test
    void testDisableRateLimitingBySettingRate()
    {
        SidecarRateLimiter rateLimiter = SidecarRateLimiter.create(1);
        rateLimiter.acquire(2); // reserve some permits
        assertThat(rateLimiter.tryAcquire()).isFalse();
        assertThat(rateLimiter.acquire(2)).isGreaterThan(0.0);

        rateLimiter.rate(-1);
        assertThat(rateLimiter.tryAcquire()).isTrue();
        rateLimiter.acquire(2000); // reserve some permits
        assertThat(rateLimiter.acquire(2000)).isEqualTo(0.0);
    }

    @Test
    void testEnableRateLimitingBySettingRate()
    {
        SidecarRateLimiter rateLimiter = SidecarRateLimiter.create(-1);
        rateLimiter.acquire(2000); // reserve some permits
        assertThat(rateLimiter.tryAcquire()).isTrue();
        assertThat(rateLimiter.acquire(2000)).isEqualTo(0.0);
        assertThat(rateLimiter.tryAcquire()).isTrue();

        rateLimiter.rate(1);
        rateLimiter.acquire(2); // reserve some permits
        assertThat(rateLimiter.tryAcquire()).isFalse();
        assertThat(rateLimiter.acquire(2)).isGreaterThan(0.0);
    }

    @Test
    void testAcquireZeroPermitsDoesNotThrow()
    {
        SidecarRateLimiter rateLimiter = SidecarRateLimiter.create(100);
        assertThat(rateLimiter.acquire(0)).isEqualTo(0);
        assertThat(rateLimiter.acquire(5)).isEqualTo(0);
        assertThat(rateLimiter.acquire(500)).isNotEqualTo(0);
    }

    @Test
    void testWaitTimeReturned()
    {
        SidecarRateLimiter rateLimiter = SidecarRateLimiter.create(10);

        rateLimiter.acquire(10);
        assertThat(rateLimiter.queryWaitTimeInMicros()).isGreaterThanOrEqualTo(500000);

        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        rateLimiter.acquire(100);
        assertThat(rateLimiter.queryWaitTimeInMicros()).isGreaterThanOrEqualTo(8000000);
    }

    @Test
    void testClockResetWithRateUpdate()
    {
        SidecarRateLimiter rateLimiter = SidecarRateLimiter.create(-1);
        rateLimiter.acquire(2000);
        assertThat(rateLimiter.queryWaitTimeInMicros()).isEqualTo(0);

        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

        rateLimiter.rate(1);
        rateLimiter.acquire(4);
        assertThat(rateLimiter.queryWaitTimeInMicros()).isGreaterThanOrEqualTo(3000000);
    }
}
