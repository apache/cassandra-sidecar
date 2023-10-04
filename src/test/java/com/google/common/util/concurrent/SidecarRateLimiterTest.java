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
        assertThat(enabledRateLimiter.isRateLimited()).isTrue();
        assertThat(enabledRateLimiter.rate()).isEqualTo(100);
        assertThat(enabledRateLimiter.tryAcquire()).isTrue();
        enabledRateLimiter.rate(150);
        assertThat(enabledRateLimiter.doGetRate()).isEqualTo(150);
        assertThat(enabledRateLimiter.queryEarliestAvailable(0)).isGreaterThan(0);

        // Creates a SidecarRateLimiter that is disabled
        SidecarRateLimiter disabledRateLimiter = SidecarRateLimiter.create(-1);
        assertThat(disabledRateLimiter).isNotNull();
        assertThat(disabledRateLimiter.isRateLimited()).isFalse();
        assertThat(disabledRateLimiter.rate()).isEqualTo(0);
        assertThat(disabledRateLimiter.queryEarliestAvailable(1000L)).isEqualTo(0);
    }

    @Test
    void testDisableRateLimiting()
    {
        SidecarRateLimiter rateLimiter = SidecarRateLimiter.create(100);
        assertThat(rateLimiter.isRateLimited()).isTrue();

        rateLimiter.disableRateLimiting();
        assertThat(rateLimiter.isRateLimited()).isFalse();
    }

    @Test
    void testDisableRateLimitingBySettingRate()
    {
        SidecarRateLimiter rateLimiter = SidecarRateLimiter.create(100);
        assertThat(rateLimiter.isRateLimited()).isTrue();

        rateLimiter.rate(-1);
        assertThat(rateLimiter.isRateLimited()).isFalse();

        rateLimiter.doSetRate(100, 0);
        assertThat(rateLimiter.isRateLimited()).isTrue();

        rateLimiter.doSetRate(-1, 0);
        assertThat(rateLimiter.isRateLimited()).isFalse();
    }

    @Test
    void testEnableRateLimitingBySettingRate()
    {
        SidecarRateLimiter rateLimiter = SidecarRateLimiter.create(-1);
        assertThat(rateLimiter.isRateLimited()).isFalse();

        rateLimiter.rate(500);
        assertThat(rateLimiter.isRateLimited()).isTrue();

        rateLimiter.doSetRate(-1, 0);
        assertThat(rateLimiter.isRateLimited()).isFalse();

        rateLimiter.doSetRate(500, 0);
        assertThat(rateLimiter.isRateLimited()).isTrue();
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
    void testReserveZeroPermitsDoesNotThrow()
    {
        SidecarRateLimiter rateLimiter = SidecarRateLimiter.create(100);
        assertThat(rateLimiter.reserve(0)).isEqualTo(0);
    }
}
