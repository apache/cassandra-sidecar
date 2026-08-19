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

package org.apache.cassandra.sidecar.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link RequestExecutor#schedule(long, Runnable)}.
 */
class RequestExecutorTest
{
    private RequestExecutor executor;

    @BeforeEach
    void setup()
    {
        executor = new RequestExecutor(mock(HttpClient.class));
    }

    @AfterEach
    void tearDown() throws Exception
    {
        executor.close();
    }

    @Test
    void scheduleDoesNotRunImmediatelyWhenDelayIsPositive()
    {
        AtomicInteger invocationCount = new AtomicInteger(0);
        executor.schedule(200, invocationCount::incrementAndGet);
        // must not have run synchronously -- it was scheduled 200ms in the future
        assertThat(invocationCount.get()).isEqualTo(0);
    }

    @Test
    void scheduleRunsExactlyOnceAfterThePositiveDelayElapses() throws InterruptedException
    {
        AtomicInteger invocationCount = new AtomicInteger(0);
        CountDownLatch ran = new CountDownLatch(1);
        executor.schedule(50, () -> {
            invocationCount.incrementAndGet();
            ran.countDown();
        });

        assertThat(ran.await(1, TimeUnit.SECONDS)).isTrue();
        // give a buggy duplicate invocation (immediate-fire) time to have already happened by now
        Thread.sleep(200);
        assertThat(invocationCount.get()).isEqualTo(1);
    }

    @Test
    void scheduleRunsImmediatelyWhenDelayIsZero()
    {
        AtomicInteger invocationCount = new AtomicInteger(0);
        executor.schedule(0, invocationCount::incrementAndGet);
        assertThat(invocationCount.get()).isEqualTo(1);
    }

    @Test
    void scheduleRunsImmediatelyWhenDelayIsNegative()
    {
        AtomicInteger invocationCount = new AtomicInteger(0);
        executor.schedule(-1, invocationCount::incrementAndGet);
        assertThat(invocationCount.get()).isEqualTo(1);
    }
}
