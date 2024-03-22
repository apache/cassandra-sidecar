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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.metrics.ResourceMetrics;

import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test {@link ExecutorPools}
 */
@ExtendWith(VertxExtension.class)
public class ExecutorPoolsTest
{
    private ExecutorPools pools;
    private ResourceMetrics metrics;
    private Vertx vertx;

    @BeforeEach
    public void before()
    {
        vertx = Vertx.vertx();
        metrics = new ResourceMetrics(registry());
        pools = new ExecutorPools(vertx, new ServiceConfigurationImpl(), metrics);
    }

    @AfterEach
    public void after()
    {
        registry().removeMatching((name, metric) -> true);
        vertx.close().onComplete(v -> pools.close()).result();
    }

    @Test
    public void testClosingExecutorPoolShouldThrow()
    {
        assertThatThrownBy(() -> pools.service().close())
        .hasMessage("Closing TaskExecutorPool is not supported!")
        .isExactlyInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> pools.internal().close())
        .hasMessage("Closing TaskExecutorPool is not supported!")
        .isExactlyInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testOrdered(VertxTestContext context)
    {
        // not thread-safe
        class IntWrapper
        {
            int i = 0;

            void increment()
            {
                i += 1;
            }
        }

        ExecutorPools.TaskExecutorPool pool = pools.internal();
        IntWrapper v = new IntWrapper();
        int total = 100;
        CountDownLatch stop = new CountDownLatch(total);
        Set<String> threadNames = new HashSet<>();
        for (int i = 0; i < total; i++)
        {
            // Start 100 parallel executions that each submits the ordered execution
            pool.executeBlocking(promise -> {
                pool.executeBlocking(p -> {
                    v.increment();
                    threadNames.add(Thread.currentThread().getName());
                    stop.countDown();
                }, true);
            }, false);
        }

        vertx.setTimer(100, handle -> {
            assertThat(metrics.longTasks.metric.getCount()).isEqualTo(200);
            context.completeNow();
        });

        assertThat(Uninterruptibles.awaitUninterruptibly(stop, 10, TimeUnit.SECONDS))
        .describedAs("Test should finish in 10 seconds")
        .isTrue();
        // Although IntWrapper is not thread safe, the serial execution (ordered) prevents any race condition.
        assertThat(v.i).isEqualTo(total);
    }
}
