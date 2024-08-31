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

package org.apache.cassandra.sidecar.tasks;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Test;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link PeriodicTaskExecutor} class
 */
class PeriodicTaskExecutorTest
{
    @Test
    void testLoopFailure()
    {
        Vertx vertx = Vertx.vertx();
        ExecutorPools executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());
        PeriodicTaskExecutor taskExecutor = new PeriodicTaskExecutor(executorPools);

        int totalFailures = 5;
        AtomicInteger failuresCount = new AtomicInteger(0);
        CountDownLatch closeLatch = new CountDownLatch(1);
        AtomicBoolean isClosed = new AtomicBoolean(false);
        taskExecutor.schedule(new PeriodicTask()
        {
            @Override
            public long delay()
            {
                return 20;
            }

            @Override
            public void execute(Promise<Void> promise)
            {
                if (failuresCount.incrementAndGet() == totalFailures)
                {
                    taskExecutor.unschedule(this);
                }
                throw new RuntimeException("ah, it failed");
            }

            @Override
            public void close()
            {
                isClosed.set(true);
                closeLatch.countDown();
            }
        });
        Uninterruptibles.awaitUninterruptibly(closeLatch);
        assertThat(isClosed.get()).isTrue();
        assertThat(failuresCount.get()).isEqualTo(totalFailures);
    }
}
