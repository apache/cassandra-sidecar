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

package org.apache.cassandra.sidecar.utils;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.TestResourceReaper;

import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;

class EventBusUtilsTest
{
    static Vertx vertx = Vertx.vertx();

    @AfterAll
    static void cleanup()
    {
        TestResourceReaper.create().with(vertx).close();
    }

    @Test
    void testOnceLocalConsumerReceiveMessageOnlyOnce()
    {
        AtomicInteger exactOnceReceiver = new AtomicInteger(0);
        AtomicInteger longLivedReceiver = new AtomicInteger(0);
        EventBusUtils.onceLocalConsumer(vertx.eventBus(), "foo", ignored -> exactOnceReceiver.incrementAndGet());
        vertx.eventBus().localConsumer("foo", ignored -> longLivedReceiver.incrementAndGet());
        for (int i = 0; i < 10; i++)
        {
            vertx.eventBus().publish("foo", "bar");
        }

        loopAssert(1, () -> assertThat(exactOnceReceiver.get())
                            .describedAs("Should only receive the message once")
                            .isEqualTo(1));
        loopAssert(1, () -> assertThat(longLivedReceiver.get())
                            .describedAs("Should receive the message 10 times")
                            .isEqualTo(10));
        assertThat(exactOnceReceiver.get())
        .describedAs("Run the check again to prove that it receives value exactly once")
        .isEqualTo(1);
    }
}
