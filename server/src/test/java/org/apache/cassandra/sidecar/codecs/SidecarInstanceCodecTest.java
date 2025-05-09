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

package org.apache.cassandra.sidecar.codecs;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;

import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link SidecarInstanceCodec}
 */
class SidecarInstanceCodecTest
{
    static Vertx vertx;
    private static EventBus eventBus;
    private static SidecarInstanceCodec<SidecarInstanceImpl> codec;

    @BeforeAll
    static void setup()
    {
        vertx = Vertx.vertx();
        eventBus = vertx.eventBus();
        codec = new SidecarInstanceCodec<>();
        eventBus.registerDefaultCodec(SidecarInstanceImpl.class, codec);
    }

    @AfterAll
    static void tearDown()
    {
        getBlocking(TestResourceReaper.create().with(vertx).close(), 30, TimeUnit.SECONDS, "Closing vertx");
    }

    @Test
    void testCodec() throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(1);
        SidecarInstanceImpl sidecarInstance = new SidecarInstanceImpl("localhost", 1234);
        eventBus.consumer("test address", message -> {
            assertThat(message.body()).isEqualTo(sidecarInstance);
            latch.countDown();
        });
        eventBus.send("test address", sidecarInstance);
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void testEncodingDecoding()
    {
        Buffer buffer = Buffer.buffer(1024);
        SidecarInstanceImpl sidecarInstance = new SidecarInstanceImpl("127.0.0.1", 9876);
        codec.encodeToWire(buffer, sidecarInstance);
        SidecarInstance decoded = codec.decodeFromWire(0, buffer);
        assertThat(decoded).isEqualTo(sidecarInstance);
    }
}
