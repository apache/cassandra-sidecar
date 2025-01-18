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

import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;

/**
 * Utilities for {@link EventBus}
 */
public class EventBusUtils
{
    private EventBusUtils()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a local consumer against the specific address. The consumer is going to receive the message at most once
     * @param eventBus event bus for message distribution
     * @param address the address that will register it at
     * @param handler the handler that will process the received messages
     * @param <T> message type
     */
    public static <T> void onceLocalConsumer(EventBus eventBus, String address, Handler<Message<T>> handler)
    {
        MessageConsumer<T> consumer = eventBus.localConsumer(address);
        consumer.handler(msg -> {
            handler.handle(msg);
            consumer.unregister();
        });
    }
}
