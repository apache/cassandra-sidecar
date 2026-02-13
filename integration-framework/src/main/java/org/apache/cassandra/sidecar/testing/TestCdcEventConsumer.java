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
package org.apache.cassandra.sidecar.testing;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.inject.Singleton;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.msg.CdcEvent;

/**
 * Test implementation of EventConsumer for CDC integration tests.
 * Stores CDC events in a concurrent queue that can be accessed for test assertions.
 */
@Singleton
public class TestCdcEventConsumer implements EventConsumer
{
    private final Queue<CdcEvent> events = new ConcurrentLinkedQueue<>();

    @Override
    public void accept(CdcEvent event)
    {
        events.offer(event);
    }

    /**
     * @return all CDC events captured so far as a list
     */
    public List<CdcEvent> getEvents()
    {
        return new ArrayList<>(events);
    }

    /**
     * Clear all captured events
     */
    public void clear()
    {
        events.clear();
    }
}
