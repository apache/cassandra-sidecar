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

import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.datastax.driver.core.NettyOptions;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;

/**
 * This class is used to encapsulate several heavy-weight objects that can be shared across all executions
 * of tests within a test run. It intentionally does not close these resources when the Cassandra Driver's
 * cluster is closed, so they can be reused for the next test.
 */
class SharedExecutorNettyOptions extends NettyOptions
{
    private SharedExecutorNettyOptions()
    {
    }

    public static final SharedExecutorNettyOptions INSTANCE = new SharedExecutorNettyOptions();

    private final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                                                .setDaemon(true)
                                                .setNameFormat("IntegrationTest-%d")
                                                .build();
    private final HashedWheelTimer sharedHWT = new HashedWheelTimer(threadFactory);
    private final EventLoopGroup sharedEventLoopGroup = new NioEventLoopGroup(0, threadFactory);

    public EventLoopGroup eventLoopGroup(ThreadFactory threadFactory)
    {
        return sharedEventLoopGroup;
    }

    public void onClusterClose(EventLoopGroup eventLoopGroup)
    {
    }

    @Override
    public Timer timer(ThreadFactory threadFactory)
    {
        return sharedHWT;
    }

    public void onClusterClose(Timer timer)
    {
    }
}
