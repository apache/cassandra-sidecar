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

package org.apache.cassandra.sidecar.common;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import org.jetbrains.annotations.Nullable;

/**
 * Provides connections to the local Cassandra cluster as defined in the Configuration. Currently, it only supports
 * returning the local connection.
 */
public class CQLSessionProvider
{
    private static final Logger logger = LoggerFactory.getLogger(CQLSessionProvider.class);

    @Nullable
    private Session localSession;
    private final InetSocketAddress inet;
    private final WhiteListPolicy wlp;
    private final NettyOptions nettyOptions;
    private final QueryOptions queryOptions;
    private final ReconnectionPolicy reconnectionPolicy;

    public CQLSessionProvider(String host, int port, int healthCheckInterval)
    {
        // this was originally using unresolved Inet addresses, but it would fail when trying to
        // connect to a docker container
        logger.info("Connecting to {} on port {}", host, port);
        inet = new InetSocketAddress(host, port);

        wlp = new WhiteListPolicy(new RoundRobinPolicy(), Collections.singletonList(inet));
        this.nettyOptions = new NettyOptions();
        this.queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE);
        this.reconnectionPolicy = new ExponentialReconnectionPolicy(1000, healthCheckInterval);
    }

    public CQLSessionProvider(InetSocketAddress target, NettyOptions options)
    {
        inet = target;
        wlp = new WhiteListPolicy(new RoundRobinPolicy(), Collections.singletonList(inet));
        this.nettyOptions = options;
        this.queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE);
        reconnectionPolicy = new ExponentialReconnectionPolicy(100, 1000);
    }

    /**
     * Provides a Session connected only to the local node from configuration. If null it means the connection was
     * not able to be established. The session still might throw a NoHostAvailableException if the local host goes
     * offline or otherwise unavailable.
     *
     * @return Session
     */
    @Nullable
    public synchronized Session localCql()
    {
        Cluster cluster = null;
        try
        {
            if (localSession == null)
            {
                logger.info("Connecting to {}", inet);
                cluster = Cluster.builder()
                                 .addContactPointsWithPorts(inet)
                                 .withLoadBalancingPolicy(wlp)
                                 .withQueryOptions(queryOptions)
                                 .withReconnectionPolicy(reconnectionPolicy)
                                 .withoutMetrics()
                                 // tests can create a lot of these Cluster objects, to avoid creating HWTs and
                                 // event thread pools for each we have the override
                                 .withNettyOptions(nettyOptions)
                                 .build();
                localSession = cluster.connect();
                logger.info("Successfully connected to Cassandra instance!");
            }
        }
        catch (Exception e)
        {
            logger.error("Failed to reach Cassandra", e);
            if (cluster != null)
            {
                try
                {
                    cluster.close();
                }
                catch (Exception ex)
                {
                    logger.error("Failed to close cluster in cleanup", ex);
                }
            }
        }
        return localSession;
    }

    public synchronized void close()
    {
        if (localSession != null)
        {
            try
            {
                localSession.getCluster().closeAsync().get(1, TimeUnit.MINUTES);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
            catch (TimeoutException e)
            {
                logger.warn("Unable to close session after 1 minute for provider {}", this, e);
            }
            catch (ExecutionException e)
            {
                throw propagateCause(e);
            }
            localSession = null;
        }
    }

    static RuntimeException propagateCause(ExecutionException e)
    {
        Throwable cause = e.getCause();

        if (cause instanceof Error) throw ((Error) cause);

        // We could just rethrow e.getCause(). However, the cause of the ExecutionException has likely
        // been
        // created on the I/O thread receiving the response. Which means that the stacktrace associated
        // with said cause will make no mention of the current thread. This is painful for say, finding
        // out which execute() statement actually raised the exception. So instead, we re-create the
        // exception.
        if (cause instanceof DriverException) throw ((DriverException) cause).copy();
        else throw new DriverInternalError("Unexpected exception thrown", cause);
    }
}
