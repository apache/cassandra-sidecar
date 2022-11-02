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

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

/**
 * Represents a connection to Cassandra cluster. Currently, supports returning the local connection only as
 * defined in the Configuration.
 */
public class CQLSession
{
    private static final Logger logger = LoggerFactory.getLogger(CQLSession.class);

    @Nullable
    private Session localSession;
    private final InetSocketAddress inet;
    private final WhiteListPolicy wlp;
    private final NettyOptions nettyOptions;
    private final QueryOptions queryOptions;
    private final ReconnectionPolicy reconnectionPolicy;

    public CQLSession(String host, int port, long healthCheckFrequency)
    {
        // this was originally using unresolved Inet addresses, but it would fail when trying to
        // connect to a docker container
        logger.info("Connecting to {} on port {}", host, port);
        inet = new InetSocketAddress(host, port);

        wlp = new WhiteListPolicy(new RoundRobinPolicy(), Collections.singletonList(inet));
        this.nettyOptions = new NettyOptions();
        this.queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE);
        this.reconnectionPolicy = new ExponentialReconnectionPolicy(1000, healthCheckFrequency);
    }

    public CQLSession(InetSocketAddress target, NettyOptions options)
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
    public synchronized Session getLocalCql()
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
                logger.info("Successfully connected to Casssandra instance!");
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

    /**
     * @return the address of the instance where the session is established
     */
    public InetSocketAddress inet()
    {
        return inet;
    }

    public synchronized void close()
    {
        if (localSession != null)
        {
            localSession.getCluster().close();
            localSession = null;
        }
    }
}
