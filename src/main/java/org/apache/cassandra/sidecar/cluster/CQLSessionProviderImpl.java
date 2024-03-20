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

package org.apache.cassandra.sidecar.cluster;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.utils.DriverUtils;
import org.apache.cassandra.sidecar.config.DriverConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Provides connections to the local Cassandra cluster as defined in the Configuration. Currently, it only supports
 * returning the local connection.
 */
public class CQLSessionProviderImpl implements CQLSessionProvider
{
    private static final Logger logger = LoggerFactory.getLogger(CQLSessionProviderImpl.class);
    private final List<InetSocketAddress> contactPoints;
    private final int numConnections;
    private final String localDc;
    private final NettyOptions nettyOptions;
    private final ReconnectionPolicy reconnectionPolicy;
    private final List<InetSocketAddress> localInstances;
    private final DriverUtils driverUtils;
    @Nullable
    private volatile Session session;

    @VisibleForTesting
    public CQLSessionProviderImpl(List<InetSocketAddress> contactPoints,
                                  List<InetSocketAddress> localInstances,
                                  int healthCheckFrequencyMillis,
                                  String localDc,
                                  int numConnections,
                                  NettyOptions options)
    {
        this.contactPoints = contactPoints;
        this.localInstances = localInstances;
        this.localDc = localDc;
        this.numConnections = numConnections;
        this.nettyOptions = options;
        this.reconnectionPolicy = new ExponentialReconnectionPolicy(500, healthCheckFrequencyMillis);
        this.driverUtils = new DriverUtils();
    }

    public CQLSessionProviderImpl(SidecarConfiguration configuration,
                                  NettyOptions options,
                                  DriverUtils driverUtils)
    {
        this.driverUtils = driverUtils;
        DriverConfiguration driverConfiguration = configuration.driverConfiguration();
        this.contactPoints = driverConfiguration.contactPoints();
        this.localInstances = configuration.cassandraInstances()
                                           .stream()
                                           .map(i -> new InetSocketAddress(i.host(), i.port()))
                                           .collect(Collectors.toList());
        this.localDc = driverConfiguration.localDc();
        this.numConnections = driverConfiguration.numConnections();
        this.nettyOptions = options;
        int maxDelayMs = configuration.healthCheckConfiguration().checkIntervalMillis();
        this.reconnectionPolicy = new ExponentialReconnectionPolicy(500, maxDelayMs);
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

    /**
     * Provides a Session connected to the cluster. If null it means the connection was
     * could not be established. The session still might throw a NoHostAvailableException if the
     * cluster is otherwise unreachable.
     *
     * @return Session
     */
    @Override
    @Nullable
    public synchronized Session get()
    {
        if (session != null)
        {
            return session;
        }
        Cluster cluster = null;
        try
        {
            logger.info("Connecting to cluster using contact points {}", contactPoints);

            LoadBalancingPolicy lbp = new SidecarLoadBalancingPolicy(localInstances, localDc, numConnections,
                                                                     driverUtils);
            // Prevent spurious reconnects of ignored down nodes on `onUp` events
            QueryOptions queryOptions = new QueryOptions().setReprepareOnUp(false);
            cluster = Cluster.builder()
                             .addContactPointsWithPorts(contactPoints)
                             .withReconnectionPolicy(reconnectionPolicy)
                             .withoutMetrics()
                             .withLoadBalancingPolicy(lbp)
                             .withQueryOptions(queryOptions)
                             // tests can create a lot of these Cluster objects, to avoid creating HWTs and
                             // event thread pools for each we have the override
                             .withNettyOptions(nettyOptions)
                             .build();
            session = cluster.connect();
            logger.info("Successfully connected to Cassandra!");
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
        return session;
    }

    @Override
    public Session getIfConnected()
    {
        return session;
    }

    @Override
    public void close()
    {
        Session localSession;
        synchronized (this)
        {
            localSession = this.session;
            this.session = null;
        }
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
        }
    }
}
