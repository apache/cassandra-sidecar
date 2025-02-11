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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException.Service.CQL;

/**
 * A CQL Session provider that always connects to and queries all hosts provided to it.
 * Useful for integration testing, but will eventually be removed once issues with the Sidecar's
 * CQLSessionProviderImpl are resolved.
 */
public class TemporaryCqlSessionProvider implements CQLSessionProvider
{
    private static final Logger logger = LoggerFactory.getLogger(TemporaryCqlSessionProvider.class);
    private final List<InetSocketAddress> contactPoints;
    private Session localSession;
    private final NettyOptions nettyOptions;
    private final ReconnectionPolicy reconnectionPolicy;

    public TemporaryCqlSessionProvider(List<InetSocketAddress> contactPoints, NettyOptions options)
    {
        nettyOptions = options;
        reconnectionPolicy = new ExponentialReconnectionPolicy(100, 1000);
        this.contactPoints = contactPoints;
    }

    @NotNull
    @Override
    public synchronized Session get()
    {
        if (localSession != null)
        {
            return localSession;
        }

        Cluster cluster = null;
        try
        {
            logger.info("Connecting to {}", contactPoints);
            cluster = Cluster.builder()
                             .addContactPointsWithPorts(contactPoints)
                             .withReconnectionPolicy(reconnectionPolicy)
                             .withoutMetrics()
                             // tests can create a lot of these Cluster objects, to avoid creating HWTs and
                             // event thread pools for each we have the override
                             .withNettyOptions(nettyOptions)
                             .build();
            localSession = cluster.connect();
            logger.info("Successfully connected to Cassandra instance!");
        }
        catch (Exception connectionException)
        {
            logger.error("Failed to reach Cassandra", connectionException);
            if (cluster != null)
            {
                try
                {
                    cluster.close();
                }
                catch (Exception closeException)
                {
                    logger.error("Failed to close cluster in cleanup", closeException);
                    connectionException.addSuppressed(closeException);
                }
            }
            throw new CassandraUnavailableException(CQL, connectionException);
        }
        return localSession;
    }

    @Override
    public Session getIfConnected()
    {
        return this.localSession;
    }

    @Override
    public void close()
    {
        Session localSession;
        synchronized (this)
        {
            localSession = this.localSession;
            this.localSession = null;
        }

        if (localSession != null)
        {
            try
            {
                localSession.getCluster().closeAsync().get(1, TimeUnit.MINUTES);
                localSession.closeAsync().get(1, TimeUnit.MINUTES);
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

    static RuntimeException propagateCause(ExecutionException e)
    {
        Throwable cause = e.getCause();
        if (cause instanceof Error)
        {
            throw (Error) cause;
        }
        else if (cause instanceof DriverException)
        {
            throw ((DriverException) cause).copy();
        }
        else
        {
            throw new DriverInternalError("Unexpected exception thrown", cause);
        }
    }
}
