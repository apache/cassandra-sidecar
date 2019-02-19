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

package org.apache.cassandra.sidecar.routes;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class HealthCheck implements Supplier<Boolean>
{
    private static final Logger logger = LoggerFactory.getLogger(HealthCheck.class);
    private final String cassandraHost;
    private final int cassandraPort;
    private Cluster cluster;
    private Session session;

    /**
     * Constructor
     *
     * @param cassandraHost
     * @param cassandraPort
     */
    public HealthCheck(String cassandraHost, int cassandraPort)
    {
        this.cassandraHost = cassandraHost;
        this.cassandraPort = cassandraPort;
        this.cluster = createCluster(cassandraHost, cassandraPort);
    }

    /**
     * The actual health check
     *
     * @return
     */
    private boolean check()
    {

        try
        {
            if (cluster == null)
                cluster = createCluster(cassandraHost, cassandraPort);

            if (cluster == null)
                return false;

            if (session == null)
                session = cluster.connect();

            ResultSet rs = session.execute("SELECT release_version FROM system.local");
            return (rs.one() != null);
        }
        catch (Exception e)
        {
            logger.debug("Failed to reach Cassandra.", e);
            session = null;
            cluster = null;
            return false;
        }
    }

    /**
     * Get the check value
     *
     * @return true or false based on whether check was successful
     */
    @Override
    public Boolean get()
    {
        return check();
    }

    /**
     * Creates a cluster object which ensures that the requests go only to the specified C* node
     *
     * @param cassandraHost
     * @param cassandraPort
     * @return
     */
    final private synchronized Cluster createCluster(String cassandraHost, int cassandraPort)
    {
        try
        {
            List<InetSocketAddress> wl = Collections.singletonList(InetSocketAddress.createUnresolved(cassandraHost, cassandraPort));
            cluster = Cluster.builder()
                             .addContactPointsWithPorts(InetSocketAddress.createUnresolved(cassandraHost, cassandraPort))
                             .withoutMetrics()
                             .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(), wl))
                             .build();
        }
        catch (Exception e)
        {
            logger.error("Failed to create Cluster object", e);
        }

        return cluster;
    }
}
