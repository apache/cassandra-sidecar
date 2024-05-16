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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import org.apache.cassandra.sidecar.common.server.utils.DriverUtils;

/**
 * The SidecarLoadBalancingPolicy is designed to ensure that the Cassandra Metadata objects associated with the
 * CqlSessionProvider have enough non-local hosts in their allowed connections to be kept up-to-date
 * even if the local Cassandra instances are down/have their native transport disabled.
 * NOTE: This policy won't work with a child policy that is token-aware
 */
class SidecarLoadBalancingPolicy implements LoadBalancingPolicy
{
    public static final int MIN_NON_LOCAL_CONNECTIONS = 2;
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarLoadBalancingPolicy.class);
    private final Set<Host> selectedHosts = new HashSet<>();
    private final Set<InetSocketAddress> localHostAddresses;
    private final DriverUtils driverUtils;
    private final LoadBalancingPolicy childPolicy;
    private final int totalRequestedConnections;
    private final Random random = new Random();
    private final HashSet<Host> allHosts = new HashSet<>();
    private Cluster cluster;

    public SidecarLoadBalancingPolicy(List<InetSocketAddress> localHostAddresses,
                                      String localDc,
                                      int numAdditionalConnections,
                                      DriverUtils driverUtils)
    {
        this.childPolicy = createChildPolicy(localDc);
        this.localHostAddresses = new HashSet<>(localHostAddresses);
        this.driverUtils = driverUtils;
        if (numAdditionalConnections < MIN_NON_LOCAL_CONNECTIONS)
        {
            LOGGER.warn("Additional instances requested was {}, which is less than the minimum of {}. Using {}.",
                        numAdditionalConnections, MIN_NON_LOCAL_CONNECTIONS, MIN_NON_LOCAL_CONNECTIONS);
            numAdditionalConnections = MIN_NON_LOCAL_CONNECTIONS;
        }
        this.totalRequestedConnections = this.localHostAddresses.size() + numAdditionalConnections;
    }

    @Override
    public void init(Cluster cluster, Collection<Host> hosts)
    {
        this.cluster = cluster;
        this.allHosts.addAll(hosts);
        recalculateSelectedHosts();
        childPolicy.init(cluster, hosts);
    }

    @Override
    public HostDistance distance(Host host)
    {
        if (selectedHosts.contains(host) || isLocalHost(host))
        {
            return childPolicy.distance(host);
        }
        return HostDistance.IGNORED;
    }

    @Override
    public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement)
    {
        Iterator<Host> child = childPolicy.newQueryPlan(loggedKeyspace, statement);
        // Filter the child policy to only selected hosts
        return Iterators.filter(child, selectedHosts::contains);
    }

    @Override
    public synchronized void onAdd(Host host)
    {
        onUp(host);
        childPolicy.onAdd(host);
    }

    @Override
    public synchronized void onUp(Host host)
    {
        this.allHosts.add(host); // replace existing reference if there is one
        if (selectedHosts.size() < totalRequestedConnections)
        {
            recalculateSelectedHosts();
        }
        childPolicy.onUp(host);
    }

    @Override
    public synchronized void onDown(Host host)
    {
        // Don't remove local addresses from the selected host list
        if (localHostAddresses.contains(driverUtils.getSocketAddress(host)))
        {
            LOGGER.debug("Local Node {} has been marked down.", host);
            return;
        }

        boolean wasSelected = selectedHosts.remove(host);
        if (!wasSelected)
        {
            // Non-selected nodes have been marked with HostDistance.IGNORED
            // even if they may otherwise be useful. This has a side effect
            // of preventing the driver from trying to reconnect to them
            // if we miss the `onUp` event, so we need to schedule reconnects
            // for these hosts explicitly unless we have active connections.
            driverUtils.startPeriodicReconnectionAttempt(cluster, host);
        }
        recalculateSelectedHosts();
        childPolicy.onDown(host);
    }

    @Override
    public synchronized void onRemove(Host host)
    {
        this.allHosts.remove(host);
        onDown(host);
        childPolicy.onRemove(host);
    }

    @Override
    public void close()
    {
        childPolicy.close();
    }

    /**
     * Creates the child policy based on the presence of a local datacenter
     *
     * @param localDc the local datacenter to use, or null
     * @return a {@link LoadBalancingPolicy}
     */
    private LoadBalancingPolicy createChildPolicy(String localDc)
    {
        if (localDc != null)
        {
            return DCAwareRoundRobinPolicy.builder().withLocalDc(localDc).build();
        }
        return new RoundRobinPolicy();
    }

    private synchronized void recalculateSelectedHosts()
    {
        Map<Boolean, List<Host>> partitionedHosts = allHosts.stream()
                                                            .collect(Collectors.partitioningBy(this::isLocalHost));
        List<Host> localHosts = partitionedHosts.get(true);
        int numLocalHostsConfigured = localHostAddresses.size();
        if (localHosts == null || localHosts.isEmpty())
        {
            LOGGER.warn("Did not find any local hosts in allHosts.");
        }
        else
        {
            if (localHosts.size() < numLocalHostsConfigured)
            {
                LOGGER.warn("Could not find all configured local hosts in host list. ConfiguredHosts={} AvailableHosts={}",
                            numLocalHostsConfigured, localHosts.size());
            }
            selectedHosts.addAll(localHosts);
        }
        int requiredNonLocalHosts = this.totalRequestedConnections - selectedHosts.size();
        if (requiredNonLocalHosts > 0)
        {
            List<Host> nonLocalHosts = partitionedHosts.get(false);
            if (nonLocalHosts == null || nonLocalHosts.isEmpty())
            {
                LOGGER.debug("Did not find any non-local hosts in allHosts");
                return;
            }

            // Remove down and already selected hosts from consideration
            nonLocalHosts = nonLocalHosts.stream()
                                         .filter(h -> !selectedHosts.contains(h) && h.isUp())
                                         .collect(Collectors.toList());

            if (nonLocalHosts.size() < requiredNonLocalHosts)
            {
                LOGGER.warn("Could not find enough new, up non-local hosts to meet requested number {}",
                            requiredNonLocalHosts);
            }
            else
            {
                LOGGER.debug("Found enough new, up, non-local hosts to meet requested number {}",
                             requiredNonLocalHosts);
            }
            if (nonLocalHosts.size() > requiredNonLocalHosts)
            {
                Collections.shuffle(nonLocalHosts, this.random);
            }
            int hostsToAdd = Math.min(requiredNonLocalHosts, nonLocalHosts.size());
            for (int i = 0; i < hostsToAdd; i++)
            {
                selectedHosts.add(nonLocalHosts.get(i));
            }
        }
    }

    private boolean isLocalHost(Host host)
    {
        return localHostAddresses.contains(driverUtils.getSocketAddress(host));
    }
}
