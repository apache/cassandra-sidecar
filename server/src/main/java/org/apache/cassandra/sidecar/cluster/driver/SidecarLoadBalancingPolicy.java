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

package org.apache.cassandra.sidecar.cluster.driver;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import org.jetbrains.annotations.NotNull;

/**
 * The SidecarLoadBalancingPolicy is designed to ensure that the Cassandra Metadata objects associated with the
 * CqlSessionProvider have enough non-local hosts in their allowed connections to be kept up-to-date
 * even if the local Cassandra instances are down/have their native transport disabled.
 */
public class SidecarLoadBalancingPolicy extends DefaultLoadBalancingPolicy
{
    public static final int MIN_NON_LOCAL_CONNECTIONS = 2;
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarLoadBalancingPolicy.class);
    private final SecureRandom random = new SecureRandom();

    private final Set<InetSocketAddress> localHostAddresses;
    private final HashSet<Node> allHosts = new HashSet<>();
    private final int totalRequestedConnections;
    private final Set<Node> selectedHosts = new HashSet<>();

    public SidecarLoadBalancingPolicy(DriverContext context, String profileName)
    {
        super(context, profileName);
        List<InetSocketAddress> localInstances = context.getConfig().getDefaultProfile().getStringList(CustomDriverOption.LOCAL_INSTANCES)
                                                        .stream()
                                                        .map(i -> {
                                                            String[] hostPort = i.split(":");
                                                            return new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1]));
                                                        })
                                                        .collect(Collectors.toList());
        int numAdditionalConnections = context.getConfig().getDefaultProfile().getInt(CustomDriverOption.NUM_CONNECTIONS);
        this.localHostAddresses = new HashSet<>(localInstances);
        if (numAdditionalConnections < MIN_NON_LOCAL_CONNECTIONS)
        {
            LOGGER.warn("Additional instances requested was {}, which is less than the minimum of {}. Using {}.",
                        numAdditionalConnections, MIN_NON_LOCAL_CONNECTIONS, MIN_NON_LOCAL_CONNECTIONS);
            numAdditionalConnections = MIN_NON_LOCAL_CONNECTIONS;
        }
        this.totalRequestedConnections = this.localHostAddresses.size() + numAdditionalConnections;
    }

    @Override
    public void init(Map<UUID, Node> nodes, @NotNull DistanceReporter distanceReporter)
    {
        this.allHosts.addAll(nodes.values());
        recalculateSelectedHosts();
        super.init(nodes, distanceReporter);
    }

    @Override
    protected NodeDistance computeNodeDistance(@NotNull Node node)
    {
        if (selectedHosts.contains(node) || isLocalHost(node))
        {
            return super.computeNodeDistance(node);
        }
        return NodeDistance.IGNORED;
    }

    @Override
    public @NotNull Queue<Node> newQueryPlan(Request request, Session session)
    {
        Queue<Node> plan = super.newQueryPlan(request, session);
        plan.removeIf(n -> !selectedHosts.contains(n));
        return plan;
    }

    @Override
    public synchronized void onUp(@NotNull Node node)
    {
        this.allHosts.add(node); // replace existing reference if there is one
        if (selectedHosts.size() < totalRequestedConnections)
        {
            recalculateSelectedHosts();
        }
        super.onUp(node);
    }

    @Override
    public synchronized void onDown(@NotNull Node node)
    {
        // Don't remove local addresses from the selected host list
        if (localHostAddresses.contains(resolveEndpoint(node)))
        {
            LOGGER.debug("Local Node {} has been marked down.", node);
            return;
        }
        selectedHosts.remove(node);
        recalculateSelectedHosts();
        super.onDown(node);
    }

    @Override
    public void onRemove(@NotNull Node node)
    {
        this.allHosts.remove(node);
        onDown(node);
        super.onRemove(node);
    }

    private synchronized void recalculateSelectedHosts()
    {
        Map<Boolean, List<Node>> partitionedHosts = allHosts.stream()
                                                            .collect(Collectors.partitioningBy(this::isLocalHost));
        List<Node> localHosts = partitionedHosts.get(true);
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
            List<Node> nonLocalHosts = partitionedHosts.get(false);
            if (nonLocalHosts == null || nonLocalHosts.isEmpty())
            {
                LOGGER.debug("Did not find any non-local hosts in allHosts");
                return;
            }

            // Remove down and already selected hosts from consideration
            nonLocalHosts = nonLocalHosts.stream()
                                         .filter(h -> !selectedHosts.contains(h)
                                                      && (NodeState.UP.equals(h.getState())
                                                          || NodeState.UNKNOWN.equals(h.getState())))
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

    private boolean isLocalHost(Node host)
    {
        return localHostAddresses.contains(resolveEndpoint(host));
    }

    private static InetSocketAddress resolveEndpoint(Node node)
    {
        SocketAddress socketAddress = node.getEndPoint().resolve();
        Preconditions.checkState(socketAddress instanceof InetSocketAddress, "Unsupported endpoint type: " + node.getEndPoint());
        return (InetSocketAddress) socketAddress;
    }
}
