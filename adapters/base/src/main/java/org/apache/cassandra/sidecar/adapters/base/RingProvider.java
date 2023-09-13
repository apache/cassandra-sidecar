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

package org.apache.cassandra.sidecar.adapters.base;

import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.adapters.base.NodeInfo.NodeState;
import org.apache.cassandra.sidecar.adapters.base.NodeInfo.NodeStatus;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.data.RingEntry;
import org.apache.cassandra.sidecar.common.data.RingResponse;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.adapters.base.EndpointSnitchJmxOperations.ENDPOINT_SNITCH_INFO_OBJ_NAME;
import static org.apache.cassandra.sidecar.adapters.base.StorageJmxOperations.STORAGE_SERVICE_OBJ_NAME;

/**
 * Aggregates the ring view of cluster
 */
public class RingProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RingProvider.class);
    private static final String UNKNOWN_SHORT = "?";
    private static final String UNKNOWN = "Unknown";
    private static final String DECIMAL_FORMAT = "##0.00%";

    protected final JmxClient jmxClient;
    private final DnsResolver dnsResolver;

    public RingProvider(JmxClient jmxClient, DnsResolver dnsResolver)
    {
        this.jmxClient = jmxClient;
        this.dnsResolver = dnsResolver;
    }

    @SuppressWarnings("UnstableApiUsage")
    public RingResponse ring(@Nullable String keyspace) throws UnknownHostException
    {
        StorageJmxOperations storageOps = initializeStorageOps();
        EndpointSnitchJmxOperations epSnitchInfo = initializeEndpointProxy();

        // Collect required data from the probe
        List<String> liveNodes = storageOps.getLiveNodesWithPort();
        List<String> deadNodes = storageOps.getUnreachableNodesWithPort();
        Status status = new Status(liveNodes, deadNodes);
        List<String> joiningNodes = storageOps.getJoiningNodesWithPort();
        List<String> leavingNodes = storageOps.getLeavingNodesWithPort();
        List<String> movingNodes = storageOps.getMovingNodesWithPort();
        State state = new State(joiningNodes, leavingNodes, movingNodes);
        Map<String, String> loadMap = storageOps.getLoadMapWithPort();
        Map<String, String> tokensToEndpoints = storageOps.getTokenToEndpointWithPortMap();
        Map<String, String> endpointsToHostIds = storageOps.getEndpointWithPortToHostId();

        boolean showEffectiveOwnership = true;
        // Calculate per-token ownership of the ring
        Map<String, Float> ownerships;
        try
        {
            ownerships = storageOps.effectiveOwnershipWithPort(keyspace);
        }
        catch (IllegalStateException ex)
        {
            ownerships = storageOps.getOwnershipWithPort();
            LOGGER.warn("Unable to retrieve effective ownership information for keyspace={}", keyspace, ex);
            showEffectiveOwnership = false;
        }

        // DecimalFormat is not thread-safe, so we need to create a new instance per thread
        DecimalFormat ownsFormat = new DecimalFormat(DECIMAL_FORMAT);
        RingResponse response = new RingResponse(tokensToEndpoints.size());
        for (Map.Entry<String, String> entry : tokensToEndpoints.entrySet())
        {
            String endpoint = entry.getValue();
            String token = entry.getKey();
            HostAndPort hap = resolve(endpoint, dnsResolver);
            Float owns = ownerships.get(endpoint);
            RingEntry ringEntry = new RingEntry.Builder()
                                  .datacenter(epSnitchInfo.getDatacenter(endpoint))
                                  .rack(queryRack(epSnitchInfo, endpoint))
                                  .status(status.of(endpoint))
                                  .state(state.of(endpoint))
                                  .load(loadMap.getOrDefault(endpoint, UNKNOWN_SHORT))
                                  .owns(formatOwns(showEffectiveOwnership, ownsFormat, owns))
                                  .token(token)
                                  .address(hap.getHost())
                                  .port(hap.getPort())
                                  .fqdn(dnsResolver.reverseResolve(hap.getHost()))
                                  .hostId(endpointsToHostIds.getOrDefault(endpoint, UNKNOWN))
                                  .build();
            response.add(ringEntry);
        }

        return response;
    }

    protected EndpointSnitchJmxOperations initializeEndpointProxy()
    {
        return jmxClient.proxy(EndpointSnitchJmxOperations.class, ENDPOINT_SNITCH_INFO_OBJ_NAME);
    }

    protected StorageJmxOperations initializeStorageOps()
    {
        return jmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME);
    }

    /**
     * Resolves the endpoint to the format "IP_ADDRESS:PORT"
     *
     * @return host and port. Port defaults to -1 if not included in endpoint.
     * @throws UnknownHostException when endpoint cannot be resolved
     */
    @SuppressWarnings("UnstableApiUsage")
    private static HostAndPort resolve(String endpoint, DnsResolver resolver) throws UnknownHostException
    {
        HostAndPort hap = HostAndPort.fromString(endpoint);
        String address = resolver.resolve(hap.getHost());
        return HostAndPort.fromParts(address, hap.getPortOrDefault(-1));
    }

    private static String formatOwns(boolean showEffectiveOwnership, DecimalFormat ownsFormat, Float owns)
    {
        if (showEffectiveOwnership && owns != null)
            return ownsFormat.format(owns);
        return UNKNOWN_SHORT;
    }

    /**
     * Data class to get status of endpoints
     */
    static class Status
    {
        private final Set<String> liveNodes;
        private final Set<String> deadNodes;

        Status(List<String> liveNodes, List<String> deadNodes)
        {
            this.liveNodes = new HashSet<>(liveNodes);
            this.deadNodes = new HashSet<>(deadNodes);
        }

        String of(String endpoint)
        {
            if (liveNodes.contains(endpoint))
                return NodeStatus.UP.displayName();
            if (deadNodes.contains(endpoint))
                return NodeStatus.DOWN.displayName();
            return UNKNOWN_SHORT;
        }
    }

    /**
     * Data class to get states of endpoints
     */
    static class State
    {
        protected final Set<String> joiningNodes;
        private final Set<String> leavingNodes;
        private final Set<String> movingNodes;

        State(List<String> joiningNodes, List<String> leavingNodes, List<String> movingNodes)
        {
            this.joiningNodes = new HashSet<>(joiningNodes);
            this.leavingNodes = new HashSet<>(leavingNodes);
            this.movingNodes = new HashSet<>(movingNodes);
        }

        String of(String endpoint)
        {
            if (joiningNodes.contains(endpoint))
                return NodeState.JOINING.displayName();
            else if (leavingNodes.contains(endpoint))
                return NodeState.LEAVING.displayName();
            else if (movingNodes.contains(endpoint))
                return NodeState.MOVING.displayName();
            return NodeState.NORMAL.displayName();
        }
    }

    private static String queryRack(EndpointSnitchJmxOperations epSnitchInfo, String endpoint)
    {
        try
        {
            return epSnitchInfo.getRack(endpoint);
        }
        catch (UnknownHostException e)
        {
            return UNKNOWN;
        }
    }
}
