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

package org.apache.cassandra.sidecar.adapters.base.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.management.openmbean.CompositeData;

import static org.apache.cassandra.sidecar.adapters.base.data.CompositeDataUtil.extractValue;

/**
 * Representation of the replica placement of a keyspace, as returned by Cassandra's
 * {@code ReplicaGroupsInfo.replicaGroupsForKeyspaceByDatacenter}.
 * <p>
 * Replicas are identified by node id, which {@link #peers} resolves to a node's address, datacenter and state. The
 * arrays in the response are unordered, so everything is indexed by key rather than position.
 */
public class ReplicaGroups
{
    /** Token ranges and their replicas for reads */
    public final List<RangePlacement> readPlacement;
    /** Token ranges and their replicas for writes, including pending replicas */
    public final List<RangePlacement> writePlacement;
    /** Node id to the peer it identifies, for every node in either placement */
    public final Map<Integer, Peer> peers;

    public ReplicaGroups(CompositeData data)
    {
        CompositeData placement = extractValue(data, "placement");
        this.readPlacement = parsePlacement(extractValue(placement, "read_placement"));
        this.writePlacement = parsePlacement(extractValue(placement, "write_placement"));
        CompositeData[] peerMetadata = extractValue(data, "peer_metadata");
        Map<Integer, Peer> peers = new HashMap<>(peerMetadata.length);
        for (CompositeData peer : peerMetadata)
        {
            Peer parsed = new Peer(peer);
            peers.put(parsed.nodeId, parsed);
        }
        this.peers = Collections.unmodifiableMap(peers);
    }

    private static List<RangePlacement> parsePlacement(CompositeData[] ranges)
    {
        List<RangePlacement> placement = new ArrayList<>(ranges.length);
        for (CompositeData range : ranges)
        {
            placement.add(new RangePlacement(range));
        }
        return Collections.unmodifiableList(placement);
    }

    /**
     * The replicas of one token range, {@code (start, end]}, grouped by datacenter
     */
    public static class RangePlacement
    {
        /** Exclusive start of the range */
        public final String start;
        /** Inclusive end of the range. The range that wraps ends at the partitioner's minimum token. */
        public final String end;
        /** The epoch at which this range's replicas last changed */
        public final long lastModified;
        /** Datacenter to the replicas of the range in that datacenter */
        public final Map<String, List<Replica>> replicasByDatacenter;

        RangePlacement(CompositeData data)
        {
            this.start = extractValue(data, "token_range_start");
            this.end = extractValue(data, "token_range_end");
            this.lastModified = extractValue(data, "last_modified");
            CompositeData[] groups = extractValue(data, "replica_groups_by_datacenter");
            Map<String, List<Replica>> replicasByDatacenter = new HashMap<>(groups.length);
            for (CompositeData group : groups)
            {
                CompositeData[] replicaGroup = extractValue(group, "replica_group");
                List<Replica> replicas = new ArrayList<>(replicaGroup.length);
                for (CompositeData replica : replicaGroup)
                {
                    replicas.add(new Replica(replica));
                }
                replicasByDatacenter.put(extractValue(group, "datacenter"), Collections.unmodifiableList(replicas));
            }
            this.replicasByDatacenter = Collections.unmodifiableMap(replicasByDatacenter);
        }
    }

    /**
     * A replica of a token range
     */
    public static class Replica
    {
        public final int nodeId;
        /** {@code true} for a witness (transient) replica, which does not hold complete data */
        public final boolean isTransient;

        Replica(CompositeData data)
        {
            this.nodeId = extractValue(data, "node_id");
            String replicaType = extractValue(data, "replica_type");
            this.isTransient = "TRANSIENT".equals(replicaType);
        }
    }

    /**
     * A node that appears in the placement
     */
    public static class Peer
    {
        public final int nodeId;
        public final String datacenter;
        public final String rack;
        /** The node's internode address, as {@code host:port} */
        public final String broadcastAddressAndPort;
        /** The node's CQL address, as {@code host:port} */
        public final String nativeAddressAndPort;
        /** The cluster metadata state, e.g. {@code JOINED} or {@code BOOTSTRAPPING} */
        public final String nodeState;
        /** {@code UP} or {@code DOWN}, as seen by this node's failure detector */
        public final String nodeStatus;

        Peer(CompositeData data)
        {
            this.nodeId = extractValue(data, "node_id");
            this.datacenter = extractValue(data, "datacenter");
            this.rack = extractValue(data, "rack");
            this.broadcastAddressAndPort = extractValue(data, "broadcast_address_and_port");
            this.nativeAddressAndPort = extractValue(data, "native_address_and_port");
            this.nodeState = extractValue(data, "node_state");
            this.nodeStatus = extractValue(data, "node_status");
        }
    }
}
