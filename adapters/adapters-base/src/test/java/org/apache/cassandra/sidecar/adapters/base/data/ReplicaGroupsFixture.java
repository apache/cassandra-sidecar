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

import java.util.HashMap;
import java.util.Map;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

/**
 * Builds {@link CompositeData} in the shape Cassandra's {@code ReplicaGroupsInfo} MBean returns, for tests
 */
public final class ReplicaGroupsFixture
{
    private static final CompositeType REPLICA;
    private static final CompositeType DATACENTER_REPLICA_GROUP;
    private static final CompositeType PLACEMENT_BY_DATACENTER;
    private static final CompositeType PEER;
    private static final CompositeType DATA_PLACEMENT;
    private static final CompositeType REPLICA_GROUPS_BY_DATACENTER;

    static
    {
        try
        {
            REPLICA = type("Replica",
                           "node_id", SimpleType.INTEGER,
                           "replica_type", SimpleType.STRING);
            DATACENTER_REPLICA_GROUP = type("DatacenterReplicaGroup",
                                            "datacenter", SimpleType.STRING,
                                            "replica_group", ArrayType.getArrayType(REPLICA));
            PLACEMENT_BY_DATACENTER = type("PlacementByDatacenter",
                                           "token_range_start", SimpleType.STRING,
                                           "token_range_end", SimpleType.STRING,
                                           "last_modified", SimpleType.LONG,
                                           "replica_groups_by_datacenter", ArrayType.getArrayType(DATACENTER_REPLICA_GROUP));
            PEER = type("Peer",
                        "node_id", SimpleType.INTEGER,
                        "datacenter", SimpleType.STRING,
                        "rack", SimpleType.STRING,
                        "broadcast_address_and_port", SimpleType.STRING,
                        "native_address_and_port", SimpleType.STRING,
                        "node_state", SimpleType.STRING,
                        "node_status", SimpleType.STRING);
            DATA_PLACEMENT = type("DataPlacement",
                                  "replication_scope", SimpleType.STRING,
                                  "token_type", SimpleType.STRING,
                                  "read_placement", ArrayType.getArrayType(PLACEMENT_BY_DATACENTER),
                                  "write_placement", ArrayType.getArrayType(PLACEMENT_BY_DATACENTER));
            REPLICA_GROUPS_BY_DATACENTER = type("ReplicaGroupsByDatacenter",
                                                "placement", DATA_PLACEMENT,
                                                "peer_metadata", ArrayType.getArrayType(PEER));
        }
        catch (OpenDataException e)
        {
            throw new IllegalStateException(e);
        }
    }

    private ReplicaGroupsFixture()
    {
    }

    public static CompositeData replica(int nodeId, String replicaType)
    {
        return data(REPLICA, "node_id", nodeId, "replica_type", replicaType);
    }

    public static CompositeData datacenter(String datacenter, CompositeData... replicas)
    {
        return data(DATACENTER_REPLICA_GROUP, "datacenter", datacenter, "replica_group", replicas);
    }

    public static CompositeData range(String start, String end, CompositeData... datacenters)
    {
        return data(PLACEMENT_BY_DATACENTER,
                    "token_range_start", start,
                    "token_range_end", end,
                    "last_modified", 1L,
                    "replica_groups_by_datacenter", datacenters);
    }

    public static CompositeData peer(int nodeId, String datacenter, String address, String nodeState, String nodeStatus)
    {
        return data(PEER,
                    "node_id", nodeId,
                    "datacenter", datacenter,
                    "rack", "rack1",
                    "broadcast_address_and_port", address,
                    "native_address_and_port", address.replaceFirst(":\\d+$", ":9042"),
                    "node_state", nodeState,
                    "node_status", nodeStatus);
    }

    public static CompositeData[] ranges(CompositeData... ranges)
    {
        return ranges;
    }

    public static CompositeData replicaGroups(CompositeData[] readPlacement,
                                              CompositeData[] writePlacement,
                                              CompositeData... peers)
    {
        CompositeData placement = data(DATA_PLACEMENT,
                                       "replication_scope", "base(ReplicationParams{class=NetworkTopologyStrategy, dc1=3/1})",
                                       "token_type", "org.apache.cassandra.dht.Murmur3Partitioner$LongToken",
                                       "read_placement", readPlacement,
                                       "write_placement", writePlacement);
        return data(REPLICA_GROUPS_BY_DATACENTER, "placement", placement, "peer_metadata", peers);
    }

    // namesAndTypes alternates item name and OpenType
    private static CompositeType type(String name, Object... namesAndTypes) throws OpenDataException
    {
        String[] names = new String[namesAndTypes.length / 2];
        OpenType<?>[] types = new OpenType<?>[namesAndTypes.length / 2];
        for (int i = 0; i < names.length; i++)
        {
            names[i] = (String) namesAndTypes[2 * i];
            types[i] = (OpenType<?>) namesAndTypes[2 * i + 1];
        }
        return new CompositeType(name, name, names, names, types);
    }

    // namesAndValues alternates item name and value
    private static CompositeData data(CompositeType type, Object... namesAndValues)
    {
        Map<String, Object> items = new HashMap<>();
        for (int i = 0; i < namesAndValues.length; i += 2)
        {
            items.put((String) namesAndValues[i], namesAndValues[i + 1]);
        }
        try
        {
            return new CompositeDataSupport(type, items);
        }
        catch (OpenDataException e)
        {
            throw new IllegalArgumentException(e);
        }
    }
}
