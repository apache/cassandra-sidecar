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

package org.apache.cassandra.sidecar.job.restart;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.response.data.RingEntry;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Determines how widely a node's replicas are spread in a datacenter, so an operation can determine whether
 * rack-based safety reasoning applies or a more conservative scope is required. See {@link ReplicaScope}.
 */
@Singleton
public class ReplicationStrategyAnalyzer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationStrategyAnalyzer.class);
    private static final String NETWORK_TOPOLOGY_STRATEGY = "NetworkTopologyStrategy";
    private static final String LOCAL_STRATEGY = "LocalStrategy";
    private static final String REPLICATION_STRATEGY_CLASS_KEY = "class";

    private final InstanceMetadataFetcher instanceMetadataFetcher;

    @Inject
    public ReplicationStrategyAnalyzer(InstanceMetadataFetcher instanceMetadataFetcher)
    {
        this.instanceMetadataFetcher = instanceMetadataFetcher;
    }

    /**
     * Classifies the replica scope for {@code datacenter}. Returns {@link ReplicaScope#CLUSTER} (the most
     * conservative scope) if the replication strategy cannot be determined, so an unknown strategy never
     * allows taking a node down unsafely. LocalStrategy and virtual keyspaces are ignored as their data
     * is not replicated.
     *
     * @param ring       the cluster ring
     * @param datacenter the datacenter being operated on
     * @return the replica scope for the datacenter
     */
    public ReplicaScope replicaScopeForDatacenter(RingResponse ring, String datacenter)
    {
        int numRacks = rackCount(ring, datacenter);
        try
        {
            Metadata metadata = instanceMetadataFetcher.callOnFirstAvailableInstance(
                instance -> instance.delegate().metadata());
            List<Map<String, String>> replications = new ArrayList<>();
            for (KeyspaceMetadata keyspace : metadata.getKeyspaces())
            {
                if (keyspace.isVirtual())
                {
                    continue;
                }
                Map<String, String> replication = keyspace.getReplication();
                if (!LOCAL_STRATEGY.equals(strategyName(replication.get(REPLICATION_STRATEGY_CLASS_KEY))))
                {
                    replications.add(replication);
                }
            }
            return classify(numRacks, datacenter, replications);
        }
        catch (Exception e)
        {
            LOGGER.warn("Could not determine the replica scope for datacenter {}; assuming the most conservative {} scope",
                        datacenter, ReplicaScope.CLUSTER, e);
            return ReplicaScope.CLUSTER;
        }
    }

    private static int rackCount(RingResponse ring, String datacenter)
    {
        Set<String> racks = new HashSet<>();
        for (RingEntry entry : ring)
        {
            if (datacenter.equals(entry.datacenter()))
            {
                racks.add(entry.rack());
            }
        }
        return racks.size();
    }

    /**
     * Classifies the replica scope.
     *
     * @param numRacks     number of racks in the datacenter
     * @param datacenter   the datacenter being operated on
     * @param replications replication strategies (as returned by {@code KeyspaceMetadata.getReplication()}) of every
     *                     non-system keyspace
     * @return the replica scope
     */
    static ReplicaScope classify(int numRacks, String datacenter, List<Map<String, String>> replications)
    {
        int maxNtsReplicationFactor = 0;
        for (Map<String, String> replication : replications)
        {
            String strategy = strategyName(replication.get(REPLICATION_STRATEGY_CLASS_KEY));
            if (!NETWORK_TOPOLOGY_STRATEGY.equals(strategy))
            {
                return ReplicaScope.CLUSTER;
            }
            maxNtsReplicationFactor = Math.max(maxNtsReplicationFactor, replicationFactor(replication.get(datacenter)));
        }
        return maxNtsReplicationFactor > numRacks ? ReplicaScope.DATACENTER : ReplicaScope.RACK;
    }

    private static String strategyName(String strategyClass)
    {
        if (strategyClass == null)
        {
            return "";
        }
        int lastDot = strategyClass.lastIndexOf('.');
        return lastDot < 0 ? strategyClass : strategyClass.substring(lastDot + 1);
    }

    private static int replicationFactor(String value)
    {
        // A null value means the keyspace is not replicated in this datacenter and can be ignored
        return value == null ? 0 : Integer.parseInt(value);
    }
}
