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

package org.apache.cassandra.sidecar.coordination;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.utils.StringUtils;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Provides common functionality for {@link ElectorateMembership} implementations
 * that rely on token zero replication of a keyspace to determine eligibility.
 */
public abstract class AbstractTokenZeroOfKeyspaceElectorateMembership implements ElectorateMembership
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTokenZeroOfKeyspaceElectorateMembership.class);
    protected final InstanceMetadataFetcher instanceMetadataFetcher;

    public AbstractTokenZeroOfKeyspaceElectorateMembership(InstanceMetadataFetcher instanceMetadataFetcher)
    {
        this.instanceMetadataFetcher = instanceMetadataFetcher;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isMember()
    {
        Set<String> localInstancesHostsAndPorts = collectLocalInstancesHostsAndPorts();
        if (localInstancesHostsAndPorts.isEmpty())
        {
            // Unable to retrieve local instances, maybe all Cassandra connections are down?
            return false;
        }

        String keyspace = keyspaceToDetermineElectorateMembership();
        if (keyspace == null)
        {
            // pre-checks failed
            return false;
        }
        LOGGER.debug("Using keyspace={} to determine electorate membership", keyspace);

        TokenRangeReplicasResponse tokenRangeReplicas = instanceMetadataFetcher.callOnFirstAvailableInstance(instance -> {
            CassandraAdapterDelegate delegate = instance.delegate();
            StorageOperations operations = delegate.storageOperations();
            NodeSettings nodeSettings = delegate.nodeSettings();
            return operations.tokenRangeReplicas(new Name(keyspace), nodeSettings.partitioner());
        });

        return anyInstanceOwnsTokenZero(tokenRangeReplicas, localInstancesHostsAndPorts);
    }

    /**
     * @return the name of the keyspace that will be used to determine the electorate membership
     */
    protected abstract String keyspaceToDetermineElectorateMembership();

    protected Set<String> collectLocalInstancesHostsAndPorts()
    {
        Set<String> result = new HashSet<>();
        for (InstanceMetadata instance : instanceMetadataFetcher.allLocalInstances())
        {
            try
            {
                InetSocketAddress address = instance.delegate().localStorageBroadcastAddress();
                result.add(StringUtils.cassandraFormattedHostAndPort(address));
            }
            catch (CassandraUnavailableException exception)
            {
                // Log a warning message and continue
                LOGGER.warn("Unable to determine local storage broadcast address for instance. instance={}", instance, exception);
            }
        }
        return result;
    }

    /**
     * @param tokenRangeReplicas         the token range replicas for a keyspace
     * @param localInstancesHostAndPorts local instance(s) IP(s) and port(s)
     * @return {@code true} if any of the local instances is a replica of token zero for a single keyspace,
     * {@code false} otherwise
     */
    protected boolean anyInstanceOwnsTokenZero(TokenRangeReplicasResponse tokenRangeReplicas, Set<String> localInstancesHostAndPorts)
    {
        return tokenRangeReplicas.readReplicas()
                                 .stream()
                                 // only returns replicas that contain token zero
                                 .filter(this::replicaOwnsTokenZero)
                                 // and then see if any of the replicas matches the
                                 // local instance's host and port
                                 .anyMatch(replicaInfo -> {
                                     for (List<String> replicas : replicaInfo.replicasByDatacenter().values())
                                     {
                                         for (String replica : replicas)
                                         {
                                             if (localInstancesHostAndPorts.contains(replica))
                                             {
                                                 return true;
                                             }
                                         }
                                     }
                                     return false;
                                 });
    }

    /**
     * @param replicaInfo the replica info
     * @return {@code true} if the replica info owns token zero, {@code false} otherwise
     */
    protected boolean replicaOwnsTokenZero(TokenRangeReplicasResponse.ReplicaInfo replicaInfo)
    {
        BigInteger start = new BigInteger(replicaInfo.start());
        BigInteger end = new BigInteger(replicaInfo.end());
        // start is exclusive; end is inclusive
        return start.compareTo(BigInteger.ZERO) < 0 && end.compareTo(BigInteger.ZERO) >= 0;
    }
}
