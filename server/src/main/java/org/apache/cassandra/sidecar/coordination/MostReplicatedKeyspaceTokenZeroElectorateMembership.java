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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.utils.StringUtils;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * An implementation of {@link ElectorateMembership} where the current Sidecar will
 * be determined to be part of the electorate iff one of the Cassandra instances it
 * manages owns token {@code 0} for the user keyspace that has the highest replication
 * factor. If multiple keyspaces have the highest replication factor, the keyspace
 * to be used is decided by the keyspace with the name that sorts first in the
 * lexicographic sort order. If no user keyspaces are created, the internal sidecar
 * keyspace will be used.
 */
public class MostReplicatedKeyspaceTokenZeroElectorateMembership implements ElectorateMembership
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MostReplicatedKeyspaceTokenZeroElectorateMembership.class);
    private final InstanceMetadataFetcher instanceMetadataFetcher;
    private final CQLSessionProvider cqlSessionProvider;
    private final SidecarConfiguration configuration;

    public MostReplicatedKeyspaceTokenZeroElectorateMembership(InstanceMetadataFetcher instanceMetadataFetcher,
                                                               CQLSessionProvider cqlSessionProvider,
                                                               SidecarConfiguration sidecarConfiguration)
    {
        this.instanceMetadataFetcher = instanceMetadataFetcher;
        this.cqlSessionProvider = cqlSessionProvider;
        this.configuration = sidecarConfiguration;
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

        String userKeyspace = highestReplicationFactorKeyspace();
        if (userKeyspace == null)
        {
            // pre-checks failed
            return false;
        }

        TokenRangeReplicasResponse tokenRangeReplicas = instanceMetadataFetcher.callOnFirstAvailableInstance(instance -> {
            CassandraAdapterDelegate delegate = instance.delegate();
            StorageOperations operations = delegate.storageOperations();
            NodeSettings nodeSettings = delegate.nodeSettings();
            return operations.tokenRangeReplicas(new Name(userKeyspace), nodeSettings.partitioner());
        });

        return anyInstanceOwnsTokenZero(tokenRangeReplicas, localInstancesHostsAndPorts);
    }

    Set<String> collectLocalInstancesHostsAndPorts()
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
     * Performs pre-checks ensuring local instances are configured; an active session to the database is present;
     * and returns the keyspace with the highest replication factor. If multiple keyspaces have the highest
     * replication factor, the keyspace to be used is decided by the keyspace with the name that sorts first in
     * the lexicographic sort order. Defaults to the sidecar keyspace when there are no user keyspaces.
     *
     * @return user keyspace
     */
    String highestReplicationFactorKeyspace()
    {
        if (instanceMetadataFetcher.allLocalInstances().isEmpty())
        {
            LOGGER.warn("There are no local Cassandra instances managed by this Sidecar");
            return null;
        }

        Session activeSession;
        try
        {
            activeSession = cqlSessionProvider.get();
        }
        catch (CassandraUnavailableException exception)
        {
            LOGGER.warn("There is no active session to Cassandra");
            return null;
        }

        Set<String> forbiddenKeyspaces = configuration.cassandraInputValidationConfiguration().forbiddenKeyspaces();
        String sidecarKeyspaceName = configuration.serviceConfiguration().schemaKeyspaceConfiguration().keyspace();

        return activeSession.getCluster().getMetadata().getKeyspaces().stream()
                            .filter(keyspace -> !forbiddenKeyspaces.contains(keyspace.getName()))
                            // Sort by the keyspace with the highest replication factor
                            // and then sort by the keyspace name to guarantee in the
                            // sorting order across all Sidecar instances
                            .sorted(Comparator.comparingInt(this::aggregateReplicationFactor)
                                              .reversed()
                                              .thenComparing(KeyspaceMetadata::getName))
                            .map(KeyspaceMetadata::getName)
                            .findFirst()
                            .orElse(sidecarKeyspaceName);
    }

    /**
     * @param tokenRangeReplicas         the token range replicas for a keyspace
     * @param localInstancesHostAndPorts local instance(s) IP(s) and port(s)
     * @return {@code true} if any of the local instances is a replica of token zero for a single keyspace,
     * {@code false} otherwise
     */
    boolean anyInstanceOwnsTokenZero(TokenRangeReplicasResponse tokenRangeReplicas, Set<String> localInstancesHostAndPorts)
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
    boolean replicaOwnsTokenZero(TokenRangeReplicasResponse.ReplicaInfo replicaInfo)
    {
        BigInteger start = new BigInteger(replicaInfo.start());
        BigInteger end = new BigInteger(replicaInfo.end());
        // start is exclusive; end is inclusive
        return start.compareTo(BigInteger.ZERO) < 0 && end.compareTo(BigInteger.ZERO) >= 0;
    }

    /**
     * @param keyspace the keyspace
     * @return the aggregate replication factor for the {@link KeyspaceMetadata keyspace}
     */
    int aggregateReplicationFactor(KeyspaceMetadata keyspace)
    {
        int replicationFactor = 0;
        for (String value : keyspace.getReplication().values())
        {
            try
            {
                replicationFactor += Integer.parseInt(value);
            }
            catch (NumberFormatException ignored)
            {
                // skips the class property of the replication factor
            }
        }
        return replicationFactor;
    }
}
