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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.utils.StringUtils;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

/**
 * An implementation of {@link ElectorateMembership} where the current Sidecar will
 * be determined to be part of the electorate iff one of the Cassandra instances it
 * manages owns token {@code 0} for user keyspaces.
 */
public class TokenZeroElectorateMembership implements ElectorateMembership
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TokenZeroElectorateMembership.class);
    private final InstancesConfig instancesConfig;
    private final CQLSessionProvider cqlSessionProvider;
    private final SidecarConfiguration configuration;

    public TokenZeroElectorateMembership(InstancesConfig instancesConfig,
                                         CQLSessionProvider cqlSessionProvider,
                                         SidecarConfiguration sidecarConfiguration)
    {
        this.instancesConfig = instancesConfig;
        this.cqlSessionProvider = cqlSessionProvider;
        this.configuration = sidecarConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isMember()
    {
        List<String> userKeyspaces = maybeCollectUserKeyspaces();
        if (userKeyspaces.isEmpty())
        {
            return false;
        }

        for (String userKeyspace : userKeyspaces)
        {
            TokenRangeReplicasResponse tokenRangeReplicas = null;
            for (InstanceMetadata instance : instancesConfig.instances())
            {
                CassandraAdapterDelegate delegate = instance.delegate();
                if (delegate == null)
                {
                    LOGGER.debug("Delegate is unavailable for instance={}", instance);
                    continue;
                }

                StorageOperations operations = delegate.storageOperations();
                NodeSettings nodeSettings = delegate.nodeSettings();
                if (operations == null || nodeSettings == null)
                {
                    LOGGER.debug("Storage Operations / Node Settings are unavailable for instance={}", instance);
                    continue;
                }

                InetSocketAddress address = delegate.localStorageBroadcastAddress();
                if (address == null)
                {
                    LOGGER.warn("Unable to determine local storage broadcast address for instance={}", instance);
                    continue;
                }

                String localInstanceHostAndPort = StringUtils.cassandraFormattedHostAndPort(address);
                if (tokenRangeReplicas == null)
                {
                    // Token range replicas should be the same across all instances assuming the view of the ring
                    // is the same for all instances, so we just get it once, as this could be an expensive call
                    tokenRangeReplicas = operations.tokenRangeReplicas(new Name(userKeyspace), nodeSettings.partitioner());
                }

                if (instanceOwnsTokenZero(localInstanceHostAndPort, tokenRangeReplicas))
                {
                    // if any of the keyspaces owns token zero we add the instance, there's
                    // no need to check the other keyspaces as this instance is eligible
                    // return early
                    return true;
                }
            }
        }

        // This local Sidecar does not manage any Cassandra instances that owns token 0 for user keyspaces
        return false;
    }

    /**
     * Performs pre-checks ensuring local instances are configured; an active session to the database is present;
     * and collects the user keyspaces available in Cassandra.
     *
     * @return the list of user keyspaces if available, or an empty list if it is unable to retrieve the user keyspaces
     */
    List<String> maybeCollectUserKeyspaces()
    {
        if (instancesConfig.instances().isEmpty())
        {
            LOGGER.warn("There are no local Cassandra instances managed by this Sidecar");
            return Collections.emptyList();
        }

        Session activeSession = cqlSessionProvider.get();
        if (activeSession == null)
        {
            LOGGER.warn("There is no active session to Cassandra");
            return Collections.emptyList();
        }

        List<String> userKeyspaces = new ArrayList<>();
        Set<String> forbiddenKeyspaces = configuration.cassandraInputValidationConfiguration().forbiddenKeyspaces();
        for (KeyspaceMetadata keyspace : activeSession.getCluster().getMetadata().getKeyspaces())
        {
            String keyspaceName = keyspace.getName();
            if (!forbiddenKeyspaces.contains(keyspaceName))
            {
                userKeyspaces.add(keyspaceName);
            }
        }

        if (userKeyspaces.isEmpty())
        {
            LOGGER.warn("No user keyspaces found");
        }

        return userKeyspaces;
    }

    /**
     * @param localInstanceHostAndPort local instance IP and port
     * @param tokenRangeReplicas       the token range replicas for a keyspace
     * @return {@code true} if the local instance is a replica of token zero for a single keyspace, {@code false}
     * otherwise
     */
    boolean instanceOwnsTokenZero(String localInstanceHostAndPort, TokenRangeReplicasResponse tokenRangeReplicas)
    {
        return tokenRangeReplicas.readReplicas()
                                 .stream()
                                 // only returns replicas that contain token zero
                                 .filter(this::containsTokenZero)
                                 // and then see if any of the replicas matches the
                                 // local instance's host and port
                                 .anyMatch(replicaInfo -> {
                                     for (List<String> replicas : replicaInfo.replicasByDatacenter().values())
                                     {
                                         if (replicas.contains(localInstanceHostAndPort))
                                         {
                                             return true;
                                         }
                                     }
                                     return false;
                                 });
    }

    /**
     * @param replicaInfo the replica info
     * @return {@code true} if the replica info owns token zero, {@code false} otherwise
     */
    boolean containsTokenZero(TokenRangeReplicasResponse.ReplicaInfo replicaInfo)
    {
        long start = Long.parseLong(replicaInfo.start());
        long end = Long.parseLong(replicaInfo.end());
        return start <= 0L && end >= 0L;
    }
}
