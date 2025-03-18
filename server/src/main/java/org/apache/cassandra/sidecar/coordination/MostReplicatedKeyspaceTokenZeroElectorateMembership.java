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

import java.util.Comparator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
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
public class MostReplicatedKeyspaceTokenZeroElectorateMembership extends AbstractTokenZeroOfKeyspaceElectorateMembership
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MostReplicatedKeyspaceTokenZeroElectorateMembership.class);
    private final CQLSessionProvider cqlSessionProvider;
    private final SidecarConfiguration configuration;

    public MostReplicatedKeyspaceTokenZeroElectorateMembership(InstanceMetadataFetcher instanceMetadataFetcher,
                                                               CQLSessionProvider cqlSessionProvider,
                                                               SidecarConfiguration sidecarConfiguration)
    {
        super(instanceMetadataFetcher);
        this.cqlSessionProvider = cqlSessionProvider;
        this.configuration = sidecarConfiguration;
    }

    @Override
    protected String keyspaceToDetermineElectorateMembership()
    {
        return highestReplicationFactorKeyspace();
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
