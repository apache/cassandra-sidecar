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

package org.apache.cassandra.sidecar.common.server;

import java.net.InetSocketAddress;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.jetbrains.annotations.NotNull;

/**
 * Core Cassandra Adapter interface.
 *
 * <p>For now, this is just a placeholder. We will most likely want to define the interface to returns bits such as
 * compaction(), clusterMembership(), etc., which return interfaces such as Compaction, ClusterMembership.
 * We will need different implementations due to the slow move away from JMX towards CQL for some, but not all, actions.
 */
public interface ICassandraAdapter
{
    /**
     * @return metadata on the connected cluster, including known nodes and schema definitions
     * @throws CassandraUnavailableException when no CQL connection is established
     */
    @NotNull Metadata metadata() throws CassandraUnavailableException;

    /**
     * The {@link NodeSettings} for this instance.
     *
     * @return the {@link NodeSettings} instance for this instance.
     * @throws CassandraUnavailableException when no JMX connection is established
     */
    @NotNull NodeSettings nodeSettings() throws CassandraUnavailableException;

    /**
     * Execute the provided query on the locally-managed Cassandra instance
     *
     * @param query the query to execute
     * @return the {@link ResultSet}
     * @throws CassandraUnavailableException when CQL connection is not yet established
     */
    @NotNull
    default ResultSet executeLocal(String query) throws CassandraUnavailableException
    {
        return executeLocal(new SimpleStatement(query));
    }

    /**
     * Execute the provided statement on the locally-managed Cassandra instance
     *
     * @param statement the statement to execute
     * @return the {@link ResultSet}
     * @throws CassandraUnavailableException when CQL connection is not yet established
     */
    @NotNull ResultSet executeLocal(Statement statement) throws CassandraUnavailableException;

    /**
     * The address on which the local Cassandra instance is listening for CQL connections
     *
     * @return the {@link InetSocketAddress} representing the address and port.
     * @throws CassandraUnavailableException when CQL connection is not yet established
     */
    @NotNull InetSocketAddress localNativeTransportAddress() throws CassandraUnavailableException;

    /**
     * The address on which the local Cassandra instance broadcasts the intra-cluster storage traffic
     *
     * @return the {@link InetSocketAddress} representing the address and port.
     * @throws CassandraUnavailableException when CQL connection is not yet established
     */
    @NotNull InetSocketAddress localStorageBroadcastAddress() throws CassandraUnavailableException;

    /**
     * @return the {@link StorageOperations} implementation for the Cassandra cluster
     * @throws CassandraUnavailableException when Cassandra is not available
     */
    @NotNull StorageOperations storageOperations() throws CassandraUnavailableException;

    /**
     * @return the {@link MetricsOperations} implementation for the Cassandra cluster
     * @throws CassandraUnavailableException when Cassandra is not available
     */
    @NotNull MetricsOperations metricsOperations() throws CassandraUnavailableException;

    /**
     * @return the {@link ClusterMembershipOperations} implementation for handling cluster membership operations
     * @throws CassandraUnavailableException when Cassandra is not available
     */
    @NotNull ClusterMembershipOperations clusterMembershipOperations() throws CassandraUnavailableException;

    /**
     * @return the {@link TableOperations} implementation for the Cassandra cluster
     * @throws CassandraUnavailableException when Cassandra is not available
     */
    @NotNull TableOperations tableOperations();
}
