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

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DriverExtensions;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.ClusterMembershipOperations;
import org.apache.cassandra.sidecar.common.ICassandraAdapter;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.NodeSettings;
import org.apache.cassandra.sidecar.common.StorageOperations;
import org.apache.cassandra.sidecar.common.TableOperations;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.common.NodeSettings.DATA_CENTER_COLUMN_NAME;
import static org.apache.cassandra.sidecar.common.NodeSettings.PARTITIONER_COLUMN_NAME;
import static org.apache.cassandra.sidecar.common.NodeSettings.RELEASE_VERSION_COLUMN_NAME;
import static org.apache.cassandra.sidecar.common.NodeSettings.RPC_ADDRESS_COLUMN_NAME;
import static org.apache.cassandra.sidecar.common.NodeSettings.RPC_PORT_COLUMN_NAME;
import static org.apache.cassandra.sidecar.common.NodeSettings.TOKENS_COLUMN_NAME;

/**
 * A {@link ICassandraAdapter} implementation for Cassandra 4.0 and later
 */
public class CassandraAdapter implements ICassandraAdapter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraAdapter.class);
    protected final DnsResolver dnsResolver;
    protected final JmxClient jmxClient;
    private final CQLSessionProvider cqlSessionProvider;
    private final String sidecarVersion;
    private final InetSocketAddress localNativeTransportAddress;

    public CassandraAdapter(DnsResolver dnsResolver, JmxClient jmxClient, CQLSessionProvider cqlSessionProvider,
                            String sidecarVersion, InetSocketAddress localNativeTransportAddress)
    {
        this.dnsResolver = dnsResolver;
        this.jmxClient = jmxClient;
        this.cqlSessionProvider = cqlSessionProvider;
        this.sidecarVersion = sidecarVersion;
        this.localNativeTransportAddress = localNativeTransportAddress;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nullable
    public Metadata metadata()
    {
        Session activeSession = cqlSessionProvider.get();
        if (activeSession == null)
        {
            LOGGER.warn("There is no active session to Cassandra");
            return null;
        }

        if (activeSession.getCluster() == null)
        {
            LOGGER.warn("There is no available cluster for session={}", activeSession);
            return null;
        }

        if (activeSession.getCluster().getMetadata() == null)
        {
            LOGGER.warn("There is no available metadata for session={}, cluster={}",
                        activeSession, activeSession.getCluster());
        }

        return activeSession.getCluster().getMetadata();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nullable
    public NodeSettings nodeSettings()
    {
        ResultSet rs = executeLocal("SELECT "
                                              + RELEASE_VERSION_COLUMN_NAME + ", "
                                              + PARTITIONER_COLUMN_NAME + ", "
                                              + DATA_CENTER_COLUMN_NAME + ", "
                                              + RPC_ADDRESS_COLUMN_NAME + ", "
                                              + RPC_PORT_COLUMN_NAME + ", "
                                              + TOKENS_COLUMN_NAME
                                              + " FROM system.local");
        if (rs == null)
        {
            return null;
        }

        Row oneResult = rs.one();
        
        return NodeSettings.builder()
                           .releaseVersion(oneResult.getString(RELEASE_VERSION_COLUMN_NAME))
                           .partitioner(oneResult.getString(PARTITIONER_COLUMN_NAME))
                           .sidecarVersion(sidecarVersion)
                           .datacenter(oneResult.getString(DATA_CENTER_COLUMN_NAME))
                           .tokens(oneResult.getSet(TOKENS_COLUMN_NAME, String.class))
                           .rpcAddress(oneResult.getInet(RPC_ADDRESS_COLUMN_NAME))
                           .rpcPort(oneResult.getInt(RPC_PORT_COLUMN_NAME))
                           .build();
    }

    @Override
    public ResultSet executeLocal(Statement statement)
    {
        Session activeSession = cqlSessionProvider.get();
        Metadata metadata = metadata();
        // Both of the above log about lack of session/metadata, so no need to log again
        if (activeSession == null || metadata == null)
        {
            return null;
        }

        Host host = DriverExtensions.getHost(metadata, localNativeTransportAddress);
        if (host == null)
        {
            LOGGER.debug("Could not find host in metadata for address {}", localNativeTransportAddress);
            return null;
        }
        statement.setConsistencyLevel(ConsistencyLevel.ONE);
        statement.setHost(host);
        return activeSession.execute(statement);
    }

    public InetSocketAddress localNativeTransportPort()
    {
        return localNativeTransportAddress;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StorageOperations storageOperations()
    {
        return new CassandraStorageOperations(jmxClient, dnsResolver);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClusterMembershipOperations clusterMembershipOperations()
    {
        return new CassandraClusterMembershipOperations(jmxClient);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableOperations tableOperations()
    {
        return new CassandraTableOperations(jmxClient);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "CassandraAdapter" + "@" + Integer.toHexString(hashCode());
    }
}
