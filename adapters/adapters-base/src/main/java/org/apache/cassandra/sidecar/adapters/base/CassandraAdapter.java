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
import java.util.Map;
import java.util.Objects;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.ClusterMembershipOperations;
import org.apache.cassandra.sidecar.common.server.CompactionManagerOperations;
import org.apache.cassandra.sidecar.common.server.CompactionStatsOperations;
import org.apache.cassandra.sidecar.common.server.ICassandraAdapter;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.MetricsOperations;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.TableOperations;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.utils.DriverUtils;
import org.apache.cassandra.sidecar.db.schema.TableSchemaFetcher;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException.Service.CQL;

/**
 * A {@link ICassandraAdapter} implementation for Cassandra 4.0 and later
 */
public class CassandraAdapter implements ICassandraAdapter
{
    protected final DnsResolver dnsResolver;
    protected final JmxClient jmxClient;
    protected final CQLSessionProvider cqlSessionProvider;
    protected final InetSocketAddress localNativeTransportAddress;
    protected final DriverUtils driverUtils;
    private volatile Host host;
    private final StorageOperations storageOperations;
    private final ClusterMembershipOperations clusterMembershipOperations;
    private final TableOperations tableOperations;
    private final CompactionManagerOperations compactionManagerOperations;
    private final MetricsOperations metricsOperations;
    private final CompactionStatsOperations compactionStatsOperations;

    public CassandraAdapter(DnsResolver dnsResolver,
                            JmxClient jmxClient,
                            CQLSessionProvider cqlSessionProvider,
                            InetSocketAddress localNativeTransportAddress,
                            DriverUtils driverUtils,
                            TableSchemaFetcher tableSchemaFetcher)
    {
        this.dnsResolver = dnsResolver;
        this.jmxClient = jmxClient;
        this.cqlSessionProvider = cqlSessionProvider;
        this.localNativeTransportAddress = localNativeTransportAddress;
        this.driverUtils = driverUtils;
        this.storageOperations = Objects.requireNonNull(createStorageOperations(dnsResolver, jmxClient), "storageOperations is required");
        this.clusterMembershipOperations = Objects.requireNonNull(createClusterMembershipOperations(jmxClient), "clusterMembershipOperations is required");
        this.tableOperations = Objects.requireNonNull(createTableOperations(jmxClient), "tableOperations is required");
        this.compactionManagerOperations = Objects.requireNonNull(createCompactionManagerOperations(jmxClient), "compactionManagerOperations is required");
        this.metricsOperations = Objects.requireNonNull(createMetricsOperations(jmxClient, tableSchemaFetcher), "metricsOperations is required");
        this.compactionStatsOperations
        = Objects.requireNonNull(createCompactionStatsOperations(storageOperations,
                                                                 metricsOperations,
                                                                 compactionManagerOperations),
                                 "compactionStatsOperations is required");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NotNull
    public Metadata metadata() throws CassandraUnavailableException
    {
        return cqlSessionProvider.get().getCluster().getMetadata();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NotNull
    public NodeSettings nodeSettings()
    {
        throw new UnsupportedOperationException("Node settings are not provided by this adapter");
    }

    @Override
    @NotNull
    public Map<String, String> v2NodeSettings()
    {
        throw new UnsupportedOperationException("V2 node settings are not provided by this adapter");
    }

    @Override
    @NotNull
    public ResultSet executeLocal(Statement statement)
    {
        Session activeSession = cqlSessionProvider.get();
        Metadata metadata = metadata();
        Host host = getHost(metadata);
        statement.setConsistencyLevel(ConsistencyLevel.ONE);
        statement.setHost(host);
        return activeSession.execute(statement);
    }

    @Override
    @NotNull
    public InetSocketAddress localNativeTransportAddress()
    {
        return localNativeTransportAddress;
    }

    @Override
    @NotNull
    public InetSocketAddress localStorageBroadcastAddress()
    {
        Metadata metadata = metadata();
        return getHost(metadata).getBroadcastSocketAddress();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NotNull
    public StorageOperations storageOperations()
    {
        return storageOperations;
    }

    @Override
    @NotNull
    public MetricsOperations metricsOperations()
    {
        return metricsOperations;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NotNull
    public ClusterMembershipOperations clusterMembershipOperations()
    {
        return clusterMembershipOperations;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NotNull
    public TableOperations tableOperations()
    {
        return tableOperations;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NotNull
    public CompactionManagerOperations compactionManagerOperations()
    {
        return compactionManagerOperations;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NotNull
    public CompactionStatsOperations compactionStatsOperations()
    {
        return compactionStatsOperations;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "CassandraAdapter" + "@" + Integer.toHexString(hashCode());
    }

    @NotNull
    protected StorageOperations createStorageOperations(DnsResolver dnsResolver, JmxClient jmxClient)
    {
        return new CassandraStorageOperations(jmxClient, dnsResolver);
    }

    @NotNull
    protected ClusterMembershipOperations createClusterMembershipOperations(JmxClient jmxClient)
    {
        return new CassandraClusterMembershipOperations(jmxClient);
    }

    @NotNull
    protected TableOperations createTableOperations(JmxClient jmxClient)
    {
        return new CassandraTableOperations(jmxClient);
    }

    @NotNull
    protected CompactionManagerOperations createCompactionManagerOperations(JmxClient jmxClient)
    {
        return new CassandraCompactionManagerOperations(jmxClient);
    }

    @NotNull
    protected MetricsOperations createMetricsOperations(JmxClient jmxClient,
                                                        TableSchemaFetcher tableSchemaFetcher)
    {
        return new CassandraMetricsOperations(jmxClient, tableSchemaFetcher, this);
    }

    @NotNull
    protected CompactionStatsOperations createCompactionStatsOperations(StorageOperations storageOperations,
                                                                        MetricsOperations metricsOperations,
                                                                        CompactionManagerOperations compactionManagerOperations)
    {
        return new CassandraCompactionStatsOperations(storageOperations, metricsOperations, compactionManagerOperations);
    }

    @NotNull
    protected Host getHost(Metadata metadata)
    {
        if (host != null)
        {
            return host;
        }

        synchronized (this)
        {
            if (host == null)
            {
                host = driverUtils.getHost(metadata, localNativeTransportAddress);
                if (host == null)
                {
                    throw new CassandraUnavailableException(CQL, "No Host available in Metadata for address: " + localNativeTransportAddress);
                }
            }
        }
        return host;
    }
}
