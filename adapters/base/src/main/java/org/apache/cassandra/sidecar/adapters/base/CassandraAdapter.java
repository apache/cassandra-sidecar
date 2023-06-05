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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.ClusterMembershipOperations;
import org.apache.cassandra.sidecar.common.ICassandraAdapter;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.NodeSettings;
import org.apache.cassandra.sidecar.common.StorageOperations;
import org.apache.cassandra.sidecar.common.TableOperations;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link ICassandraAdapter} implementation for Cassandra 4.0 and later
 */
public class CassandraAdapter implements ICassandraAdapter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraAdapter.class);
    protected final DnsResolver dnsResolver;
    protected final JmxClient jmxClient;
    private final CQLSessionProvider session;
    private final String sidecarVersion;

    public CassandraAdapter(DnsResolver dnsResolver, JmxClient jmxClient, CQLSessionProvider session,
                            String sidecarVersion)
    {
        this.dnsResolver = dnsResolver;
        this.jmxClient = jmxClient;
        this.session = session;
        this.sidecarVersion = sidecarVersion;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nullable
    public Metadata metadata()
    {
        Session activeSession = session.localCql();
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
        Session activeSession = session.localCql();
        if (activeSession == null)
        {
            return null;
        }

        Row oneResult = activeSession.execute("select release_version, partitioner from system.local")
                                     .one();

        return new NodeSettings(oneResult.getString("release_version"),
                                oneResult.getString("partitioner"),
                                sidecarVersion);
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
