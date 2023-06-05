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

package org.apache.cassandra.sidecar.cassandra40;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.ClusterMembershipOperations;
import org.apache.cassandra.sidecar.common.ICassandraAdapter;
import org.apache.cassandra.sidecar.common.ICassandraFactory;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.MinimumVersion;
import org.apache.cassandra.sidecar.common.NodeSettings;
import org.apache.cassandra.sidecar.common.StorageOperations;
import org.apache.cassandra.sidecar.common.TableOperations;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.jetbrains.annotations.Nullable;

/**
 * Factory to produce the 4.0 adapter
 */
@MinimumVersion("4.0.0")
public class Cassandra40Factory implements ICassandraFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Cassandra40Factory.class);

    private final DnsResolver dnsResolver;
    private final String sidecarVersion;

    public Cassandra40Factory(DnsResolver dnsResolver, String sidecarVersion)
    {
        this.dnsResolver = dnsResolver;
        this.sidecarVersion = sidecarVersion;
    }

    /**
     * Returns a new adapter for Cassandra 4.0 clusters.
     *
     * @param session   the session to the Cassandra database
     * @param jmxClient the JMX client to connect to the Cassandra database
     * @return a new adapter for the 4.0 clusters
     */
    @Override
    public ICassandraAdapter create(CQLSessionProvider session, JmxClient jmxClient)
    {
        return new ICassandraAdapter()
        {
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
                return new Cassandra40StorageOperations(jmxClient, dnsResolver);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public ClusterMembershipOperations clusterMembershipOperations()
            {
                return new Cassandra40ClusterMembershipOperations(jmxClient);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TableOperations tableOperations()
            {
                return new Cassandra40TableOperations(jmxClient);
            }

            /**
             * {@inheritDoc}
             */
            public String toString()
            {
                return "Cassandra40Adapter" + "@" + Integer.toHexString(hashCode());
            }
        };
    }
}
