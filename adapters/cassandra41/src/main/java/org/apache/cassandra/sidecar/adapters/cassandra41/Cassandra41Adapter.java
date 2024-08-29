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

package org.apache.cassandra.sidecar.adapters.cassandra41;

import java.net.InetSocketAddress;

import org.apache.cassandra.sidecar.adapters.base.CassandraAdapter;
import org.apache.cassandra.sidecar.adapters.base.CassandraMetricsOperations;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.ICassandraAdapter;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.MetricsOperations;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.utils.DriverUtils;

/**
 * A {@link ICassandraAdapter} implementation for Cassandra 4.1 and later
 */
public class Cassandra41Adapter extends CassandraAdapter
{
    public Cassandra41Adapter(DnsResolver dnsResolver,
                              JmxClient jmxClient,
                              CQLSessionProvider session,
                              InetSocketAddress localNativeTransportAddress,
                              DriverUtils driverUtils)
    {
        super(dnsResolver, jmxClient, session, localNativeTransportAddress, driverUtils);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StorageOperations storageOperations()
    {
        return new Cassandra41StorageOperations(jmxClient, dnsResolver);
    }

    @Override
    public MetricsOperations metricsOperations()
    {
        return new CassandraMetricsOperations(jmxClient);
    }
}
