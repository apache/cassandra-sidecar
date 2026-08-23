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

package org.apache.cassandra.sidecar.adapters.cassandra60;

import java.net.InetSocketAddress;

import org.apache.cassandra.sidecar.adapters.cassandra50.Cassandra50Adapter;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.ICassandraAdapter;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.utils.DriverUtils;
import org.apache.cassandra.sidecar.db.schema.TableSchemaFetcher;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link ICassandraAdapter} implementation for Cassandra 6.0
 * <p>
 * Cassandra 6.0 keeps the table and compaction manager MBean contracts of 5.0.
 */
public class Cassandra60Adapter extends Cassandra50Adapter
{
    public Cassandra60Adapter(DnsResolver dnsResolver,
                              JmxClient jmxClient,
                              CQLSessionProvider session,
                              InetSocketAddress localNativeTransportAddress,
                              DriverUtils driverUtils,
                              TableSchemaFetcher tableSchemaFetcher)
    {
        super(dnsResolver, jmxClient, session, localNativeTransportAddress, driverUtils, tableSchemaFetcher);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NotNull
    protected StorageOperations createStorageOperations(DnsResolver dnsResolver, JmxClient jmxClient)
    {
        return new Cassandra60StorageOperations(jmxClient, dnsResolver);
    }
}
