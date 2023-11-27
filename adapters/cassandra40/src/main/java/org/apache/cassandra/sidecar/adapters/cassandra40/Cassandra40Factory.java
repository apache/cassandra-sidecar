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

package org.apache.cassandra.sidecar.adapters.cassandra40;

import java.net.InetSocketAddress;

import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.ICassandraAdapter;
import org.apache.cassandra.sidecar.common.ICassandraFactory;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.MinimumVersion;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.utils.DriverUtils;

/**
 * Factory to produce the 4.0 adapter
 */
@MinimumVersion("4.0.0")
public class Cassandra40Factory implements ICassandraFactory
{
    protected final DnsResolver dnsResolver;
    private final DriverUtils driverUtils;

    public Cassandra40Factory(DnsResolver dnsResolver, DriverUtils driverUtils)
    {
        this.dnsResolver = dnsResolver;
        this.driverUtils = driverUtils;
    }

    /**
     * Returns a new adapter for Cassandra 4.0 clusters.
     *
     * @param session                     the session to the Cassandra database
     * @param jmxClient                   the JMX client to connect to the Cassandra database
     * @param localNativeTransportAddress the native transport address and port of the instance
     * @return a new adapter for the 4.0 clusters
     */
    @Override
    public ICassandraAdapter create(CQLSessionProvider session, JmxClient jmxClient,
                                    InetSocketAddress localNativeTransportAddress)
    {
        return new Cassandra40Adapter(dnsResolver, jmxClient, session, localNativeTransportAddress, driverUtils);
    }
}
