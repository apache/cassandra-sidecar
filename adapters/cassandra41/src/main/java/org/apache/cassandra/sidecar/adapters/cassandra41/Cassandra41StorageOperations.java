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

import java.util.Map;

import org.apache.cassandra.sidecar.adapters.base.CassandraStorageOperations;
import org.apache.cassandra.sidecar.adapters.base.RingProvider;
import org.apache.cassandra.sidecar.adapters.base.TokenRangeReplicaProvider;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.StorageOperations;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An implementation of the {@link StorageOperations} that interfaces with Cassandra 4.1 and later
 */
public class Cassandra41StorageOperations extends CassandraStorageOperations
{
    /**
     * Creates a new instance with the provided {@link JmxClient} and {@link DnsResolver}
     *
     * @param jmxClient   the JMX client used to communicate with the Cassandra instance
     * @param dnsResolver the DNS resolver used to lookup replicas
     */
    public Cassandra41StorageOperations(JmxClient jmxClient, DnsResolver dnsResolver)
    {
        super(jmxClient, dnsResolver);
    }

    /**
     * Creates a new instances with the provided {@link JmxClient}, {@link RingProvider}, and
     * {@link TokenRangeReplicaProvider}. This constructor is exposed for extensibility.
     *
     * @param jmxClient                 the JMX client used to communicate with the Cassandra instance
     * @param ringProvider              the ring provider instance
     * @param tokenRangeReplicaProvider the token range replica provider
     */
    public Cassandra41StorageOperations(JmxClient jmxClient,
                                        RingProvider ringProvider,
                                        TokenRangeReplicaProvider tokenRangeReplicaProvider)
    {
        super(jmxClient, ringProvider, tokenRangeReplicaProvider);
    }

    @Override
    public void takeSnapshot(@NotNull String tag,
                             @NotNull String keyspace,
                             @NotNull String table,
                             @Nullable Map<String, String> options)
    {
        super.takeSnapshotInternal(tag, keyspace, table, options);
    }
}
