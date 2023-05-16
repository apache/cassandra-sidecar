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

import java.net.UnknownHostException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.StorageOperations;
import org.apache.cassandra.sidecar.common.data.RingResponse;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.requireNonNull;
import static org.apache.cassandra.sidecar.cassandra40.StorageJmxOperations.STORAGE_SERVICE_OBJ_NAME;

/**
 * An implementation of the {@link StorageOperations} that interfaces with Cassandra 4.0
 */
public class Cassandra40StorageOperations implements StorageOperations
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Cassandra40StorageOperations.class);
    private final JmxClient jmxClient;
    private final RingProvider ringProvider;

    /**
     * Creates a new instance with the provided {@link JmxClient}
     *
     * @param jmxClient the JMX client used to communicate with the Cassandra instance
     */
    public Cassandra40StorageOperations(JmxClient jmxClient, DnsResolver dnsResolver)
    {
        this.jmxClient = jmxClient;
        this.ringProvider = new RingProvider(jmxClient, dnsResolver);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void takeSnapshot(@NotNull String tag, @NotNull String keyspace, @NotNull String table,
                             @Nullable Map<String, String> options)
    {
        requireNonNull(tag, "snapshot tag must be non-null");
        requireNonNull(keyspace, "keyspace for the  must be non-null");
        requireNonNull(table, "table must be non-null");
        jmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME)
                 .takeSnapshot(tag, options, keyspace + "." + table);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearSnapshot(@NotNull String tag, @NotNull String keyspace, @NotNull String table)
    {
        requireNonNull(tag, "snapshot tag must be non-null");
        requireNonNull(keyspace, "keyspace for the  must be non-null");
        requireNonNull(table, "table must be non-null");
        LOGGER.debug("Table is not supported by Cassandra JMX endpoints. " +
                     "Clearing snapshot with tag={} and keyspace={}; table={} is ignored", tag, keyspace, table);
        final String[] keyspaces = { keyspace };
        jmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME)
                 .clearSnapshot(tag, keyspaces);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RingResponse ring(@Nullable String keyspace) throws UnknownHostException
    {
        return ringProvider.ring(keyspace);
    }
}
