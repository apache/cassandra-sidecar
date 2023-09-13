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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.StorageOperations;
import org.apache.cassandra.sidecar.common.data.RingResponse;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.exceptions.NodeBootstrappingException;
import org.apache.cassandra.sidecar.common.exceptions.SnapshotAlreadyExistsException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.requireNonNull;
import static org.apache.cassandra.sidecar.adapters.base.StorageJmxOperations.STORAGE_SERVICE_OBJ_NAME;

/**
 * An implementation of the {@link StorageOperations} that interfaces with Cassandra 4.0 and later
 */
public class CassandraStorageOperations implements StorageOperations
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraStorageOperations.class);
    protected final JmxClient jmxClient;
    protected final RingProvider ringProvider;
    protected final TokenRangeReplicaProvider tokenRangeReplicaProvider;

    /**
     * Creates a new instance with the provided {@link JmxClient}
     *
     * @param jmxClient the JMX client used to communicate with the Cassandra instance
     */
    public CassandraStorageOperations(JmxClient jmxClient, DnsResolver dnsResolver)
    {
        this(jmxClient, new RingProvider(jmxClient, dnsResolver));
    }

    public CassandraStorageOperations(JmxClient jmxClient, RingProvider ringProvider)
    {
        this.jmxClient = jmxClient;
        this.ringProvider = ringProvider;
        this.tokenRangeReplicaProvider = new TokenRangeReplicaProvider(jmxClient);
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
        try
        {
            jmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME)
                     .takeSnapshot(tag, options, keyspace + "." + table);
        }
        catch (IOException e)
        {
            String errorMessage = e.getMessage();
            if (errorMessage != null)
            {
                if (errorMessage.contains("Snapshot " + tag + " already exists"))
                {
                    throw new SnapshotAlreadyExistsException(e);
                }
                else if (errorMessage.contains("Keyspace " + keyspace + " does not exist"))
                {
                    throw new IllegalArgumentException(e);
                }
                else if (errorMessage.contains("Cannot snapshot until bootstrap completes"))
                {
                    throw new NodeBootstrappingException(e);
                }
            }
            throw new RuntimeException(e);
        }
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

    /**
     * {@inheritDoc}
     */
    @Override
    public TokenRangeReplicasResponse tokenRangeReplicas(@NotNull String keyspace, @NotNull String partitioner)
    {
        return tokenRangeReplicaProvider.tokenRangeReplicas(keyspace, Partitioner.fromClassName(partitioner));
    }
}
