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

package org.apache.cassandra.sidecar.utils;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.exceptions.NoSuchCassandraInstanceException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Helper class to retrieve instance information from an instanceId or hostname.
 */
@Singleton
public class InstanceMetadataFetcher
{
    private final InstancesMetadata instancesMetadata;

    @Inject
    public InstanceMetadataFetcher(InstancesMetadata instancesMetadata)
    {
        this.instancesMetadata = instancesMetadata;
    }

    /**
     * Returns the {@link InstanceMetadata} for the given {@code host}. When the {@code host} is {@code null},
     * returns the first instance from the list of configured instances.
     *
     * @param host the Cassandra instance hostname or IP address
     * @return the {@link InstanceMetadata} for the given {@code host}, or the first instance when {@code host} is
     * {@code null}
     */
    @NotNull
    public InstanceMetadata instance(@Nullable String host) throws NoSuchCassandraInstanceException
    {
        return host == null
               ? firstInstance()
               : instancesMetadata.instanceFromHost(host);
    }

    /**
     * Returns the {@link InstanceMetadata} for the given {@code instanceId}, or the first instance when
     * {@code instanceId} is {@code null}.
     *
     * @param instanceId the identifier for the Cassandra instance
     * @return the {@link InstanceMetadata} for the given {@code instanceId}, or the first instance when
     * {@code instanceId} is {@code null}
     * @throws NoSuchCassandraInstanceException when the Cassandra instance with {@code instanceId} does not exist
     */
    @NotNull
    public InstanceMetadata instance(int instanceId) throws NoSuchCassandraInstanceException
    {
        return instancesMetadata.instanceFromId(instanceId);
    }

    /**
     * Returns the {@link CassandraAdapterDelegate} for the given {@code host}. When the {@code host} is {@code null},
     * returns the delegate for the first instance from the list of configured instances.
     *
     * @param host the Cassandra instance hostname or IP address
     * @return the {@link CassandraAdapterDelegate} for the given {@code host}, or the first instance when {@code host}
     * is {@code null}
     * @throws NoSuchCassandraInstanceException when the Cassandra instance with {@code host} does not exist
     * @throws CassandraUnavailableException  when Cassandra is not yet connected
     */
    @NotNull
    public CassandraAdapterDelegate delegate(@Nullable String host) throws NoSuchCassandraInstanceException, CassandraUnavailableException
    {
        return instance(host).delegate();
    }

    /**
     * Returns the {@link CassandraAdapterDelegate} for the given {@code instanceId}
     *
     * @param instanceId the identifier for the Cassandra instance
     * @return the {@link CassandraAdapterDelegate} for the given {@code instanceId}
     * @throws NoSuchCassandraInstanceException when the Cassandra instance with {@code instanceId} does not exist
     * @throws CassandraUnavailableException  when Cassandra is not yet connected
     */
    @NotNull
    public CassandraAdapterDelegate delegate(int instanceId) throws NoSuchCassandraInstanceException, CassandraUnavailableException
    {
        return instance(instanceId).delegate();
    }

    /**
     * @return the first instance from the list of configured instances
     * @throws IllegalStateException when there are no configured instances
     */
    public InstanceMetadata firstInstance()
    {
        ensureInstancesMetadataConfigured();
        return instancesMetadata.instances().get(0);
    }

    /**
     * @return any instance from the list of configured instances
     * @throws IllegalStateException when there are no configured instances
     */
    public InstanceMetadata anyInstance()
    {
        ensureInstancesMetadataConfigured();
        List<InstanceMetadata> instances = instancesMetadata.instances();
        if (instances.size() == 1)
        {
            return instances.get(0);
        }

        int randomPick = ThreadLocalRandom.current().nextInt(instances.size());
        return instances.get(randomPick);
    }

    private void ensureInstancesMetadataConfigured()
    {
        if (instancesMetadata.instances().isEmpty())
        {
            throw new IllegalStateException("There are no instances configured!");
        }
    }
}
