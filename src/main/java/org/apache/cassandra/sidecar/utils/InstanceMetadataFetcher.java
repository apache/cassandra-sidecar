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
import java.util.Random;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Helper class to retrieve instance information from an instanceId or hostname.
 */
@Singleton
public class InstanceMetadataFetcher
{
    private static final Random RANDOM = new Random();

    private final InstancesConfig instancesConfig;

    @Inject
    public InstanceMetadataFetcher(InstancesConfig instancesConfig)
    {
        this.instancesConfig = instancesConfig;
    }

    /**
     * Returns the {@link InstanceMetadata} for the given {@code host}. When the {@code host} is {@code null},
     * returns the first instance from the list of configured instances.
     *
     * @param host the Cassandra instance host
     * @return the {@link InstanceMetadata} for the given {@code host}, or the first instance when {@code host} is
     * {@code null}
     */
    public InstanceMetadata instance(@Nullable String host)
    {
        return host == null
               ? firstInstance()
               : instancesConfig.instanceFromHost(host);
    }

    /**
     * Returns the {@link InstanceMetadata} for the given {@code instanceId}, or the first instance when
     * {@code instanceId} is {@code null}.
     *
     * @param instanceId the identifier for the Cassandra instance
     * @return the {@link InstanceMetadata} for the given {@code instanceId}, or the first instance when
     * {@code instanceId} is {@code null}
     */
    public InstanceMetadata instance(int instanceId)
    {
        return instancesConfig.instanceFromId(instanceId);
    }

    /**
     * Returns the {@link CassandraAdapterDelegate} for the given {@code host}. When the {@code host} is {@code null},
     * returns the delegate for the first instance from the list of configured instances.
     *
     * @param host the Cassandra instance host
     * @return the {@link CassandraAdapterDelegate} for the given {@code host}, or the first instance when {@code host}
     * is {@code null}
     */
    @Nullable
    public CassandraAdapterDelegate delegate(String host)
    {
        return instance(host).delegate();
    }

    /**
     * Returns the {@link CassandraAdapterDelegate} for the given {@code instanceId}
     *
     * @param instanceId the identifier for the Cassandra instance
     * @return the {@link CassandraAdapterDelegate} for the given {@code instanceId}, or the first instance when
     * {@code instanceId} is {@code null}
     */
    public CassandraAdapterDelegate delegate(int instanceId)
    {
        return instance(instanceId).delegate();
    }

    /**
     * @return the first instance from the list of configured instances
     * @throws IllegalStateException when there are no configured instances
     */
    public InstanceMetadata firstInstance()
    {
        ensureInstancesConfigured();
        return instancesConfig.instances().get(0);
    }

    /**
     * @return any instance from the list of configured instances
     * @throws IllegalStateException when there are no configured instances
     */
    public InstanceMetadata anyInstance()
    {
        ensureInstancesConfigured();
        List<InstanceMetadata> instances = instancesConfig.instances();
        if (instances.size() == 1)
        {
            return instances.get(0);
        }
        int randomPick = RANDOM.nextInt(instances.size());
        return instances.get(randomPick);
    }

    private void ensureInstancesConfigured()
    {
        if (instancesConfig.instances().isEmpty())
        {
            throw new IllegalStateException("There are no instances configured!");
        }
    }
}
