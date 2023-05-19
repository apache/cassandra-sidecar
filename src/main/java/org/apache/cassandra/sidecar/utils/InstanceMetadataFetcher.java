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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.routes.AbstractHandler.extractHostAddressWithoutPort;

/**
 * Helper class to retrieve instance information from an instanceId or hostname.
 */
@Singleton
public class InstanceMetadataFetcher
{
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
               : instancesConfig.instanceFromHost(extractHostAddressWithoutPort(host));
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
    public CassandraAdapterDelegate delegate(String host)
    {
        return instance(host).delegate();
    }

    /**
     * Returns the {@link CassandraAdapterDelegate} for the given {@code instanceId}, or the delegate for the first
     * instance when {@code instanceId} is {@code null}.
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
        if (instancesConfig.instances().isEmpty())
        {
            throw new IllegalStateException("There are no instances configured!");
        }
        return instancesConfig.instances().get(0);
    }
}
