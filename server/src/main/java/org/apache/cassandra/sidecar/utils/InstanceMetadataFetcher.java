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
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.adapters.base.exception.OperationUnavailableException;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.exceptions.NoSuchCassandraInstanceException;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException.Service.CQL_AND_JMX;

/**
 * Helper class to retrieve instance information from an instanceId or hostname.
 */
@Singleton
public class InstanceMetadataFetcher
{
    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceMetadataFetcher.class);

    private final InstancesMetadata instancesMetadata;

    @Inject
    public InstanceMetadataFetcher(InstancesMetadata instancesMetadata)
    {
        this.instancesMetadata = instancesMetadata;
    }

    /**
     * Returns the {@link InstanceMetadata} for the given {@code host}.
     *
     * @param host the Cassandra instance hostname or IP address
     * @return the {@link InstanceMetadata} for the given {@code host}
     */
    @NotNull
    public InstanceMetadata instance(@NotNull String host) throws NoSuchCassandraInstanceException
    {
        return instancesMetadata.instanceFromHost(host);
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
     * Returns the {@link CassandraAdapterDelegate} for the given {@code host}
     *
     * <p><b>Note</b>: the method to retrieve delegate should not be the responsibility of this class. However, for historic reasons, it is kept.
     * That said, it does <i>not</i> warrant adding any more convenient methods to return members of {@link InstanceMetadata}.
     *
     * @param host the Cassandra instance hostname or IP address
     * @return the {@link CassandraAdapterDelegate} for the given {@code host}
     * @throws NoSuchCassandraInstanceException when the Cassandra instance with {@code host} does not exist
     * @throws CassandraUnavailableException when Cassandra is not yet connected
     */
    @NotNull
    public CassandraAdapterDelegate delegate(@NotNull String host) throws NoSuchCassandraInstanceException, CassandraUnavailableException
    {
        return instance(host).delegate();
    }

    /**
     * Iterate through the local instances and call the function on the first available instance, i.e. no CassandraUnavailableException
     * or OperationUnavailableException is thrown for the operations
     *
     * @param function function applies to {@link InstanceMetadata}
     * @return function eval result. Null can be returned when all local instances are exhausted
     * @param <T> type of the result
     * @throws CassandraUnavailableException when all local instances are exhausted.
     */
    @NotNull
    public <T> T callOnFirstAvailableInstance(Function<InstanceMetadata, T> function) throws CassandraUnavailableException
    {
        for (InstanceMetadata instance : allLocalInstances())
        {
            try
            {
                return function.apply(instance);
            }
            catch (CassandraUnavailableException | OperationUnavailableException exception)
            {
                // no-op; try the next instance
                LOGGER.debug("CassandraAdapterDelegate is not available for instance. instance={}", instance, exception);
            }
        }

        throw new CassandraUnavailableException(CQL_AND_JMX, "All local Cassandra nodes are exhausted. But none is available");
    }

    /**
     * @return all the configured local instances
     */
    public List<InstanceMetadata> allLocalInstances()
    {
        ensureInstancesMetadataConfigured();
        return instancesMetadata.instances();
    }

    private void ensureInstancesMetadataConfigured()
    {
        if (instancesMetadata.instances().isEmpty())
        {
            throw new IllegalStateException("There are no instances configured!");
        }
    }
}
