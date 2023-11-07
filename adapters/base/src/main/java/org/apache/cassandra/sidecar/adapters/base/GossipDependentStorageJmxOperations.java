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
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.adapters.base.exception.OperationUnavailableException;

/**
 * A wrapper class for {@link StorageJmxOperations} that ensures gossip is enabled during initialization.
 */
public class GossipDependentStorageJmxOperations implements StorageJmxOperations
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GossipDependentStorageJmxOperations.class);

    private final StorageJmxOperations delegate;

    public GossipDependentStorageJmxOperations(StorageJmxOperations delegate)
    {
        this.delegate = Objects.requireNonNull(delegate, "delegate must be not-null");
        ensureGossipIsEnabled();
    }

    @Override
    public List<String> getLiveNodesWithPort()
    {
        return delegate.getLiveNodesWithPort();
    }

    @Override
    public List<String> getUnreachableNodesWithPort()
    {
        return delegate.getUnreachableNodesWithPort();
    }

    @Override
    public List<String> getJoiningNodesWithPort()
    {
        return delegate.getJoiningNodesWithPort();
    }

    @Override
    public List<String> getLeavingNodesWithPort()
    {
        return delegate.getLeavingNodesWithPort();
    }

    @Override
    public List<String> getMovingNodesWithPort()
    {
        return delegate.getMovingNodesWithPort();
    }

    @Override
    public Map<String, String> getLoadMapWithPort()
    {
        return delegate.getLoadMapWithPort();
    }

    @Override
    public Map<String, String> getTokenToEndpointWithPortMap()
    {
        return delegate.getTokenToEndpointWithPortMap();
    }

    @Override
    public Map<String, Float> effectiveOwnershipWithPort(String keyspace) throws IllegalStateException
    {
        return delegate.effectiveOwnershipWithPort(keyspace);
    }

    @Override
    public Map<String, Float> getOwnershipWithPort()
    {
        return delegate.getOwnershipWithPort();
    }

    @Override
    public Map<String, String> getEndpointWithPortToHostId()
    {
        return delegate.getEndpointWithPortToHostId();
    }

    @Override
    public void takeSnapshot(String tag, Map<String, String> options, String... entities) throws IOException
    {
        delegate.takeSnapshot(tag, options, entities);
    }

    @Override
    public void clearSnapshot(String tag, String... keyspaceNames)
    {
        delegate.clearSnapshot(tag, keyspaceNames);
    }

    @Override
    public Map<List<String>, List<String>> getRangeToEndpointWithPortMap(String keyspace)
    {
        return delegate.getRangeToEndpointWithPortMap(keyspace);
    }

    @Override
    public Map<List<String>, List<String>> getPendingRangeToEndpointWithPortMap(String keyspace)
    {
        return delegate.getPendingRangeToEndpointWithPortMap(keyspace);
    }

    @Override
    public boolean isGossipRunning()
    {
        return delegate.isGossipRunning();
    }

    /**
     * Ensures that gossip is running on the Cassandra instance
     *
     * @throws OperationUnavailableException when gossip is not running
     */
    public void ensureGossipIsEnabled()
    {
        if (delegate.isGossipRunning())
            return;

        LOGGER.warn("Gossip is disabled and unavailable for the operation");
        throw new OperationUnavailableException(OperationUnavailableException.OperationType.GOSSIP_DISABLED);
    }
}
