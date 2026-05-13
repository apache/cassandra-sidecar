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

package org.apache.cassandra.sidecar.handlers;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Inject;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.job.NodeDrainJob;
import org.apache.cassandra.sidecar.job.OperationalJobManager;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.OperationalJobUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Provides REST API for asynchronously draining the corresponding Cassandra node
 */
public class NodeDrainHandler extends AbstractHandler<Void> implements AccessProtected
{
    private final OperationalJobManager jobManager;
    private final ServiceConfiguration config;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    @Inject
    protected NodeDrainHandler(InstanceMetadataFetcher metadataFetcher,
                               ExecutorPools executorPools,
                               ServiceConfiguration serviceConfiguration,
                               CassandraInputValidator validator,
                               OperationalJobManager jobManager)
    {
        super(metadataFetcher, executorPools, validator);
        this.jobManager = jobManager;
        this.config = serviceConfiguration;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.DRAIN_NODE.toAuthorization());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               @NotNull String host,
                               SocketAddress remoteAddress,
                               Void unused)
    {
        CassandraAdapterDelegate delegate = metadataFetcher.delegate(host);
        StorageOperations operations = delegate.storageOperations();
        NodeSettings nodeSettings = delegate.nodeSettings();
        UUID nodeId = nodeSettings.hostId();
        NodeDrainJob job = new NodeDrainJob(UUIDs.timeBased(), nodeId, operations);
        this.jobManager.trySubmitJob(job,
                                     (completedJob, exception) ->
                                     OperationalJobUtils.sendStatusBasedResponse(context, completedJob, exception),
                                     executorPools.service(),
                                     config.operationalJobExecutionMaxWaitTime());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Void extractParamsOrThrow(RoutingContext context)
    {
        // No parameters needed for drain operation
        return null;
    }
}
