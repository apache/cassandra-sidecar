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

package org.apache.cassandra.sidecar.routes;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.acl.authorization.VariableAwareResource;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * A handler that provides ring information for a specific keyspace for the Cassandra cluster
 */
@Singleton
public class KeyspaceRingHandler extends AbstractHandler<Name> implements AccessProtected
{
    @Inject
    public KeyspaceRingHandler(InstanceMetadataFetcher metadataFetcher,
                               ExecutorPools executorPools,
                               CassandraInputValidator validator)
    {
        super(metadataFetcher, executorPools, validator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        List<String> eligibleResources = VariableAwareResource.DATA_WITH_KEYSPACE.expandedResources();
        return Collections.singleton(BasicPermissions.READ_RING.toAuthorization(eligibleResources));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               @NotNull String host,
                               SocketAddress remoteAddress,
                               Name keyspace)
    {
        StorageOperations operations = metadataFetcher.delegate(host).storageOperations();
        executorPools.service()
                     .executeBlocking(() -> operations.ring(keyspace))
                     .onSuccess(context::json)
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, keyspace));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void processFailure(Throwable cause,
                                  RoutingContext context,
                                  String host,
                                  SocketAddress remoteAddress,
                                  Name keyspace)
    {
        if (cause instanceof IllegalArgumentException &&
            StringUtils.contains(cause.getMessage(), ", does not exist"))
        {
            context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, cause.getMessage(), cause));
            return;
        }

        super.processFailure(cause, context, host, remoteAddress, keyspace);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Name extractParamsOrThrow(RoutingContext context)
    {
        return keyspace(context, true);
    }
}
